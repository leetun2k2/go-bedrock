// Package streamx provides durable, ordered event processing backed by
// Redis Streams and consumer groups. Handlers are acknowledged only on
// success; errors and timeouts leave their entry pending so XAUTOCLAIM can
// redeliver it to a live consumer. Handlers must be idempotent, since a
// message can be delivered more than once.
package streamx

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/leetun2k2/go-bedrock/redisx"
	"github.com/redis/go-redis/v9"
)

// Message is a single stream entry delivered to a Handler.
type Message struct {
	ID     string
	Values map[string]interface{}
}

// Handler processes one message. Returning nil acknowledges the message,
// removing it from the pending entries list. Returning an error, or taking
// longer than the configured HandlerTimeout, leaves the message pending so
// it becomes claimable again.
type Handler func(ctx context.Context, msg Message) error

// Client publishes to and consumes from a Redis stream through a consumer
// group. It borrows a *redisx.Client and does not close the shared
// connection.
type Client struct {
	client *redis.Client

	stream   string
	group    string
	consumer string

	maxLen         int64
	concurrency    int
	minIdleTime    time.Duration
	claimInterval  time.Duration
	handlerTimeout time.Duration
	blockTimeout   time.Duration

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

// New constructs a Client backed by client using cfg. New validates cfg and
// returns an error without touching Redis if the configuration is invalid.
func New(client *redisx.Client, cfg Config) (*Client, error) {
	if client == nil {
		return nil, errors.New("streamx: client must not be nil")
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &Client{
		client:         client.Client(),
		stream:         cfg.Stream,
		group:          cfg.Group,
		consumer:       cfg.Consumer,
		maxLen:         cfg.MaxLen,
		concurrency:    cfg.Concurrency,
		minIdleTime:    cfg.MinIdleTime,
		claimInterval:  cfg.ClaimInterval,
		handlerTimeout: cfg.HandlerTimeout,
		blockTimeout:   cfg.blockTimeout(),
	}, nil
}

// Publish appends values to the stream, trimming to approximately MaxLen
// entries when configured, and returns the generated entry ID.
func (c *Client) Publish(ctx context.Context, values map[string]interface{}) (string, error) {
	args := &redis.XAddArgs{
		Stream: c.stream,
		Values: values,
	}
	if c.maxLen > 0 {
		args.MaxLen = c.maxLen
		args.Approx = true
	}

	id, err := c.client.XAdd(ctx, args).Result()
	if err != nil {
		return "", fmt.Errorf("streamx: publish: %w", err)
	}

	return id, nil
}

// Consume ensures the consumer group exists, then starts a read loop and a
// background reclaimer, dispatching messages to handler with at most
// Concurrency invocations running at once. Consume blocks until ctx is
// canceled or Close is called. It returns an error if Consume is already
// running.
func (c *Client) Consume(ctx context.Context, handler Handler) error {
	c.mu.Lock()
	if c.cancel != nil {
		c.mu.Unlock()
		return errors.New("streamx: consume already running")
	}

	if err := c.ensureGroup(ctx); err != nil {
		c.mu.Unlock()
		return err
	}

	cctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	done := make(chan struct{})
	c.done = done
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.cancel = nil
		c.done = nil
		c.mu.Unlock()
		close(done)
	}()
	defer cancel()

	sem := make(chan struct{}, c.concurrency)
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		c.runReader(cctx, handler, sem, &wg)
	}()
	go func() {
		defer wg.Done()
		c.runClaimer(cctx, handler, sem, &wg)
	}()

	wg.Wait()

	return nil
}

// Close stops a running Consume call and waits for its read loop,
// reclaimer, and in-flight handlers to exit. Close is safe to call even if
// Consume is not running.
func (c *Client) Close() error {
	c.mu.Lock()
	cancel := c.cancel
	done := c.done
	c.mu.Unlock()

	if cancel == nil {
		return nil
	}

	cancel()
	<-done

	return nil
}

// ensureGroup creates the consumer group at the start of the stream,
// tolerating a group that already exists.
func (c *Client) ensureGroup(ctx context.Context) error {
	err := c.client.XGroupCreateMkStream(ctx, c.stream, c.group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("streamx: create group: %w", err)
	}
	return nil
}

// runReader repeatedly reads new messages for this consumer and dispatches
// them to handler until ctx is canceled.
func (c *Client) runReader(ctx context.Context, handler Handler, sem chan struct{}, wg *sync.WaitGroup) {
	for {
		if ctx.Err() != nil {
			return
		}

		res, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    c.group,
			Consumer: c.consumer,
			Streams:  []string{c.stream, ">"},
			Count:    int64(c.concurrency),
			Block:    c.blockTimeout,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) || errors.Is(err, context.Canceled) {
				continue
			}
			// Back off briefly on unexpected errors (e.g. transient network
			// issues) so the loop does not spin tightly.
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}

		for _, stream := range res {
			for _, xmsg := range stream.Messages {
				c.dispatch(ctx, sem, wg, handler, Message{ID: xmsg.ID, Values: xmsg.Values})
			}
		}
	}
}

// runClaimer periodically reclaims pending messages that have been idle for
// at least MinIdleTime and dispatches them to handler until ctx is
// canceled.
func (c *Client) runClaimer(ctx context.Context, handler Handler, sem chan struct{}, wg *sync.WaitGroup) {
	ticker := time.NewTicker(c.claimInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.claimStale(ctx, handler, sem, wg)
		}
	}
}

// claimStale walks the pending entries list, reclaiming and dispatching
// every entry idle for at least MinIdleTime.
func (c *Client) claimStale(ctx context.Context, handler Handler, sem chan struct{}, wg *sync.WaitGroup) {
	start := "0-0"

	for {
		msgs, next, err := c.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   c.stream,
			Group:    c.group,
			Consumer: c.consumer,
			MinIdle:  c.minIdleTime,
			Start:    start,
			Count:    int64(c.concurrency),
		}).Result()
		if err != nil {
			return
		}

		for _, xmsg := range msgs {
			c.dispatch(ctx, sem, wg, handler, Message{ID: xmsg.ID, Values: xmsg.Values})
		}

		if next == "0-0" || len(msgs) == 0 {
			return
		}
		start = next
	}
}

// dispatch acquires a slot bounded by Concurrency and runs handler for msg
// in a new goroutine tracked by wg. dispatch returns without running
// handler if ctx is canceled before a slot becomes available.
func (c *Client) dispatch(ctx context.Context, sem chan struct{}, wg *sync.WaitGroup, handler Handler, msg Message) {
	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		return
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() { <-sem }()
		c.process(ctx, handler, msg)
	}()
}

// process runs handler for msg, bounded by HandlerTimeout, and acknowledges
// the message only on success. The handler and the acknowledgement use a
// context detached from ctx's cancellation so an in-flight invocation
// started before Close finishes cleanly instead of being aborted.
func (c *Client) process(ctx context.Context, handler Handler, msg Message) {
	hctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.handlerTimeout)
	defer cancel()

	if err := handler(hctx, msg); err != nil {
		return
	}

	ackCtx, ackCancel := context.WithTimeout(context.WithoutCancel(ctx), c.handlerTimeout)
	defer ackCancel()

	_ = c.client.XAck(ackCtx, c.stream, c.group, msg.ID).Err()
}
