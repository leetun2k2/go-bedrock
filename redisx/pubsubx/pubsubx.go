// Package pubsubx provides real-time channel and pattern broadcasting
// backed by Redis Pub/Sub. It preserves Redis's at-most-once semantics:
// there is no persistence, acknowledgement, or replay, and offline
// subscribers miss messages published while they are disconnected.
package pubsubx

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

// closeTimeout bounds Unsubscribe/Close calls so Subscription.Close
// completes even if the caller never supplies its own deadline.
const closeTimeout = 5 * time.Second

// Message is a single Pub/Sub delivery. Pattern is empty for messages
// received through Subscribe and set to the matched pattern for messages
// received through PSubscribe.
type Message struct {
	Channel string
	Pattern string
	Payload []byte
}

// Client publishes and subscribes to Redis Pub/Sub channels. It is safe
// for concurrent use and does not own the Redis connection it borrows.
type Client struct {
	client *redis.Client
	prefix string
}

// New constructs a Client backed by client using cfg. New returns an error
// if client is nil.
func New(client *redisx.Client, cfg Config) (*Client, error) {
	if client == nil {
		return nil, errors.New("pubsubx: client must not be nil")
	}

	return &Client{
		client: client.Client(),
		prefix: cfg.Prefix,
	}, nil
}

// Publish sends payload to channel and returns the number of active
// receivers reported by Redis.
func (c *Client) Publish(ctx context.Context, channel string, payload []byte) (int64, error) {
	if strings.TrimSpace(channel) == "" {
		return 0, errors.New("pubsubx: channel must not be empty")
	}

	n, err := c.client.Publish(ctx, c.prefixed(channel), payload).Result()
	if err != nil {
		return 0, fmt.Errorf("pubsubx: publish: %w", err)
	}

	return n, nil
}

// Subscribe subscribes to channels and delivers messages through the
// returned Subscription until ctx is done or the Subscription is closed.
func (c *Client) Subscribe(ctx context.Context, channels ...string) (*Subscription, error) {
	if len(channels) == 0 {
		return nil, errors.New("pubsubx: at least one channel is required")
	}

	ps := c.client.Subscribe(ctx, c.prefixedAll(channels)...)
	if _, err := ps.Receive(ctx); err != nil {
		_ = ps.Close()
		return nil, fmt.Errorf("pubsubx: subscribe: %w", err)
	}

	return c.newSubscription(ctx, ps, ps.Unsubscribe), nil
}

// PSubscribe subscribes to patterns and delivers matching messages through
// the returned Subscription until ctx is done or the Subscription is
// closed.
func (c *Client) PSubscribe(ctx context.Context, patterns ...string) (*Subscription, error) {
	if len(patterns) == 0 {
		return nil, errors.New("pubsubx: at least one pattern is required")
	}

	ps := c.client.PSubscribe(ctx, c.prefixedAll(patterns)...)
	if _, err := ps.Receive(ctx); err != nil {
		_ = ps.Close()
		return nil, fmt.Errorf("pubsubx: psubscribe: %w", err)
	}

	return c.newSubscription(ctx, ps, ps.PUnsubscribe), nil
}

func (c *Client) prefixed(name string) string {
	if c.prefix == "" {
		return name
	}
	return c.prefix + ":" + name
}

func (c *Client) prefixedAll(names []string) []string {
	out := make([]string, len(names))
	for i, name := range names {
		out[i] = c.prefixed(name)
	}
	return out
}

// Subscription delivers messages from a Subscribe or PSubscribe call.
type Subscription struct {
	ps          *redis.PubSub
	unsubscribe func(ctx context.Context, names ...string) error
	prefix      string
	msgs        chan Message
	done        chan struct{}

	closeOnce sync.Once
	closeErr  error
}

func (c *Client) newSubscription(ctx context.Context, ps *redis.PubSub, unsubscribe func(ctx context.Context, names ...string) error) *Subscription {
	sub := &Subscription{
		ps:          ps,
		unsubscribe: unsubscribe,
		prefix:      c.prefix,
		msgs:        make(chan Message),
		done:        make(chan struct{}),
	}

	go sub.loop(ctx)

	return sub
}

func (s *Subscription) unprefixed(name string) string {
	if s.prefix == "" || name == "" {
		return name
	}
	return strings.TrimPrefix(name, s.prefix+":")
}

// Channel returns the channel on which delivered messages arrive. The
// channel is closed once the Subscription is closed or its context is
// done.
func (s *Subscription) Channel() <-chan Message {
	return s.msgs
}

// Close unsubscribes, closes the underlying Pub/Sub connection, and waits
// for the background delivery loop to stop. Close is safe to call more
// than once and from multiple goroutines.
func (s *Subscription) Close() error {
	s.closeOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
		defer cancel()

		if err := s.unsubscribe(ctx); err != nil && !errors.Is(err, redis.ErrClosed) {
			s.closeErr = err
		}
		if err := s.ps.Close(); err != nil && !errors.Is(err, redis.ErrClosed) && s.closeErr == nil {
			s.closeErr = err
		}
	})

	<-s.done

	if s.closeErr != nil {
		return fmt.Errorf("pubsubx: close: %w", s.closeErr)
	}
	return nil
}

func (s *Subscription) loop(ctx context.Context) {
	defer close(s.done)
	defer close(s.msgs)

	redisCh := s.ps.Channel()

	for {
		select {
		case <-ctx.Done():
			_ = s.ps.Close()
			return
		case msg, ok := <-redisCh:
			if !ok {
				return
			}

			out := Message{
				Channel: s.unprefixed(msg.Channel),
				Pattern: s.unprefixed(msg.Pattern),
				Payload: []byte(msg.Payload),
			}

			select {
			case s.msgs <- out:
			case <-ctx.Done():
				_ = s.ps.Close()
				return
			}
		}
	}
}
