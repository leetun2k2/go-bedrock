// Package queuex provides a small reliable background job queue backed by
// Redis lists and sorted sets. Claimed jobs carry an opaque claim token that
// is checked atomically on ACK, NACK, and reclaim, so a stale worker cannot
// delete or reset a job that has already been reclaimed after its
// visibility timeout expired.
package queuex

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/leetun2k2/go-bedrock/redisx"
	"github.com/redis/go-redis/v9"
)

// pollInterval bounds how long a worker sleeps between empty claim attempts
// so it can observe context cancellation promptly instead of blocking
// indefinitely.
const pollInterval = time.Second

// claimScript atomically pops the next pending job and records its claim
// token and visibility deadline, so no job can be lost between the pop and
// the claim record. It returns a nil job id when pending is empty.
// KEYS[1] = pending list, KEYS[2] = processing zset.
// ARGV[1] = job key prefix, ARGV[2] = claim token, ARGV[3] = deadline (unix ms).
const claimScript = `
local id = redis.call('LPOP', KEYS[1])
if not id then
	return false
end
local job_key = ARGV[1] .. id
redis.call('HSET', job_key, 'token', ARGV[2])
redis.call('ZADD', KEYS[2], ARGV[3], id)
local payload = redis.call('HGET', job_key, 'payload')
local attempts = redis.call('HGET', job_key, 'attempts')
return {id, payload, attempts}
`

// ackScript removes a job only if the caller's claim token still matches
// the stored token, i.e. the job was not reclaimed since it was claimed.
// KEYS[1] = processing zset.
// ARGV[1] = job key prefix, ARGV[2] = job id, ARGV[3] = claim token.
const ackScript = `
local job_key = ARGV[1] .. ARGV[2]
local stored = redis.call('HGET', job_key, 'token')
if stored ~= ARGV[3] then
	return 0
end
redis.call('ZREM', KEYS[1], ARGV[2])
redis.call('DEL', job_key)
return 1
`

// nackScript returns a job to pending only if the caller's claim token
// still matches the stored token.
// KEYS[1] = processing zset, KEYS[2] = pending list.
// ARGV[1] = job key prefix, ARGV[2] = job id, ARGV[3] = claim token.
const nackScript = `
local job_key = ARGV[1] .. ARGV[2]
local stored = redis.call('HGET', job_key, 'token')
if stored ~= ARGV[3] then
	return 0
end
redis.call('ZREM', KEYS[1], ARGV[2])
redis.call('HINCRBY', job_key, 'attempts', 1)
redis.call('HDEL', job_key, 'token')
redis.call('RPUSH', KEYS[2], ARGV[2])
return 1
`

// reclaimScript returns one expired job to pending, invalidating its claim
// token so a stale worker's later ACK or NACK is rejected.
// KEYS[1] = processing zset, KEYS[2] = pending list.
// ARGV[1] = job key prefix, ARGV[2] = job id, ARGV[3] = now (unix ms).
const reclaimScript = `
local score = redis.call('ZSCORE', KEYS[1], ARGV[2])
if not score or tonumber(score) > tonumber(ARGV[3]) then
	return 0
end
redis.call('ZREM', KEYS[1], ARGV[2])
local job_key = ARGV[1] .. ARGV[2]
redis.call('HINCRBY', job_key, 'attempts', 1)
redis.call('HDEL', job_key, 'token')
redis.call('RPUSH', KEYS[2], ARGV[2])
return 1
`

// Job is a unit of work read from the queue.
type Job struct {
	// ID uniquely identifies the job.
	ID string
	// Payload is the opaque data supplied at Enqueue time.
	Payload []byte
	// Attempts counts earlier NACK or reclaim operations. It is zero on the
	// first delivery.
	Attempts int

	claimToken string
}

// Handler processes one job. Returning nil acknowledges the job, removing
// it from the queue. Returning an error returns the job to pending with its
// attempt count incremented. Handlers must be idempotent: a job can be
// delivered more than once.
type Handler func(ctx context.Context, job Job) error

// Queue is a reliable, at-least-once background job queue. It borrows a
// *redisx.Client and does not close the shared connection.
type Queue struct {
	client      *redis.Client
	visibility  time.Duration
	workerCount int

	pendingKey    string
	processingKey string
	jobKeyPrefix  string

	claimLua *redis.Script
	ack      *redis.Script
	nack     *redis.Script
	reclaim  *redis.Script

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

// New constructs a Queue backed by client using cfg. New validates cfg and
// returns an error without touching Redis if the configuration is invalid.
func New(client *redisx.Client, cfg Config) (*Queue, error) {
	if client == nil {
		return nil, errors.New("queuex: client must not be nil")
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	base := cfg.QueueName
	if cfg.Prefix != "" {
		base = cfg.Prefix + ":" + cfg.QueueName
	}

	return &Queue{
		client:        client.Client(),
		visibility:    cfg.VisibilityTimeout,
		workerCount:   cfg.WorkerCount,
		pendingKey:    base + ":pending",
		processingKey: base + ":processing",
		jobKeyPrefix:  base + ":job:",
		claimLua:      redis.NewScript(claimScript),
		ack:           redis.NewScript(ackScript),
		nack:          redis.NewScript(nackScript),
		reclaim:       redis.NewScript(reclaimScript),
	}, nil
}

// Enqueue stores payload and appends a new job to the pending list. It
// returns the generated job ID.
func (q *Queue) Enqueue(ctx context.Context, payload []byte) (string, error) {
	id, err := randomHex(16)
	if err != nil {
		return "", fmt.Errorf("queuex: enqueue: %w", err)
	}

	pipe := q.client.TxPipeline()
	pipe.HSet(ctx, q.jobKeyPrefix+id, "payload", payload, "attempts", 0)
	pipe.RPush(ctx, q.pendingKey, id)
	if _, err := pipe.Exec(ctx); err != nil {
		return "", fmt.Errorf("queuex: enqueue: %w", err)
	}

	return id, nil
}

// Consume starts up to WorkerCount concurrent workers plus a background
// reclaimer, and blocks until ctx is canceled or Close is called. It
// returns nil on clean shutdown, or an error if Consume is already running.
func (q *Queue) Consume(ctx context.Context, handler Handler) error {
	q.mu.Lock()
	if q.cancel != nil {
		q.mu.Unlock()
		return errors.New("queuex: consume already running")
	}

	cctx, cancel := context.WithCancel(ctx)
	q.cancel = cancel
	done := make(chan struct{})
	q.done = done
	q.mu.Unlock()

	defer func() {
		q.mu.Lock()
		q.cancel = nil
		q.done = nil
		q.mu.Unlock()
		close(done)
	}()
	defer cancel()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		q.runReclaimer(cctx)
	}()

	for i := 0; i < q.workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			q.runWorker(cctx, handler)
		}()
	}

	wg.Wait()

	return nil
}

// Close stops a running Consume call and waits for its workers and
// reclaimer to exit. Close is safe to call even if Consume is not running.
func (q *Queue) Close() error {
	q.mu.Lock()
	cancel := q.cancel
	done := q.done
	q.mu.Unlock()

	if cancel == nil {
		return nil
	}

	cancel()
	<-done

	return nil
}

// runWorker repeatedly claims and processes jobs until ctx is canceled.
func (q *Queue) runWorker(ctx context.Context, handler Handler) {
	for {
		if ctx.Err() != nil {
			return
		}

		job, err := q.claim(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}

		if job == nil {
			q.sleep(ctx, pollInterval)
			continue
		}

		q.process(ctx, handler, *job)
	}
}

// process runs handler for job and acknowledges or returns it based on the
// result.
func (q *Queue) process(ctx context.Context, handler Handler, job Job) {
	if handler(ctx, job) == nil {
		_ = q.ackJob(context.WithoutCancel(ctx), job.ID, job.claimToken)
		return
	}

	_ = q.nackJob(context.WithoutCancel(ctx), job.ID, job.claimToken)
}

// claim atomically pops the next pending job and records a fresh claim
// token and visibility deadline for it in a single Lua script, so a job can
// never be popped without being recorded. It returns a nil job and nil
// error when pending is empty; callers should back off before retrying.
func (q *Queue) claim(ctx context.Context) (*Job, error) {
	token, err := randomHex(16)
	if err != nil {
		return nil, fmt.Errorf("queuex: claim: %w", err)
	}

	deadline := time.Now().Add(q.visibility).UnixMilli()

	raw, err := q.claimLua.Run(ctx, q.client, []string{q.pendingKey, q.processingKey}, q.jobKeyPrefix, token, deadline).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("queuex: claim: %w", err)
	}

	values, ok := raw.([]interface{})
	if !ok {
		// Empty pending list: script returned Lua false, which the Redis
		// protocol maps to a nil bulk reply.
		return nil, nil
	}
	if len(values) != 3 {
		return nil, fmt.Errorf("queuex: claim: unexpected script result: %v", raw)
	}

	id, ok := values[0].(string)
	if !ok {
		return nil, fmt.Errorf("queuex: claim: unexpected id type: %T", values[0])
	}

	payload, attempts, err := parseJobFields(values[1:])
	if err != nil {
		return nil, fmt.Errorf("queuex: claim: %w", err)
	}

	return &Job{
		ID:         id,
		Payload:    payload,
		Attempts:   attempts,
		claimToken: token,
	}, nil
}

// sleep pauses for d or until ctx is canceled, whichever comes first.
func (q *Queue) sleep(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

// ackJob removes a job if token still matches its stored claim token.
func (q *Queue) ackJob(ctx context.Context, id, token string) error {
	if err := q.ack.Run(ctx, q.client, []string{q.processingKey}, q.jobKeyPrefix, id, token).Err(); err != nil {
		return fmt.Errorf("queuex: ack: %w", err)
	}
	return nil
}

// nackJob returns a job to pending if token still matches its stored claim
// token.
func (q *Queue) nackJob(ctx context.Context, id, token string) error {
	if err := q.nack.Run(ctx, q.client, []string{q.processingKey, q.pendingKey}, q.jobKeyPrefix, id, token).Err(); err != nil {
		return fmt.Errorf("queuex: nack: %w", err)
	}
	return nil
}

// runReclaimer periodically returns expired processing jobs to pending
// until ctx is canceled.
func (q *Queue) runReclaimer(ctx context.Context) {
	interval := q.visibility / 2
	if interval <= 0 {
		interval = q.visibility
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			q.reclaimExpired(ctx)
		}
	}
}

// reclaimExpired moves every processing job whose deadline has passed back
// to pending, invalidating its claim token.
func (q *Queue) reclaimExpired(ctx context.Context) {
	now := time.Now().UnixMilli()

	ids, err := q.client.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     q.processingKey,
		ByScore: true,
		Start:   "-inf",
		Stop:    strconv.FormatInt(now, 10),
		Count:   100,
	}).Result()
	if err != nil {
		return
	}

	for _, id := range ids {
		q.reclaim.Run(ctx, q.client, []string{q.processingKey, q.pendingKey}, q.jobKeyPrefix, id, now)
	}
}

// parseJobFields converts the {payload, attempts} script reply into typed
// values.
func parseJobFields(values []interface{}) ([]byte, int, error) {
	payload, ok := values[0].(string)
	if !ok {
		return nil, 0, fmt.Errorf("unexpected payload type: %T", values[0])
	}

	attemptsStr, ok := values[1].(string)
	if !ok {
		return nil, 0, fmt.Errorf("unexpected attempts type: %T", values[1])
	}

	attempts, err := strconv.Atoi(attemptsStr)
	if err != nil {
		return nil, 0, fmt.Errorf("invalid attempts value %q: %w", attemptsStr, err)
	}

	return []byte(payload), attempts, nil
}

// randomHex returns a random hex-encoded identifier with n random bytes.
func randomHex(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
