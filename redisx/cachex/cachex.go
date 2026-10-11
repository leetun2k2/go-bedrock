// Package cachex provides a cache-aside API backed by Redis strings and TTL.
// Callers own source loading and serialization; cachex stores and retrieves
// opaque byte values.
package cachex

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	mathrand "math/rand/v2"
	"strings"
	"time"

	"github.com/leetun2k2/go-bedrock/redisx"
	"github.com/redis/go-redis/v9"
)

// ErrNotFound is returned by Get when the key does not exist in the cache.
var ErrNotFound = errors.New("cachex: key not found")

// releaseLockScript deletes KEYS[1] only if its value still equals ARGV[1],
// so a lock owner never releases a lock it no longer holds.
const releaseLockScript = `
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("del", KEYS[1])
else
	return 0
end
`

// lockRetryMinDelay and lockRetryMaxDelay bound the jittered sleep between
// lock-acquisition attempts while waiting out a cache stampede.
const (
	lockRetryMinDelay = 20 * time.Millisecond
	lockRetryMaxDelay = 50 * time.Millisecond
)

// Cache is a cache-aside client backed by Redis. It is safe for concurrent
// use and does not own the Redis connection it borrows.
type Cache struct {
	client     *redis.Client
	prefix     string
	defaultTTL time.Duration
	lockTTL    time.Duration
	releaser   *redis.Script
}

// New constructs a Cache backed by client using cfg. New validates cfg and
// returns an error without touching Redis if the configuration is invalid.
func New(client *redisx.Client, cfg Config) (*Cache, error) {
	if client == nil {
		return nil, errors.New("cachex: client must not be nil")
	}

	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &Cache{
		client:     client.Client(),
		prefix:     cfg.Prefix,
		defaultTTL: cfg.DefaultTTL,
		lockTTL:    cfg.LockTTL,
		releaser:   redis.NewScript(releaseLockScript),
	}, nil
}

// Get returns the cached bytes for key. It returns ErrNotFound if key does
// not exist in the cache.
func (c *Cache) Get(ctx context.Context, key string) ([]byte, error) {
	if strings.TrimSpace(key) == "" {
		return nil, errors.New("cachex: key must not be empty")
	}

	val, err := c.client.Get(ctx, c.dataKey(key)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("cachex: get: %w", err)
	}

	return val, nil
}

// Set stores value under key with ttl. If ttl is zero or negative, the
// Cache's DefaultTTL is used instead.
func (c *Cache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if strings.TrimSpace(key) == "" {
		return errors.New("cachex: key must not be empty")
	}

	if ttl <= 0 {
		ttl = c.defaultTTL
	}

	if err := c.client.Set(ctx, c.dataKey(key), value, ttl).Err(); err != nil {
		return fmt.Errorf("cachex: set: %w", err)
	}

	return nil
}

// Delete removes key from the cache. Delete is a no-op if key does not exist.
func (c *Cache) Delete(ctx context.Context, key string) error {
	if strings.TrimSpace(key) == "" {
		return errors.New("cachex: key must not be empty")
	}

	if err := c.client.Del(ctx, c.dataKey(key)).Err(); err != nil {
		return fmt.Errorf("cachex: delete: %w", err)
	}

	return nil
}

// GetOrLoad returns the cached bytes for key, calling loader to populate the
// cache on a miss. If ttl is zero or negative, the Cache's DefaultTTL is
// used for the cached value.
//
// Concurrent callers racing on the same cold key coordinate through a
// distributed lock so loader runs once per lock owner: the first caller to
// acquire the lock runs loader and populates the cache, while the rest wait
// briefly and then read the now-populated key. loader errors and context
// cancellation never populate the cache.
func (c *Cache) GetOrLoad(ctx context.Context, key string, ttl time.Duration, loader func(ctx context.Context) ([]byte, error)) ([]byte, error) {
	if strings.TrimSpace(key) == "" {
		return nil, errors.New("cachex: key must not be empty")
	}
	if loader == nil {
		return nil, errors.New("cachex: loader must not be nil")
	}

	if val, err := c.Get(ctx, key); err == nil {
		return val, nil
	} else if !errors.Is(err, ErrNotFound) {
		return nil, err
	}

	token, err := newLockToken()
	if err != nil {
		return nil, fmt.Errorf("cachex: %w", err)
	}

	lockKey := c.lockKey(key)
	if err := c.waitForLock(ctx, lockKey, token, key); err != nil {
		if errors.Is(err, errValuePopulated) {
			return c.Get(ctx, key)
		}
		return nil, err
	}
	defer c.releaseLock(lockKey, token)

	if val, err := c.Get(ctx, key); err == nil {
		return val, nil
	} else if !errors.Is(err, ErrNotFound) {
		return nil, err
	}

	val, err := loader(ctx)
	if err != nil {
		return nil, fmt.Errorf("cachex: loader: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("cachex: %w", err)
	}

	if val != nil {
		if err := c.Set(ctx, key, val, ttl); err != nil {
			return nil, err
		}
	}

	return val, nil
}

// waitForLock blocks until either the lock at lockKey is acquired or key
// becomes populated by another lock owner. It returns nil once the lock is
// held by this caller (token).
func (c *Cache) waitForLock(ctx context.Context, lockKey, token, key string) error {
	for {
		acquired, err := c.acquireLock(ctx, lockKey, token)
		if err != nil {
			return err
		}
		if acquired {
			return nil
		}

		if _, err := c.Get(ctx, key); err == nil {
			return errValuePopulated
		} else if !errors.Is(err, ErrNotFound) {
			return err
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("cachex: %w", ctx.Err())
		case <-time.After(jitteredRetryDelay()):
		}
	}
}

// errValuePopulated signals that another lock owner populated the cache
// while this caller waited, so no lock is held and loader must not run.
var errValuePopulated = errors.New("cachex: value populated while waiting")

func (c *Cache) acquireLock(ctx context.Context, lockKey, token string) (bool, error) {
	ok, err := c.client.SetNX(ctx, lockKey, token, c.lockTTL).Result()
	if err != nil {
		return false, fmt.Errorf("cachex: acquire lock: %w", err)
	}
	return ok, nil
}

func (c *Cache) releaseLock(lockKey, token string) {
	ctx, cancel := context.WithTimeout(context.Background(), lockReleaseTimeout)
	defer cancel()
	_ = c.releaser.Run(ctx, c.client, []string{lockKey}, token).Err()
}

// lockReleaseTimeout bounds lock release so it completes even if the
// caller's context was already canceled.
const lockReleaseTimeout = 2 * time.Second

func (c *Cache) dataKey(key string) string {
	return c.prefix + ":" + key
}

func (c *Cache) lockKey(key string) string {
	return c.prefix + ":lock:" + key
}

func jitteredRetryDelay() time.Duration {
	span := lockRetryMaxDelay - lockRetryMinDelay
	return lockRetryMinDelay + time.Duration(mathrand.Int64N(int64(span)+1))
}

func newLockToken() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate lock token: %w", err)
	}
	return hex.EncodeToString(buf), nil
}
