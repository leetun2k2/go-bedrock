// Package prefetchx provides a preloaded reference cache backed by Redis
// strings, pipelines, and safety TTLs. Callers bulk load reference data
// ahead of the request path; reads never fall back to a primary store, so a
// cache miss is a visible error rather than a database read.
package prefetchx

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/leetun2k2/go-bedrock/redisx"
	"github.com/redis/go-redis/v9"
)

// ErrNotFound is returned by Get when the key was never loaded or has
// expired.
var ErrNotFound = errors.New("prefetchx: key not found")

// Item is one key-value pair supplied to Load.
type Item struct {
	Key   string
	Value []byte
}

// Cache is a preloaded reference cache backed by Redis. It is safe for
// concurrent use and does not own the Redis connection it borrows.
type Cache struct {
	client    *redis.Client
	prefix    string
	safetyTTL time.Duration
	batchSize int
}

// New constructs a Cache backed by client using cfg. New validates cfg and
// returns an error without touching Redis if the configuration is invalid.
func New(client *redisx.Client, cfg Config) (*Cache, error) {
	if client == nil {
		return nil, errors.New("prefetchx: client must not be nil")
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &Cache{
		client:    client.Client(),
		prefix:    cfg.Prefix,
		safetyTTL: cfg.SafetyTTL,
		batchSize: cfg.BatchSize,
	}, nil
}

// Load bulk writes items to Redis in batches of the Cache's BatchSize, each
// batch sent as one pipeline. Every written key gets the Cache's SafetyTTL.
// Load checks ctx between batches and stops without starting another batch
// if ctx is done; a batch already in flight always completes.
func (c *Cache) Load(ctx context.Context, items []Item) error {
	for start := 0; start < len(items); start += c.batchSize {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("prefetchx: %w", err)
		}

		end := start + c.batchSize
		if end > len(items) {
			end = len(items)
		}
		batch := items[start:end]

		if _, err := c.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			for _, item := range batch {
				pipe.Set(ctx, c.dataKey(item.Key), item.Value, c.safetyTTL)
			}
			return nil
		}); err != nil {
			return fmt.Errorf("prefetchx: load: %w", err)
		}
	}

	return nil
}

// Get returns the preloaded bytes for key. It returns ErrNotFound if key was
// never loaded or has expired. Get never reads from a backing store.
func (c *Cache) Get(ctx context.Context, key string) ([]byte, error) {
	if strings.TrimSpace(key) == "" {
		return nil, errors.New("prefetchx: key must not be empty")
	}

	val, err := c.client.Get(ctx, c.dataKey(key)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("prefetchx: get: %w", err)
	}

	return val, nil
}

// Upsert overwrites key with value and refreshes the Cache's SafetyTTL.
func (c *Cache) Upsert(ctx context.Context, key string, value []byte) error {
	if strings.TrimSpace(key) == "" {
		return errors.New("prefetchx: key must not be empty")
	}

	if err := c.client.Set(ctx, c.dataKey(key), value, c.safetyTTL).Err(); err != nil {
		return fmt.Errorf("prefetchx: upsert: %w", err)
	}

	return nil
}

// Delete removes key from the cache. Delete is a no-op if key does not
// exist.
func (c *Cache) Delete(ctx context.Context, key string) error {
	if strings.TrimSpace(key) == "" {
		return errors.New("prefetchx: key must not be empty")
	}

	if err := c.client.Del(ctx, c.dataKey(key)).Err(); err != nil {
		return fmt.Errorf("prefetchx: delete: %w", err)
	}

	return nil
}

func (c *Cache) dataKey(key string) string {
	return c.prefix + ":" + key
}
