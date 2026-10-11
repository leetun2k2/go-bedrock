package streamx

import (
	"fmt"
	"strings"
	"time"
)

// Config configures a durable stream consumer.
//
// Stream, Group, and Consumer identify the Redis stream key, the consumer
// group, and this consumer's instance name within that group. MaxLen caps
// stream retention via approximate trimming on Publish; zero means
// unconstrained. Concurrency bounds how many handler invocations run at
// once. MinIdleTime and ClaimInterval control how aggressively stale
// pending entries are reclaimed via XAUTOCLAIM. HandlerTimeout sets the
// deadline on each handler context. BlockTimeout bounds each XREADGROUP call
// so the read loop can observe context cancellation promptly.
type Config struct {
	Stream         string
	Group          string
	Consumer       string
	MaxLen         int64
	Concurrency    int
	MinIdleTime    time.Duration
	ClaimInterval  time.Duration
	HandlerTimeout time.Duration
	BlockTimeout   time.Duration
}

// validate reports whether c holds a usable configuration.
func (c Config) validate() error {
	if strings.TrimSpace(c.Stream) == "" {
		return fmt.Errorf("streamx: stream must not be empty")
	}

	if strings.TrimSpace(c.Group) == "" {
		return fmt.Errorf("streamx: group must not be empty")
	}

	if strings.TrimSpace(c.Consumer) == "" {
		return fmt.Errorf("streamx: consumer must not be empty")
	}

	if c.Concurrency <= 0 {
		return fmt.Errorf("streamx: concurrency must be greater than zero: %d", c.Concurrency)
	}

	if c.MinIdleTime <= 0 {
		return fmt.Errorf("streamx: min idle time must be greater than zero: %s", c.MinIdleTime)
	}

	if c.ClaimInterval <= 0 {
		return fmt.Errorf("streamx: claim interval must be greater than zero: %s", c.ClaimInterval)
	}

	if c.HandlerTimeout <= 0 {
		return fmt.Errorf("streamx: handler timeout must be greater than zero: %s", c.HandlerTimeout)
	}

	return nil
}

// blockTimeout returns the XREADGROUP block duration, defaulting to one
// second when unset so the read loop still polls for cancellation.
func (c Config) blockTimeout() time.Duration {
	if c.BlockTimeout > 0 {
		return c.BlockTimeout
	}
	return time.Second
}
