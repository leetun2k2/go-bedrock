package ratelimitx

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Config configures a token-bucket rate limiter.
//
// Prefix namespaces every bucket key so multiple limiters can share one Redis
// database without colliding. Capacity is the maximum number of tokens a
// bucket can hold. RefillAmount tokens are added every RefillInterval,
// elapsed server-side, up to Capacity.
type Config struct {
	Prefix         string
	Capacity       int64
	RefillAmount   int64
	RefillInterval time.Duration
}

// validate reports whether c holds a usable configuration.
func (c Config) validate() error {
	if strings.TrimSpace(c.Prefix) == "" {
		return errors.New("ratelimitx: prefix must not be empty")
	}

	if c.Capacity <= 0 {
		return fmt.Errorf("ratelimitx: capacity must be greater than zero: %d", c.Capacity)
	}

	if c.RefillAmount <= 0 {
		return fmt.Errorf("ratelimitx: refill amount must be greater than zero: %d", c.RefillAmount)
	}

	if c.RefillInterval <= 0 {
		return fmt.Errorf("ratelimitx: refill interval must be greater than zero: %s", c.RefillInterval)
	}

	return nil
}
