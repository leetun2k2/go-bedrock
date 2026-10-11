package prefetchx

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Config configures a prefetch Cache.
//
// Prefix namespaces every key so multiple prefetch caches can share one
// Redis database without colliding. SafetyTTL bounds how long preloaded
// data survives without a refresh, so a stalled loader cannot pin stale
// data forever. BatchSize bounds how many items one Load pipeline writes
// at a time.
type Config struct {
	Prefix    string
	SafetyTTL time.Duration
	BatchSize int
}

// validate reports whether c holds a usable configuration.
func (c Config) validate() error {
	if strings.TrimSpace(c.Prefix) == "" {
		return errors.New("prefetchx: prefix must not be empty")
	}

	if c.SafetyTTL <= 0 {
		return fmt.Errorf("prefetchx: safety ttl must be greater than zero: %s", c.SafetyTTL)
	}

	if c.BatchSize <= 0 {
		return fmt.Errorf("prefetchx: batch size must be greater than zero: %d", c.BatchSize)
	}

	return nil
}
