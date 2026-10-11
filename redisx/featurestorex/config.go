package featurestorex

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Config configures a Store.
//
// Prefix namespaces every entity key so multiple feature stores can share
// one Redis database without colliding. EntityTTL is an optional fallback
// TTL applied to the whole entity hash; it is independent from any
// per-field TTL applied through SetWithTTL.
type Config struct {
	Prefix    string
	EntityTTL time.Duration
}

// validate reports whether c holds a usable configuration.
func (c Config) validate() error {
	if strings.TrimSpace(c.Prefix) == "" {
		return errors.New("featurestorex: prefix must not be empty")
	}

	if c.EntityTTL < 0 {
		return fmt.Errorf("featurestorex: entity ttl must not be negative: %s", c.EntityTTL)
	}

	return nil
}
