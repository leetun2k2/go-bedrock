package sessionx

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Config configures a session Store.
//
// Prefix namespaces every session and user-index key so multiple stores can
// share one Redis database without colliding. TTL is the sliding expiration
// applied to a session and its user index entry on every create, read, or
// update.
type Config struct {
	Prefix string
	TTL    time.Duration
}

// validate reports whether c holds a usable configuration.
func (c Config) validate() error {
	if strings.TrimSpace(c.Prefix) == "" {
		return errors.New("sessionx: prefix must not be empty")
	}

	if c.TTL <= 0 {
		return fmt.Errorf("sessionx: ttl must be greater than zero: %s", c.TTL)
	}

	return nil
}
