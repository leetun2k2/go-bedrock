package cachex

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// defaultLockTTL is used when Config.LockTTL is zero.
const defaultLockTTL = 5 * time.Second

// Config configures a cache-aside Cache.
//
// Prefix namespaces every key so multiple caches can share one Redis
// database without colliding. DefaultTTL is used by Set and GetOrLoad when
// no explicit ttl is given. LockTTL bounds how long a stampede-prevention
// lock is held before it expires on its own.
type Config struct {
	Prefix     string
	DefaultTTL time.Duration
	LockTTL    time.Duration
}

// withDefaults returns a copy of c with zero-value fields replaced by defaults.
func (c Config) withDefaults() Config {
	if c.LockTTL <= 0 {
		c.LockTTL = defaultLockTTL
	}
	return c
}

// validate reports whether c holds a usable configuration.
func (c Config) validate() error {
	if strings.TrimSpace(c.Prefix) == "" {
		return errors.New("cachex: prefix must not be empty")
	}

	if c.DefaultTTL <= 0 {
		return fmt.Errorf("cachex: default ttl must be greater than zero: %s", c.DefaultTTL)
	}

	return nil
}
