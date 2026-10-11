package leaderboardx

import (
	"errors"
	"strings"
	"time"
)

// Config configures a Leaderboard.
//
// Prefix namespaces every board and metadata key so multiple leaderboards can
// share one Redis database without colliding. TTL is the optional expiry
// applied to a board and its metadata on every score update. TTL of zero
// means the board never expires.
type Config struct {
	Prefix string
	TTL    time.Duration
}

// validate reports whether c holds a usable configuration.
func (c Config) validate() error {
	if strings.TrimSpace(c.Prefix) == "" {
		return errors.New("leaderboardx: prefix must not be empty")
	}

	if c.TTL < 0 {
		return errors.New("leaderboardx: ttl must not be negative")
	}

	return nil
}
