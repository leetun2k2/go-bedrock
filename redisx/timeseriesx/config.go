package timeseriesx

import (
	"errors"
	"strings"
)

// Config configures a Client.
//
// Prefix namespaces every time-series key so multiple timeseriesx clients can
// share one Redis database without colliding. Prefix is also stored as a
// hidden label on every series the client creates, which lets MultiRange
// scope its label filters to this client's own series.
type Config struct {
	Prefix string
}

// validate reports whether c holds a usable configuration.
func (c Config) validate() error {
	if strings.TrimSpace(c.Prefix) == "" {
		return errors.New("timeseriesx: prefix must not be empty")
	}

	return nil
}
