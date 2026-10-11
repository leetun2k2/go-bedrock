package matchingx

import (
	"errors"
	"strings"
)

var validDistanceMetrics = map[string]bool{
	"COSINE": true,
	"L2":     true,
	"IP":     true,
}

// Config configures a Matcher.
//
// Prefix namespaces every item hash key so multiple matchingx matchers can
// share one Redis database without colliding. IndexName names the Redis
// Search index created by CreateIndex. TagFields and NumericFields declare
// the structured fields available for filtering in Match.
type Config struct {
	Prefix         string
	IndexName      string
	Dimensions     int
	DistanceMetric string // COSINE, L2, IP (defaults to COSINE if empty)
	TagFields      []string
	NumericFields  []string
}

// validate reports whether c holds a usable configuration.
func (c Config) validate() error {
	if strings.TrimSpace(c.Prefix) == "" {
		return errors.New("matchingx: prefix must not be empty")
	}

	if strings.TrimSpace(c.IndexName) == "" {
		return errors.New("matchingx: index name must not be empty")
	}

	if c.Dimensions <= 0 {
		return errors.New("matchingx: dimensions must be greater than zero")
	}

	metric := strings.ToUpper(strings.TrimSpace(c.DistanceMetric))
	if metric != "" && !validDistanceMetrics[metric] {
		return errors.New("matchingx: distance metric must be COSINE, L2, or IP")
	}

	return nil
}

// withDefaults returns a copy of c with defaults applied.
func (c Config) withDefaults() Config {
	metric := strings.ToUpper(strings.TrimSpace(c.DistanceMetric))
	if metric == "" {
		metric = "COSINE"
	}
	c.DistanceMetric = metric
	return c
}
