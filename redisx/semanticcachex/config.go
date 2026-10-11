package semanticcachex

import (
	"errors"
	"strings"
	"time"
)

var validDistanceMetrics = map[string]bool{
	"COSINE": true,
	"L2":     true,
	"IP":     true,
}

// Config configures a Cache.
//
// Prefix namespaces every cache entry hash key so multiple caches can share
// one Redis database without colliding. IndexName names the Redis Search
// index created by CreateIndex. Threshold is the maximum vector distance a
// Lookup accepts as a hit. TTL is the default expiry applied by Set when no
// explicit ttl is given.
type Config struct {
	Prefix         string
	IndexName      string
	Dimensions     int
	DistanceMetric string // COSINE, L2, IP (defaults to COSINE if empty)
	Threshold      float64
	TTL            time.Duration
}

// validate reports whether c holds a usable configuration.
func (c Config) validate() error {
	if strings.TrimSpace(c.Prefix) == "" {
		return errors.New("semanticcachex: prefix must not be empty")
	}

	if strings.TrimSpace(c.IndexName) == "" {
		return errors.New("semanticcachex: index name must not be empty")
	}

	if c.Dimensions <= 0 {
		return errors.New("semanticcachex: dimensions must be greater than zero")
	}

	if c.Threshold <= 0 {
		return errors.New("semanticcachex: threshold must be greater than zero")
	}

	if c.TTL <= 0 {
		return errors.New("semanticcachex: ttl must be greater than zero")
	}

	metric := strings.ToUpper(strings.TrimSpace(c.DistanceMetric))
	if metric != "" && !validDistanceMetrics[metric] {
		return errors.New("semanticcachex: distance metric must be COSINE, L2, or IP")
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
