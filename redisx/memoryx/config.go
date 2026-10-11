package memoryx

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

// Config configures a Memory store.
//
// Prefix namespaces every key this package writes (working state hashes,
// semantic memory JSON documents, and event streams) so multiple agents can
// share one Redis database without colliding. IndexName names the Redis
// Search index created by CreateIndex. DedupThreshold is the maximum vector
// distance at which Remember treats a candidate as a near-duplicate and
// skips the write; a zero or negative value disables the dedup check.
// WorkingMemoryTTL is the expiry applied to thread state hashes.
// EventRetention bounds the number of entries kept per event stream.
type Config struct {
	Prefix           string
	IndexName        string
	Dimensions       int
	DistanceMetric   string // COSINE, L2, IP (defaults to COSINE if empty)
	DedupThreshold   float64
	WorkingMemoryTTL time.Duration
	EventRetention   int64
}

// validate reports whether c holds a usable configuration.
func (c Config) validate() error {
	if strings.TrimSpace(c.Prefix) == "" {
		return errors.New("memoryx: prefix must not be empty")
	}

	if strings.TrimSpace(c.IndexName) == "" {
		return errors.New("memoryx: index name must not be empty")
	}

	if c.Dimensions <= 0 {
		return errors.New("memoryx: dimensions must be greater than zero")
	}

	if c.WorkingMemoryTTL <= 0 {
		return errors.New("memoryx: working memory ttl must be greater than zero")
	}

	if c.EventRetention <= 0 {
		return errors.New("memoryx: event retention must be greater than zero")
	}

	metric := strings.ToUpper(strings.TrimSpace(c.DistanceMetric))
	if metric != "" && !validDistanceMetrics[metric] {
		return errors.New("memoryx: distance metric must be COSINE, L2, or IP")
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
