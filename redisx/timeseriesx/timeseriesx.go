// Package timeseriesx stores and queries bounded numeric time-series samples
// using Redis TimeSeries commands (TS.*). It maps directly onto TS.CREATE,
// TS.ADD, TS.RANGE, TS.MRANGE, and TS.CREATERULE, matching the official Redis
// time-series dashboard example.
package timeseriesx

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/leetun2k2/go-bedrock/redisx"
	"github.com/redis/go-redis/v9"
)

// namespaceLabel is attached to every series a Client creates and used to
// scope MultiRange's label filters to this client's own namespace.
const namespaceLabel = "tsx_prefix"

// ErrCapability is returned when the connected Redis server does not support
// the TimeSeries module.
var ErrCapability = errors.New("timeseriesx: redis timeseries module required")

var validDuplicatePolicies = map[string]bool{
	"BLOCK": true,
	"FIRST": true,
	"LAST":  true,
	"MIN":   true,
	"MAX":   true,
	"SUM":   true,
}

var validAggregators = map[string]bool{
	"AVG":   true,
	"SUM":   true,
	"MIN":   true,
	"MAX":   true,
	"RANGE": true,
	"COUNT": true,
	"FIRST": true,
	"LAST":  true,
	"STD.P": true,
	"STD.S": true,
	"VAR.P": true,
	"VAR.S": true,
	"TWA":   true,
}

// Sample is a single time-series observation.
type Sample struct {
	Timestamp int64 // Unix milliseconds
	Value     float64
}

// SeriesSamples is one series' samples and labels, as returned by MultiRange.
type SeriesSamples struct {
	Key     string
	Labels  map[string]string
	Samples []Sample
}

// CreateOptions configures a new series.
type CreateOptions struct {
	Retention       time.Duration
	Labels          map[string]string
	DuplicatePolicy string // BLOCK, FIRST, LAST, MIN, MAX, or SUM
}

// RangeOptions configures a Range query. Aggregator and BucketDuration are
// optional and must be set together.
type RangeOptions struct {
	Aggregator     string
	BucketDuration time.Duration
	Count          int64
}

// MultiRangeOptions configures a MultiRange query. Filters is required and
// holds Redis TimeSeries label filters (e.g. "type=cpu").
type MultiRangeOptions struct {
	Filters []string
	RangeOptions
}

// Client stores and queries time-series samples backed by Redis TimeSeries.
// It is safe for concurrent use and does not own the Redis connection it
// borrows.
type Client struct {
	client *redis.Client
	prefix string
}

// New constructs a Client backed by client using cfg. New validates cfg and
// returns an error without touching Redis if the configuration is invalid.
func New(client *redisx.Client, cfg Config) (*Client, error) {
	if client == nil {
		return nil, errors.New("timeseriesx: client must not be nil")
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &Client{
		client: client.Client(),
		prefix: cfg.Prefix,
	}, nil
}

// Create creates series with opts. Create is idempotent only when an
// existing series already has a compatible configuration; Redis otherwise
// returns an error.
func (c *Client) Create(ctx context.Context, series string, opts CreateOptions) error {
	if err := validateSeries(series); err != nil {
		return err
	}

	if opts.Retention < 0 {
		return errors.New("timeseriesx: retention must not be negative")
	}

	policy := strings.ToUpper(strings.TrimSpace(opts.DuplicatePolicy))
	if policy != "" && !validDuplicatePolicies[policy] {
		return fmt.Errorf("timeseriesx: invalid duplicate policy %q", opts.DuplicatePolicy)
	}

	labels := make(map[string]string, len(opts.Labels)+1)
	for k, v := range opts.Labels {
		labels[k] = v
	}
	labels[namespaceLabel] = c.prefix

	tsOpts := &redis.TSOptions{
		Retention:       int(opts.Retention.Milliseconds()),
		Labels:          labels,
		DuplicatePolicy: policy,
	}

	if err := c.client.TSCreateWithArgs(ctx, c.key(series), tsOpts).Err(); err != nil {
		return wrapErr("create", err)
	}

	return nil
}

// Add appends a sample to series at timestamp and returns the stored
// timestamp. If timestamp is less than or equal to zero, Add uses the
// server-generated timestamp ('*').
func (c *Client) Add(ctx context.Context, series string, timestamp int64, value float64) (int64, error) {
	if err := validateSeries(series); err != nil {
		return 0, err
	}

	var tsArg interface{} = "*"
	if timestamp > 0 {
		tsArg = timestamp
	}

	id, err := c.client.TSAdd(ctx, c.key(series), tsArg, value).Result()
	if err != nil {
		return 0, wrapErr("add", err)
	}

	return id, nil
}

// Range returns series' samples between from and to (Unix milliseconds,
// inclusive), optionally aggregated. Pass -1 for from or 1 for to to use
// Redis's earliest ('-') or latest ('+') sentinels.
func (c *Client) Range(ctx context.Context, series string, from, to int64, opts RangeOptions) ([]Sample, error) {
	if err := validateSeries(series); err != nil {
		return nil, err
	}

	if err := validateRangeOptions(opts); err != nil {
		return nil, err
	}

	args := []interface{}{"TS.RANGE", c.key(series), formatTimestamp(from), formatTimestamp(to)}

	if opts.Count > 0 {
		args = append(args, "COUNT", opts.Count)
	}

	if opts.Aggregator != "" {
		agg, err := normalizeAggregator(opts.Aggregator)
		if err != nil {
			return nil, err
		}
		args = append(args, "AGGREGATION", agg, opts.BucketDuration.Milliseconds())
	}

	raw, err := c.client.Do(ctx, args...).Result()
	if err != nil {
		return nil, wrapErr("range", err)
	}

	samples, err := parseSamples(raw)
	if err != nil {
		return nil, fmt.Errorf("timeseriesx: range: %w", err)
	}

	return samples, nil
}

// MultiRange returns samples and labels for every series matching opts.
// Filters, scoped to this Client's own namespace, between from and to (Unix
// milliseconds, inclusive). Pass -1 for from or 1 for to to use Redis's
// earliest ('-') or latest ('+') sentinels.
func (c *Client) MultiRange(ctx context.Context, from, to int64, opts MultiRangeOptions) ([]SeriesSamples, error) {
	if len(opts.Filters) == 0 {
		return nil, errors.New("timeseriesx: filters must not be empty")
	}

	if err := validateRangeOptions(opts.RangeOptions); err != nil {
		return nil, err
	}

	args := []interface{}{"TS.MRANGE", formatTimestamp(from), formatTimestamp(to), "WITHLABELS"}

	if opts.Count > 0 {
		args = append(args, "COUNT", opts.Count)
	}

	if opts.Aggregator != "" {
		agg, err := normalizeAggregator(opts.Aggregator)
		if err != nil {
			return nil, err
		}
		args = append(args, "AGGREGATION", agg, opts.BucketDuration.Milliseconds())
	}

	args = append(args, "FILTER", namespaceLabel+"="+c.prefix)
	for _, f := range opts.Filters {
		args = append(args, f)
	}

	raw, err := c.client.Do(ctx, args...).Result()
	if err != nil {
		return nil, wrapErr("multi range", err)
	}

	entries, err := c.parseMultiRange(raw)
	if err != nil {
		return nil, fmt.Errorf("timeseriesx: multi range: %w", err)
	}

	return entries, nil
}

// CreateRule installs a compaction rule that downsamples sourceSeries into
// destSeries using aggregator over bucketDuration buckets. destSeries must
// already exist.
func (c *Client) CreateRule(ctx context.Context, sourceSeries, destSeries string, aggregator string, bucketDuration time.Duration) error {
	if err := validateSeries(sourceSeries); err != nil {
		return err
	}

	if err := validateSeries(destSeries); err != nil {
		return err
	}

	if bucketDuration <= 0 {
		return errors.New("timeseriesx: bucket duration must be greater than zero")
	}

	agg, err := normalizeAggregator(aggregator)
	if err != nil {
		return err
	}

	err = c.client.Do(ctx, "TS.CREATERULE", c.key(sourceSeries), c.key(destSeries),
		"AGGREGATION", agg, bucketDuration.Milliseconds()).Err()
	if err != nil {
		return wrapErr("create rule", err)
	}

	return nil
}

// key returns the Redis key for series.
func (c *Client) key(series string) string {
	return c.prefix + ":" + series
}

// stripKey removes this Client's prefix from key, returning the original
// series name.
func (c *Client) stripKey(key string) string {
	return strings.TrimPrefix(key, c.prefix+":")
}

// parseMultiRange converts a raw TS.MRANGE reply into SeriesSamples, sorted
// by key for deterministic output.
func (c *Client) parseMultiRange(raw interface{}) ([]SeriesSamples, error) {
	var entries []SeriesSamples

	switch root := raw.(type) {
	case map[interface{}]interface{}:
		for k, v := range root {
			entry, err := c.parseSeriesEntry(toString(k), v)
			if err != nil {
				return nil, err
			}
			entries = append(entries, entry)
		}
	case []interface{}:
		for _, item := range root {
			pair, ok := item.([]interface{})
			if !ok || len(pair) < 2 {
				return nil, fmt.Errorf("unexpected multi range entry %#v", item)
			}
			entry, err := c.parseSeriesEntry(toString(pair[0]), pair[1:])
			if err != nil {
				return nil, err
			}
			entries = append(entries, entry)
		}
	default:
		return nil, fmt.Errorf("unexpected multi range reply type %T", raw)
	}

	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })

	return entries, nil
}

// parseSeriesEntry converts one TS.MRANGE series entry into a SeriesSamples.
func (c *Client) parseSeriesEntry(key string, value interface{}) (SeriesSamples, error) {
	items, ok := value.([]interface{})
	if !ok || len(items) == 0 {
		return SeriesSamples{}, fmt.Errorf("unexpected series entry for %s: %#v", key, value)
	}

	labels := parseLabels(items[0])
	delete(labels, namespaceLabel)

	samples, err := parseSamples(items[len(items)-1])
	if err != nil {
		return SeriesSamples{}, fmt.Errorf("series %s: %w", key, err)
	}

	return SeriesSamples{
		Key:     c.stripKey(key),
		Labels:  labels,
		Samples: samples,
	}, nil
}

// validateSeries reports whether series is non-empty.
func validateSeries(series string) error {
	if strings.TrimSpace(series) == "" {
		return errors.New("timeseriesx: series must not be empty")
	}
	return nil
}

// validateRangeOptions reports whether opts holds a usable configuration.
func validateRangeOptions(opts RangeOptions) error {
	if opts.Count < 0 {
		return errors.New("timeseriesx: count must not be negative")
	}

	if opts.Aggregator != "" && opts.BucketDuration <= 0 {
		return errors.New("timeseriesx: bucket duration must be greater than zero when aggregator is set")
	}

	return nil
}

// normalizeAggregator validates and uppercases aggregator.
func normalizeAggregator(aggregator string) (string, error) {
	agg := strings.ToUpper(strings.TrimSpace(aggregator))
	if !validAggregators[agg] {
		return "", fmt.Errorf("timeseriesx: invalid aggregator %q", aggregator)
	}
	return agg, nil
}

// formatTimestamp returns the Redis TS.RANGE/TS.MRANGE argument for ts: the
// '-' sentinel for -1, the '+' sentinel for 1, or ts itself otherwise.
func formatTimestamp(ts int64) interface{} {
	switch ts {
	case -1:
		return "-"
	case 1:
		return "+"
	default:
		return ts
	}
}

// parseSamples converts a raw TS.RANGE-shaped reply into Samples.
func parseSamples(raw interface{}) ([]Sample, error) {
	arr, ok := raw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected range reply type %T", raw)
	}

	samples := make([]Sample, 0, len(arr))
	for _, item := range arr {
		pair, ok := item.([]interface{})
		if !ok || len(pair) < 2 {
			return nil, fmt.Errorf("unexpected sample reply %#v", item)
		}

		ts, err := toInt64(pair[0])
		if err != nil {
			return nil, err
		}

		val, err := toFloat64(pair[1])
		if err != nil {
			return nil, err
		}

		samples = append(samples, Sample{Timestamp: ts, Value: val})
	}

	return samples, nil
}

// parseLabels converts a raw TS.MRANGE WITHLABELS label reply (a RESP3 map or
// a RESP2 array of [name, value] pairs) into a label map.
func parseLabels(raw interface{}) map[string]string {
	switch v := raw.(type) {
	case map[interface{}]interface{}:
		m := make(map[string]string, len(v))
		for k, val := range v {
			m[toString(k)] = toString(val)
		}
		return m
	case []interface{}:
		m := make(map[string]string, len(v))
		for _, pair := range v {
			p, ok := pair.([]interface{})
			if ok && len(p) == 2 {
				m[toString(p[0])] = toString(p[1])
			}
		}
		return m
	default:
		return map[string]string{}
	}
}

// toString converts a generic Redis reply scalar to a string.
func toString(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	default:
		return fmt.Sprint(t)
	}
}

// toInt64 converts a generic Redis reply scalar to an int64.
func toInt64(v interface{}) (int64, error) {
	switch t := v.(type) {
	case int64:
		return t, nil
	case int:
		return int64(t), nil
	case string:
		n, err := strconv.ParseInt(t, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse timestamp %q: %w", t, err)
		}
		return n, nil
	default:
		return 0, fmt.Errorf("unexpected timestamp type %T", v)
	}
}

// toFloat64 converts a generic Redis reply scalar to a float64.
func toFloat64(v interface{}) (float64, error) {
	switch t := v.(type) {
	case float64:
		return t, nil
	case string:
		f, err := strconv.ParseFloat(t, 64)
		if err != nil {
			return 0, fmt.Errorf("parse value %q: %w", t, err)
		}
		return f, nil
	default:
		return 0, fmt.Errorf("unexpected value type %T", v)
	}
}

// wrapErr wraps err with the timeseriesx prefix and op, mapping unknown
// command errors to ErrCapability.
func wrapErr(op string, err error) error {
	if err == nil {
		return nil
	}

	if strings.Contains(strings.ToLower(err.Error()), "unknown command") {
		return fmt.Errorf("timeseriesx: %s: %w", op, ErrCapability)
	}

	return fmt.Errorf("timeseriesx: %s: %w", op, err)
}
