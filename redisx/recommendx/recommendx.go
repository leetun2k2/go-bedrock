// Package recommendx retrieves recommendation candidates by matching query
// vectors against stored item vectors with
// structured tag and numeric filters, using Redis Search vector KNN queries
// (FT.CREATE and FT.SEARCH).
package recommendx

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/leetun2k2/go-bedrock/redisx"
	"github.com/redis/go-redis/v9"
)

// ErrCapability is returned when the connected Redis server does not support
// Redis Search vector commands.
var ErrCapability = errors.New("recommendx: redis search module required")

// tagSpecialChars matches characters that must be escaped inside a RediSearch
// TAG query value.
var tagSpecialChars = regexp.MustCompile(`([,.<>{}\[\]"':;!@#$%^&*()\-+=~|\s])`)

// Item is a vector and its structured fields, stored as a Redis hash.
type Item struct {
	ID            string
	Vector        []float32
	TagFields     map[string]string
	NumericFields map[string]float64
}

// Query is a vector search request with optional structured filters.
type Query struct {
	Vector         []float32
	Limit          int
	TagFilters     map[string]string     // field name -> exact value
	NumericFilters map[string][2]float64 // field name -> [min, max]
}

// Recommendation is one ranked item returned by Recommend.
type Recommendation struct {
	ID            string
	Score         float64 // distance/score returned by Redis Search
	TagFields     map[string]string
	NumericFields map[string]float64
}

// Recommender retrieves ranked items backed by Redis Search. It is
// safe for concurrent use and does not own the Redis connection it borrows.
type Recommender struct {
	client        *redis.Client
	prefix        string
	indexName     string
	dimensions    int
	metric        string
	tagFields     []string
	numericFields []string
}

// New constructs a Recommender backed by client using cfg. New validates cfg and
// returns an error without touching Redis if the configuration is invalid.
func New(client *redisx.Client, cfg Config) (*Recommender, error) {
	if client == nil {
		return nil, errors.New("recommendx: client must not be nil")
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg = cfg.withDefaults()

	return &Recommender{
		client:        client.Client(),
		prefix:        cfg.Prefix,
		indexName:     cfg.IndexName,
		dimensions:    cfg.Dimensions,
		metric:        cfg.DistanceMetric,
		tagFields:     append([]string(nil), cfg.TagFields...),
		numericFields: append([]string(nil), cfg.NumericFields...),
	}, nil
}

// CreateIndex creates the Redis Search index used by Match. It returns nil when
// an index with the same name already exists, without checking its schema.
func (r *Recommender) CreateIndex(ctx context.Context) error {
	args := []interface{}{
		"FT.CREATE", r.indexName,
		"ON", "HASH",
		"PREFIX", "1", r.prefix + ":",
		"SCHEMA",
		"vector", "VECTOR", "HNSW", "6",
		"TYPE", "FLOAT32",
		"DIM", r.dimensions,
		"DISTANCE_METRIC", r.metric,
	}

	for _, f := range r.tagFields {
		args = append(args, f, "TAG")
	}
	for _, f := range r.numericFields {
		args = append(args, f, "NUMERIC")
	}

	if err := r.client.Do(ctx, args...).Err(); err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "already exists") {
			return nil
		}
		return wrapErr("create index", err)
	}

	return nil
}

// UpsertItem writes item's vector and structured fields to its hash,
// creating or replacing the fields the index reads.
func (r *Recommender) UpsertItem(ctx context.Context, item Item) error {
	if strings.TrimSpace(item.ID) == "" {
		return errors.New("recommendx: item id must not be empty")
	}

	if len(item.Vector) != r.dimensions {
		return fmt.Errorf("recommendx: vector must have %d dimensions, got %d", r.dimensions, len(item.Vector))
	}

	fields := make(map[string]interface{}, 1+len(item.TagFields)+len(item.NumericFields))
	fields["vector"] = encodeVector(item.Vector)

	for k, v := range item.TagFields {
		if !contains(r.tagFields, k) {
			return fmt.Errorf("recommendx: unknown tag field %q", k)
		}
		fields[k] = v
	}

	for k, v := range item.NumericFields {
		if !contains(r.numericFields, k) {
			return fmt.Errorf("recommendx: unknown numeric field %q", k)
		}
		fields[k] = strconv.FormatFloat(v, 'f', -1, 64)
	}

	if err := r.client.HSet(ctx, r.key(item.ID), fields).Err(); err != nil {
		return wrapErr("upsert item", err)
	}

	return nil
}

// DeleteItem removes id's item hash, taking it out of future matches.
func (r *Recommender) DeleteItem(ctx context.Context, id string) error {
	if strings.TrimSpace(id) == "" {
		return errors.New("recommendx: item id must not be empty")
	}

	if err := r.client.Del(ctx, r.key(id)).Err(); err != nil {
		return wrapErr("delete item", err)
	}

	return nil
}

// Recommend returns at most query.Limit items nearest to query.Vector, ordered by
// score, restricted to items matching query's tag and numeric filters.
func (r *Recommender) Recommend(ctx context.Context, query Query) ([]Recommendation, error) {
	if len(query.Vector) != r.dimensions {
		return nil, fmt.Errorf("recommendx: vector must have %d dimensions, got %d", r.dimensions, len(query.Vector))
	}

	if query.Limit <= 0 {
		return nil, errors.New("recommendx: limit must be greater than zero")
	}

	filter, err := r.buildFilter(query)
	if err != nil {
		return nil, err
	}

	queryStr := fmt.Sprintf("(%s)=>[KNN %d @vector $vec AS score]", filter, query.Limit)

	args := []interface{}{
		"FT.SEARCH", r.indexName, queryStr,
		"PARAMS", "2", "vec", encodeVector(query.Vector),
		"SORTBY", "score",
		"LIMIT", "0", query.Limit,
		"DIALECT", "2",
	}

	raw, err := r.client.Do(ctx, args...).Result()
	if err != nil {
		return nil, wrapErr("match", err)
	}

	results, err := r.parseSearchReply(raw)
	if err != nil {
		return nil, fmt.Errorf("recommendx: recommend: %w", err)
	}

	sort.Slice(results, func(i, j int) bool { return results[i].Score < results[j].Score })

	return results, nil
}

// buildFilter builds the RediSearch pre-filter query string for query,
// returning "*" when no filters are set.
func (r *Recommender) buildFilter(query Query) (string, error) {
	var parts []string

	for field, val := range query.TagFilters {
		if !contains(r.tagFields, field) {
			return "", fmt.Errorf("recommendx: unknown tag field %q", field)
		}
		parts = append(parts, fmt.Sprintf("@%s:{%s}", field, escapeTagValue(val)))
	}

	for field, rng := range query.NumericFilters {
		if !contains(r.numericFields, field) {
			return "", fmt.Errorf("recommendx: unknown numeric field %q", field)
		}
		parts = append(parts, fmt.Sprintf("@%s:[%s %s]", field, formatFloat(rng[0]), formatFloat(rng[1])))
	}

	if len(parts) == 0 {
		return "*", nil
	}

	sort.Strings(parts)

	return strings.Join(parts, " "), nil
}

// parseSearchReply converts a raw FT.SEARCH reply into Recommendations. Redis
// replies with a RESP3 map (one "results" entry per document, each holding
// "id" and an "extra_attributes" field map) or, under RESP2, a flat array of
// [total, key, fields, key, fields, ...].
func (r *Recommender) parseSearchReply(raw interface{}) ([]Recommendation, error) {
	switch root := raw.(type) {
	case map[interface{}]interface{}:
		return r.parseSearchReplyMap(root)
	case []interface{}:
		return r.parseSearchReplyArray(root)
	default:
		return nil, fmt.Errorf("unexpected search reply type %T", raw)
	}
}

// parseSearchReplyMap parses the RESP3 map-shaped FT.SEARCH reply.
func (r *Recommender) parseSearchReplyMap(root map[interface{}]interface{}) ([]Recommendation, error) {
	docs, ok := root["results"].([]interface{})
	if !ok {
		return nil, nil
	}

	results := make([]Recommendation, 0, len(docs))

	for _, d := range docs {
		doc, ok := d.(map[interface{}]interface{})
		if !ok {
			return nil, fmt.Errorf("unexpected result document type %T", d)
		}

		fields, _ := doc["extra_attributes"].(map[interface{}]interface{})

		result, err := r.parseResult(toString(doc["id"]), fields)
		if err != nil {
			return nil, err
		}

		results = append(results, result)
	}

	return results, nil
}

// parseSearchReplyArray parses the RESP2 flat-array FT.SEARCH reply.
func (r *Recommender) parseSearchReplyArray(arr []interface{}) ([]Recommendation, error) {
	if len(arr) == 0 {
		return nil, nil
	}

	results := make([]Recommendation, 0, (len(arr)-1)/2)

	for i := 1; i+1 < len(arr); i += 2 {
		rawFields, ok := arr[i+1].([]interface{})
		if !ok {
			return nil, fmt.Errorf("unexpected fields reply type %T", arr[i+1])
		}

		fields := make(map[interface{}]interface{}, len(rawFields)/2)
		for j := 0; j+1 < len(rawFields); j += 2 {
			fields[rawFields[j]] = rawFields[j+1]
		}

		result, err := r.parseResult(toString(arr[i]), fields)
		if err != nil {
			return nil, err
		}

		results = append(results, result)
	}

	return results, nil
}

// parseResult converts one FT.SEARCH document (key and field map) into a
// Recommendation.
func (r *Recommender) parseResult(key string, fields map[interface{}]interface{}) (Recommendation, error) {
	result := Recommendation{
		ID:            r.stripKey(key),
		TagFields:     map[string]string{},
		NumericFields: map[string]float64{},
	}

	for rawName, value := range fields {
		name := toString(rawName)

		switch {
		case name == "score":
			score, err := toFloat64(value)
			if err != nil {
				return Recommendation{}, fmt.Errorf("parse score: %w", err)
			}
			result.Score = score
		case name == "vector":
			// Skip the raw vector blob; callers already have the vector.
		case contains(r.tagFields, name):
			result.TagFields[name] = toString(value)
		case contains(r.numericFields, name):
			f, err := toFloat64(value)
			if err != nil {
				return Recommendation{}, fmt.Errorf("parse field %q: %w", name, err)
			}
			result.NumericFields[name] = f
		}
	}

	return result, nil
}

// key returns the Redis hash key for item id.
func (r *Recommender) key(id string) string {
	return r.prefix + ":" + id
}

// stripKey removes this Recommender's prefix from key, returning the original
// item id.
func (r *Recommender) stripKey(key string) string {
	return strings.TrimPrefix(key, r.prefix+":")
}

// encodeVector encodes vec as IEEE 754 float32 little-endian bytes, the
// binary format Redis Search expects for vector fields.
func encodeVector(vec []float32) []byte {
	buf := make([]byte, 4*len(vec))
	for i, v := range vec {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(v))
	}
	return buf
}

// escapeTagValue escapes RediSearch TAG query special characters in v.
func escapeTagValue(v string) string {
	return tagSpecialChars.ReplaceAllString(v, `\$1`)
}

// formatFloat formats f for use in a RediSearch NUMERIC range query.
func formatFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// contains reports whether list contains s.
func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
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

// wrapErr wraps err with the recommendx prefix and op, mapping unknown command
// errors to ErrCapability.
func wrapErr(op string, err error) error {
	if err == nil {
		return nil
	}

	if strings.Contains(strings.ToLower(err.Error()), "unknown command") {
		return fmt.Errorf("recommendx: %s: %w", op, ErrCapability)
	}

	return fmt.Errorf("recommendx: %s: %w", op, err)
}
