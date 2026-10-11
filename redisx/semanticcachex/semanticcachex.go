// Package semanticcachex caches LLM responses keyed by prompt embedding
// similarity, using Redis Search vector KNN queries (FT.CREATE and
// FT.SEARCH) to find a cached response for a semantically similar prompt.
// It follows the official Redis semantic cache example.
package semanticcachex

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/leetun2k2/go-bedrock/redisx"
	"github.com/redis/go-redis/v9"
)

// ErrNotFound is returned by Lookup when no cache entry is within the
// configured distance threshold.
var ErrNotFound = errors.New("semanticcachex: cache entry not found")

// ErrCapability is returned when the connected Redis server does not support
// Redis Search vector commands.
var ErrCapability = errors.New("semanticcachex: redis search module required")

// clearBatchSize bounds how many keys ClearNamespace deletes per FT.SEARCH
// page, keeping each round trip bounded in size.
const clearBatchSize = 1000

// tagSpecialChars matches characters that must be escaped inside a RediSearch
// TAG query value.
var tagSpecialChars = regexp.MustCompile(`([,.<>{}\[\]"':;!@#$%^&*()\-+=~|\s])`)

// Entry is one semantic cache entry: a prompt, its embedding, the cached
// response, and the metadata scope it was written for. Distance is set by
// Lookup to the vector distance of the returned entry; it is ignored by Set.
type Entry struct {
	ID        string
	Prompt    string
	Embedding []float32
	Response  []byte
	Tenant    string
	Locale    string
	Model     string
	Distance  float64
}

// LookupRequest is a semantic cache lookup, scoped by the metadata fields
// that were non-empty when the entry was written.
type LookupRequest struct {
	Embedding []float32
	Tenant    string
	Locale    string
	Model     string
}

// Cache looks up and stores LLM responses by prompt embedding similarity,
// backed by Redis Search. It is safe for concurrent use and does not own the
// Redis connection it borrows.
type Cache struct {
	client     *redis.Client
	prefix     string
	indexName  string
	dimensions int
	metric     string
	threshold  float64
	ttl        time.Duration
}

// New constructs a Cache backed by client using cfg. New validates cfg and
// returns an error without touching Redis if the configuration is invalid.
func New(client *redisx.Client, cfg Config) (*Cache, error) {
	if client == nil {
		return nil, errors.New("semanticcachex: client must not be nil")
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg = cfg.withDefaults()

	return &Cache{
		client:     client.Client(),
		prefix:     cfg.Prefix,
		indexName:  cfg.IndexName,
		dimensions: cfg.Dimensions,
		metric:     cfg.DistanceMetric,
		threshold:  cfg.Threshold,
		ttl:        cfg.TTL,
	}, nil
}

// CreateIndex creates the Redis Search index used by Lookup. CreateIndex is
// idempotent: it returns nil if an index with this name already exists.
func (c *Cache) CreateIndex(ctx context.Context) error {
	args := []interface{}{
		"FT.CREATE", c.indexName,
		"ON", "HASH",
		"PREFIX", "1", c.prefix + ":",
		"SCHEMA",
		"prompt", "TEXT",
		"response", "TEXT",
		"tenant", "TAG",
		"locale", "TAG",
		"model", "TAG",
		"vector", "VECTOR", "HNSW", "6",
		"TYPE", "FLOAT32",
		"DIM", c.dimensions,
		"DISTANCE_METRIC", c.metric,
	}

	if err := c.client.Do(ctx, args...).Err(); err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "already exists") {
			return nil
		}
		return wrapErr("create index", err)
	}

	return nil
}

// Set writes entry's prompt, embedding, response, and metadata to its hash
// and applies an expiry. Set uses ttl if positive, otherwise the Cache's
// configured default TTL. Set generates entry.ID if it is empty.
func (c *Cache) Set(ctx context.Context, entry Entry, ttl time.Duration) error {
	if len(entry.Embedding) != c.dimensions {
		return fmt.Errorf("semanticcachex: embedding must have %d dimensions, got %d", c.dimensions, len(entry.Embedding))
	}

	if strings.TrimSpace(entry.Tenant) == "" {
		return errors.New("semanticcachex: tenant must not be empty")
	}

	id := strings.TrimSpace(entry.ID)
	if id == "" {
		generated, err := generateID()
		if err != nil {
			return fmt.Errorf("semanticcachex: generate id: %w", err)
		}
		id = generated
	}

	expiry := ttl
	if expiry <= 0 {
		expiry = c.ttl
	}

	fields := map[string]interface{}{
		"prompt":   entry.Prompt,
		"response": base64.StdEncoding.EncodeToString(entry.Response),
		"tenant":   entry.Tenant,
		"locale":   entry.Locale,
		"model":    entry.Model,
		"vector":   encodeVector(entry.Embedding),
	}

	key := c.key(id)

	pipe := c.client.TxPipeline()
	pipe.HSet(ctx, key, fields)
	pipe.Expire(ctx, key, expiry)

	if _, err := pipe.Exec(ctx); err != nil {
		return wrapErr("set", err)
	}

	return nil
}

// Delete removes id's cache entry hash key.
func (c *Cache) Delete(ctx context.Context, id string) error {
	if strings.TrimSpace(id) == "" {
		return errors.New("semanticcachex: id must not be empty")
	}

	if err := c.client.Del(ctx, c.key(id)).Err(); err != nil {
		return wrapErr("delete", err)
	}

	return nil
}

// Lookup returns the nearest cache entry to req.Embedding within req's
// metadata scope. Lookup returns ErrNotFound if no entry matches or the
// nearest entry's distance exceeds the configured threshold.
func (c *Cache) Lookup(ctx context.Context, req LookupRequest) (Entry, error) {
	if len(req.Embedding) != c.dimensions {
		return Entry{}, fmt.Errorf("semanticcachex: embedding must have %d dimensions, got %d", c.dimensions, len(req.Embedding))
	}

	if strings.TrimSpace(req.Tenant) == "" {
		return Entry{}, errors.New("semanticcachex: tenant must not be empty")
	}

	filter := c.buildScopeFilter(req.Tenant, req.Locale, req.Model)
	queryStr := fmt.Sprintf("(%s)=>[KNN 1 @vector $vec AS score]", filter)

	args := []interface{}{
		"FT.SEARCH", c.indexName, queryStr,
		"PARAMS", "2", "vec", encodeVector(req.Embedding),
		"SORTBY", "score",
		"LIMIT", "0", "1",
		"DIALECT", "2",
	}

	raw, err := c.client.Do(ctx, args...).Result()
	if err != nil {
		return Entry{}, wrapErr("lookup", err)
	}

	entry, found, err := c.parseSearchReply(raw)
	if err != nil {
		return Entry{}, fmt.Errorf("semanticcachex: lookup: %w", err)
	}

	if !found || entry.Distance > c.threshold {
		return Entry{}, ErrNotFound
	}

	return entry, nil
}

// ClearNamespace deletes every cache entry scoped to tenant.
func (c *Cache) ClearNamespace(ctx context.Context, tenant string) error {
	if strings.TrimSpace(tenant) == "" {
		return errors.New("semanticcachex: tenant must not be empty")
	}

	queryStr := fmt.Sprintf("@tenant:{%s}", escapeTagValue(tenant))

	for {
		args := []interface{}{
			"FT.SEARCH", c.indexName, queryStr,
			"NOCONTENT",
			"LIMIT", "0", clearBatchSize,
			"DIALECT", "2",
		}

		raw, err := c.client.Do(ctx, args...).Result()
		if err != nil {
			return wrapErr("clear namespace", err)
		}

		keys, err := c.parseKeys(raw)
		if err != nil {
			return fmt.Errorf("semanticcachex: clear namespace: %w", err)
		}

		if len(keys) == 0 {
			return nil
		}

		if err := c.client.Del(ctx, keys...).Err(); err != nil {
			return wrapErr("clear namespace", err)
		}

		if len(keys) < clearBatchSize {
			return nil
		}
	}
}

// buildScopeFilter builds the RediSearch pre-filter query string restricting
// a search to the given tenant, locale, and model, returning "*" when none
// are set.
func (c *Cache) buildScopeFilter(tenant, locale, model string) string {
	var parts []string

	if tenant != "" {
		parts = append(parts, fmt.Sprintf("@tenant:{%s}", escapeTagValue(tenant)))
	}
	if locale != "" {
		parts = append(parts, fmt.Sprintf("@locale:{%s}", escapeTagValue(locale)))
	}
	if model != "" {
		parts = append(parts, fmt.Sprintf("@model:{%s}", escapeTagValue(model)))
	}

	if len(parts) == 0 {
		return "*"
	}

	return strings.Join(parts, " ")
}

// parseSearchReply converts a raw FT.SEARCH reply into at most one Entry.
// Redis replies with a RESP3 map (a "results" entry per document, holding
// "id" and an "extra_attributes" field map) or, under RESP2, a flat array of
// [total, key, fields, key, fields, ...].
func (c *Cache) parseSearchReply(raw interface{}) (Entry, bool, error) {
	switch root := raw.(type) {
	case map[interface{}]interface{}:
		return c.parseSearchReplyMap(root)
	case []interface{}:
		return c.parseSearchReplyArray(root)
	default:
		return Entry{}, false, fmt.Errorf("unexpected search reply type %T", raw)
	}
}

// parseSearchReplyMap parses the RESP3 map-shaped FT.SEARCH reply.
func (c *Cache) parseSearchReplyMap(root map[interface{}]interface{}) (Entry, bool, error) {
	docs, ok := root["results"].([]interface{})
	if !ok || len(docs) == 0 {
		return Entry{}, false, nil
	}

	doc, ok := docs[0].(map[interface{}]interface{})
	if !ok {
		return Entry{}, false, fmt.Errorf("unexpected result document type %T", docs[0])
	}

	fields, _ := doc["extra_attributes"].(map[interface{}]interface{})

	entry, err := c.parseEntry(toString(doc["id"]), fields)
	if err != nil {
		return Entry{}, false, err
	}

	return entry, true, nil
}

// parseSearchReplyArray parses the RESP2 flat-array FT.SEARCH reply.
func (c *Cache) parseSearchReplyArray(arr []interface{}) (Entry, bool, error) {
	if len(arr) < 3 {
		return Entry{}, false, nil
	}

	rawFields, ok := arr[2].([]interface{})
	if !ok {
		return Entry{}, false, fmt.Errorf("unexpected fields reply type %T", arr[2])
	}

	fields := make(map[interface{}]interface{}, len(rawFields)/2)
	for i := 0; i+1 < len(rawFields); i += 2 {
		fields[rawFields[i]] = rawFields[i+1]
	}

	entry, err := c.parseEntry(toString(arr[1]), fields)
	if err != nil {
		return Entry{}, false, err
	}

	return entry, true, nil
}

// parseEntry converts one FT.SEARCH document (key and field map) into an
// Entry.
func (c *Cache) parseEntry(key string, fields map[interface{}]interface{}) (Entry, error) {
	entry := Entry{ID: c.stripKey(key)}

	for rawName, value := range fields {
		switch toString(rawName) {
		case "score":
			distance, err := toFloat64(value)
			if err != nil {
				return Entry{}, fmt.Errorf("parse score: %w", err)
			}
			entry.Distance = distance
		case "prompt":
			entry.Prompt = toString(value)
		case "response":
			response, err := base64.StdEncoding.DecodeString(toString(value))
			if err != nil {
				return Entry{}, fmt.Errorf("parse response: %w", err)
			}
			entry.Response = response
		case "tenant":
			entry.Tenant = toString(value)
		case "locale":
			entry.Locale = toString(value)
		case "model":
			entry.Model = toString(value)
		}
	}

	return entry, nil
}

// parseKeys converts a NOCONTENT FT.SEARCH reply into the list of matched
// Redis keys.
func (c *Cache) parseKeys(raw interface{}) ([]string, error) {
	switch root := raw.(type) {
	case map[interface{}]interface{}:
		docs, ok := root["results"].([]interface{})
		if !ok {
			return nil, nil
		}
		keys := make([]string, 0, len(docs))
		for _, d := range docs {
			doc, ok := d.(map[interface{}]interface{})
			if !ok {
				return nil, fmt.Errorf("unexpected result document type %T", d)
			}
			keys = append(keys, toString(doc["id"]))
		}
		return keys, nil
	case []interface{}:
		if len(root) == 0 {
			return nil, nil
		}
		keys := make([]string, 0, len(root)-1)
		for _, k := range root[1:] {
			keys = append(keys, toString(k))
		}
		return keys, nil
	default:
		return nil, fmt.Errorf("unexpected search reply type %T", raw)
	}
}

// key returns the Redis hash key for a cache entry id.
func (c *Cache) key(id string) string {
	return c.prefix + ":" + id
}

// stripKey removes this Cache's prefix from key, returning the original
// entry id.
func (c *Cache) stripKey(key string) string {
	return strings.TrimPrefix(key, c.prefix+":")
}

// generateID returns a random hex-encoded identifier for a cache entry.
func generateID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
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

// wrapErr wraps err with the semanticcachex prefix and op, mapping unknown
// command errors to ErrCapability.
func wrapErr(op string, err error) error {
	if err == nil {
		return nil
	}

	if strings.Contains(strings.ToLower(err.Error()), "unknown command") {
		return fmt.Errorf("semanticcachex: %s: %w", op, ErrCapability)
	}

	return fmt.Errorf("semanticcachex: %s: %w", op, err)
}
