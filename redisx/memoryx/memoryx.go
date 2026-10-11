// Package memoryx persists agent working state, semantic memories, and a
// bounded event history. It keeps three memory tiers explicit instead of
// hiding them behind one generic store: a per-thread working-memory hash
// with TTL, a JSON + Redis Search vector index for semantic recall, and a
// length-bounded Redis Stream for event history. It follows the official
// Redis memory layer example.
package memoryx

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/leetun2k2/go-bedrock/redisx"
	"github.com/redis/go-redis/v9"
)

// ErrNotFound is returned when a requested thread state does not exist.
var ErrNotFound = errors.New("memoryx: not found")

// ErrCapability is returned when the connected Redis server does not support
// the Redis Search and JSON commands semantic memory depends on.
var ErrCapability = errors.New("memoryx: redis search and json modules required")

// tagSpecialChars matches characters that must be escaped inside a RediSearch
// TAG query value.
var tagSpecialChars = regexp.MustCompile(`([,.<>{}\[\]"':;!@#$%^&*()\-+=~|\s])`)

// MemoryRecord is one semantic memory: an embedding, the text it was derived
// from, and the user/namespace/kind scope it belongs to. Distance is set by
// Recall to the vector distance of the returned record; it is ignored by
// Remember.
type MemoryRecord struct {
	ID        string
	User      string
	Namespace string
	Kind      string
	Text      string
	Vector    []float32
	Metadata  map[string]string
	Distance  float64
}

// RecallQuery is a semantic memory search, scoped by user and namespace and
// optionally narrowed by kind.
type RecallQuery struct {
	User      string
	Namespace string
	Kind      string // optional
	Vector    []float32
	Limit     int
}

// Event is one event-history entry appended to a thread's stream.
type Event struct {
	ID      string
	Type    string
	Payload map[string]interface{}
}

// jsonRecord is the JSON document shape stored at a semantic memory key.
type jsonRecord struct {
	User      string            `json:"user"`
	Namespace string            `json:"namespace"`
	Kind      string            `json:"kind"`
	Text      string            `json:"text"`
	Vector    []float32         `json:"vector"`
	Metadata  map[string]string `json:"metadata"`
}

// Memory persists agent working state, semantic memories, and event history
// backed by Redis. It is safe for concurrent use and does not own the Redis
// connection it borrows.
type Memory struct {
	client         *redis.Client
	prefix         string
	indexName      string
	dimensions     int
	metric         string
	dedupThreshold float64
	workingTTL     time.Duration
	eventRetention int64

	forget *redis.Script
}

// New constructs a Memory backed by client using cfg. New validates cfg and
// returns an error without touching Redis if the configuration is invalid.
func New(client *redisx.Client, cfg Config) (*Memory, error) {
	if client == nil {
		return nil, errors.New("memoryx: client must not be nil")
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg = cfg.withDefaults()

	return &Memory{
		client:         client.Client(),
		prefix:         cfg.Prefix,
		indexName:      cfg.IndexName,
		dimensions:     cfg.Dimensions,
		metric:         cfg.DistanceMetric,
		dedupThreshold: cfg.DedupThreshold,
		workingTTL:     cfg.WorkingMemoryTTL,
		eventRetention: cfg.EventRetention,
		forget:         redis.NewScript(forgetScript),
	}, nil
}

// SaveState replaces threadID's working-memory hash with state and refreshes
// its TTL.
func (m *Memory) SaveState(ctx context.Context, threadID string, state map[string]string) error {
	if strings.TrimSpace(threadID) == "" {
		return errors.New("memoryx: thread id must not be empty")
	}
	if len(state) == 0 {
		return errors.New("memoryx: state must not be empty")
	}

	fields := make(map[string]interface{}, len(state))
	for k, v := range state {
		fields[k] = v
	}

	key := m.stateKey(threadID)

	pipe := m.client.TxPipeline()
	pipe.HSet(ctx, key, fields)
	pipe.Expire(ctx, key, m.workingTTL)

	if _, err := pipe.Exec(ctx); err != nil {
		return wrapErr("save state", err)
	}

	return nil
}

// LoadState returns threadID's working-memory hash. LoadState returns
// ErrNotFound if the thread has no saved state or it has expired.
func (m *Memory) LoadState(ctx context.Context, threadID string) (map[string]string, error) {
	if strings.TrimSpace(threadID) == "" {
		return nil, errors.New("memoryx: thread id must not be empty")
	}

	state, err := m.client.HGetAll(ctx, m.stateKey(threadID)).Result()
	if err != nil {
		return nil, wrapErr("load state", err)
	}

	if len(state) == 0 {
		return nil, ErrNotFound
	}

	return state, nil
}

// DeleteState removes threadID's working-memory hash.
func (m *Memory) DeleteState(ctx context.Context, threadID string) error {
	if strings.TrimSpace(threadID) == "" {
		return errors.New("memoryx: thread id must not be empty")
	}

	if err := m.client.Del(ctx, m.stateKey(threadID)).Err(); err != nil {
		return wrapErr("delete state", err)
	}

	return nil
}

// CreateIndex creates the Redis Search JSON index used by Recall. CreateIndex
// is idempotent: it returns nil if an index with this name already exists.
func (m *Memory) CreateIndex(ctx context.Context) error {
	args := []interface{}{
		"FT.CREATE", m.indexName,
		"ON", "JSON",
		"PREFIX", "1", m.memPrefix(),
		"SCHEMA",
		"$.user", "AS", "user", "TAG",
		"$.namespace", "AS", "namespace", "TAG",
		"$.kind", "AS", "kind", "TAG",
		"$.text", "AS", "text", "TEXT",
		"$.vector", "AS", "vector", "VECTOR", "HNSW", "6",
		"TYPE", "FLOAT32",
		"DIM", m.dimensions,
		"DISTANCE_METRIC", m.metric,
	}

	if err := m.client.Do(ctx, args...).Err(); err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "already exists") {
			return nil
		}
		return wrapErr("create index", err)
	}

	return nil
}

// Remember stores mem as a semantic memory and returns its id. Before
// insertion, Remember checks for a near-duplicate: if the Memory's dedup
// threshold is positive and mem.Vector is non-empty, it recalls the nearest
// scoped memory and, if found within the threshold, skips the write and
// returns the existing id with stored set to false. This check is
// best-effort under concurrent writes.
func (m *Memory) Remember(ctx context.Context, mem MemoryRecord) (string, bool, error) {
	if strings.TrimSpace(mem.User) == "" {
		return "", false, errors.New("memoryx: user must not be empty")
	}
	if strings.TrimSpace(mem.Namespace) == "" {
		return "", false, errors.New("memoryx: namespace must not be empty")
	}
	if len(mem.Vector) != m.dimensions {
		return "", false, fmt.Errorf("memoryx: vector must have %d dimensions, got %d", m.dimensions, len(mem.Vector))
	}

	if m.dedupThreshold > 0 && len(mem.Vector) > 0 {
		nearest, err := m.Recall(ctx, RecallQuery{
			User:      mem.User,
			Namespace: mem.Namespace,
			Vector:    mem.Vector,
			Limit:     1,
		})
		if err != nil {
			return "", false, err
		}
		if len(nearest) > 0 && nearest[0].Distance < m.dedupThreshold {
			return nearest[0].ID, false, nil
		}
	}

	id, err := generateID()
	if err != nil {
		return "", false, fmt.Errorf("memoryx: generate id: %w", err)
	}

	doc := jsonRecord{
		User:      mem.User,
		Namespace: mem.Namespace,
		Kind:      mem.Kind,
		Text:      mem.Text,
		Vector:    mem.Vector,
		Metadata:  mem.Metadata,
	}

	body, err := json.Marshal(doc)
	if err != nil {
		return "", false, fmt.Errorf("memoryx: encode record: %w", err)
	}

	if err := m.client.Do(ctx, "JSON.SET", m.memKey(id), "$", string(body)).Err(); err != nil {
		return "", false, wrapErr("remember", err)
	}

	return id, true, nil
}

// Recall returns at most q.Limit memories nearest to q.Vector, ordered by
// distance, restricted to q's user, namespace, and optional kind.
func (m *Memory) Recall(ctx context.Context, q RecallQuery) ([]MemoryRecord, error) {
	if strings.TrimSpace(q.User) == "" {
		return nil, errors.New("memoryx: user must not be empty")
	}
	if strings.TrimSpace(q.Namespace) == "" {
		return nil, errors.New("memoryx: namespace must not be empty")
	}
	if len(q.Vector) != m.dimensions {
		return nil, fmt.Errorf("memoryx: vector must have %d dimensions, got %d", m.dimensions, len(q.Vector))
	}
	if q.Limit <= 0 {
		return nil, errors.New("memoryx: limit must be greater than zero")
	}

	filter := fmt.Sprintf("@user:{%s} @namespace:{%s}", escapeTagValue(q.User), escapeTagValue(q.Namespace))
	if q.Kind != "" {
		filter += fmt.Sprintf(" @kind:{%s}", escapeTagValue(q.Kind))
	}

	queryStr := fmt.Sprintf("(%s)=>[KNN %d @vector $vec AS score]", filter, q.Limit)

	args := []interface{}{
		"FT.SEARCH", m.indexName, queryStr,
		"PARAMS", "2", "vec", encodeVector(q.Vector),
		"SORTBY", "score",
		"LIMIT", "0", q.Limit,
		"DIALECT", "2",
	}

	raw, err := m.client.Do(ctx, args...).Result()
	if err != nil {
		return nil, wrapErr("recall", err)
	}

	records, err := m.parseSearchReply(raw)
	if err != nil {
		return nil, fmt.Errorf("memoryx: recall: %w", err)
	}

	sort.Slice(records, func(i, j int) bool { return records[i].Distance < records[j].Distance })

	return records, nil
}

// forgetScript deletes a semantic memory document only if its stored user
// and namespace match the caller's, so a memory ID observed or guessed
// outside its owning scope cannot be used to delete another tenant's
// memory. KEYS[1] is the memory key. ARGV[1] is the expected user, ARGV[2]
// is the expected namespace.
const forgetScript = `
local raw = redis.call('JSON.GET', KEYS[1], '$.user', '$.namespace')
if not raw then
	return 0
end
local decoded = cjson.decode(raw)
local user = decoded['$.user'][1]
local namespace = decoded['$.namespace'][1]
if user ~= ARGV[1] or namespace ~= ARGV[2] then
	return 0
end
redis.call('DEL', KEYS[1])
return 1
`

// Forget deletes id's semantic memory document, but only if it belongs to
// user and namespace. Forget does not report an error if the document does
// not exist or belongs to a different scope.
func (m *Memory) Forget(ctx context.Context, id, user, namespace string) error {
	if strings.TrimSpace(id) == "" {
		return errors.New("memoryx: id must not be empty")
	}
	if strings.TrimSpace(user) == "" {
		return errors.New("memoryx: user must not be empty")
	}
	if strings.TrimSpace(namespace) == "" {
		return errors.New("memoryx: namespace must not be empty")
	}

	keys := []string{m.memKey(id)}
	if err := m.forget.Run(ctx, m.client, keys, user, namespace).Err(); err != nil {
		return wrapErr("forget", err)
	}

	return nil
}

// AppendEvent appends one event to threadID's event stream, trimming the
// stream to the Memory's configured retention. AppendEvent returns the
// generated stream entry id.
func (m *Memory) AppendEvent(ctx context.Context, threadID string, eventType string, payload map[string]interface{}) (string, error) {
	if strings.TrimSpace(threadID) == "" {
		return "", errors.New("memoryx: thread id must not be empty")
	}
	if strings.TrimSpace(eventType) == "" {
		return "", errors.New("memoryx: event type must not be empty")
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("memoryx: encode payload: %w", err)
	}

	id, err := m.client.XAdd(ctx, &redis.XAddArgs{
		Stream: m.eventsKey(threadID),
		MaxLen: m.eventRetention,
		Approx: true,
		Values: map[string]interface{}{
			"event_type": eventType,
			"payload":    body,
		},
	}).Result()
	if err != nil {
		return "", wrapErr("append event", err)
	}

	return id, nil
}

// RecentEvents returns up to count of threadID's most recent events, in
// chronological (oldest first) order.
func (m *Memory) RecentEvents(ctx context.Context, threadID string, count int64) ([]Event, error) {
	if strings.TrimSpace(threadID) == "" {
		return nil, errors.New("memoryx: thread id must not be empty")
	}
	if count <= 0 {
		return nil, errors.New("memoryx: count must be greater than zero")
	}

	msgs, err := m.client.XRevRangeN(ctx, m.eventsKey(threadID), "+", "-", count).Result()
	if err != nil {
		return nil, wrapErr("recent events", err)
	}

	events := make([]Event, len(msgs))
	for i, msg := range msgs {
		event, err := parseEvent(msg)
		if err != nil {
			return nil, fmt.Errorf("memoryx: recent events: %w", err)
		}
		events[len(msgs)-1-i] = event
	}

	return events, nil
}

// parseEvent converts one XRevRange message into an Event.
func parseEvent(msg redis.XMessage) (Event, error) {
	event := Event{ID: msg.ID}

	if v, ok := msg.Values["event_type"]; ok {
		event.Type = toString(v)
	}

	if v, ok := msg.Values["payload"]; ok {
		var payload map[string]interface{}
		if err := json.Unmarshal([]byte(toString(v)), &payload); err != nil {
			return Event{}, fmt.Errorf("decode payload: %w", err)
		}
		event.Payload = payload
	}

	return event, nil
}

// parseSearchReply converts a raw FT.SEARCH reply into MemoryRecords. Redis
// replies with a RESP3 map (one "results" entry per document, each holding
// "id" and an "extra_attributes" field map) or, under RESP2, a flat array of
// [total, key, fields, key, fields, ...]. For a JSON index, the field map
// holds the whole document under the "$" key plus the KNN "score".
func (m *Memory) parseSearchReply(raw interface{}) ([]MemoryRecord, error) {
	switch root := raw.(type) {
	case map[interface{}]interface{}:
		return m.parseSearchReplyMap(root)
	case []interface{}:
		return m.parseSearchReplyArray(root)
	default:
		return nil, fmt.Errorf("unexpected search reply type %T", raw)
	}
}

// parseSearchReplyMap parses the RESP3 map-shaped FT.SEARCH reply.
func (m *Memory) parseSearchReplyMap(root map[interface{}]interface{}) ([]MemoryRecord, error) {
	docs, ok := root["results"].([]interface{})
	if !ok {
		return nil, nil
	}

	records := make([]MemoryRecord, 0, len(docs))

	for _, d := range docs {
		doc, ok := d.(map[interface{}]interface{})
		if !ok {
			return nil, fmt.Errorf("unexpected result document type %T", d)
		}

		fields, _ := doc["extra_attributes"].(map[interface{}]interface{})

		record, err := m.parseRecord(toString(doc["id"]), fields)
		if err != nil {
			return nil, err
		}

		records = append(records, record)
	}

	return records, nil
}

// parseSearchReplyArray parses the RESP2 flat-array FT.SEARCH reply.
func (m *Memory) parseSearchReplyArray(arr []interface{}) ([]MemoryRecord, error) {
	if len(arr) == 0 {
		return nil, nil
	}

	records := make([]MemoryRecord, 0, (len(arr)-1)/2)

	for i := 1; i+1 < len(arr); i += 2 {
		rawFields, ok := arr[i+1].([]interface{})
		if !ok {
			return nil, fmt.Errorf("unexpected fields reply type %T", arr[i+1])
		}

		fields := make(map[interface{}]interface{}, len(rawFields)/2)
		for j := 0; j+1 < len(rawFields); j += 2 {
			fields[rawFields[j]] = rawFields[j+1]
		}

		record, err := m.parseRecord(toString(arr[i]), fields)
		if err != nil {
			return nil, err
		}

		records = append(records, record)
	}

	return records, nil
}

// parseRecord converts one FT.SEARCH document (key and field map) into a
// MemoryRecord.
func (m *Memory) parseRecord(key string, fields map[interface{}]interface{}) (MemoryRecord, error) {
	record := MemoryRecord{ID: m.stripMemKey(key)}

	if v, ok := fields["score"]; ok {
		distance, err := toFloat64(v)
		if err != nil {
			return MemoryRecord{}, fmt.Errorf("parse score: %w", err)
		}
		record.Distance = distance
	}

	raw, ok := fields["$"]
	if !ok {
		return MemoryRecord{}, errors.New("missing json document in search reply")
	}

	var doc jsonRecord
	if err := json.Unmarshal([]byte(toString(raw)), &doc); err != nil {
		return MemoryRecord{}, fmt.Errorf("decode json document: %w", err)
	}

	record.User = doc.User
	record.Namespace = doc.Namespace
	record.Kind = doc.Kind
	record.Text = doc.Text
	record.Vector = doc.Vector
	record.Metadata = doc.Metadata

	return record, nil
}

// stateKey returns the Redis hash key for threadID's working state.
func (m *Memory) stateKey(threadID string) string {
	return m.prefix + ":state:" + threadID
}

// memPrefix returns the Redis key prefix for semantic memory documents.
func (m *Memory) memPrefix() string {
	return m.prefix + ":mem:"
}

// memKey returns the Redis JSON key for semantic memory id.
func (m *Memory) memKey(id string) string {
	return m.memPrefix() + id
}

// stripMemKey removes this Memory's memory prefix from key, returning the
// original memory id.
func (m *Memory) stripMemKey(key string) string {
	return strings.TrimPrefix(key, m.memPrefix())
}

// eventsKey returns the Redis stream key for threadID's event history.
func (m *Memory) eventsKey(threadID string) string {
	return m.prefix + ":events:" + threadID
}

// generateID returns a random hex-encoded identifier for a semantic memory.
func generateID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

// encodeVector encodes vec as IEEE 754 float32 little-endian bytes, the
// binary format Redis Search expects for a KNN query vector parameter.
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

// wrapErr wraps err with the memoryx prefix and op, mapping unknown command
// errors to ErrCapability.
func wrapErr(op string, err error) error {
	if err == nil {
		return nil
	}

	if strings.Contains(strings.ToLower(err.Error()), "unknown command") {
		return fmt.Errorf("memoryx: %s: %w", op, ErrCapability)
	}

	return fmt.Errorf("memoryx: %s: %w", op, err)
}
