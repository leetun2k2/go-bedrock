// Package sessionx provides shared web session storage backed by Redis. Each
// session is a hash with a sliding TTL, indexed by a per-user set of session
// IDs so all of a user's sessions can be revoked together. Session creation,
// deletion, and logout-all run through Lua scripts so the session hash and
// its user index never drift out of sync.
package sessionx

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/leetun2k2/go-bedrock/redisx"
	"github.com/redis/go-redis/v9"
)

// userIDField is the reserved hash field used to remember which user a
// session belongs to, so Delete and the sliding-TTL scripts can locate the
// matching user index set without a second lookup key.
const userIDField = "_sessionx_user_id"

// ErrNotFound is returned when a session does not exist or has expired.
var ErrNotFound = errors.New("sessionx: session not found")

// createScript stores session fields, sets the session TTL, and adds the
// session ID to the user index set with a matching TTL. If the session ID
// already belonged to a different user, it is atomically removed from that
// user's index set first, so a reassigned session ID never leaves a stale
// membership behind. KEYS[1] is the session key, KEYS[2] is the user index
// set key. ARGV[1] is the TTL in seconds, ARGV[2] is the session ID, ARGV[3]
// is the user ID, ARGV[4] is the user index key prefix, and ARGV[5:] are
// field/value pairs.
const createScript = `
local ttl = ARGV[1]
local session_id = ARGV[2]
local user_id = ARGV[3]
local user_prefix = ARGV[4]

local prior_user_id = redis.call('HGET', KEYS[1], '` + userIDField + `')
if prior_user_id and prior_user_id ~= user_id then
	redis.call('SREM', user_prefix .. prior_user_id, session_id)
end

if #ARGV > 4 then
	redis.call('HSET', KEYS[1], unpack(ARGV, 5))
end
redis.call('EXPIRE', KEYS[1], ttl)
redis.call('SADD', KEYS[2], session_id)
redis.call('EXPIRE', KEYS[2], ttl)
return 1
`

// getScript returns the full session hash and refreshes the sliding TTL on
// the session and its user index set. KEYS[1] is the session key. ARGV[1] is
// the TTL in seconds, ARGV[2] is the user index key prefix.
const getScript = `
local data = redis.call('HGETALL', KEYS[1])
if #data == 0 then
	return {}
end

redis.call('EXPIRE', KEYS[1], ARGV[1])

local user_id = redis.call('HGET', KEYS[1], '` + userIDField + `')
if user_id then
	redis.call('EXPIRE', ARGV[2] .. user_id, ARGV[1])
end

return data
`

// setFieldsScript updates fields on an existing session and refreshes the
// sliding TTL. KEYS[1] is the session key. ARGV[1] is the TTL in seconds,
// ARGV[2] is the user index key prefix, and ARGV[3:] are field/value pairs.
const setFieldsScript = `
if redis.call('EXISTS', KEYS[1]) == 0 then
	return 0
end

if #ARGV > 2 then
	redis.call('HSET', KEYS[1], unpack(ARGV, 3))
end
redis.call('EXPIRE', KEYS[1], ARGV[1])

local user_id = redis.call('HGET', KEYS[1], '` + userIDField + `')
if user_id then
	redis.call('EXPIRE', ARGV[2] .. user_id, ARGV[1])
end

return 1
`

// touchScript refreshes the sliding TTL on a session and its user index set
// without modifying fields. KEYS[1] is the session key. ARGV[1] is the TTL
// in seconds, ARGV[2] is the user index key prefix.
const touchScript = `
if redis.call('EXISTS', KEYS[1]) == 0 then
	return 0
end

redis.call('EXPIRE', KEYS[1], ARGV[1])

local user_id = redis.call('HGET', KEYS[1], '` + userIDField + `')
if user_id then
	redis.call('EXPIRE', ARGV[2] .. user_id, ARGV[1])
end

return 1
`

// deleteScript reads the owning user ID from the session hash, deletes the
// session key, and removes the session ID from the user index set. KEYS[1]
// is the session key. ARGV[1] is the user index key prefix, ARGV[2] is the
// session ID.
const deleteScript = `
local user_id = redis.call('HGET', KEYS[1], '` + userIDField + `')
redis.call('DEL', KEYS[1])

if user_id then
	redis.call('SREM', ARGV[1] .. user_id, ARGV[2])
end

return 1
`

// deleteUserSessionsScript reads all session IDs from the user index set,
// deletes each session key, and clears the user index set. KEYS[1] is the
// user index set key. ARGV[1] is the session key prefix.
const deleteUserSessionsScript = `
local session_ids = redis.call('SMEMBERS', KEYS[1])
for _, session_id in ipairs(session_ids) do
	redis.call('DEL', ARGV[1] .. session_id)
end
redis.call('DEL', KEYS[1])
return #session_ids
`

// Store is a Redis-backed session store with sliding TTL and multi-device
// logout support. It is safe for concurrent use and does not own the Redis
// connection it borrows.
type Store struct {
	client *redis.Client

	sessionPrefix string
	userPrefix    string
	ttlSeconds    int64

	create             *redis.Script
	get                *redis.Script
	setFields          *redis.Script
	touch              *redis.Script
	delete             *redis.Script
	deleteUserSessions *redis.Script
}

// New constructs a Store backed by client using cfg. New validates cfg and
// returns an error without touching Redis if the configuration is invalid.
func New(client *redisx.Client, cfg Config) (*Store, error) {
	if client == nil {
		return nil, errors.New("sessionx: client must not be nil")
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &Store{
		client:             client.Client(),
		sessionPrefix:      cfg.Prefix + ":session:",
		userPrefix:         cfg.Prefix + ":user:",
		ttlSeconds:         int64(cfg.TTL.Seconds()),
		create:             redis.NewScript(createScript),
		get:                redis.NewScript(getScript),
		setFields:          redis.NewScript(setFieldsScript),
		touch:              redis.NewScript(touchScript),
		delete:             redis.NewScript(deleteScript),
		deleteUserSessions: redis.NewScript(deleteUserSessionsScript),
	}, nil
}

// Create atomically stores fields in a session hash, sets the session TTL,
// and adds sessionID to userID's session set with a matching sliding TTL.
func (s *Store) Create(ctx context.Context, sessionID, userID string, fields map[string]string) error {
	if strings.TrimSpace(sessionID) == "" {
		return errors.New("sessionx: session id must not be empty")
	}

	if strings.TrimSpace(userID) == "" {
		return errors.New("sessionx: user id must not be empty")
	}

	if _, reserved := fields[userIDField]; reserved {
		return fmt.Errorf("sessionx: field name %q is reserved", userIDField)
	}

	argv := make([]interface{}, 0, 4+2*(len(fields)+1))
	argv = append(argv, s.ttlSeconds, sessionID, userID, s.userPrefix, userIDField, userID)
	for field, value := range fields {
		argv = append(argv, field, value)
	}

	keys := []string{s.sessionKey(sessionID), s.userKey(userID)}
	if err := s.create.Run(ctx, s.client, keys, argv...).Err(); err != nil {
		return fmt.Errorf("sessionx: create: %w", err)
	}

	return nil
}

// Get returns a session's fields, refreshing the sliding TTL on the session
// and its user index set. Get returns ErrNotFound if the session does not
// exist or has expired.
func (s *Store) Get(ctx context.Context, sessionID string) (map[string]string, error) {
	if strings.TrimSpace(sessionID) == "" {
		return nil, errors.New("sessionx: session id must not be empty")
	}

	keys := []string{s.sessionKey(sessionID)}
	res, err := s.get.Run(ctx, s.client, keys, s.ttlSeconds, s.userPrefix).Result()
	if err != nil {
		return nil, fmt.Errorf("sessionx: get: %w", err)
	}

	values, ok := res.([]interface{})
	if !ok {
		return nil, fmt.Errorf("sessionx: get: unexpected script result: %v", res)
	}

	if len(values) == 0 {
		return nil, ErrNotFound
	}

	fields := make(map[string]string, len(values)/2)
	for i := 0; i+1 < len(values); i += 2 {
		key, ok := values[i].(string)
		if !ok {
			return nil, fmt.Errorf("sessionx: get: unexpected field type: %T", values[i])
		}
		if key == userIDField {
			continue
		}
		value, ok := values[i+1].(string)
		if !ok {
			return nil, fmt.Errorf("sessionx: get: unexpected value type: %T", values[i+1])
		}
		fields[key] = value
	}

	return fields, nil
}

// SetFields updates fields on an existing session and refreshes its sliding
// TTL. SetFields returns ErrNotFound if the session does not exist.
func (s *Store) SetFields(ctx context.Context, sessionID string, fields map[string]string) error {
	if strings.TrimSpace(sessionID) == "" {
		return errors.New("sessionx: session id must not be empty")
	}

	if _, reserved := fields[userIDField]; reserved {
		return fmt.Errorf("sessionx: field name %q is reserved", userIDField)
	}

	argv := make([]interface{}, 0, 2+2*len(fields))
	argv = append(argv, s.ttlSeconds, s.userPrefix)
	for field, value := range fields {
		argv = append(argv, field, value)
	}

	keys := []string{s.sessionKey(sessionID)}
	ok, err := s.setFields.Run(ctx, s.client, keys, argv...).Int64()
	if err != nil {
		return fmt.Errorf("sessionx: set fields: %w", err)
	}

	if ok == 0 {
		return ErrNotFound
	}

	return nil
}

// Touch extends the TTL of a session and its user index set without
// modifying fields. Touch returns ErrNotFound if the session does not exist.
func (s *Store) Touch(ctx context.Context, sessionID string) error {
	if strings.TrimSpace(sessionID) == "" {
		return errors.New("sessionx: session id must not be empty")
	}

	keys := []string{s.sessionKey(sessionID)}
	ok, err := s.touch.Run(ctx, s.client, keys, s.ttlSeconds, s.userPrefix).Int64()
	if err != nil {
		return fmt.Errorf("sessionx: touch: %w", err)
	}

	if ok == 0 {
		return ErrNotFound
	}

	return nil
}

// Delete atomically removes a session and its membership in the owning
// user's session set. Delete does not report an error if the session does
// not exist.
func (s *Store) Delete(ctx context.Context, sessionID string) error {
	if strings.TrimSpace(sessionID) == "" {
		return errors.New("sessionx: session id must not be empty")
	}

	keys := []string{s.sessionKey(sessionID)}
	if err := s.delete.Run(ctx, s.client, keys, s.userPrefix, sessionID).Err(); err != nil {
		return fmt.Errorf("sessionx: delete: %w", err)
	}

	return nil
}

// DeleteUserSessions atomically deletes every session belonging to userID
// and clears the user's session set, logging the user out of all devices.
// DeleteUserSessions does not report an error if the user has no sessions.
func (s *Store) DeleteUserSessions(ctx context.Context, userID string) error {
	if strings.TrimSpace(userID) == "" {
		return errors.New("sessionx: user id must not be empty")
	}

	keys := []string{s.userKey(userID)}
	if err := s.deleteUserSessions.Run(ctx, s.client, keys, s.sessionPrefix).Err(); err != nil {
		return fmt.Errorf("sessionx: delete user sessions: %w", err)
	}

	return nil
}

// sessionKey returns the Redis key for a session hash.
func (s *Store) sessionKey(sessionID string) string {
	return s.sessionPrefix + sessionID
}

// userKey returns the Redis key for a user's session index set.
func (s *Store) userKey(userID string) string {
	return s.userPrefix + userID
}
