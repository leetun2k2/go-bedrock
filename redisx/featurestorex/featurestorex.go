// Package featurestorex serves precomputed online ML features by entity,
// backed by a Redis hash per entity. Callers own feature encoding and
// default-value policy; the Store only reports field presence so model code
// can apply defaults for missing or expired features. Per-field expiry
// requires Redis 7.4+ (HEXPIRE).
package featurestorex

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/leetun2k2/go-bedrock/redisx"
	"github.com/redis/go-redis/v9"
)

// Store serves online features from Redis hashes. It is safe for
// concurrent use and does not own the Redis connection it borrows.
type Store struct {
	client    *redis.Client
	prefix    string
	entityTTL time.Duration
}

// New constructs a Store backed by client using cfg. New validates cfg and
// returns an error without touching Redis if the configuration is invalid.
func New(client *redisx.Client, cfg Config) (*Store, error) {
	if client == nil {
		return nil, errors.New("featurestorex: client must not be nil")
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &Store{
		client:    client.Client(),
		prefix:    cfg.Prefix,
		entityTTL: cfg.EntityTTL,
	}, nil
}

// Set writes features to entity's hash via HSET, updating only the supplied
// fields. If the Store's EntityTTL is greater than zero, Set refreshes the
// entity key's TTL.
func (s *Store) Set(ctx context.Context, entity string, features map[string]string) error {
	if err := validateEntity(entity); err != nil {
		return err
	}
	if len(features) == 0 {
		return errors.New("featurestorex: features must not be empty")
	}

	key := s.entityKey(entity)

	if err := s.client.HSet(ctx, key, toHSetArgs(features)...).Err(); err != nil {
		return fmt.Errorf("featurestorex: set: %w", err)
	}

	if s.entityTTL > 0 {
		if err := s.client.Expire(ctx, key, s.entityTTL).Err(); err != nil {
			return fmt.Errorf("featurestorex: set: %w", err)
		}
	}

	return nil
}

// SetWithTTL writes features to entity's hash via HSET, then applies
// fieldTTL to each written field via HEXPIRE so fields expire independently
// of the entity key. SetWithTTL ensures the entity key's own TTL is at
// least max(EntityTTL, fieldTTL) so the key does not outlive its fields
// indefinitely.
func (s *Store) SetWithTTL(ctx context.Context, entity string, features map[string]string, fieldTTL time.Duration) error {
	if err := validateEntity(entity); err != nil {
		return err
	}
	if len(features) == 0 {
		return errors.New("featurestorex: features must not be empty")
	}
	if fieldTTL <= 0 {
		return fmt.Errorf("featurestorex: field ttl must be greater than zero: %s", fieldTTL)
	}

	key := s.entityKey(entity)

	if err := s.client.HSet(ctx, key, toHSetArgs(features)...).Err(); err != nil {
		return fmt.Errorf("featurestorex: set with ttl: %w", err)
	}

	fields := make([]string, 0, len(features))
	for field := range features {
		fields = append(fields, field)
	}

	if err := s.client.HExpire(ctx, key, fieldTTL, fields...).Err(); err != nil {
		return fmt.Errorf("featurestorex: set with ttl: %w", err)
	}

	if err := s.ensureKeyTTL(ctx, key, fieldTTL); err != nil {
		return fmt.Errorf("featurestorex: set with ttl: %w", err)
	}

	return nil
}

// ensureKeyTTL raises key's TTL to max(EntityTTL, fieldTTL) if the key's
// current TTL is lower. It leaves a longer existing TTL untouched.
func (s *Store) ensureKeyTTL(ctx context.Context, key string, fieldTTL time.Duration) error {
	wanted := s.entityTTL
	if fieldTTL > wanted {
		wanted = fieldTTL
	}
	if wanted <= 0 {
		return nil
	}

	current, err := s.client.TTL(ctx, key).Result()
	if err != nil {
		return err
	}
	// A TTL of -1 means key has no expiry (persistent), which already
	// satisfies "at least wanted"; never shrink it.
	if current == -1 || current >= wanted {
		return nil
	}

	return s.client.Expire(ctx, key, wanted).Err()
}

// Get reads the requested features from entity's hash via HMGET. The
// returned map contains only fields that are present and unexpired; missing
// or expired fields are omitted so the caller can detect absence.
func (s *Store) Get(ctx context.Context, entity string, features ...string) (map[string]string, error) {
	if err := validateEntity(entity); err != nil {
		return nil, err
	}
	if len(features) == 0 {
		return nil, errors.New("featurestorex: at least one feature is required")
	}

	values, err := s.client.HMGet(ctx, s.entityKey(entity), features...).Result()
	if err != nil {
		return nil, fmt.Errorf("featurestorex: get: %w", err)
	}

	return toFeatureMap(features, values), nil
}

// GetMany reads the requested features for each entity in a single Redis
// pipeline, returning one map per entity in the same order as entities.
func (s *Store) GetMany(ctx context.Context, entities []string, features ...string) ([]map[string]string, error) {
	if len(entities) == 0 {
		return nil, errors.New("featurestorex: entities must not be empty")
	}
	if len(features) == 0 {
		return nil, errors.New("featurestorex: at least one feature is required")
	}
	for _, entity := range entities {
		if err := validateEntity(entity); err != nil {
			return nil, err
		}
	}

	pipe := s.client.Pipeline()
	cmds := make([]*redis.SliceCmd, len(entities))
	for i, entity := range entities {
		cmds[i] = pipe.HMGet(ctx, s.entityKey(entity), features...)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("featurestorex: get many: %w", err)
	}

	results := make([]map[string]string, len(entities))
	for i, cmd := range cmds {
		values, err := cmd.Result()
		if err != nil {
			return nil, fmt.Errorf("featurestorex: get many: %w", err)
		}
		results[i] = toFeatureMap(features, values)
	}

	return results, nil
}

// Delete removes entity's hash key. Delete is a no-op if entity does not
// exist.
func (s *Store) Delete(ctx context.Context, entity string) error {
	if err := validateEntity(entity); err != nil {
		return err
	}

	if err := s.client.Del(ctx, s.entityKey(entity)).Err(); err != nil {
		return fmt.Errorf("featurestorex: delete: %w", err)
	}

	return nil
}

func (s *Store) entityKey(entity string) string {
	return s.prefix + ":" + entity
}

func validateEntity(entity string) error {
	if strings.TrimSpace(entity) == "" {
		return errors.New("featurestorex: entity must not be empty")
	}
	return nil
}

func toHSetArgs(features map[string]string) []interface{} {
	args := make([]interface{}, 0, len(features)*2)
	for field, value := range features {
		args = append(args, field, value)
	}
	return args
}

func toFeatureMap(features []string, values []interface{}) map[string]string {
	result := make(map[string]string, len(features))
	for i, value := range values {
		if value == nil {
			continue
		}
		result[features[i]] = value.(string)
	}
	return result
}
