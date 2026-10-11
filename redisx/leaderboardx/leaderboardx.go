// Package leaderboardx maintains real-time ranked boards backed by a Redis
// sorted set per board, with optional per-member metadata stored in a
// companion hash. Scores support ties and descending rank order, matching
// the official Redis leaderboard example.
package leaderboardx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/leetun2k2/go-bedrock/redisx"
	"github.com/redis/go-redis/v9"
)

// ErrNotFound is returned when a member does not exist on a board.
var ErrNotFound = errors.New("leaderboardx: member not found")

// Entry describes a single member's position on a board.
type Entry struct {
	Member   string
	Score    float64
	Rank     int64 // zero-based rank, 0 is the highest score
	Metadata map[string]string
}

// Leaderboard maintains ranked scores across one or more named boards. It is
// safe for concurrent use and does not own the Redis connection it borrows.
type Leaderboard struct {
	client *redis.Client

	prefix     string
	ttlSeconds int64
}

// New constructs a Leaderboard backed by client using cfg. New validates cfg
// and returns an error without touching Redis if the configuration is
// invalid.
func New(client *redisx.Client, cfg Config) (*Leaderboard, error) {
	if client == nil {
		return nil, errors.New("leaderboardx: client must not be nil")
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &Leaderboard{
		client:     client.Client(),
		prefix:     cfg.Prefix,
		ttlSeconds: int64(cfg.TTL.Seconds()),
	}, nil
}

// SetScore sets member's score on board, replacing any previous score.
func (l *Leaderboard) SetScore(ctx context.Context, board, member string, score float64) error {
	if err := validateBoardMember(board, member); err != nil {
		return err
	}

	boardKey := l.boardKey(board)
	if err := l.client.ZAdd(ctx, boardKey, redis.Z{Score: score, Member: member}).Err(); err != nil {
		return fmt.Errorf("leaderboardx: set score: %w", err)
	}

	if err := l.applyTTL(ctx, boardKey); err != nil {
		return fmt.Errorf("leaderboardx: set score: %w", err)
	}

	return nil
}

// Increment adds delta to member's score on board, creating the member with
// score delta if it did not previously exist. Increment returns the new
// score.
func (l *Leaderboard) Increment(ctx context.Context, board, member string, delta float64) (float64, error) {
	if err := validateBoardMember(board, member); err != nil {
		return 0, err
	}

	boardKey := l.boardKey(board)
	score, err := l.client.ZIncrBy(ctx, boardKey, delta, member).Result()
	if err != nil {
		return 0, fmt.Errorf("leaderboardx: increment: %w", err)
	}

	if err := l.applyTTL(ctx, boardKey); err != nil {
		return 0, fmt.Errorf("leaderboardx: increment: %w", err)
	}

	return score, nil
}

// Rank returns member's zero-based descending rank, score, and metadata on
// board. Rank returns ErrNotFound if member does not exist on board.
func (l *Leaderboard) Rank(ctx context.Context, board, member string) (Entry, error) {
	if err := validateBoardMember(board, member); err != nil {
		return Entry{}, err
	}

	boardKey := l.boardKey(board)

	rank, err := l.client.ZRevRank(ctx, boardKey, member).Result()
	if errors.Is(err, redis.Nil) {
		return Entry{}, ErrNotFound
	}
	if err != nil {
		return Entry{}, fmt.Errorf("leaderboardx: rank: %w", err)
	}

	score, err := l.client.ZScore(ctx, boardKey, member).Result()
	if err != nil {
		return Entry{}, fmt.Errorf("leaderboardx: rank: %w", err)
	}

	meta, err := l.GetMetadata(ctx, board, member)
	if err != nil {
		return Entry{}, fmt.Errorf("leaderboardx: rank: %w", err)
	}

	return Entry{Member: member, Score: score, Rank: rank, Metadata: meta}, nil
}

// Top returns the top limit entries on board in descending score order.
func (l *Leaderboard) Top(ctx context.Context, board string, limit int64) ([]Entry, error) {
	if strings.TrimSpace(board) == "" {
		return nil, errors.New("leaderboardx: board must not be empty")
	}

	if limit <= 0 {
		return nil, errors.New("leaderboardx: limit must be greater than zero")
	}

	zs, err := l.client.ZRevRangeWithScores(ctx, l.boardKey(board), 0, limit-1).Result()
	if err != nil {
		return nil, fmt.Errorf("leaderboardx: top: %w", err)
	}

	entries, err := l.buildEntries(ctx, board, zs, 0)
	if err != nil {
		return nil, fmt.Errorf("leaderboardx: top: %w", err)
	}

	return entries, nil
}

// Around returns the entries on board within radius positions of member's
// rank, inclusive and in descending score order. Around returns ErrNotFound
// if member does not exist on board.
func (l *Leaderboard) Around(ctx context.Context, board, member string, radius int64) ([]Entry, error) {
	if err := validateBoardMember(board, member); err != nil {
		return nil, err
	}

	if radius < 0 {
		return nil, errors.New("leaderboardx: radius must not be negative")
	}

	boardKey := l.boardKey(board)

	rank, err := l.client.ZRevRank(ctx, boardKey, member).Result()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("leaderboardx: around: %w", err)
	}

	start := rank - radius
	if start < 0 {
		start = 0
	}
	stop := rank + radius

	zs, err := l.client.ZRevRangeWithScores(ctx, boardKey, start, stop).Result()
	if err != nil {
		return nil, fmt.Errorf("leaderboardx: around: %w", err)
	}

	entries, err := l.buildEntries(ctx, board, zs, start)
	if err != nil {
		return nil, fmt.Errorf("leaderboardx: around: %w", err)
	}

	return entries, nil
}

// Remove deletes member from board and discards its metadata. Remove does
// not report an error if member does not exist.
func (l *Leaderboard) Remove(ctx context.Context, board, member string) error {
	if err := validateBoardMember(board, member); err != nil {
		return err
	}

	if err := l.client.ZRem(ctx, l.boardKey(board), member).Err(); err != nil {
		return fmt.Errorf("leaderboardx: remove: %w", err)
	}

	if err := l.client.HDel(ctx, l.metaKey(board), member).Err(); err != nil {
		return fmt.Errorf("leaderboardx: remove: %w", err)
	}

	return nil
}

// SetMetadata stores meta for member on board, replacing any previous
// metadata. SetMetadata does not require member to already have a score.
func (l *Leaderboard) SetMetadata(ctx context.Context, board, member string, meta map[string]string) error {
	if err := validateBoardMember(board, member); err != nil {
		return err
	}

	encoded, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("leaderboardx: set metadata: %w", err)
	}

	metaKey := l.metaKey(board)
	if err := l.client.HSet(ctx, metaKey, member, encoded).Err(); err != nil {
		return fmt.Errorf("leaderboardx: set metadata: %w", err)
	}

	if err := l.applyTTL(ctx, metaKey); err != nil {
		return fmt.Errorf("leaderboardx: set metadata: %w", err)
	}

	return nil
}

// GetMetadata returns member's metadata on board. GetMetadata returns a nil
// map without an error if member has no metadata.
func (l *Leaderboard) GetMetadata(ctx context.Context, board, member string) (map[string]string, error) {
	if err := validateBoardMember(board, member); err != nil {
		return nil, err
	}

	encoded, err := l.client.HGet(ctx, l.metaKey(board), member).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("leaderboardx: get metadata: %w", err)
	}

	return decodeMetadata(encoded)
}

// buildEntries converts sorted-set members into Entry values, assigning
// ranks starting at startRank and batch-fetching metadata with a pipeline.
func (l *Leaderboard) buildEntries(ctx context.Context, board string, zs []redis.Z, startRank int64) ([]Entry, error) {
	entries := make([]Entry, len(zs))
	members := make([]string, len(zs))

	for i, z := range zs {
		member, ok := z.Member.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected member type: %T", z.Member)
		}

		entries[i] = Entry{Member: member, Score: z.Score, Rank: startRank + int64(i)}
		members[i] = member
	}

	metaByMember, err := l.batchMetadata(ctx, board, members)
	if err != nil {
		return nil, err
	}

	for i, member := range members {
		entries[i].Metadata = metaByMember[member]
	}

	return entries, nil
}

// batchMetadata fetches metadata for members on board in a single pipeline
// round trip. Members without stored metadata are omitted from the result.
func (l *Leaderboard) batchMetadata(ctx context.Context, board string, members []string) (map[string]map[string]string, error) {
	if len(members) == 0 {
		return nil, nil
	}

	metaKey := l.metaKey(board)

	pipe := l.client.Pipeline()
	cmds := make([]*redis.StringCmd, len(members))
	for i, member := range members {
		cmds[i] = pipe.HGet(ctx, metaKey, member)
	}

	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("batch metadata: %w", err)
	}

	result := make(map[string]map[string]string, len(members))
	for i, cmd := range cmds {
		encoded, err := cmd.Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("batch metadata: %w", err)
		}

		meta, err := decodeMetadata(encoded)
		if err != nil {
			return nil, err
		}
		result[members[i]] = meta
	}

	return result, nil
}

// applyTTL expires key after the configured TTL. applyTTL is a no-op when no
// TTL was configured.
func (l *Leaderboard) applyTTL(ctx context.Context, key string) error {
	if l.ttlSeconds <= 0 {
		return nil
	}

	return l.client.Expire(ctx, key, time.Duration(l.ttlSeconds)*time.Second).Err()
}

// boardKey returns the Redis key for board's sorted set.
func (l *Leaderboard) boardKey(board string) string {
	return l.prefix + ":" + board
}

// metaKey returns the Redis key for board's metadata hash.
func (l *Leaderboard) metaKey(board string) string {
	return l.prefix + ":" + board + ":meta"
}

// validateBoardMember reports whether board and member are both non-empty.
func validateBoardMember(board, member string) error {
	if strings.TrimSpace(board) == "" {
		return errors.New("leaderboardx: board must not be empty")
	}

	if strings.TrimSpace(member) == "" {
		return errors.New("leaderboardx: member must not be empty")
	}

	return nil
}

// decodeMetadata unmarshals a JSON-encoded metadata value.
func decodeMetadata(encoded string) (map[string]string, error) {
	var meta map[string]string
	if err := json.Unmarshal([]byte(encoded), &meta); err != nil {
		return nil, fmt.Errorf("decode metadata: %w", err)
	}
	return meta, nil
}
