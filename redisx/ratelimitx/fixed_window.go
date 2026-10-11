package ratelimitx

import (
	"context"
	"fmt"
	"math"

	"github.com/leetun2k2/go-bedrock/redisx"
	"github.com/redis/go-redis/v9"
)

// fixedWindowScript counts accepted cost within the epoch-aligned window
// containing Redis server time, resetting when the window rolls over.
// KEYS[1] is the window key. ARGV: limit, windowMs, cost, ttlSeconds.
// Returns {allowed (0/1), remaining, retryAfterMs}.
const fixedWindowScript = `
local limit = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local ttl_seconds = tonumber(ARGV[4])

local time_parts = redis.call('TIME')
local now_ms = tonumber(time_parts[1]) * 1000 + math.floor(tonumber(time_parts[2]) / 1000)

local window_id = math.floor(now_ms / window_ms)
local window_end = (window_id + 1) * window_ms

local count = 0
local stored = redis.call('HMGET', KEYS[1], 'count', 'window_id')
if stored[1] and stored[2] and tonumber(stored[2]) == window_id then
	count = tonumber(stored[1])
end

local allowed = 0
local retry_after_ms = 0

if count + cost <= limit then
	count = count + cost
	allowed = 1
else
	retry_after_ms = window_end - now_ms
end

redis.call('HMSET', KEYS[1], 'count', count, 'window_id', window_id)
redis.call('EXPIRE', KEYS[1], ttl_seconds)

return {allowed, limit - count, retry_after_ms}
`

// fixedWindowLimiter is a distributed fixed-window rate limiter.
type fixedWindowLimiter struct {
	client     *redis.Client
	prefix     string
	limit      int64
	windowMs   int64
	ttlSeconds int64
	script     *redis.Script
}

// NewFixedWindow constructs a fixed-window Limiter backed by client using
// cfg. It validates cfg and returns an error without touching Redis if the
// configuration is invalid.
func NewFixedWindow(client *redisx.Client, cfg FixedWindowConfig) (Limiter, error) {
	if err := validateClient(client); err != nil {
		return nil, err
	}

	windowMs, err := cfg.validate()
	if err != nil {
		return nil, err
	}

	ttlSeconds := int64(math.Ceil((2 * cfg.Window).Seconds()))
	if ttlSeconds < 1 {
		ttlSeconds = 1
	}

	return &fixedWindowLimiter{
		client:     client.Client(),
		prefix:     cfg.Prefix,
		limit:      cfg.Limit,
		windowMs:   windowMs,
		ttlSeconds: ttlSeconds,
		script:     redis.NewScript(fixedWindowScript),
	}, nil
}

// Allow attempts to count cost against the window containing key. A new
// window starts with zero usage; capacity resets at the next epoch-aligned
// boundary.
func (l *fixedWindowLimiter) Allow(ctx context.Context, key string, cost int64) (Result, error) {
	if err := validateKeyCost(key, cost, l.limit); err != nil {
		return Result{}, err
	}

	windowKey := l.prefix + ":" + key

	res, err := l.script.Run(ctx, l.client, []string{windowKey},
		l.limit, l.windowMs, cost, l.ttlSeconds,
	).Result()
	if err != nil {
		return Result{}, fmt.Errorf("ratelimitx: allow: %w", err)
	}

	return parseResult(res)
}
