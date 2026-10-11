package ratelimitx

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/leetun2k2/go-bedrock/redisx"
	"github.com/redis/go-redis/v9"
)

// tokenBucketScript refills a bucket by elapsed Redis server time and
// deducts cost if enough tokens are available. KEYS[1] is the bucket key.
// ARGV: capacity, refillAmount, refillIntervalMs, cost, ttlSeconds.
// Returns {allowed (0/1), remaining tokens, retryAfterMs}.
const tokenBucketScript = `
local capacity = tonumber(ARGV[1])
local refill_amount = tonumber(ARGV[2])
local refill_interval_ms = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])
local ttl_seconds = tonumber(ARGV[5])

local time_parts = redis.call('TIME')
local now_ms = tonumber(time_parts[1]) * 1000 + math.floor(tonumber(time_parts[2]) / 1000)

local tokens = capacity
local last_refill = now_ms

local bucket = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
if bucket[1] and bucket[2] then
	tokens = tonumber(bucket[1])
	last_refill = tonumber(bucket[2])

	local elapsed = now_ms - last_refill
	if elapsed > 0 then
		local intervals = math.floor(elapsed / refill_interval_ms)
		if intervals > 0 then
			tokens = math.min(capacity, tokens + intervals * refill_amount)
			last_refill = last_refill + intervals * refill_interval_ms
		end
	end
end

local allowed = 0
local retry_after_ms = 0

if tokens >= cost then
	tokens = tokens - cost
	allowed = 1
else
	local deficit = cost - tokens
	local intervals_needed = math.ceil(deficit / refill_amount)
	retry_after_ms = (last_refill + intervals_needed * refill_interval_ms) - now_ms
	if retry_after_ms < 0 then
		retry_after_ms = 0
	end
end

redis.call('HMSET', KEYS[1], 'tokens', tokens, 'ts', last_refill)
redis.call('EXPIRE', KEYS[1], ttl_seconds)

return {allowed, tokens, retry_after_ms}
`

// tokenBucketLimiter is a distributed token-bucket rate limiter.
type tokenBucketLimiter struct {
	client           *redis.Client
	prefix           string
	capacity         int64
	refillAmount     int64
	refillIntervalMs int64
	ttlSeconds       int64
	script           *redis.Script
}

// NewTokenBucket constructs a token-bucket Limiter backed by client using
// cfg. It validates cfg and returns an error without touching Redis if the
// configuration is invalid.
func NewTokenBucket(client *redisx.Client, cfg TokenBucketConfig) (Limiter, error) {
	if err := validateClient(client); err != nil {
		return nil, err
	}

	refillIntervalMs, err := cfg.validate()
	if err != nil {
		return nil, err
	}

	fillIntervals := int64(math.Ceil(float64(cfg.Capacity) / float64(cfg.RefillAmount)))
	ttl := cfg.RefillInterval * time.Duration(fillIntervals+1)
	ttlSeconds := int64(math.Ceil(ttl.Seconds()))
	if ttlSeconds < 1 {
		ttlSeconds = 1
	}

	return &tokenBucketLimiter{
		client:           client.Client(),
		prefix:           cfg.Prefix,
		capacity:         cfg.Capacity,
		refillAmount:     cfg.RefillAmount,
		refillIntervalMs: refillIntervalMs,
		ttlSeconds:       ttlSeconds,
		script:           redis.NewScript(tokenBucketScript),
	}, nil
}

// Allow attempts to consume cost tokens from the bucket identified by key.
// The first call for a key starts with a full bucket. Tokens refill by
// elapsed Redis server time up to the configured capacity.
func (l *tokenBucketLimiter) Allow(ctx context.Context, key string, cost int64) (Result, error) {
	if err := validateKeyCost(key, cost, l.capacity); err != nil {
		return Result{}, err
	}

	bucketKey := l.prefix + ":" + key

	res, err := l.script.Run(ctx, l.client, []string{bucketKey},
		l.capacity, l.refillAmount, l.refillIntervalMs, cost, l.ttlSeconds,
	).Result()
	if err != nil {
		return Result{}, fmt.Errorf("ratelimitx: allow: %w", err)
	}

	return parseResult(res)
}
