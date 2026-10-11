package ratelimitx

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/leetun2k2/go-bedrock/redisx"
	"github.com/redis/go-redis/v9"
)

// leakyBucketScript drains occupancy by elapsed Redis server time and admits
// cost if the bucket has enough headroom. It is the mathematical mirror of
// the token-bucket script: it rejects overflow and does not queue work.
// KEYS[1] is the bucket key. ARGV: capacity, leakAmount, leakIntervalMs,
// cost, ttlSeconds. Returns {allowed (0/1), remaining headroom, retryAfterMs}.
const leakyBucketScript = `
local capacity = tonumber(ARGV[1])
local leak_amount = tonumber(ARGV[2])
local leak_interval_ms = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])
local ttl_seconds = tonumber(ARGV[5])

local time_parts = redis.call('TIME')
local now_ms = tonumber(time_parts[1]) * 1000 + math.floor(tonumber(time_parts[2]) / 1000)

local occupancy = 0
local last_leak = now_ms

local bucket = redis.call('HMGET', KEYS[1], 'occupancy', 'ts')
if bucket[1] and bucket[2] then
	occupancy = tonumber(bucket[1])
	last_leak = tonumber(bucket[2])

	local elapsed = now_ms - last_leak
	if elapsed > 0 then
		local intervals = math.floor(elapsed / leak_interval_ms)
		if intervals > 0 then
			occupancy = math.max(0, occupancy - intervals * leak_amount)
			last_leak = last_leak + intervals * leak_interval_ms
		end
	end
end

local allowed = 0
local retry_after_ms = 0

if occupancy + cost <= capacity then
	occupancy = occupancy + cost
	allowed = 1
else
	local excess = occupancy + cost - capacity
	local intervals_needed = math.ceil(excess / leak_amount)
	retry_after_ms = (last_leak + intervals_needed * leak_interval_ms) - now_ms
	if retry_after_ms < 0 then
		retry_after_ms = 0
	end
end

redis.call('HMSET', KEYS[1], 'occupancy', occupancy, 'ts', last_leak)
redis.call('EXPIRE', KEYS[1], ttl_seconds)

return {allowed, capacity - occupancy, retry_after_ms}
`

// leakyBucketLimiter is a distributed leaky-bucket meter rate limiter.
type leakyBucketLimiter struct {
	client         *redis.Client
	prefix         string
	capacity       int64
	leakAmount     int64
	leakIntervalMs int64
	ttlSeconds     int64
	script         *redis.Script
}

// NewLeakyBucket constructs a leaky-bucket meter Limiter backed by client
// using cfg. It validates cfg and returns an error without touching Redis if
// the configuration is invalid.
func NewLeakyBucket(client *redisx.Client, cfg LeakyBucketConfig) (Limiter, error) {
	if err := validateClient(client); err != nil {
		return nil, err
	}

	leakIntervalMs, err := cfg.validate()
	if err != nil {
		return nil, err
	}

	drainIntervals := int64(math.Ceil(float64(cfg.Capacity) / float64(cfg.LeakAmount)))
	ttl := cfg.LeakInterval * time.Duration(drainIntervals+1)
	ttlSeconds := int64(math.Ceil(ttl.Seconds()))
	if ttlSeconds < 1 {
		ttlSeconds = 1
	}

	return &leakyBucketLimiter{
		client:         client.Client(),
		prefix:         cfg.Prefix,
		capacity:       cfg.Capacity,
		leakAmount:     cfg.LeakAmount,
		leakIntervalMs: leakIntervalMs,
		ttlSeconds:     ttlSeconds,
		script:         redis.NewScript(leakyBucketScript),
	}, nil
}

// Allow attempts to add cost occupancy to the bucket identified by key. The
// first call for a key starts empty. Occupancy drains by elapsed Redis
// server time down to zero.
func (l *leakyBucketLimiter) Allow(ctx context.Context, key string, cost int64) (Result, error) {
	if err := validateKeyCost(key, cost, l.capacity); err != nil {
		return Result{}, err
	}

	bucketKey := l.prefix + ":" + key

	res, err := l.script.Run(ctx, l.client, []string{bucketKey},
		l.capacity, l.leakAmount, l.leakIntervalMs, cost, l.ttlSeconds,
	).Result()
	if err != nil {
		return Result{}, fmt.Errorf("ratelimitx: allow: %w", err)
	}

	return parseResult(res)
}
