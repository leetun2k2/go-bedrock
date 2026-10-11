package ratelimitx

import (
	"context"
	"fmt"
	"math"

	"github.com/leetun2k2/go-bedrock/redisx"
	"github.com/redis/go-redis/v9"
)

// slidingWindowCounterScript estimates the active rolling window from the
// current and previous epoch-aligned window counters, weighting the
// previous counter by the unelapsed fraction of the current window and
// rounding that weighted contribution up to stay conservative. KEYS[1] is
// the counter key. ARGV: limit, windowMs, cost, ttlSeconds. Returns
// {allowed (0/1), remaining, retryAfterMs}.
const slidingWindowCounterScript = `
local limit = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local ttl_seconds = tonumber(ARGV[4])

local time_parts = redis.call('TIME')
local now_ms = tonumber(time_parts[1]) * 1000 + math.floor(tonumber(time_parts[2]) / 1000)

local window_id = math.floor(now_ms / window_ms)
local window_start = window_id * window_ms
local elapsed = now_ms - window_start

local stored = redis.call('HMGET', KEYS[1], 'curr_count', 'curr_id', 'prev_count')

local curr_count = 0
local prev_count = 0

if stored[2] then
	local stored_curr_id = tonumber(stored[2])
	if stored_curr_id == window_id then
		curr_count = tonumber(stored[1])
		prev_count = tonumber(stored[3])
	elseif stored_curr_id == window_id - 1 then
		prev_count = tonumber(stored[1])
	end
end

local unelapsed_fraction = (window_ms - elapsed) / window_ms
local weighted_prev = math.ceil(prev_count * unelapsed_fraction)
local estimate = weighted_prev + curr_count

local allowed = 0
local retry_after_ms = 0

if estimate + cost <= limit then
	curr_count = curr_count + cost
	allowed = 1
else
	retry_after_ms = (window_start + window_ms) - now_ms
end

redis.call('HMSET', KEYS[1], 'curr_count', curr_count, 'curr_id', window_id, 'prev_count', prev_count)
redis.call('EXPIRE', KEYS[1], ttl_seconds)

local remaining = limit - (weighted_prev + curr_count)
if remaining < 0 then
	remaining = 0
end

return {allowed, remaining, retry_after_ms}
`

// slidingWindowCounterLimiter is a distributed sliding-window-counter rate
// limiter.
type slidingWindowCounterLimiter struct {
	client     *redis.Client
	prefix     string
	limit      int64
	windowMs   int64
	ttlSeconds int64
	script     *redis.Script
}

// NewSlidingWindowCounter constructs a sliding-window-counter Limiter backed
// by client using cfg. It validates cfg and returns an error without
// touching Redis if the configuration is invalid.
func NewSlidingWindowCounter(client *redisx.Client, cfg SlidingWindowCounterConfig) (Limiter, error) {
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

	return &slidingWindowCounterLimiter{
		client:     client.Client(),
		prefix:     cfg.Prefix,
		limit:      cfg.Limit,
		windowMs:   windowMs,
		ttlSeconds: ttlSeconds,
		script:     redis.NewScript(slidingWindowCounterScript),
	}, nil
}

// Allow attempts to admit cost against the conservative rolling-window
// estimate for key, combining the current and previous epoch-aligned window
// counters.
func (l *slidingWindowCounterLimiter) Allow(ctx context.Context, key string, cost int64) (Result, error) {
	if err := validateKeyCost(key, cost, l.limit); err != nil {
		return Result{}, err
	}

	counterKey := l.prefix + ":" + key

	res, err := l.script.Run(ctx, l.client, []string{counterKey},
		l.limit, l.windowMs, cost, l.ttlSeconds,
	).Result()
	if err != nil {
		return Result{}, fmt.Errorf("ratelimitx: allow: %w", err)
	}

	return parseResult(res)
}
