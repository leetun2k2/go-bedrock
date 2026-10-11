// Package ratelimitx provides distributed rate limiters backed by Redis:
// fixed window, token bucket, leaky bucket meter, and sliding window
// counter. Each algorithm is selected through an explicit constructor and
// shares one Limiter contract. Every decision runs in one atomic Lua script
// keyed off Redis server time, so concurrent callers never overspend the
// same capacity.
package ratelimitx

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/leetun2k2/go-bedrock/redisx"
)

// Result reports the outcome of an Allow call.
type Result struct {
	// Allowed reports whether the requested cost was admitted.
	Allowed bool
	// Remaining is the number of units still available at the current instant.
	Remaining int64
	// RetryAfter is the earliest wait after which the same cost may pass,
	// assuming no other accepted request. It is zero when Allowed is true.
	RetryAfter time.Duration
}

// Limiter decides whether a weighted request may proceed. Implementations
// are safe for concurrent use and do not own the Redis connection they
// borrow.
type Limiter interface {
	// Allow attempts to admit cost units for key. A cost above the
	// algorithm's configured limit or capacity returns a validation error
	// without touching Redis.
	Allow(ctx context.Context, key string, cost int64) (Result, error)
}

// validateClient reports whether client is usable.
func validateClient(client *redisx.Client) error {
	if client == nil {
		return errors.New("ratelimitx: client must not be nil")
	}
	return nil
}

// validatePrefix reports whether prefix is usable as a key namespace.
func validatePrefix(prefix string) error {
	if strings.TrimSpace(prefix) == "" {
		return errors.New("ratelimitx: prefix must not be empty")
	}
	return nil
}

// validatePositive reports whether v is a usable positive quantity for a
// field named name.
func validatePositive(name string, v int64) error {
	if v <= 0 {
		return fmt.Errorf("ratelimitx: %s must be greater than zero: %d", name, v)
	}
	return nil
}

// validateIntervalMs reports whether d is a usable whole-millisecond
// interval for a field named name, and returns it in milliseconds.
// Sub-millisecond intervals are rejected rather than truncated, since
// truncation would silently make a limiter more permissive.
func validateIntervalMs(name string, d time.Duration) (int64, error) {
	if d < time.Millisecond {
		return 0, fmt.Errorf("ratelimitx: %s must be at least 1ms: %s", name, d)
	}

	if d%time.Millisecond != 0 {
		return 0, fmt.Errorf("ratelimitx: %s must be aligned to a millisecond: %s", name, d)
	}

	return d.Milliseconds(), nil
}

// validateKeyCost reports whether key and cost are usable for an Allow call
// against a limit or capacity of max.
func validateKeyCost(key string, cost, max int64) error {
	if strings.TrimSpace(key) == "" {
		return errors.New("ratelimitx: key must not be empty")
	}

	if cost <= 0 {
		return fmt.Errorf("ratelimitx: cost must be greater than zero: %d", cost)
	}

	if cost > max {
		return fmt.Errorf("ratelimitx: cost %d exceeds limit %d", cost, max)
	}

	return nil
}

// toInt64 converts a script reply element to int64.
func toInt64(v interface{}) (int64, error) {
	i, ok := v.(int64)
	if !ok {
		return 0, fmt.Errorf("expected int64, got %T", v)
	}
	return i, nil
}

// parseResult converts a three-element Lua script reply into a Result.
func parseResult(res interface{}) (Result, error) {
	values, ok := res.([]interface{})
	if !ok || len(values) != 3 {
		return Result{}, fmt.Errorf("ratelimitx: allow: unexpected script result: %v", res)
	}

	allowed, err := toInt64(values[0])
	if err != nil {
		return Result{}, fmt.Errorf("ratelimitx: allow: %w", err)
	}

	remaining, err := toInt64(values[1])
	if err != nil {
		return Result{}, fmt.Errorf("ratelimitx: allow: %w", err)
	}

	retryAfterMs, err := toInt64(values[2])
	if err != nil {
		return Result{}, fmt.Errorf("ratelimitx: allow: %w", err)
	}

	return Result{
		Allowed:    allowed == 1,
		Remaining:  remaining,
		RetryAfter: time.Duration(retryAfterMs) * time.Millisecond,
	}, nil
}
