package ratelimitx

import "time"

// FixedWindowConfig configures a fixed-window limiter. Accepted cost counts
// against Limit within the current epoch-aligned Window; capacity resets at
// the next window boundary.
type FixedWindowConfig struct {
	Prefix string
	Limit  int64
	Window time.Duration
}

func (c FixedWindowConfig) validate() (windowMs int64, err error) {
	if err := validatePrefix(c.Prefix); err != nil {
		return 0, err
	}

	if err := validatePositive("limit", c.Limit); err != nil {
		return 0, err
	}

	return validateIntervalMs("window", c.Window)
}

// TokenBucketConfig configures a token-bucket limiter. A bucket starts full
// at Capacity, gains RefillAmount tokens after each complete
// RefillInterval, and never exceeds Capacity.
type TokenBucketConfig struct {
	Prefix         string
	Capacity       int64
	RefillAmount   int64
	RefillInterval time.Duration
}

func (c TokenBucketConfig) validate() (refillIntervalMs int64, err error) {
	if err := validatePrefix(c.Prefix); err != nil {
		return 0, err
	}

	if err := validatePositive("capacity", c.Capacity); err != nil {
		return 0, err
	}

	if err := validatePositive("refill amount", c.RefillAmount); err != nil {
		return 0, err
	}

	return validateIntervalMs("refill interval", c.RefillInterval)
}

// LeakyBucketConfig configures a leaky-bucket meter limiter. A bucket starts
// empty at zero occupancy, drains LeakAmount after each complete
// LeakInterval down to zero, and admits cost only up to Capacity.
type LeakyBucketConfig struct {
	Prefix       string
	Capacity     int64
	LeakAmount   int64
	LeakInterval time.Duration
}

func (c LeakyBucketConfig) validate() (leakIntervalMs int64, err error) {
	if err := validatePrefix(c.Prefix); err != nil {
		return 0, err
	}

	if err := validatePositive("capacity", c.Capacity); err != nil {
		return 0, err
	}

	if err := validatePositive("leak amount", c.LeakAmount); err != nil {
		return 0, err
	}

	return validateIntervalMs("leak interval", c.LeakInterval)
}

// SlidingWindowCounterConfig configures a sliding-window-counter limiter. It
// estimates the active rolling window from the current and previous
// epoch-aligned Window counters and admits only when the conservative
// estimate plus cost does not exceed Limit.
type SlidingWindowCounterConfig struct {
	Prefix string
	Limit  int64
	Window time.Duration
}

func (c SlidingWindowCounterConfig) validate() (windowMs int64, err error) {
	if err := validatePrefix(c.Prefix); err != nil {
		return 0, err
	}

	if err := validatePositive("limit", c.Limit); err != nil {
		return 0, err
	}

	return validateIntervalMs("window", c.Window)
}
