package queuex

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Config configures a reliable job queue.
//
// Prefix and QueueName together namespace every Redis key the queue touches,
// so multiple queues can share one Redis database without colliding.
// VisibilityTimeout bounds how long a claimed job may run before the
// reclaimer considers the worker dead and returns the job to pending.
// WorkerCount bounds the number of handler invocations that run at once.
type Config struct {
	Prefix            string
	QueueName         string
	VisibilityTimeout time.Duration
	WorkerCount       int
}

// validate reports whether c holds a usable configuration.
func (c Config) validate() error {
	if strings.TrimSpace(c.QueueName) == "" {
		return errors.New("queuex: queue name must not be empty")
	}

	if c.VisibilityTimeout <= 0 {
		return fmt.Errorf("queuex: visibility timeout must be greater than zero: %s", c.VisibilityTimeout)
	}

	if c.WorkerCount <= 0 {
		return fmt.Errorf("queuex: worker count must be greater than zero: %d", c.WorkerCount)
	}

	return nil
}
