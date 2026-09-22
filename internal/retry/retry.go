// Package retry provides bounded exponential backoff retry loops for transient
// infrastructure failures such as a Kafka outage or a brief database restart.
package retry

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"
)

// Config controls how many attempts a retry loop makes and how long it waits
// between them. Backoffs grow exponentially and shrink randomly by up to 20% so
// that several failing processes do not hammer the recovering service in
// lockstep.
type Config struct {
	Base       time.Duration
	Max        time.Duration
	MaxRetries int
}

// Default waits 100ms to 2s across 20 attempts, covering roughly 3.5 minutes of
// an outage before control reverts to the caller.
var Default = Config{Base: 100 * time.Millisecond, Max: 2 * time.Second, MaxRetries: 20}

// Do invokes fn repeatedly until it returns nil, waiting between attempts with
// exponential backoff. If the context ends during a wait, the context error is
// returned. If every attempt fails, the last error is returned.
func Do(ctx context.Context, cfg Config, operation string, fn func() error) error {
	var lastErr error
	for attempt := 1; ; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		lastErr = err
		if attempt >= cfg.MaxRetries {
			return fmt.Errorf("%s: %w", operation, lastErr)
		}
		delay := cfg.Backoff(attempt)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

// Backoff returns the wait before the attempt with the given 1-based number.
func (c Config) Backoff(attempt int) time.Duration {
	delay := c.baseBackoff(attempt)
	shrink := rand.Int64N(int64(delay/5 + 1))
	delay -= time.Duration(shrink)
	if delay < time.Millisecond {
		delay = time.Millisecond
	}
	return delay
}

// baseBackoff returns the exponential wait for the attempt before jitter is
// applied. It doubles each attempt, capped at Max.
func (c Config) baseBackoff(attempt int) time.Duration {
	delay := c.Base
	for i := 1; i < attempt; i++ {
		delay *= 2
		if delay >= c.Max {
			return c.Max
		}
	}
	if delay > c.Max {
		return c.Max
	}
	return delay
}