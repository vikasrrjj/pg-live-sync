package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDoSuccessOnFirstAttempt(t *testing.T) {
	calls := 0
	err := Do(context.Background(), Default, "probe", func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func TestDoExhaustsRetries(t *testing.T) {
	cfg := Config{Base: time.Millisecond, Max: 2 * time.Millisecond, MaxRetries: 3}
	sentinel := errors.New("boom")
	calls := 0
	err := Do(context.Background(), cfg, "probe", func() error {
		calls++
		return sentinel
	})
	if err == nil {
		t.Fatal("expected an error")
	}
	if calls != cfg.MaxRetries {
		t.Fatalf("expected %d calls, got %d", cfg.MaxRetries, calls)
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected wrapped last error, got %v", err)
	}
}

func TestDoRecoversMidRetries(t *testing.T) {
	cfg := Config{Base: time.Millisecond, Max: 2 * time.Millisecond, MaxRetries: 5}
	calls := 0
	err := Do(context.Background(), cfg, "probe", func() error {
		calls++
		if calls < 3 {
			return errors.New("not yet")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected recovery, got %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestDoHonoursContextCancellation(t *testing.T) {
	cfg := Config{Base: time.Hour, Max: time.Hour, MaxRetries: 10}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := Do(ctx, cfg, "probe", func() error { return errors.New("boom") })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
}

func TestBackoffBaseIsBoundedAndGrowing(t *testing.T) {
	cfg := Config{Base: 10 * time.Millisecond, Max: 100 * time.Millisecond, MaxRetries: 100}
	previous := time.Duration(0)
	for attempt := 1; attempt <= 40; attempt++ {
		base := cfg.baseBackoff(attempt)
		if base < cfg.Base {
			t.Fatalf("attempt %d: base %v below configured base", attempt, base)
		}
		if base > cfg.Max {
			t.Fatalf("attempt %d: base %v above max", attempt, base)
		}
		if attempt > 1 && base < previous {
			t.Fatalf("attempt %d: base %v shrank below previous %v", attempt, base, previous)
		}
		previous = base
	}
}

func TestBackoffJitterStaysWithinBounds(t *testing.T) {
	cfg := Config{Base: 10 * time.Millisecond, Max: 100 * time.Millisecond, MaxRetries: 100}
	for attempt := 1; attempt <= 40; attempt++ {
		base := cfg.baseBackoff(attempt)
		for i := 0; i < 50; i++ {
			delay := cfg.Backoff(attempt)
			if delay < time.Millisecond {
				t.Fatalf("attempt %d: backoff %v below floor", attempt, delay)
			}
			if delay > base {
				t.Fatalf("attempt %d: backoff %v above base %v", attempt, delay, base)
			}
		}
	}
}