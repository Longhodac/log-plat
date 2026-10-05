// Package backoff computes retry delays with exponential growth and full jitter.
package backoff

import (
	"context"
	"math/rand/v2"
	"time"
)

// Policy doubles the ceiling on each attempt up to Max and sleeps a uniformly
// random duration below it ("full jitter"), so a fleet of agents that lost
// the collector at the same moment does not reconnect in lockstep.
type Policy struct {
	Base time.Duration
	Max  time.Duration
}

// Default suits reconnecting to a collector or retrying a bulk write.
var Default = Policy{Base: 100 * time.Millisecond, Max: 30 * time.Second}

// Ceiling is the upper bound of the delay for a zero-based attempt.
func (p Policy) Ceiling(attempt int) time.Duration {
	d := p.Base
	for i := 0; i < attempt && d < p.Max; i++ {
		d *= 2
	}
	return min(d, p.Max)
}

// Delay returns a random duration in [0, Ceiling(attempt)].
func (p Policy) Delay(attempt int) time.Duration {
	return time.Duration(rand.Int64N(int64(p.Ceiling(attempt)) + 1)) //nolint:gosec // jitter needs spread, not unpredictability
}

// Sleep waits Delay(attempt) or until ctx is done, returning ctx.Err() if cut short.
func (p Policy) Sleep(ctx context.Context, attempt int) error {
	t := time.NewTimer(p.Delay(attempt))
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Retry calls fn until it succeeds or ctx is done, sleeping between attempts.
// onErr sees each failure, for logging.
func (p Policy) Retry(ctx context.Context, fn func() error, onErr func(attempt int, err error)) error {
	for attempt := 0; ; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		onErr(attempt, err)
		if err := p.Sleep(ctx, attempt); err != nil {
			return err
		}
	}
}
