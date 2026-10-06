// Package ratelimit is a token bucket per key, kept in Redis so that every
// replica of a service draws from the same bucket.
package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
)

var decisions = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "logplat_ratelimit_decisions_total",
	Help: "Rate limit decisions by component and result: allowed, limited, or error (Redis failed and the request was let through).",
}, []string{"component", "result"})

// bucketScript refills and spends atomically in one Redis round trip. It reads
// the clock from Redis, so callers with skewed clocks cannot gain tokens. The
// key expires once the bucket would be full again, so idle keys cost nothing.
//
// ARGV: rate (tokens/s), capacity, cost. Returns {allowed, wait_ms}.
var bucketScript = redis.NewScript(`
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local rate, cap, cost = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local d = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens, ts = tonumber(d[1]), tonumber(d[2])
if tokens == nil then tokens = cap; ts = now end
tokens = math.min(cap, tokens + math.max(0, now - ts) * rate / 1000)
local allowed, wait = 0, 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  wait = math.ceil((cost - tokens) * 1000 / rate)
end
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', now)
redis.call('PEXPIRE', KEYS[1], math.ceil(cap / rate * 1000) + 1000)
return {allowed, wait}
`)

// Limiter spends tokens from per-key buckets.
type Limiter struct {
	rdb       redis.Scripter
	component string
	prefix    string
	rate      float64 // tokens per second
	burst     int     // bucket capacity
}

// New returns a Limiter. rate is tokens per second and burst is the bucket
// size, the most a key can spend at once after being idle.
func New(rdb redis.Scripter, component string, rate float64, burst int) (*Limiter, error) {
	if rate <= 0 || burst < 1 {
		return nil, fmt.Errorf("ratelimit: need rate > 0 and burst >= 1, got rate=%v burst=%d", rate, burst)
	}
	return &Limiter{rdb: rdb, component: component, prefix: "logplat:rl:" + component + ":", rate: rate, burst: burst}, nil
}

// Allow tries to spend cost tokens from key's bucket. When it cannot, it
// returns how long until the bucket holds enough. A cost above the burst is
// clamped to the burst, so an oversized request waits for a full bucket
// instead of never succeeding.
//
// If Redis fails, Allow lets the request through and returns the error for the
// caller to log. Blocking all traffic because the limiter is down is worse
// than briefly not limiting.
func (l *Limiter) Allow(ctx context.Context, key string, cost int) (allowed bool, retryAfter time.Duration, err error) {
	cost = min(max(cost, 1), l.burst)
	res, err := bucketScript.Run(ctx, l.rdb, []string{l.prefix + key}, l.rate, l.burst, cost).Int64Slice()
	if err != nil {
		decisions.WithLabelValues(l.component, "error").Inc()
		return true, 0, err
	}
	if len(res) != 2 {
		decisions.WithLabelValues(l.component, "error").Inc()
		return true, 0, errors.New("ratelimit: unexpected script reply")
	}
	if res[0] == 1 {
		decisions.WithLabelValues(l.component, "allowed").Inc()
		return true, 0, nil
	}
	decisions.WithLabelValues(l.component, "limited").Inc()
	return false, time.Duration(res[1]) * time.Millisecond, nil
}

// Wait blocks until cost tokens are spent or ctx ends. It returns the total
// time spent waiting. A Redis error ends the wait with the request allowed.
func (l *Limiter) Wait(ctx context.Context, key string, cost int) (time.Duration, error) {
	start := time.Now()
	for {
		ok, retry, err := l.Allow(ctx, key, cost)
		if ok {
			return time.Since(start), err
		}
		t := time.NewTimer(max(retry, time.Millisecond))
		select {
		case <-ctx.Done():
			t.Stop()
			return time.Since(start), ctx.Err()
		case <-t.C:
		}
	}
}
