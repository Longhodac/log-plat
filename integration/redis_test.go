//go:build integration

package integration

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Longhodac/log-plat/internal/cache"
	"github.com/Longhodac/log-plat/internal/ratelimit"
)

func newRedis(t *testing.T) *redis.Client {
	t.Helper()
	c := cache.NewClient(redisAddr)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func limiter(t *testing.T, rdb *redis.Client, rate float64, burst int) *ratelimit.Limiter {
	t.Helper()
	l, err := ratelimit.New(rdb, uniq(t), rate, burst)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func TestBucketAllowsABurstThenLimitsWithAUsableRetryAfter(t *testing.T) {
	l := limiter(t, newRedis(t), 10, 5)
	ctx := context.Background()
	for i := range 5 {
		if ok, _, err := l.Allow(ctx, "svc", 1); !ok || err != nil {
			t.Fatalf("request %d of the burst: allowed=%v err=%v", i+1, ok, err)
		}
	}
	ok, retry, err := l.Allow(ctx, "svc", 1)
	if ok || err != nil {
		t.Fatalf("request past the burst: allowed=%v err=%v; want limited", ok, err)
	}
	if retry < 50*time.Millisecond || retry > 150*time.Millisecond {
		t.Errorf("retry-after %v, want about 100ms (one token at 10/s)", retry)
	}
	time.Sleep(retry + 30*time.Millisecond)
	if ok, _, _ := l.Allow(ctx, "svc", 1); !ok {
		t.Error("still limited after waiting the advised retry-after")
	}
}

func TestBucketsAreIndependentPerKey(t *testing.T) {
	l := limiter(t, newRedis(t), 0.001, 3)
	ctx := context.Background()
	for range 3 {
		if ok, _, _ := l.Allow(ctx, "noisy", 1); !ok {
			t.Fatal("noisy was limited inside its burst")
		}
	}
	if ok, _, _ := l.Allow(ctx, "noisy", 1); ok {
		t.Fatal("noisy was not limited past its burst")
	}
	if ok, _, _ := l.Allow(ctx, "quiet", 1); !ok {
		t.Error("quiet was limited because noisy used its budget")
	}
}

// Two Limiter values on separate connections stand in for two replicas of a
// service. Together they must hand out exactly the burst and no more.
func TestReplicasShareOneBudgetExactly(t *testing.T) {
	name := uniq(t)
	mk := func() *ratelimit.Limiter {
		l, err := ratelimit.New(newRedis(t), name, 0.001, 50)
		if err != nil {
			t.Fatal(err)
		}
		return l
	}
	replicas := []*ratelimit.Limiter{mk(), mk()}
	var allowed atomic.Int32
	var wg sync.WaitGroup
	for i := range 400 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ok, _, err := replicas[i%2].Allow(context.Background(), "svc", 1); err != nil {
				t.Error(err)
			} else if ok {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()
	if allowed.Load() != 50 {
		t.Errorf("400 concurrent requests across 2 replicas: %d allowed, want exactly the burst of 50", allowed.Load())
	}
}

func TestOversizedCostIsClampedToTheBurst(t *testing.T) {
	l := limiter(t, newRedis(t), 0.001, 10)
	ctx := context.Background()
	if ok, _, err := l.Allow(ctx, "svc", 1000); !ok || err != nil {
		t.Fatalf("a cost above the burst on a full bucket: allowed=%v err=%v; it must not be refused forever", ok, err)
	}
	if ok, _, _ := l.Allow(ctx, "svc", 1); ok {
		t.Error("the clamped request should have emptied the bucket")
	}
}

func TestWaitPacesToTheConfiguredRate(t *testing.T) {
	l := limiter(t, newRedis(t), 100, 10)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	for range 30 {
		if _, err := l.Wait(ctx, "svc", 1); err != nil {
			t.Fatal(err)
		}
	}
	// 10 come from the burst and 20 more at 100/s take about 200ms.
	if took := time.Since(start); took < 150*time.Millisecond || took > 1500*time.Millisecond {
		t.Errorf("30 tokens took %v, want about 200ms", took)
	}
}

func TestWaitStopsWhenTheContextEnds(t *testing.T) {
	l := limiter(t, newRedis(t), 0.01, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := l.Wait(ctx, "svc", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Wait(ctx, "svc", 1); err == nil {
		t.Error("Wait returned success on an empty bucket that refills in 100s")
	}
}

func TestIdleBucketsExpire(t *testing.T) {
	rdb := newRedis(t)
	name := uniq(t)
	l, err := ratelimit.New(rdb, name, 10, 5)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := l.Allow(context.Background(), "svc", 1); err != nil {
		t.Fatal(err)
	}
	ttl, err := rdb.PTTL(context.Background(), "logplat:rl:"+name+":svc").Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl <= 0 || ttl > 2*time.Second {
		t.Errorf("bucket TTL %v, want a short positive expiry so idle keys disappear", ttl)
	}
}

func TestDeadRedisFailsOpen(t *testing.T) {
	dead := cache.NewClient("127.0.0.1:1")
	defer dead.Close()
	l, err := ratelimit.New(dead, "dead", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	ok, _, err := l.Allow(context.Background(), "svc", 1)
	if !ok || err == nil {
		t.Fatalf("allowed=%v err=%v; want the request let through and the failure reported", ok, err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("a dead Redis cost %v per request, want well under the client's default timeouts", took)
	}
}

func TestCacheStoresAndExpires(t *testing.T) {
	c := cache.Redis{Client: newRedis(t), Prefix: uniq(t) + ":"}
	ctx := context.Background()
	if _, ok, err := c.Get(ctx, "k"); ok || err != nil {
		t.Fatalf("missing key: ok=%v err=%v", ok, err)
	}
	if err := c.Set(ctx, "k", []byte("v"), 400*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if v, ok, err := c.Get(ctx, "k"); !ok || err != nil || string(v) != "v" {
		t.Fatalf("stored key: %q ok=%v err=%v", v, ok, err)
	}
	time.Sleep(600 * time.Millisecond)
	if _, ok, _ := c.Get(ctx, "k"); ok {
		t.Error("entry outlived its TTL")
	}
}
