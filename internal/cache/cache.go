// Package cache is a small byte cache in Redis with an expiry on every entry.
package cache

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis stores values under a key prefix.
type Redis struct {
	Client redis.Cmdable
	Prefix string
}

// Get returns the value for key. A missing key is (nil, false, nil).
func (c Redis) Get(ctx context.Context, key string) ([]byte, bool, error) {
	v, err := c.Client.Get(ctx, c.Prefix+key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return v, true, nil
}

// Set stores value for ttl.
func (c Redis) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return c.Client.Set(ctx, c.Prefix+key, value, ttl).Err()
}

// NewClient returns a Redis client for the cache and the rate limiter. Both
// fail open, so the timeouts are short: a dead Redis should cost a request a
// fraction of a second, not the client's default of several.
func NewClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         addr,
		DialTimeout:  250 * time.Millisecond,
		ReadTimeout:  250 * time.Millisecond,
		WriteTimeout: 250 * time.Millisecond,
		MaxRetries:   1,
	})
}
