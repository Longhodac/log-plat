// Package envcfg reads typed configuration from environment variables and
// collects every problem so a misconfigured service reports them all at once.
package envcfg

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Reader accumulates parse errors. Check Err once after reading every field.
type Reader struct {
	errs []error
}

func (r *Reader) lookup(key string) (string, bool) {
	v, ok := os.LookupEnv(key)
	return strings.TrimSpace(v), ok && strings.TrimSpace(v) != ""
}

// String returns the value of key, or def when unset.
func (r *Reader) String(key, def string) string {
	if v, ok := r.lookup(key); ok {
		return v
	}
	return def
}

// Required returns the value of key and records an error when it is unset.
func (r *Reader) Required(key string) string {
	v, ok := r.lookup(key)
	if !ok {
		r.errs = append(r.errs, fmt.Errorf("%s is required", key))
	}
	return v
}

// List splits a comma-separated value, dropping empty items.
func (r *Reader) List(key string, def []string) []string {
	v, ok := r.lookup(key)
	if !ok {
		return def
	}
	var out []string
	for _, s := range strings.Split(v, ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// Int returns key as an int, or def when unset.
func (r *Reader) Int(key string, def int) int {
	v, ok := r.lookup(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: %w", key, err))
	}
	return n
}

// Duration returns key parsed by time.ParseDuration, or def when unset.
func (r *Reader) Duration(key string, def time.Duration) time.Duration {
	v, ok := r.lookup(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: %w", key, err))
	}
	return d
}

// Err returns every error recorded so far.
func (r *Reader) Err() error { return errors.Join(r.errs...) }
