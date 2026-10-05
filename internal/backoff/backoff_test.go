package backoff

import (
	"testing"
	"time"
)

func TestCeilingGrowsAndCaps(t *testing.T) {
	p := Policy{Base: 100 * time.Millisecond, Max: time.Second}
	want := []time.Duration{100, 200, 400, 800, 1000, 1000}
	for i, w := range want {
		if got := p.Ceiling(i); got != w*time.Millisecond {
			t.Errorf("Ceiling(%d) = %v, want %v", i, got, w*time.Millisecond)
		}
	}
	if got := p.Ceiling(1000); got != time.Second {
		t.Errorf("Ceiling(1000) = %v, want cap without overflow", got)
	}
}

func TestDelayIsJitteredWithinCeiling(t *testing.T) {
	p := Policy{Base: time.Millisecond, Max: time.Second}
	seen := map[time.Duration]bool{}
	for range 200 {
		d := p.Delay(5)
		if d < 0 || d > p.Ceiling(5) {
			t.Fatalf("Delay(5) = %v outside [0, %v]", d, p.Ceiling(5))
		}
		seen[d] = true
	}
	if len(seen) < 50 {
		t.Errorf("only %d distinct delays in 200 draws; jitter looks broken", len(seen))
	}
}
