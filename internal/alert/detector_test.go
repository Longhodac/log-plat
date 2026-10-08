package alert

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

// testConfig: a 10 s window against the 60 s before it.
func testConfig() Config {
	return Config{Window: 10 * time.Second, Bucket: time.Second, Baseline: 60 * time.Second, MinErrors: 10, Ratio: 5, Cooldown: 30 * time.Second, Samples: 3}
}

func newDet() (*Detector, *clock) {
	c := &clock{t: time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)}
	return NewDetector(testConfig(), c.now), c
}

// errors adds n errors for svc at the current time.
func errorsNow(d *Detector, c *clock, svc string, n int) {
	for i := range n {
		d.Observe(svc, "error", c.now(), fmt.Sprintf("failure %d", i))
	}
}

// steady adds `perSec` errors every second for secs seconds, advancing the clock.
func steady(d *Detector, c *clock, svc string, perSec, secs int) {
	for range secs {
		errorsNow(d, c, svc, perSec)
		c.advance(time.Second)
	}
}

func kinds(as []Alert) string {
	var s []string
	for _, a := range as {
		s = append(s, a.Service+":"+string(a.Kind))
	}
	return strings.Join(s, ",")
}

func TestSpikeFiresOnceAndCarriesTheNumbersAndSamples(t *testing.T) {
	d, c := newDet()
	steady(d, c, "hdfs", 0, 60) // a quiet minute establishes the baseline
	for i := range 6 {
		d.Observe("hdfs", "error", c.now(), fmt.Sprintf("disk %d failed", i))
	}
	steady(d, c, "hdfs", 3, 5) // 6 + 15 = 21 errors inside the window
	got := d.Evaluate()
	if len(got) != 1 || got[0].Kind != Firing || got[0].Service != "hdfs" {
		t.Fatalf("alerts = %v, want one firing for hdfs", kinds(got))
	}
	if got[0].Errors != 6+15 || got[0].Usual != 0 || got[0].Window != 10*time.Second {
		t.Errorf("alert = %+v; want 21 errors, usual 0, window 10s", got[0])
	}
	if len(got[0].Samples) != 3 || !strings.HasPrefix(got[0].Samples[2], "failure") {
		t.Errorf("samples = %q, want the 3 most recent messages", got[0].Samples)
	}
}

func TestSustainedSpikeAlertsOnceThenResolvesOnce(t *testing.T) {
	d, c := newDet()
	steady(d, c, "api", 0, 60)
	steady(d, c, "api", 4, 6) // 24 errors in the window
	if got := d.Evaluate(); kinds(got) != "api:firing" {
		t.Fatalf("first evaluation = %q, want api:firing", kinds(got))
	}
	for range 10 { // the spike continues for another 10 evaluations
		steady(d, c, "api", 4, 1)
		if got := d.Evaluate(); len(got) != 0 {
			t.Fatalf("a sustained spike alerted again: %v", kinds(got))
		}
	}
	steady(d, c, "api", 0, 11) // errors stop; the window empties
	if got := d.Evaluate(); kinds(got) != "api:resolved" {
		t.Fatalf("after the errors stopped got %q, want api:resolved", kinds(got))
	}
	if got := d.Evaluate(); len(got) != 0 {
		t.Errorf("resolved fired twice: %v", kinds(got))
	}
}

func TestCooldownHoldsBackAnImmediateSecondSpike(t *testing.T) {
	d, c := newDet()
	steady(d, c, "api", 0, 60)
	steady(d, c, "api", 5, 5)
	d.Evaluate() // firing
	steady(d, c, "api", 0, 11)
	if got := d.Evaluate(); kinds(got) != "api:resolved" {
		t.Fatalf("got %q, want resolved", kinds(got))
	}
	steady(d, c, "api", 20, 1) // a new spike 12 s after the first alert
	if got := d.Evaluate(); len(got) != 0 {
		t.Fatalf("inside the cooldown got %v, want nothing", kinds(got))
	}
	c.advance(30 * time.Second)
	// The two earlier spikes now sit in the baseline, so the third must clear a
	// raised bar: 45 errors over 60 s is 7.5 per window, times a ratio of 5.
	steady(d, c, "api", 20, 1)
	if got := d.Evaluate(); len(got) != 0 {
		t.Fatalf("a spike no bigger than the recent ones alerted: %v", kinds(got))
	}
	steady(d, c, "api", 60, 1)
	if got := d.Evaluate(); kinds(got) != "api:firing" {
		t.Errorf("after the cooldown a clearly larger spike got %q, want a new firing alert", kinds(got))
	}
}

func TestSmallBurstsNeverAlertEvenAgainstAnEmptyBaseline(t *testing.T) {
	d, c := newDet()
	steady(d, c, "rare", 0, 60)
	errorsNow(d, c, "rare", 9) // one under MinErrors, infinitely more than a baseline of zero
	if got := d.Evaluate(); len(got) != 0 {
		t.Errorf("9 errors alerted: %v", kinds(got))
	}
}

func TestANoisyServiceMustExceedItsOwnNormal(t *testing.T) {
	d, c := newDet()
	steady(d, c, "noisy", 3, 70) // about 30 errors per 10 s window, all the time
	if got := d.Evaluate(); len(got) != 0 {
		t.Fatalf("a steady 30 per window alerted: %v", kinds(got))
	}
	steady(d, c, "noisy", 6, 3) // 30*5=150 would be needed; this is only modestly higher
	if got := d.Evaluate(); len(got) != 0 {
		t.Fatalf("a modest rise on a noisy service alerted: %v", kinds(got))
	}
	steady(d, c, "noisy", 60, 3)
	got := d.Evaluate()
	if kinds(got) != "noisy:firing" || got[0].Usual < 10 {
		t.Errorf("a 20x jump got %v (usual %.0f), want firing with a nonzero usual", kinds(got), got[0].Usual)
	}
}

func TestServicesAreIndependentAndNonErrorsAreIgnored(t *testing.T) {
	d, c := newDet()
	steady(d, c, "a", 0, 60)
	errorsNow(d, c, "a", 30)
	for range 100 {
		d.Observe("b", "warn", c.now(), "just a warning")
		d.Observe("b", "info", c.now(), "fine")
	}
	errorsNow(d, c, "c", 2)
	if got := d.Evaluate(); kinds(got) != "a:firing" {
		t.Errorf("got %q, want only a:firing", kinds(got))
	}
	if d.Services() != 2 {
		t.Errorf("tracking %d services, want a and c only", d.Services())
	}
}

func TestOldAndFutureEntriesAreHandledSafely(t *testing.T) {
	d, c := newDet()
	for range 500 { // a replayed backlog from an hour ago
		d.Observe("old", "error", c.now().Add(-time.Hour), "ancient")
	}
	if got := d.Evaluate(); len(got) != 0 || d.Services() != 0 {
		t.Fatalf("an old backlog created state or alerts: %v, %d services", kinds(got), d.Services())
	}
	for range 30 { // a producer whose clock is a day ahead
		d.Observe("skewed", "fatal", c.now().Add(24*time.Hour), "from the future")
	}
	if got := d.Evaluate(); kinds(got) != "skewed:firing" {
		t.Errorf("future-dated errors got %q; they should count as happening now", kinds(got))
	}
}

func TestStateIsReleasedOnceAServiceGoesQuiet(t *testing.T) {
	d, c := newDet()
	errorsNow(d, c, "brief", 3)
	d.Evaluate()
	c.advance(2 * time.Minute)
	d.Evaluate()
	if d.Services() != 0 {
		t.Errorf("still tracking %d services long after their errors aged out", d.Services())
	}
}
