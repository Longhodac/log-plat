package collector

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	logplatv1 "github.com/Longhodac/log-plat/gen/logplat/v1"
)

func TestValidate(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cases := map[string]struct {
		mutate func(*logplatv1.LogEntry)
		want   string
	}{
		"valid": {func(*logplatv1.LogEntry) {}, ""},
		"old event is fine": {func(e *logplatv1.LogEntry) {
			e.Timestamp = timestamppb.New(time.Date(2008, 11, 9, 0, 0, 0, 0, time.UTC))
		}, ""},
		"bad id":       {func(e *logplatv1.LogEntry) { e.Id = "nope" }, "invalid id"},
		"empty":        {func(e *logplatv1.LogEntry) { e.Message = "" }, "empty message"},
		"huge":         {func(e *logplatv1.LogEntry) { e.Message = strings.Repeat("x", MaxMessageBytes+1) }, "message too large"},
		"no timestamp": {func(e *logplatv1.LogEntry) { e.Timestamp = nil }, "missing or invalid timestamp"},
		"future":       {func(e *logplatv1.LogEntry) { e.Timestamp = timestamppb.New(now.Add(25 * time.Hour)) }, "timestamp too far in the future"},
		"level":        {func(e *logplatv1.LogEntry) { e.Level = 99 }, "unknown level"},
		"source":       {func(e *logplatv1.LogEntry) { e.Source = "" }, "missing source"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			e := entry(0)
			e.Timestamp, e.ObservedAt = timestamppb.New(now), timestamppb.New(now)
			c.mutate(e)
			if got := Validate(e, now); got != c.want {
				t.Errorf("Validate = %q, want %q", got, c.want)
			}
		})
	}
}
