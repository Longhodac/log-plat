package agent

import (
	"testing"
	"time"

	logplatv1 "github.com/Longhodac/log-plat/gen/logplat/v1"
)

func TestParseTimestampAndLevel(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name  string
		line  string
		ts    time.Time
		level logplatv1.Level
	}{
		{"hdfs", "081109 203615 148 INFO dfs.DataNode$PacketResponder: PacketResponder 1 terminating",
			time.Date(2008, 11, 9, 20, 36, 15, 0, time.UTC), logplatv1.Level_LEVEL_INFO},
		{"apache", "[Sun Dec 04 04:47:44 2005] [error] mod_jk child workerEnv in error state 6",
			time.Date(2005, 12, 4, 4, 47, 44, 0, time.UTC), logplatv1.Level_LEVEL_ERROR},
		{"log4j", "2015-10-18 18:01:47,978 WARN [main] org.apache.hadoop.Foo: retrying",
			time.Date(2015, 10, 18, 18, 1, 47, 978e6, time.UTC), logplatv1.Level_LEVEL_WARN},
		{"spark", "17/06/09 20:10:40 INFO executor.CoarseGrainedExecutorBackend: Registered",
			time.Date(2017, 6, 9, 20, 10, 40, 0, time.UTC), logplatv1.Level_LEVEL_INFO},
		{"rfc3339", "2026-02-28T23:59:59.5+01:00 level=debug msg=hi",
			time.Date(2026, 2, 28, 22, 59, 59, 5e8, time.UTC), logplatv1.Level_LEVEL_UNSPECIFIED},
		{"syslog previous year", "Dec 10 06:55:46 LabSZ sshd[24200]: error: Received disconnect",
			time.Date(2025, 12, 10, 6, 55, 46, 0, time.UTC), logplatv1.Level_LEVEL_ERROR},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ts, ok := ParseTimestamp(c.line, now)
			if !ok || !ts.Equal(c.ts) {
				t.Errorf("ParseTimestamp = %v, %v; want %v", ts, ok, c.ts)
			}
			if got := ParseLevel(c.line); got != c.level {
				t.Errorf("ParseLevel = %v, want %v", got, c.level)
			}
		})
	}
}

func TestParseRejectsUnknownAndIgnoresDeepKeywords(t *testing.T) {
	if _, ok := ParseTimestamp("hello world", time.Now()); ok {
		t.Error("parsed a timestamp out of plain text")
	}
	line := "a b c d e f g h this is an ERROR far into the message"
	if got := ParseLevel(line); got != logplatv1.Level_LEVEL_UNSPECIFIED {
		t.Errorf("ParseLevel picked %v from beyond the scan window", got)
	}
}
