package agent

import (
	"strings"
	"time"

	logplatv1 "github.com/Longhodac/log-plat/gen/logplat/v1"
)

// timeFormats are the leading timestamp shapes found in Loghub datasets and
// common application logs. Zone-less formats are read as UTC.
var timeFormats = []struct {
	layout string
	start  int // byte offset of the timestamp in the line
	noYear bool
}{
	{layout: "2006-01-02 15:04:05,000"},            // Hadoop, Zookeeper, log4j
	{layout: "2006-01-02 15:04:05.000"},            // OpenStack, many Go/Java apps
	{layout: "2006-01-02 15:04:05"},                // plain SQL-style
	{layout: "060102 150405"},                      // HDFS_v1
	{layout: "06/01/02 15:04:05"},                  // Spark
	{layout: "Mon Jan 02 15:04:05 2006", start: 1}, // Apache error log, inside [...]
	{layout: "Jan _2 15:04:05", noYear: true},      // syslog (Linux, OpenSSH, Mac)
}

// ParseTimestamp extracts the event time from the start of line. It returns
// ok=false when no known format matches, so callers can fall back to the
// observation time.
func ParseTimestamp(line string, now time.Time) (time.Time, bool) {
	if tok, _, _ := strings.Cut(line, " "); len(tok) >= 20 && tok[4] == '-' && tok[10] == 'T' {
		if t, err := time.Parse(time.RFC3339Nano, tok); err == nil {
			return t, true
		}
	}
	for _, f := range timeFormats {
		end := f.start + len(f.layout)
		if len(line) < end {
			continue
		}
		t, err := time.Parse(f.layout, line[f.start:end])
		if err != nil {
			continue
		}
		if f.noYear {
			t = t.AddDate(now.Year(), 0, 0)
			if t.After(now.Add(24 * time.Hour)) {
				t = t.AddDate(-1, 0, 0) // December lines read in January
			}
		}
		return t, true
	}
	return time.Time{}, false
}

var levelWords = map[string]logplatv1.Level{
	"TRACE":    logplatv1.Level_LEVEL_TRACE,
	"DEBUG":    logplatv1.Level_LEVEL_DEBUG,
	"INFO":     logplatv1.Level_LEVEL_INFO,
	"NOTICE":   logplatv1.Level_LEVEL_INFO,
	"WARN":     logplatv1.Level_LEVEL_WARN,
	"WARNING":  logplatv1.Level_LEVEL_WARN,
	"ERROR":    logplatv1.Level_LEVEL_ERROR,
	"ERR":      logplatv1.Level_LEVEL_ERROR,
	"SEVERE":   logplatv1.Level_LEVEL_ERROR,
	"FATAL":    logplatv1.Level_LEVEL_FATAL,
	"CRIT":     logplatv1.Level_LEVEL_FATAL,
	"CRITICAL": logplatv1.Level_LEVEL_FATAL,
	"EMERG":    logplatv1.Level_LEVEL_FATAL,
	"ALERT":    logplatv1.Level_LEVEL_FATAL,
}

// levelScanTokens bounds how far into a line ParseLevel looks, so a word like
// "error" deep inside a message body does not set the level.
const levelScanTokens = 8

// ParseLevel finds the first severity keyword among the leading tokens.
func ParseLevel(line string) logplatv1.Level {
	for i, tok := range strings.Fields(line) {
		if i >= levelScanTokens {
			break
		}
		tok = strings.ToUpper(strings.Trim(tok, "[]():,<>"))
		if l, ok := levelWords[tok]; ok {
			return l
		}
	}
	return logplatv1.Level_LEVEL_UNSPECIFIED
}
