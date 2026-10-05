// Package doc defines how a log entry is stored in OpenSearch: the document
// shape, the index it lands in, and the index template that maps it.
package doc

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	logplatv1 "github.com/Longhodac/log-plat/gen/logplat/v1"
)

// Doc is the stored form of a LogEntry. Its _id in OpenSearch is ID.
type Doc struct {
	ID         string    `json:"id"`
	Service    string    `json:"service"`
	Timestamp  time.Time `json:"timestamp"`
	ObservedAt time.Time `json:"observed_at"`
	IndexedAt  time.Time `json:"indexed_at"`
	Level      string    `json:"level"`
	Message    string    `json:"message"`
	Host       string    `json:"host,omitempty"`
	AgentID    string    `json:"agent_id"`
	Source     string    `json:"source"`
	Offset     int64     `json:"offset"`
	Epoch      uint32    `json:"epoch"`
}

// FromEntry converts an entry that has already passed collector validation.
func FromEntry(e *logplatv1.LogEntry, indexedAt time.Time) Doc {
	return Doc{
		ID:         e.GetId(),
		Service:    e.GetService(),
		Timestamp:  e.GetTimestamp().AsTime().UTC(),
		ObservedAt: e.GetObservedAt().AsTime().UTC(),
		IndexedAt:  indexedAt.UTC(),
		Level:      LevelName(e.GetLevel()),
		Message:    e.GetMessage(),
		Host:       e.GetHost(),
		AgentID:    e.GetAgentId(),
		Source:     e.GetSource(),
		Offset:     e.GetOffset(),
		Epoch:      e.GetEpoch(),
	}
}

// IndexName returns the daily index for an event time.
//
// The index is a pure function of the entry's own timestamp. A redelivered
// entry therefore targets the same index as its first delivery, which is
// what lets the _id overwrite instead of duplicating across indices.
func IndexName(prefix string, ts time.Time) string {
	return prefix + "-" + ts.UTC().Format("2006.01.02")
}

// Pattern matches every daily index for prefix.
func Pattern(prefix string) string { return prefix + "-*" }

var levelNames = map[logplatv1.Level]string{
	logplatv1.Level_LEVEL_UNSPECIFIED: "unknown",
	logplatv1.Level_LEVEL_TRACE:       "trace",
	logplatv1.Level_LEVEL_DEBUG:       "debug",
	logplatv1.Level_LEVEL_INFO:        "info",
	logplatv1.Level_LEVEL_WARN:        "warn",
	logplatv1.Level_LEVEL_ERROR:       "error",
	logplatv1.Level_LEVEL_FATAL:       "fatal",
}

// LevelName is the lowercase name stored in OpenSearch.
func LevelName(l logplatv1.Level) string {
	if n, ok := levelNames[l]; ok {
		return n
	}
	return "unknown"
}

// ParseLevelName is the inverse of LevelName.
func ParseLevelName(s string) (logplatv1.Level, bool) {
	s = strings.ToLower(s)
	for l, n := range levelNames {
		if n == s {
			return l, true
		}
	}
	return 0, false
}

// TemplateName is the index template name for prefix.
func TemplateName(prefix string) string { return prefix + "-template" }

// Template is the index template body. Mappings are strict: an unexpected
// field is a bug, and rejecting it beats silently growing the mapping.
func Template(prefix string, shards, replicas int, refresh string) ([]byte, error) {
	if shards < 1 || replicas < 0 {
		return nil, fmt.Errorf("invalid shards=%d replicas=%d", shards, replicas)
	}
	kw := map[string]any{"type": "keyword"}
	date := map[string]any{"type": "date"}
	return json.Marshal(map[string]any{
		"index_patterns": []string{Pattern(prefix)},
		"template": map[string]any{
			"settings": map[string]any{
				"number_of_shards":   shards,
				"number_of_replicas": replicas,
				"refresh_interval":   refresh,
			},
			"mappings": map[string]any{
				"dynamic": "strict",
				"properties": map[string]any{
					"id":          kw,
					"service":     kw,
					"timestamp":   date,
					"observed_at": date,
					"indexed_at":  date,
					"level":       kw,
					"message":     map[string]any{"type": "text"},
					"host":        kw,
					"agent_id":    kw,
					"source":      kw,
					"offset":      map[string]any{"type": "long"},
					"epoch":       map[string]any{"type": "integer"},
				},
			},
		},
	})
}
