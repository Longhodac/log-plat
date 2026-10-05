package collector

import (
	"time"
	"unicode/utf8"

	logplatv1 "github.com/Longhodac/log-plat/gen/logplat/v1"
	"github.com/Longhodac/log-plat/internal/logid"
)

const (
	// MaxMessageBytes matches the agent's line cap.
	MaxMessageBytes = 64 << 10
	// MaxFutureSkew tolerates agent clocks running ahead of the collector.
	MaxFutureSkew = 24 * time.Hour
)

// Validate returns why e cannot be stored, or "" if it can. Rejections are
// permanent: the agent does not retry a rejected entry.
func Validate(e *logplatv1.LogEntry, now time.Time) string {
	switch {
	case !logid.Valid(e.GetId()):
		return "invalid id"
	case e.GetMessage() == "":
		return "empty message"
	case len(e.GetMessage()) > MaxMessageBytes:
		return "message too large"
	case !utf8.ValidString(e.GetMessage()):
		return "message is not valid UTF-8"
	case e.GetTimestamp() == nil || !e.GetTimestamp().IsValid():
		return "missing or invalid timestamp"
	case e.GetTimestamp().AsTime().After(now.Add(MaxFutureSkew)):
		return "timestamp too far in the future"
	case e.GetObservedAt() == nil || !e.GetObservedAt().IsValid():
		return "missing or invalid observed_at"
	case logplatv1.Level_name[int32(e.GetLevel())] == "":
		return "unknown level"
	case e.GetSource() == "":
		return "missing source"
	}
	return ""
}
