package agent

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Format says how to read one line of a tailed file.
type Format string

const (
	// FormatText treats each line as the message, with a timestamp and level
	// parsed from its start.
	FormatText Format = "text"
	// FormatDockerJSON reads Docker's json-file driver, where every line is
	// {"log":"...\n","stream":"stdout","time":"..."}. The message is the log
	// field, the event time is the time field, and the host is the container name.
	FormatDockerJSON Format = "docker-json"
)

// ParseFormat validates a configured format name. Empty means text.
func ParseFormat(s string) (Format, bool) {
	switch Format(s) {
	case "", FormatText:
		return FormatText, true
	case FormatDockerJSON:
		return FormatDockerJSON, true
	}
	return "", false
}

type dockerLine struct {
	Log  string `json:"log"`
	Time string `json:"time"`
}

// parseDockerLine unwraps one json-file line. ok is false for a line that is
// not valid json-file output, and the caller then keeps it as plain text so a
// strange line is never dropped. An empty message with ok true means the
// container wrote a blank line, which carries nothing to ship.
func parseDockerLine(line string) (msg string, ts time.Time, ok bool) {
	var d dockerLine
	if err := json.Unmarshal([]byte(line), &d); err != nil || (d.Log == "" && d.Time == "") {
		return "", time.Time{}, false
	}
	if t, err := time.Parse(time.RFC3339Nano, d.Time); err == nil {
		ts = t
	}
	return strings.TrimRight(d.Log, "\r\n"), ts, true
}

// containerName returns the name Docker gave the container whose log is at
// logPath (".../<id>/<id>-json.log"), read from config.v2.json beside it. It
// falls back to the 12-character short ID when the file is missing or odd.
func containerName(logPath string) string {
	dir := filepath.Dir(logPath)
	if raw, err := os.ReadFile(filepath.Join(dir, "config.v2.json")); err == nil {
		var c struct {
			Name string `json:"Name"`
		}
		if json.Unmarshal(raw, &c) == nil {
			if n := strings.TrimPrefix(c.Name, "/"); n != "" {
				return n
			}
		}
	}
	id := filepath.Base(dir)
	return id[:min(len(id), 12)]
}

// containerLabels reads the labels Docker recorded for the container whose log
// is at logPath, from config.v2.json beside it. Compose sets labels such as
// com.docker.compose.project, which is how a deployment says which containers
// are its own.
func containerLabels(logPath string) map[string]string {
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(logPath), "config.v2.json"))
	if err != nil {
		return nil
	}
	var c struct {
		Config struct {
			Labels map[string]string `json:"Labels"`
		} `json:"Config"`
	}
	if json.Unmarshal(raw, &c) != nil {
		return nil
	}
	return c.Config.Labels
}

// matchesLabel reports whether the container whose log is at logPath carries
// the label "key=value". An empty selector matches every container.
func matchesLabel(logPath, selector string) bool {
	if selector == "" {
		return true
	}
	key, value, _ := strings.Cut(selector, "=")
	got, ok := containerLabels(logPath)[key]
	return ok && got == value
}
