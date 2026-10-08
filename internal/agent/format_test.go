package agent

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	logplatv1 "github.com/Longhodac/log-plat/gen/logplat/v1"
)

func TestParseDockerLine(t *testing.T) {
	msg, ts, ok := parseDockerLine(`{"log":"checkout failed: card declined\n","stream":"stderr","time":"2026-10-08T12:00:01.5Z"}`)
	if !ok || msg != "checkout failed: card declined" || !ts.Equal(time.Date(2026, 10, 8, 12, 0, 1, 5e8, time.UTC)) {
		t.Fatalf("got %q %v %v", msg, ts, ok)
	}
	if msg, _, ok := parseDockerLine(`{"log":"\n","stream":"stdout","time":"2026-10-08T12:00:01Z"}`); !ok || msg != "" {
		t.Errorf("blank container line = %q, %v; want empty message with ok so the agent skips it", msg, ok)
	}
	for _, bad := range []string{"plain text, not json", `{"unrelated":1}`, `{"log":`, ""} {
		if _, _, ok := parseDockerLine(bad); ok {
			t.Errorf("parseDockerLine(%q) ok = true, want false so the line is kept as text", bad)
		}
	}
}

func TestContainerName(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "0123456789abcdef0123")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	log := filepath.Join(dir, "0123456789abcdef0123-json.log")
	if got := containerName(log); got != "0123456789ab" {
		t.Errorf("without config.v2.json got %q, want the 12-character short ID", got)
	}
	if err := os.WriteFile(filepath.Join(dir, "config.v2.json"), []byte(`{"Name":"/checkout"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := containerName(log); got != "checkout" {
		t.Errorf("got %q, want checkout", got)
	}
}

func TestParseLevelReadsStructuredAndDotnetLogs(t *testing.T) {
	cases := map[string]logplatv1.Level{
		`{"level":"error","msg":"boom"}`:                       logplatv1.Level_LEVEL_ERROR,
		`{"severity":"WARNING","message":"slow"}`:              logplatv1.Level_LEVEL_WARN,
		`{"severity_text":"Info","body":"ok"}`:                 logplatv1.Level_LEVEL_INFO,
		`{"msg":"no level here"}`:                              logplatv1.Level_LEVEL_UNSPECIFIED,
		`fail: Microsoft.Hosting.Lifetime[0] unhandled`:        logplatv1.Level_LEVEL_ERROR,
		`warn: Cart.Service[0] retrying`:                       logplatv1.Level_LEVEL_WARN,
		`{not json at all ERROR in the first few tokens here}`: logplatv1.Level_LEVEL_ERROR,
	}
	for line, want := range cases {
		if got := ParseLevel(line); got != want {
			t.Errorf("ParseLevel(%q) = %v, want %v", line, got, want)
		}
	}
}

// A container's json-file log, tailed through the real agent and collector.
func TestDockerJSONLogsShipWithContainerNameAndMessage(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "containers", "abc123def4567890")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config.v2.json"), []byte(`{"Name":"/payment"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, "abc123def4567890-json.log")
	var content string
	for i := range 20 {
		stream, msg := "stdout", fmt.Sprintf("info: charge %d ok", i)
		if i%5 == 0 {
			stream, msg = "stderr", fmt.Sprintf("fail: charge %d declined", i)
		}
		content += fmt.Sprintf(`{"log":"%s\n","stream":"%s","time":"2026-10-08T12:00:%02d.000000000Z"}`+"\n", msg, stream, i)
	}
	content += `{"log":"\n","stream":"stdout","time":"2026-10-08T12:01:00Z"}` + "\n" // blank line, skipped
	content += "a line that is not json-file output\n"
	if err := os.WriteFile(logPath, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	h := newHarness(t)
	h.start()
	cfg := testConfig(filepath.Join(root, "state"), filepath.Join(root, "containers", "*", "*-json.log"))
	cfg.Format = FormatDockerJSON
	stop := runAgent(t, cfg, h.client())
	waitFor(t, "21 entries (20 container lines and the odd one, not the blank)", func() bool {
		h.pub.mu.Lock()
		defer h.pub.mu.Unlock()
		return len(h.pub.entries) >= 21
	})
	if err := stop(); err != nil {
		t.Fatal(err)
	}

	h.pub.mu.Lock()
	defer h.pub.mu.Unlock()
	if len(h.pub.entries) != 21 {
		for _, e := range h.pub.entries {
			t.Logf("entry %q host %q", e.GetMessage(), e.GetHost())
		}
		t.Fatalf("%d entries, want 21", len(h.pub.entries))
	}
	byMsg := map[string]*logplatv1.LogEntry{}
	for _, e := range h.pub.entries {
		byMsg[e.GetMessage()] = e
	}
	e := byMsg["fail: charge 5 declined"]
	if e == nil {
		t.Fatalf("message not unwrapped from the json-file envelope: %v", byMsg)
	}
	if e.GetHost() != "payment" || e.GetLevel() != logplatv1.Level_LEVEL_ERROR ||
		!e.GetTimestamp().AsTime().Equal(time.Date(2026, 10, 8, 12, 0, 5, 0, time.UTC)) {
		t.Errorf("entry = host %q level %v time %v; want payment, ERROR, the Docker time field",
			e.GetHost(), e.GetLevel(), e.GetTimestamp().AsTime())
	}
	if byMsg["a line that is not json-file output"] == nil {
		t.Error("a non-json line was dropped; it should be kept as plain text")
	}
}

func writeContainer(t *testing.T, root, id, name string, labels map[string]string, lines ...string) string {
	t.Helper()
	dir := filepath.Join(root, "containers", id)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	cfg, _ := json.Marshal(map[string]any{"Name": "/" + name, "Config": map[string]any{"Labels": labels}})
	if err := os.WriteFile(filepath.Join(dir, "config.v2.json"), cfg, 0o600); err != nil {
		t.Fatal(err)
	}
	var content string
	for i, l := range lines {
		content += fmt.Sprintf(`{"log":%q,"stream":"stdout","time":"2026-10-08T12:00:%02d.000000000Z"}`+"\n", l+"\n", i)
	}
	path := filepath.Join(dir, id+"-json.log")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestMatchesLabel(t *testing.T) {
	root := t.TempDir()
	p := writeContainer(t, root, "aaaaaaaaaaaa1111", "ad", map[string]string{"com.docker.compose.project": "opentelemetry-demo", "tier": "backend"}, "x")
	for sel, want := range map[string]bool{
		"": true, "com.docker.compose.project=opentelemetry-demo": true, "tier=backend": true,
		"com.docker.compose.project=logplat": false, "missing=label": false, "com.docker.compose.project": false,
	} {
		if got := matchesLabel(p, sel); got != want {
			t.Errorf("matchesLabel(%q) = %v, want %v", sel, got, want)
		}
	}
	if matchesLabel(filepath.Join(root, "nowhere", "x-json.log"), "a=b") {
		t.Error("a container with no config matched a label selector")
	}
}

// The agent must not ship containers outside the label it was given, or a
// deployment tailing Docker's logs would ship its own.
func TestDockerLabelKeepsTheAgentOffOtherContainers(t *testing.T) {
	root := t.TempDir()
	writeContainer(t, root, "11111111aaaa0000", "checkout", map[string]string{"app": "demo"}, "info: order placed", "fail: card declined")
	writeContainer(t, root, "22222222bbbb0000", "kafka", map[string]string{"app": "platform"}, "info: platform noise", "info: more platform noise")

	h := newHarness(t)
	h.start()
	cfg := testConfig(filepath.Join(root, "state"), filepath.Join(root, "containers", "*", "*-json.log"))
	cfg.Format, cfg.DockerLabel = FormatDockerJSON, "app=demo"
	stop := runAgent(t, cfg, h.client())
	waitFor(t, "the demo container's two lines", func() bool {
		h.pub.mu.Lock()
		defer h.pub.mu.Unlock()
		return len(h.pub.entries) >= 2
	})
	time.Sleep(300 * time.Millisecond) // time for the other container to be wrongly picked up
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	h.pub.mu.Lock()
	defer h.pub.mu.Unlock()
	if len(h.pub.entries) != 2 {
		t.Fatalf("%d entries shipped, want only the 2 from the labeled container", len(h.pub.entries))
	}
	for _, e := range h.pub.entries {
		if e.GetHost() != "checkout" {
			t.Errorf("entry from host %q shipped; only checkout carries the label", e.GetHost())
		}
	}
}
