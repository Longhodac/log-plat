package logid

import "testing"

func TestNewIsDeterministicAndValid(t *testing.T) {
	a := New("agent-1", "/var/log/app.log", 0, 42)
	if a != New("agent-1", "/var/log/app.log", 0, 42) {
		t.Fatal("same inputs produced different IDs")
	}
	if !Valid(a) {
		t.Fatalf("New produced invalid ID %q", a)
	}
}

func TestNewDistinguishesEveryField(t *testing.T) {
	base := New("a", "s", 0, 0)
	for name, id := range map[string]string{
		"agent":  New("b", "s", 0, 0),
		"source": New("a", "t", 0, 0),
		"epoch":  New("a", "s", 1, 0),
		"offset": New("a", "s", 0, 1),
		// Length prefixes keep field boundaries unambiguous.
		"boundary": New("as", "", 0, 0),
	} {
		if id == base {
			t.Errorf("changing %s did not change the ID", name)
		}
	}
}

func TestValid(t *testing.T) {
	for id, want := range map[string]bool{
		"0123456789abcdef0123456789abcdef":  true,
		"0123456789ABCDEF0123456789abcdef":  false,
		"0123456789abcdef0123456789abcde":   false,
		"0123456789abcdef0123456789abcdefg": false,
		"":                                  false,
	} {
		if Valid(id) != want {
			t.Errorf("Valid(%q) = %v, want %v", id, !want, want)
		}
	}
}

func BenchmarkNew(b *testing.B) {
	for i := 0; b.Loop(); i++ {
		New("agent-1", "/var/log/ingest/hdfs.log", 0, int64(i))
	}
}
