package loggen

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeInput(t *testing.T, lines ...string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "in.log")
	if err := os.WriteFile(p, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoopsInputToReachCountAndSkipsBlankLines(t *testing.T) {
	in := writeInput(t, "a", "", "b\r", "c")
	out := filepath.Join(t.TempDir(), "out.log")
	st, err := Run(context.Background(), Config{In: in, Out: out, Count: 7})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(out)
	if want := "a\nb\nc\na\nb\nc\na\n"; string(raw) != want || st.Lines != 7 {
		t.Fatalf("wrote %q (%d lines), want %q", raw, st.Lines, want)
	}
}

func TestOnePassWithoutCount(t *testing.T) {
	in := writeInput(t, "a", "b")
	out := filepath.Join(t.TempDir(), "out.log")
	if st, err := Run(context.Background(), Config{In: in, Out: out}); err != nil || st.Lines != 2 {
		t.Fatalf("Run = %+v, %v; want 2 lines", st, err)
	}
}

func TestRateIsHonored(t *testing.T) {
	in := writeInput(t, "x")
	out := filepath.Join(t.TempDir(), "out.log")
	st, err := Run(context.Background(), Config{In: in, Out: out, Rate: 2000, Count: 600})
	if err != nil {
		t.Fatal(err)
	}
	// 600 lines at 2000/s is 300ms; allow scheduling slack but catch an
	// unpaced writer, which finishes in well under a millisecond.
	if st.Elapsed < 250*time.Millisecond || st.Elapsed > 2*time.Second {
		t.Errorf("600 lines at 2000/s took %v, want about 300ms", st.Elapsed)
	}
}
