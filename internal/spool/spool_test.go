package spool

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func drain(t *testing.T, r *Reader) []string {
	t.Helper()
	var out []string
	for {
		rec, ok, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			return out
		}
		out = append(out, string(rec.Payload))
	}
}

func appendN(t *testing.T, s *Spool, from, to int) []Pos {
	t.Helper()
	var pos []Pos
	for i := from; i < to; i++ {
		p, err := s.Append([]byte(fmt.Sprintf("rec-%03d", i)))
		if err != nil {
			t.Fatal(err)
		}
		pos = append(pos, p)
	}
	return pos
}

func TestUnackedRecordsSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{SegmentBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	appendN(t, s, 0, 10)
	r := s.NewReader(s.Acked())
	var acked Pos
	for range 4 {
		rec, _, _ := r.Next()
		acked = rec.Next
	}
	if err := s.Ack(acked); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(dir, Options{SegmentBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	got := drain(t, s.NewReader(s.Acked()))
	if len(got) != 6 || got[0] != "rec-004" || got[5] != "rec-009" {
		t.Fatalf("after reopen read %q, want rec-004..rec-009", got)
	}
}

func TestAckDeletesWholeSegmentsOnly(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{SegmentBytes: 40})
	if err != nil {
		t.Fatal(err)
	}
	pos := appendN(t, s, 0, 6) // 15-byte records, two per segment
	segFiles := func() int {
		m, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
		return len(m)
	}
	if n := segFiles(); n != 3 {
		t.Fatalf("%d segments, want 3", n)
	}
	if err := s.Ack(pos[3]); err != nil { // acks rec-000..rec-002
		t.Fatal(err)
	}
	if n := segFiles(); n != 2 {
		t.Errorf("%d segments after ack into the second, want 2", n)
	}
	got := drain(t, s.NewReader(s.Acked()))
	if len(got) != 3 || got[0] != "rec-003" {
		t.Errorf("read %q after ack, want rec-003..rec-005", got)
	}
}

func TestTornTailIsTruncatedOnOpen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	appendN(t, s, 0, 3)
	s.Close()

	seg := filepath.Join(dir, fmt.Sprintf("%020d.seg", 1))
	f, err := os.OpenFile(seg, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0, 0, 0, 50, 1, 2, 3, 4, 'p', 'a', 'r'}); err != nil { // header promises 50 bytes, 3 arrive
		t.Fatal(err)
	}
	f.Close()

	s, err = Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if got := drain(t, s.NewReader(s.Acked())); len(got) != 3 {
		t.Fatalf("read %d records, want the 3 intact ones", len(got))
	}
	appendN(t, s, 3, 4)
	if got := drain(t, s.NewReader(s.Acked())); len(got) != 4 || got[3] != "rec-003" {
		t.Fatalf("append after recovery read back %q", got)
	}
}

func TestFullSpoolRefusesAppends(t *testing.T) {
	s, err := Open(t.TempDir(), Options{MaxBytes: 40})
	if err != nil {
		t.Fatal(err)
	}
	pos := appendN(t, s, 0, 2)
	if _, err := s.Append([]byte("rec-002")); !errors.Is(err, ErrFull) {
		t.Fatalf("Append past MaxBytes = %v, want ErrFull", err)
	}
	if err := s.Ack(Pos{Seg: pos[1].Seg, Off: pos[1].Off + 15}); err != nil {
		t.Fatal(err)
	}
	// Acked bytes in the head segment still count until the segment rolls,
	// so the spool stays full: backpressure, not data loss.
	if _, err := s.Append([]byte("rec-002")); !errors.Is(err, ErrFull) {
		t.Fatalf("Append = %v, want ErrFull while the head segment holds the bytes", err)
	}
}

func TestReaderSeesAppendsAfterCatchingUp(t *testing.T) {
	s, err := Open(t.TempDir(), Options{SegmentBytes: 20})
	if err != nil {
		t.Fatal(err)
	}
	r := s.NewReader(s.Acked())
	if _, ok, _ := r.Next(); ok {
		t.Fatal("empty spool returned a record")
	}
	appendN(t, s, 0, 3)
	select {
	case <-s.Notify():
	default:
		t.Fatal("Append did not signal Notify")
	}
	if got := drain(t, r); len(got) != 3 {
		t.Fatalf("reader read %d records across segments, want 3", len(got))
	}
}

// BenchmarkAppend measures one fsynced append of a typical 1000-line batch.
func BenchmarkAppend(b *testing.B) {
	s, err := Open(b.TempDir(), Options{MaxBytes: 1 << 40})
	if err != nil {
		b.Fatal(err)
	}
	payload := make([]byte, 200<<10)
	b.SetBytes(int64(len(payload)))
	for b.Loop() {
		if _, err := s.Append(payload); err != nil {
			b.Fatal(err)
		}
	}
}
