package lineio

import (
	"strings"
	"testing"
)

func collect(f *Framer, chunks ...string) []Line {
	var out []Line
	for _, c := range chunks {
		f.Feed([]byte(c), func(l Line) {
			out = append(out, Line{Offset: l.Offset, Text: append([]byte(nil), l.Text...)})
		})
	}
	return out
}

func TestFramerOffsetsAcrossChunks(t *testing.T) {
	f := NewFramer(100, 0)
	got := collect(f, "ab", "c\nde", "f\r\n\n", "gh")
	want := []Line{{100, []byte("abc")}, {104, []byte("def")}}
	if len(got) != len(want) {
		t.Fatalf("got %d lines, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i].Offset != want[i].Offset || string(got[i].Text) != string(want[i].Text) {
			t.Errorf("line %d = {%d %q}, want {%d %q}", i, got[i].Offset, got[i].Text, want[i].Offset, want[i].Text)
		}
	}
	if f.Next() != 110 {
		t.Errorf("Next() = %d, want 110 (blank line consumed, partial %q held)", f.Next(), "gh")
	}
	if f.Pending() != 2 {
		t.Errorf("Pending() = %d, want 2", f.Pending())
	}
}

func TestFramerResumeAtNextMatchesSinglePass(t *testing.T) {
	input := "one\ntwo\nthree\nfour\n"
	whole := collect(NewFramer(0, 0), input)

	first := NewFramer(0, 0)
	part := collect(first, input[:10])
	resumed := NewFramer(first.Next(), 0)
	part = append(part, collect(resumed, input[first.Next():])...)

	if len(part) != len(whole) {
		t.Fatalf("resumed framing produced %d lines, single pass %d", len(part), len(whole))
	}
	for i := range whole {
		if part[i].Offset != whole[i].Offset {
			t.Errorf("line %d offset %d, want %d", i, part[i].Offset, whole[i].Offset)
		}
	}
}

func TestFramerSplitsLongLines(t *testing.T) {
	f := NewFramer(0, 4)
	got := collect(f, strings.Repeat("x", 10)+"\n")
	offsets := []int64{0, 4, 8}
	if len(got) != len(offsets) {
		t.Fatalf("got %d chunks, want %d", len(got), len(offsets))
	}
	for i, off := range offsets {
		if got[i].Offset != off {
			t.Errorf("chunk %d offset %d, want %d", i, got[i].Offset, off)
		}
	}
	if string(got[2].Text) != "xx" {
		t.Errorf("last chunk %q, want %q", got[2].Text, "xx")
	}
}

func TestFramerFlushEmitsPartial(t *testing.T) {
	f := NewFramer(0, 0)
	collect(f, "done\npart")
	var tail []string
	f.Flush(func(l Line) { tail = append(tail, string(l.Text)) })
	if len(tail) != 1 || tail[0] != "part" || f.Next() != 9 {
		t.Fatalf("Flush emitted %q, Next=%d; want [part], 9", tail, f.Next())
	}
}

func BenchmarkFramerFeed(b *testing.B) {
	line := "081109 203518 143 INFO dfs.DataNode$DataXceiver: Receiving block blk_-1608999687919862906 src: /10.250.19.102:54106 dest: /10.250.19.102:50010\n"
	chunk := []byte(strings.Repeat(line, 64<<10/len(line)))
	b.SetBytes(int64(len(chunk)))
	f := NewFramer(0, 0)
	for b.Loop() {
		f.Feed(chunk, func(Line) {})
	}
}
