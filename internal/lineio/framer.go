// Package lineio frames a byte stream into lines with stable byte offsets.
//
// The agent's tailer and the zero-loss checker both frame files through
// Framer, so they agree on which offsets (and therefore which IDs) exist.
package lineio

import "bytes"

// DefaultMaxLen caps a line. Longer lines are split into MaxLen chunks, each
// with its own offset, so one runaway line cannot grow memory without bound.
const DefaultMaxLen = 64 << 10

// Line is a framed line. Text excludes the trailing "\n" or "\r\n" and
// aliases the framer's buffer: it is valid only during the emit callback.
type Line struct {
	Offset int64
	Text   []byte
}

// Framer splits bytes into lines. Blank lines are consumed but not emitted.
type Framer struct {
	maxLen int
	base   int64 // file offset of buf[0]
	buf    []byte
}

// NewFramer returns a Framer whose first byte is at file offset start.
func NewFramer(start int64, maxLen int) *Framer {
	if maxLen <= 0 {
		maxLen = DefaultMaxLen
	}
	return &Framer{maxLen: maxLen, base: start}
}

// Feed appends p and emits every line it completes.
func (f *Framer) Feed(p []byte, emit func(Line)) {
	f.buf = append(f.buf, p...)
	start := 0
	for start < len(f.buf) {
		rest := f.buf[start:]
		i := bytes.IndexByte(rest, '\n')
		switch {
		case i >= 0 && i <= f.maxLen:
			f.emit(start, rest[:i], emit)
			start += i + 1
		case len(rest) >= f.maxLen:
			f.emit(start, rest[:f.maxLen], emit)
			start += f.maxLen
		default:
			f.compact(start)
			return
		}
	}
	f.compact(start)
}

// Flush emits any buffered partial line. Call it only when no more bytes will
// arrive, for example after a file is rotated away.
func (f *Framer) Flush(emit func(Line)) {
	if len(f.buf) > 0 {
		f.emit(0, f.buf, emit)
		f.compact(len(f.buf))
	}
}

// Next is the file offset just past the last consumed line. Resuming a
// Framer at Next never re-emits or skips a line.
func (f *Framer) Next() int64 { return f.base }

// Pending is the number of buffered bytes that do not yet form a line.
func (f *Framer) Pending() int { return len(f.buf) }

func (f *Framer) emit(at int, text []byte, emit func(Line)) {
	text = bytes.TrimSuffix(text, []byte{'\r'})
	if len(text) > 0 {
		emit(Line{Offset: f.base + int64(at), Text: text})
	}
}

func (f *Framer) compact(consumed int) {
	f.base += int64(consumed)
	n := copy(f.buf, f.buf[consumed:])
	f.buf = f.buf[:n]
}
