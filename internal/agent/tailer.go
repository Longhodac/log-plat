package agent

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"syscall"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	logplatv1 "github.com/Longhodac/log-plat/gen/logplat/v1"
	"github.com/Longhodac/log-plat/internal/lineio"
	"github.com/Longhodac/log-plat/internal/logid"
)

// chunk is what one read of a file produced: the entries, and the file state
// to record once those entries are durably spooled.
type chunk struct {
	source  string
	entries []*logplatv1.LogEntry
	bytes   int
	state   FileState
}

type tailer struct {
	path     string
	agentID  string
	host     string
	start    FileState
	out      chan<- chunk
	poll     time.Duration
	readSize int
	log      *slog.Logger
	now      func() time.Time
}

func inode(fi os.FileInfo) uint64 {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return st.Ino
	}
	return 0
}

// open opens path and reconciles it with the last recorded state: a different
// inode means the file was replaced, a shorter file means it was truncated.
// Either way the agent starts a new epoch at offset 0, so IDs from the new
// content can never collide with IDs from the old.
func (t *tailer) open(st FileState) (*os.File, FileState, error) {
	f, err := os.Open(t.path)
	if err != nil {
		return nil, st, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, st, err
	}
	ino := inode(fi)
	switch {
	case st.Inode != 0 && st.Inode != ino:
		st = FileState{Epoch: st.Epoch + 1}
	case fi.Size() < st.Offset:
		st = FileState{Epoch: st.Epoch + 1}
	}
	st.Inode = ino
	if _, err := f.Seek(st.Offset, io.SeekStart); err != nil {
		f.Close()
		return nil, st, err
	}
	return f, st, nil
}

// run tails the file until ctx is done or the file disappears.
func (t *tailer) run(ctx context.Context) error {
	f, st, err := t.open(t.start)
	if err != nil {
		return err
	}
	defer func() { f.Close() }()
	if st.Epoch != t.start.Epoch {
		t.log.Info("file replaced or truncated; starting new epoch", "path", t.path, "epoch", st.Epoch)
	}

	fr := lineio.NewFramer(st.Offset, lineio.DefaultMaxLen)
	buf := make([]byte, t.readSize)
	for {
		n, err := f.Read(buf)
		if n > 0 {
			c := t.frame(fr, buf[:n], st, false)
			if err := t.send(ctx, c); err != nil {
				return err
			}
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		if n > 0 {
			continue
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(t.poll):
		}

		cur, err := os.Stat(t.path)
		pos := fr.Next() + int64(fr.Pending())
		switch {
		case errors.Is(err, os.ErrNotExist) || (err == nil && inode(cur) != st.Inode):
			// Rotated away or deleted. Everything still in the old file has
			// been read; a trailing partial line will never be completed.
			if fr.Pending() > 0 {
				if err := t.send(ctx, t.frame(fr, nil, st, true)); err != nil {
					return err
				}
			}
			if err != nil {
				t.log.Info("file removed; tailer stopping", "path", t.path)
				return nil
			}
			f.Close()
			if f, st, err = t.open(FileState{Epoch: st.Epoch, Inode: st.Inode, Offset: 0}); err != nil {
				return err
			}
			fr = lineio.NewFramer(0, lineio.DefaultMaxLen)
			t.log.Info("file rotated; following new file", "path", t.path, "epoch", st.Epoch)
		case err != nil:
			return err
		case cur.Size() < pos:
			if _, err := f.Seek(0, io.SeekStart); err != nil {
				return err
			}
			st = FileState{Epoch: st.Epoch + 1, Inode: st.Inode}
			fr = lineio.NewFramer(0, lineio.DefaultMaxLen)
			t.log.Info("file truncated; starting new epoch", "path", t.path, "epoch", st.Epoch)
		}
	}
}

func (t *tailer) frame(fr *lineio.Framer, p []byte, st FileState, flush bool) chunk {
	now := t.now()
	c := chunk{source: t.path}
	emit := func(l lineio.Line) {
		text := strings.ToValidUTF8(string(l.Text), "�")
		ts, ok := ParseTimestamp(text, now)
		if !ok {
			ts = now
		}
		c.entries = append(c.entries, &logplatv1.LogEntry{
			Id:         logid.New(t.agentID, t.path, st.Epoch, l.Offset),
			Timestamp:  timestamppb.New(ts),
			ObservedAt: timestamppb.New(now),
			Level:      ParseLevel(text),
			Message:    text,
			Host:       t.host,
			AgentId:    t.agentID,
			Source:     t.path,
			Offset:     l.Offset,
			Epoch:      st.Epoch,
		})
		c.bytes += len(text)
	}
	if flush {
		fr.Flush(emit)
	} else {
		fr.Feed(p, emit)
	}
	st.Offset = fr.Next()
	c.state = st
	linesRead.WithLabelValues(t.path).Add(float64(len(c.entries)))
	return c
}

func (t *tailer) send(ctx context.Context, c chunk) error {
	select {
	case t.out <- c:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
