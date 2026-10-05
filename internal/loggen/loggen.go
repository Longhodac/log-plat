// Package loggen replays a log file into another file at a controlled rate,
// so the agent can tail it like a live application log.
package loggen

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

// Config describes one replay.
type Config struct {
	In    string
	Out   string
	Rate  int // lines per second; 0 writes as fast as possible
	Count int // lines to write; 0 means one pass over In
	Tick  time.Duration
}

// Stats summarizes a finished replay.
type Stats struct {
	Lines   int           `json:"lines"`
	Bytes   int64         `json:"bytes"`
	Elapsed time.Duration `json:"elapsed_ns"`
	Rate    float64       `json:"lines_per_sec"`
	Out     string        `json:"out"`
}

// source yields input lines, reopening In to loop when Count exceeds one pass.
type source struct {
	path string
	loop bool
	f    *os.File
	sc   *bufio.Scanner
	seen bool
}

func (s *source) next() ([]byte, error) {
	for {
		if s.sc == nil {
			f, err := os.Open(s.path)
			if err != nil {
				return nil, err
			}
			s.f = f
			s.sc = bufio.NewScanner(f)
			s.sc.Buffer(make([]byte, 0, 1<<20), 1<<20)
		}
		if s.sc.Scan() {
			line := bytes.TrimRight(s.sc.Bytes(), "\r")
			if len(line) == 0 {
				continue
			}
			s.seen = true
			return line, nil
		}
		if err := s.sc.Err(); err != nil {
			return nil, err
		}
		s.f.Close()
		s.sc = nil
		if !s.loop || !s.seen {
			return nil, io.EOF
		}
	}
}

// Run replays until Count lines are written, the input ends (Count 0), or ctx is done.
func Run(ctx context.Context, cfg Config) (Stats, error) {
	if cfg.Tick <= 0 {
		cfg.Tick = 10 * time.Millisecond
	}
	out, err := os.OpenFile(cfg.Out, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return Stats{}, err
	}
	defer out.Close()
	w := bufio.NewWriterSize(out, 256<<10)
	src := &source{path: cfg.In, loop: cfg.Count > 0}
	st := Stats{Out: cfg.Out}
	start := time.Now()

	write := func(n int) (done bool, err error) {
		for range n {
			if cfg.Count > 0 && st.Lines >= cfg.Count {
				return true, nil
			}
			line, err := src.next()
			if errors.Is(err, io.EOF) {
				return true, nil
			}
			if err != nil {
				return true, err
			}
			w.Write(line)
			w.WriteByte('\n')
			st.Lines++
			st.Bytes += int64(len(line) + 1)
		}
		return false, w.Flush()
	}

	finish := func(err error) (Stats, error) {
		if ferr := w.Flush(); err == nil {
			err = ferr
		}
		st.Elapsed = time.Since(start)
		if st.Elapsed > 0 {
			st.Rate = float64(st.Lines) / st.Elapsed.Seconds()
		}
		return st, err
	}

	if cfg.Rate <= 0 {
		for {
			if ctx.Err() != nil {
				return finish(nil)
			}
			done, err := write(4096)
			if done || err != nil {
				return finish(err)
			}
		}
	}

	t := time.NewTicker(cfg.Tick)
	defer t.Stop()
	for {
		// Pace against the start time, not per tick, so timer jitter does
		// not accumulate into rate drift.
		due := int(time.Since(start).Seconds()*float64(cfg.Rate)) - st.Lines
		if due > 0 {
			done, err := write(due)
			if done || err != nil {
				return finish(err)
			}
		}
		select {
		case <-ctx.Done():
			return finish(nil)
		case <-t.C:
		}
	}
}

// String renders stats for humans.
func (s Stats) String() string {
	return fmt.Sprintf("wrote %d lines (%d bytes) to %s in %s (%.0f lines/s)", s.Lines, s.Bytes, s.Out, s.Elapsed.Round(time.Millisecond), s.Rate)
}
