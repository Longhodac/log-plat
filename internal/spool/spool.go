// Package spool is the agent's on-disk write-ahead queue.
//
// Every batch is appended and fsynced here before it is sent, so batches
// survive collector outages and agent restarts. A record stays on disk until
// the collector acknowledges it; acknowledged segments are deleted.
//
// On disk, a spool is a directory of segment files (NNNNNNNNNNNNNNNNNNNN.seg)
// holding records framed as [len uint32][crc32c uint32][payload], plus a
// cursor file naming the first unacknowledged record.
package spool

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
)

const (
	headerLen  = 8
	maxRecord  = 64 << 20
	cursorFile = "cursor.json"
	segSuffix  = ".seg"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// ErrFull means the spool reached its size limit. The caller should stop
// reading input until acknowledgements free space.
var ErrFull = errors.New("spool: full")

// Pos addresses a record: the segment and the byte offset within it.
type Pos struct {
	Seg uint64 `json:"seg"`
	Off int64  `json:"off"`
}

// Less orders positions.
func (p Pos) Less(q Pos) bool {
	return p.Seg < q.Seg || (p.Seg == q.Seg && p.Off < q.Off)
}

// Options bounds the spool.
type Options struct {
	SegmentBytes int64 // roll to a new segment beyond this size
	MaxBytes     int64 // Append returns ErrFull beyond this total
}

// Spool is safe for one writer and one reader at a time.
type Spool struct {
	dir  string
	opts Options

	mu       sync.Mutex
	sizes    map[uint64]int64 // committed bytes per segment
	segs     []uint64         // ascending
	w        *os.File
	acked    Pos
	dirty    bool
	total    int64
	notifyCh chan struct{}
}

// Open recovers the spool in dir, creating it if needed. A torn record at the
// tail of the newest segment (a crash mid-append) is truncated away; it was
// never fsynced, so the caller never treated it as durable.
func Open(dir string, opts Options) (*Spool, error) {
	if opts.SegmentBytes <= 0 {
		opts.SegmentBytes = 16 << 20
	}
	if opts.MaxBytes <= 0 {
		opts.MaxBytes = 1 << 30
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	s := &Spool{dir: dir, opts: opts, sizes: map[uint64]int64{}, notifyCh: make(chan struct{}, 1)}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		name, ok := strings.CutSuffix(e.Name(), segSuffix)
		if !ok {
			continue
		}
		id, err := strconv.ParseUint(name, 10, 64)
		if err != nil {
			continue
		}
		fi, err := e.Info()
		if err != nil {
			return nil, err
		}
		s.segs = append(s.segs, id)
		s.sizes[id] = fi.Size()
	}
	slices.Sort(s.segs)

	if len(s.segs) == 0 {
		if err := s.createSegment(1); err != nil {
			return nil, err
		}
	} else {
		last := s.segs[len(s.segs)-1]
		valid, err := scanValid(s.segPath(last))
		if err != nil {
			return nil, err
		}
		if err := os.Truncate(s.segPath(last), valid); err != nil {
			return nil, err
		}
		s.sizes[last] = valid
		if s.w, err = os.OpenFile(s.segPath(last), os.O_WRONLY|os.O_APPEND, 0o600); err != nil {
			return nil, err
		}
	}
	for _, id := range s.segs {
		s.total += s.sizes[id]
	}

	s.acked = Pos{Seg: s.segs[0]}
	if raw, err := os.ReadFile(filepath.Join(dir, cursorFile)); err == nil {
		var p Pos
		if json.Unmarshal(raw, &p) == nil && s.acked.Less(p) {
			s.acked = s.clamp(p)
		}
	}
	return s, nil
}

// clamp keeps a recovered cursor inside the committed data. A cursor past
// the end can only come from a damaged cursor file; resending is safe,
// skipping is not, so the cursor moves back.
func (s *Spool) clamp(p Pos) Pos {
	size, ok := s.sizes[p.Seg]
	if !ok || p.Off > size {
		return Pos{Seg: s.segs[0]}
	}
	return p
}

func (s *Spool) segPath(id uint64) string {
	return filepath.Join(s.dir, fmt.Sprintf("%020d%s", id, segSuffix))
}

func (s *Spool) createSegment(id uint64) error {
	f, err := os.OpenFile(s.segPath(id), os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if s.w != nil {
		if err := s.w.Close(); err != nil {
			f.Close()
			return err
		}
	}
	s.w = f
	s.segs = append(s.segs, id)
	s.sizes[id] = 0
	return nil
}

func (s *Spool) head() uint64 { return s.segs[len(s.segs)-1] }

// Append durably writes one record and returns once it is fsynced.
func (s *Spool) Append(payload []byte) (Pos, error) {
	n := int64(headerLen + len(payload))
	s.mu.Lock()
	defer s.mu.Unlock()
	if int64(len(payload)) > maxRecord {
		return Pos{}, fmt.Errorf("spool: record of %d bytes exceeds limit", len(payload))
	}
	if s.total+n > s.opts.MaxBytes {
		return Pos{}, ErrFull
	}
	if head := s.head(); s.sizes[head] > 0 && s.sizes[head]+n > s.opts.SegmentBytes {
		if err := s.createSegment(head + 1); err != nil {
			return Pos{}, err
		}
	}
	rec := make([]byte, n)
	binary.BigEndian.PutUint32(rec[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(rec[4:8], crc32.Checksum(payload, crcTable))
	copy(rec[headerLen:], payload)
	if _, err := s.w.Write(rec); err != nil {
		return Pos{}, err
	}
	if err := s.w.Sync(); err != nil {
		return Pos{}, err
	}
	head := s.head()
	at := Pos{Seg: head, Off: s.sizes[head]}
	s.sizes[head] += n
	s.total += n
	select {
	case s.notifyCh <- struct{}{}:
	default:
	}
	return at, nil
}

// Notify receives a value after appends. It is meant for a single reader.
func (s *Spool) Notify() <-chan struct{} { return s.notifyCh }

// Acked is the position of the first unacknowledged record.
func (s *Spool) Acked() Pos {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.acked
}

// Ack marks every record before next as delivered and deletes segments that
// are now entirely acknowledged. It never moves the cursor backwards.
func (s *Spool) Ack(next Pos) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.acked.Less(next) {
		return nil
	}
	s.acked = next
	s.dirty = true
	for len(s.segs) > 1 && s.segs[0] < next.Seg {
		id := s.segs[0]
		if err := os.Remove(s.segPath(id)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		s.total -= s.sizes[id]
		delete(s.sizes, id)
		s.segs = s.segs[1:]
	}
	return nil
}

// SaveCursor persists the ack cursor if it moved. A stale cursor only causes
// resends, which downstream deduplicates by log ID, so this runs on a timer
// rather than on every ack.
func (s *Spool) SaveCursor() error {
	s.mu.Lock()
	if !s.dirty {
		s.mu.Unlock()
		return nil
	}
	raw, _ := json.Marshal(s.acked)
	s.dirty = false
	s.mu.Unlock()
	return WriteFileAtomic(filepath.Join(s.dir, cursorFile), raw)
}

// Bytes is the on-disk size of all segments.
func (s *Spool) Bytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.total
}

// Close saves the cursor and closes the active segment.
func (s *Spool) Close() error {
	err := s.SaveCursor()
	s.mu.Lock()
	defer s.mu.Unlock()
	return errors.Join(err, s.w.Close())
}

// Reader iterates committed records from a position.
type Reader struct {
	s   *Spool
	pos Pos
	f   *os.File
	seg uint64
}

// NewReader starts reading at from, normally s.Acked().
func (s *Spool) NewReader(from Pos) *Reader { return &Reader{s: s, pos: from} }

// Record is one payload plus the position just past it.
type Record struct {
	Payload []byte
	Next    Pos
}

// Next returns the next committed record, or ok=false when the reader has
// caught up with the writer.
func (r *Reader) Next() (rec Record, ok bool, err error) {
	for {
		r.s.mu.Lock()
		size, exists := r.s.sizes[r.pos.Seg]
		var nextSeg uint64
		for _, id := range r.s.segs {
			if id > r.pos.Seg {
				nextSeg = id
				break
			}
		}
		r.s.mu.Unlock()

		if !exists && nextSeg == 0 {
			return Record{}, false, fmt.Errorf("spool: segment %d vanished under reader", r.pos.Seg)
		}
		if !exists || r.pos.Off >= size {
			if nextSeg == 0 {
				return Record{}, false, nil
			}
			r.pos = Pos{Seg: nextSeg}
			continue
		}
		return r.read()
	}
}

func (r *Reader) read() (Record, bool, error) {
	if r.f == nil || r.seg != r.pos.Seg {
		if r.f != nil {
			r.f.Close()
		}
		f, err := os.Open(r.s.segPath(r.pos.Seg))
		if err != nil {
			return Record{}, false, err
		}
		r.f, r.seg = f, r.pos.Seg
	}
	var hdr [headerLen]byte
	if _, err := r.f.ReadAt(hdr[:], r.pos.Off); err != nil {
		return Record{}, false, fmt.Errorf("spool: read header at %v: %w", r.pos, err)
	}
	n := int64(binary.BigEndian.Uint32(hdr[0:4]))
	if n > maxRecord {
		return Record{}, false, fmt.Errorf("spool: record length %d at %v exceeds limit", n, r.pos)
	}
	payload := make([]byte, n)
	if _, err := r.f.ReadAt(payload, r.pos.Off+headerLen); err != nil {
		return Record{}, false, fmt.Errorf("spool: read payload at %v: %w", r.pos, err)
	}
	if crc32.Checksum(payload, crcTable) != binary.BigEndian.Uint32(hdr[4:8]) {
		return Record{}, false, fmt.Errorf("spool: checksum mismatch at %v", r.pos)
	}
	r.pos.Off += headerLen + int64(len(payload))
	return Record{Payload: payload, Next: r.pos}, true, nil
}

// Close releases the reader's file handle.
func (r *Reader) Close() error {
	if r.f == nil {
		return nil
	}
	return r.f.Close()
}

// scanValid returns the length of the longest prefix of whole, checksummed records.
func scanValid(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	var off int64
	var hdr [headerLen]byte
	for {
		if _, err := f.ReadAt(hdr[:], off); err != nil {
			if errors.Is(err, io.EOF) {
				return off, nil
			}
			return 0, err
		}
		n := int64(binary.BigEndian.Uint32(hdr[0:4]))
		if n > maxRecord || off+headerLen+n > fi.Size() {
			return off, nil
		}
		payload := make([]byte, n)
		if _, err := f.ReadAt(payload, off+headerLen); err != nil {
			if errors.Is(err, io.EOF) {
				return off, nil
			}
			return 0, err
		}
		if crc32.Checksum(payload, crcTable) != binary.BigEndian.Uint32(hdr[4:8]) {
			return off, nil
		}
		off += headerLen + int64(len(payload))
	}
}

// WriteFileAtomic replaces path so a crash leaves either the old or the new
// contents, never a partial file.
func WriteFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
