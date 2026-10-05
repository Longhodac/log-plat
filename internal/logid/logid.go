// Package logid derives log IDs from where a line lives, not from when it was read.
package logid

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
)

// Len is the length of an ID in hex characters (128 bits).
const Len = 32

// New returns the ID for the line that starts at offset in source.
//
// The same (agentID, source, epoch, offset) always yields the same ID, so an
// agent that re-reads a line after a crash produces a duplicate that
// OpenSearch collapses instead of a second document.
func New(agentID, source string, epoch uint32, offset int64) string {
	h := sha256.New()
	var n [8]byte
	for _, s := range []string{agentID, source} {
		binary.BigEndian.PutUint64(n[:], uint64(len(s)))
		h.Write(n[:])
		h.Write([]byte(s))
	}
	binary.BigEndian.PutUint64(n[:], uint64(epoch))
	h.Write(n[:])
	binary.BigEndian.PutUint64(n[:], uint64(offset))
	h.Write(n[:])
	sum := h.Sum(nil)
	return hex.EncodeToString(sum[:Len/2])
}

// Valid reports whether id has the shape New produces.
func Valid(id string) bool {
	if len(id) != Len {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
