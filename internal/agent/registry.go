package agent

import (
	"encoding/json"
	"errors"
	"os"
	"sync"

	"github.com/Longhodac/log-plat/internal/spool"
)

// FileState is how far into a file the agent has spooled.
type FileState struct {
	Offset int64  `json:"offset"`
	Epoch  uint32 `json:"epoch"`
	Inode  uint64 `json:"inode"`
}

// Registry persists FileState per path. It records only spooled positions:
// it may lag the spool (lines get re-read and produce identical IDs) but it
// must never run ahead of it.
type Registry struct {
	path  string
	mu    sync.Mutex
	files map[string]FileState
	dirty bool
}

// LoadRegistry reads path, or starts empty if it does not exist.
func LoadRegistry(path string) (*Registry, error) {
	r := &Registry{path: path, files: map[string]FileState{}}
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return r, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(raw, &r.files); err != nil {
		return nil, err
	}
	return r, nil
}

// Get returns the state for path.
func (r *Registry) Get(path string) (FileState, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st, ok := r.files[path]
	return st, ok
}

// Set records st for path in memory.
func (r *Registry) Set(path string, st FileState) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.files[path] = st
	r.dirty = true
}

// Save writes the registry if it changed.
func (r *Registry) Save() error {
	r.mu.Lock()
	if !r.dirty {
		r.mu.Unlock()
		return nil
	}
	raw, err := json.Marshal(r.files)
	r.dirty = false
	r.mu.Unlock()
	if err != nil {
		return err
	}
	return spool.WriteFileAtomic(r.path, raw)
}
