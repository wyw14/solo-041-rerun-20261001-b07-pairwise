package journal

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"

	"github.com/wyw14/cry-97/internal/model"
)

// CursorStore persists the last accepted sample sequence for every
// (line, sample kind) pair. Each sample kind carries its own monotonically
// increasing sequence, so the durable cursor tracks them independently.
type CursorStore struct {
	path string
	mu   sync.Mutex
	data map[model.LineID]map[model.SampleKind]uint64
}

func NewCursorStore(path string) (*CursorStore, error) {
	if path == "" {
		return nil, errors.New("cursor path is required")
	}
	store := &CursorStore{path: path, data: make(map[model.LineID]map[model.SampleKind]uint64)}
	if err := store.load(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *CursorStore) Current(lineID model.LineID, kind model.SampleKind) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data[lineID][kind]
}

// LineCursors returns a copy of every per-kind cursor for a line.
func (s *CursorStore) LineCursors(lineID model.LineID) map[model.SampleKind]uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[model.SampleKind]uint64)
	for kind, value := range s.data[lineID] {
		result[kind] = value
	}
	return result
}

func (s *CursorStore) Commit(lineID model.LineID, kind model.SampleKind, sequence uint64) error {
	if lineID == "" || kind == "" || sequence == 0 {
		return errors.New("cursor identity is incomplete")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if byKind := s.data[lineID]; sequence <= byKind[kind] {
		return nil
	}
	next := make(map[model.LineID]map[model.SampleKind]uint64, len(s.data)+1)
	for existingLine, existingKinds := range s.data {
		clonedKinds := make(map[model.SampleKind]uint64, len(existingKinds)+1)
		for existingKind, value := range existingKinds {
			clonedKinds[existingKind] = value
		}
		next[existingLine] = clonedKinds
	}
	if next[lineID] == nil {
		next[lineID] = make(map[model.SampleKind]uint64)
	}
	next[lineID][kind] = sequence
	if err := writeCursorFile(s.path, next); err != nil {
		return err
	}
	s.data = next
	return nil
}

func (s *CursorStore) load() error {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(data, &s.data)
}

func writeCursorFile(path string, value map[model.LineID]map[model.SampleKind]uint64) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	temporary := path + ".next"
	if err := os.WriteFile(temporary, append(data, '\n'), 0o640); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}
