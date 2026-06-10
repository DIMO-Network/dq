package materializer

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// memStore is a local in-memory ObjectStore fake.
type memStore struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newMemStore() *memStore {
	return &memStore{objects: make(map[string][]byte)}
}

func (s *memStore) List(_ context.Context, prefix string) ([]ObjectInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var infos []ObjectInfo
	for key, body := range s.objects {
		if strings.HasPrefix(key, prefix) {
			infos = append(infos, ObjectInfo{Key: key, Size: int64(len(body))})
		}
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].Key < infos[j].Key })
	return infos, nil
}

func (s *memStore) GetObject(_ context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	body, ok := s.objects[key]
	if !ok {
		return nil, fmt.Errorf("%s: %w", key, ErrNotFound)
	}
	out := make([]byte, len(body))
	copy(out, body)
	return out, nil
}

func (s *memStore) PutObject(_ context.Context, key string, body []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored := make([]byte, len(body))
	copy(stored, body)
	s.objects[key] = stored
	return nil
}

func (s *memStore) keys(prefix string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var keys []string
	for key := range s.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

// clone returns a deep copy of the store, used to seed identical
// crash-injection scenarios.
func (s *memStore) clone() *memStore {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := newMemStore()
	for key, body := range s.objects {
		stored := make([]byte, len(body))
		copy(stored, body)
		out.objects[key] = stored
	}
	return out
}

var errInjected = errors.New("injected write failure")

// flakyStore wraps an ObjectStore and fails every PutObject after the
// first allowedPuts writes, simulating a crash at an arbitrary point in
// the commit protocol.
type flakyStore struct {
	ObjectStore
	mu          sync.Mutex
	allowedPuts int
	puts        int
}

func (f *flakyStore) PutObject(ctx context.Context, key string, body []byte) error {
	f.mu.Lock()
	if f.puts >= f.allowedPuts {
		f.mu.Unlock()
		return fmt.Errorf("put %s: %w", key, errInjected)
	}
	f.puts++
	f.mu.Unlock()
	return f.ObjectStore.PutObject(ctx, key, body)
}

// countingStore wraps an ObjectStore and counts PutObject calls so tests
// can enumerate every crash point.
type countingStore struct {
	ObjectStore
	mu   sync.Mutex
	puts int
}

func (c *countingStore) PutObject(ctx context.Context, key string, body []byte) error {
	c.mu.Lock()
	c.puts++
	c.mu.Unlock()
	return c.ObjectStore.PutObject(ctx, key, body)
}
