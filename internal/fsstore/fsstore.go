// Package fsstore is the local-filesystem materializer.ObjectStore for
// single-node deployments, mirroring din's fsstore semantics: slash keys,
// lexicographically sorted List, missing prefix lists empty, and atomic
// publish (temp file in the target directory + fsync + rename) so the
// compactor never reads a torn watermark and DuckDB never globs a partial
// parquet file.
package fsstore

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/DIMO-Network/dq/internal/materializer"
)

var (
	_ materializer.ObjectStore  = (*Store)(nil)
	_ materializer.CompactStore = (*Store)(nil)
)

// tempPrefix marks in-flight writes; List hides any dot-named entry.
const tempPrefix = ".tmp-"

// Store keeps objects under a root directory.
type Store struct {
	root string
}

// New returns a Store rooted at the absolute directory root, creating it if
// needed.
func New(root string) (*Store, error) {
	if !filepath.IsAbs(root) {
		return nil, fmt.Errorf("fsstore root must be an absolute path, got %q", root)
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("creating fsstore root: %w", err)
	}
	return &Store{root: root}, nil
}

func (s *Store) path(key string) string {
	return filepath.Join(s.root, filepath.FromSlash(key))
}

// List returns all objects whose key starts with prefix, sorted by key. A
// prefix whose directories don't exist yet lists empty — the materializer
// polls before din has written anything.
func (s *Store) List(_ context.Context, prefix string) ([]materializer.ObjectInfo, error) {
	dirPart, _ := path.Split(prefix)
	walkRoot := filepath.Join(s.root, filepath.FromSlash(dirPart))

	var out []materializer.ObjectInfo
	err := filepath.WalkDir(walkRoot, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if strings.HasPrefix(d.Name(), ".") {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil // in-flight temp files are not objects
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(s.root, p)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if !strings.HasPrefix(key, prefix) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil // deleted mid-walk (din compactor grace-delete)
			}
			return err
		}
		out = append(out, materializer.ObjectInfo{Key: key, Size: info.Size()})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("listing %s: %w", prefix, err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

// GetObject reads the object at key; a missing key wraps
// materializer.ErrNotFound per the ObjectStore contract.
func (s *Store) GetObject(_ context.Context, key string) ([]byte, error) {
	body, err := os.ReadFile(s.path(key))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", materializer.ErrNotFound, key)
		}
		return nil, fmt.Errorf("get object %s: %w", key, err)
	}
	return body, nil
}

// DeleteObject removes the object at key; missing keys are ignored (S3
// quiet-delete parity).
func (s *Store) DeleteObject(_ context.Context, key string) error {
	if err := os.Remove(s.path(key)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("delete object %s: %w", key, err)
	}
	return nil
}

// PutObject durably writes body at key: temp file in the target directory,
// fsync, atomic rename. Replacing an existing key (watermark.json on every
// batch) is atomic too.
func (s *Store) PutObject(_ context.Context, key string, body []byte) error {
	target := s.path(key)
	dir := filepath.Dir(target)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("creating directories for %s: %w", key, err)
	}
	tmp, err := os.CreateTemp(dir, tempPrefix+"*")
	if err != nil {
		return fmt.Errorf("creating temp file for %s: %w", key, err)
	}
	tmpName := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}
	if _, err := tmp.Write(body); err != nil {
		cleanup()
		return fmt.Errorf("writing %s: %w", key, err)
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("syncing %s: %w", key, err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("closing %s: %w", key, err)
	}
	if err := os.Chmod(tmpName, 0o644); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("setting mode for %s: %w", key, err)
	}
	if err := os.Rename(tmpName, target); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("publishing %s: %w", key, err)
	}
	// fsync the directory: the rename is not durable until the directory
	// entry is — and watermark.json must never silently vanish on crash.
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}
