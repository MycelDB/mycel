// Package blobstorage implements a per-space content-addressed blob store.
//
// Blobs are immutable files named by the SHA-256 digest of their content,
// stored under <space-blobs-dir>/objects/<aa>/<digest> with a two-character
// fan-out. Writes stream through a staging file in <space-blobs-dir>/tmp and
// are renamed into place after fsync, so a fully visible object is always
// complete. Identical content is stored once (dedup).
package blobstorage

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/myceldb/mycel/domain/graph"
)

var (
	ErrNotFound     = errors.New("blob storage: not found")
	ErrInvalidInput = errors.New("blob storage: invalid input")
)

const (
	objectsDirName = "objects"
	tmpDirName     = "tmp"
	// staleTmpAge is how old a staging file must be before sweep removes it,
	// protecting in-flight writes from concurrent sweeps.
	staleTmpAge = time.Hour
)

// Config carries runtime knobs for a blob store.
type Config struct {
	StaleTmpAge time.Duration
}

// Store is a content-addressed blob store rooted at a per-space directory.
type Store struct {
	path        string
	staleTmpAge time.Duration
}

// Open initializes (creating if needed) the blob store layout at path.
func Open(path string) (*Store, error) {
	return OpenWithConfig(path, Config{})
}

// OpenWithConfig initializes (creating if needed) the blob store layout at path.
func OpenWithConfig(path string, cfg Config) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("%w: path is required", ErrInvalidInput)
	}
	for _, dir := range []string{path, filepath.Join(path, objectsDirName), filepath.Join(path, tmpDirName)} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, err
		}
	}
	staleAge := cfg.StaleTmpAge
	if staleAge <= 0 {
		staleAge = staleTmpAge
	}
	return &Store{path: path, staleTmpAge: staleAge}, nil
}

// StagedBlob is a prepared blob write. New content remains in tmp until
// Promote is called; deduplicated existing content has no tmp file.
type StagedBlob struct {
	ID        graph.BlobID
	SizeBytes int64
	tmpPath   string
	existing  bool
}

// Existing reports whether this staged blob refers to content that was already
// present before Stage was called.
func (b StagedBlob) Existing() bool { return b.existing }

// Put streams r into the store and returns the content address and size.
// Content already present is deduplicated.
func (s *Store) Put(ctx context.Context, r io.Reader) (graph.BlobID, int64, error) {
	staged, err := s.Stage(ctx, r)
	if err != nil {
		return "", 0, err
	}
	if err := s.Promote(ctx, staged); err != nil {
		_ = s.Discard(ctx, staged)
		return "", 0, err
	}
	return staged.ID, staged.SizeBytes, nil
}

// Stage streams r into a temporary file and returns its content address without
// making new content visible in objects/. If the same content already exists,
// the temporary file is discarded and Promote becomes a no-op.
func (s *Store) Stage(ctx context.Context, r io.Reader) (StagedBlob, error) {
	if err := ctx.Err(); err != nil {
		return StagedBlob{}, err
	}
	if r == nil {
		return StagedBlob{}, fmt.Errorf("%w: reader is required", ErrInvalidInput)
	}
	tmp, err := os.CreateTemp(filepath.Join(s.path, tmpDirName), "put-*.blob")
	if err != nil {
		return StagedBlob{}, err
	}
	tmpPath := tmp.Name()
	cleanup := func() {
		tmp.Close()
		os.Remove(tmpPath)
	}

	hasher := sha256.New()
	size, err := io.Copy(io.MultiWriter(tmp, hasher), r)
	if err != nil {
		cleanup()
		return StagedBlob{}, err
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return StagedBlob{}, err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return StagedBlob{}, err
	}

	id, err := graph.BlobIDFromBytes(hasher.Sum(nil))
	if err != nil {
		os.Remove(tmpPath)
		return StagedBlob{}, err
	}
	objPath, err := s.objectPath(id)
	if err != nil {
		os.Remove(tmpPath)
		return StagedBlob{}, err
	}
	if _, err := os.Stat(objPath); err == nil {
		os.Remove(tmpPath)
		return StagedBlob{ID: id, SizeBytes: size, existing: true}, nil
	} else if !os.IsNotExist(err) {
		os.Remove(tmpPath)
		return StagedBlob{}, err
	}
	return StagedBlob{ID: id, SizeBytes: size, tmpPath: tmpPath}, nil
}

// Promote makes a staged blob visible in objects/. It is safe to call for
// deduplicated existing content.
func (s *Store) Promote(ctx context.Context, staged StagedBlob) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if staged.ID == "" {
		return fmt.Errorf("%w: staged blob id is required", ErrInvalidInput)
	}
	if staged.existing || staged.tmpPath == "" {
		return nil
	}
	objPath, err := s.objectPath(staged.ID)
	if err != nil {
		return err
	}
	if _, err := os.Stat(objPath); err == nil {
		return os.Remove(staged.tmpPath)
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(objPath), 0o700); err != nil {
		return err
	}
	return os.Rename(staged.tmpPath, objPath)
}

// Discard removes staged temporary content. It does not remove deduplicated
// existing objects.
func (s *Store) Discard(ctx context.Context, staged StagedBlob) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if staged.existing || staged.tmpPath == "" {
		return nil
	}
	if err := os.Remove(staged.tmpPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// Open returns a streaming reader over the blob's content.
func (s *Store) Open(ctx context.Context, id graph.BlobID) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	objPath, err := s.objectPath(id)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(objPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return f, nil
}

// Exists reports whether a blob is stored.
func (s *Store) Exists(ctx context.Context, id graph.BlobID) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	objPath, err := s.objectPath(id)
	if err != nil {
		return false, err
	}
	if _, err := os.Stat(objPath); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Size returns the stored blob's size in bytes.
func (s *Store) Size(ctx context.Context, id graph.BlobID) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	objPath, err := s.objectPath(id)
	if err != nil {
		return 0, err
	}
	info, err := os.Stat(objPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, ErrNotFound
		}
		return 0, err
	}
	return info.Size(), nil
}

// Delete removes a blob. Deleting a missing blob is not an error.
func (s *Store) Delete(ctx context.Context, id graph.BlobID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	objPath, err := s.objectPath(id)
	if err != nil {
		return err
	}
	if err := os.Remove(objPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// List returns the IDs of all stored blobs.
func (s *Store) List(ctx context.Context) ([]graph.BlobID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	objectsDir := filepath.Join(s.path, objectsDirName)
	fanouts, err := os.ReadDir(objectsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	out := []graph.BlobID{}
	for _, fan := range fanouts {
		if !fan.IsDir() {
			continue
		}
		entries, err := os.ReadDir(filepath.Join(objectsDir, fan.Name()))
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			id := graph.BlobID(entry.Name())
			if _, err := id.Bytes(); err != nil {
				continue
			}
			out = append(out, id)
		}
	}
	return out, nil
}

// SweepTmp removes stale staging files left behind by interrupted writes.
func (s *Store) SweepTmp(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tmpDir := filepath.Join(s.path, tmpDirName)
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	cutoff := time.Now().Add(-s.staleTmpAge)
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			os.Remove(filepath.Join(tmpDir, entry.Name()))
		}
	}
	return nil
}

func (s *Store) objectPath(id graph.BlobID) (string, error) {
	if _, err := id.Bytes(); err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	name := string(id)
	return filepath.Join(s.path, objectsDirName, name[:2], name), nil
}
