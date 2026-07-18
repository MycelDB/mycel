package replication

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/myceldb/mycel/internal/clustering/replsnapshot"
)

type materializedInstallTransaction struct {
	dataDir   string
	backupDir string
	files     []installedSnapshotFile
}

type installedSnapshotFile struct {
	path      string
	existed   bool
	mode      fs.FileMode
	backupRel string
}

func installMaterializedSnapshot(ctx context.Context, dataDir, unpacked, backupDir string, manifest replsnapshot.Manifest) (*materializedInstallTransaction, error) {
	policy := replsnapshot.DefaultResyncSnapshotPathPolicy()
	tx := &materializedInstallTransaction{dataDir: dataDir, backupDir: backupDir}
	if err := os.MkdirAll(backupDir, 0o700); err != nil {
		return nil, err
	}
	for _, file := range manifest.Files {
		if err := ctx.Err(); err != nil {
			_ = tx.Rollback(context.Background())
			return nil, err
		}
		if !policy.IsIncluded(file.Path) {
			_ = tx.Rollback(context.Background())
			return nil, errors.New("snapshot attempted to install unmanaged or preserved path")
		}
		src := filepath.Join(unpacked, filepath.FromSlash(file.Path))
		dst := filepath.Join(dataDir, filepath.FromSlash(file.Path))
		entry := installedSnapshotFile{path: file.Path, mode: file.Mode.Perm(), backupRel: file.Path}
		if info, err := os.Stat(dst); err == nil {
			entry.existed = true
			entry.mode = info.Mode().Perm()
			backup := filepath.Join(backupDir, filepath.FromSlash(entry.backupRel))
			if err := os.MkdirAll(filepath.Dir(backup), 0o700); err != nil {
				_ = tx.Rollback(context.Background())
				return nil, err
			}
			if err := copyRegularFile(dst, backup, entry.mode); err != nil {
				_ = tx.Rollback(context.Background())
				return nil, err
			}
		} else if !os.IsNotExist(err) {
			_ = tx.Rollback(context.Background())
			return nil, err
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
			_ = tx.Rollback(context.Background())
			return nil, err
		}
		if err := copyRegularFile(src, dst, file.Mode.Perm()); err != nil {
			_ = tx.Rollback(context.Background())
			return nil, err
		}
		tx.files = append(tx.files, entry)
	}
	return tx, nil
}

func (tx *materializedInstallTransaction) Rollback(ctx context.Context) error {
	if tx == nil {
		return nil
	}
	var firstErr error
	for i := len(tx.files) - 1; i >= 0; i-- {
		if err := ctx.Err(); err != nil && firstErr == nil {
			firstErr = err
			continue
		}
		file := tx.files[i]
		dst := filepath.Join(tx.dataDir, filepath.FromSlash(file.path))
		if file.existed {
			backup := filepath.Join(tx.backupDir, filepath.FromSlash(file.backupRel))
			if err := copyRegularFile(backup, dst, file.mode); err != nil && firstErr == nil {
				firstErr = err
			}
		} else if err := os.Remove(dst); err != nil && !os.IsNotExist(err) && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func copyRegularFile(src, dst string, mode fs.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	info, err := in.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("snapshot source is not regular file")
	}
	tmp := dst + ".snapshot.tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	if copyErr != nil {
		_ = os.Remove(tmp)
		return copyErr
	}
	if closeErr != nil {
		_ = os.Remove(tmp)
		return closeErr
	}
	return os.Rename(tmp, dst)
}
