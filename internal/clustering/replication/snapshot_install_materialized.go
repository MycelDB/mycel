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

func installMaterializedSnapshot(ctx context.Context, dataDir, unpacked string, manifest replsnapshot.Manifest) error {
	policy := replsnapshot.DefaultResyncSnapshotPathPolicy()
	for _, file := range manifest.Files {
		if err := ctx.Err(); err != nil {
			return err
		}
		if policy.IsPreserved(file.Path) || policy.IsExcluded(file.Path) {
			return errors.New("snapshot attempted to install preserved path")
		}
		src := filepath.Join(unpacked, filepath.FromSlash(file.Path))
		dst := filepath.Join(dataDir, filepath.FromSlash(file.Path))
		if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
			return err
		}
		if err := copyRegularFile(src, dst, file.Mode.Perm()); err != nil {
			return err
		}
	}
	return nil
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
