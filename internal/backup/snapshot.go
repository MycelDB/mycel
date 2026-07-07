package backup

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

func validateBackupDir(dataDir string, backupDir string) (string, string, error) {
	dataDir = strings.TrimSpace(dataDir)
	backupDir = strings.TrimSpace(backupDir)
	if dataDir == "" {
		return "", "", fmt.Errorf("data dir is required")
	}
	if backupDir == "" {
		return "", "", fmt.Errorf("backup dir is required")
	}
	dataAbs, err := filepath.Abs(dataDir)
	if err != nil {
		return "", "", err
	}
	backupAbs, err := filepath.Abs(backupDir)
	if err != nil {
		return "", "", err
	}
	dataAbs = filepath.Clean(dataAbs)
	backupAbs = filepath.Clean(backupAbs)
	dataReal, err := filepath.EvalSymlinks(dataAbs)
	if err != nil {
		return "", "", fmt.Errorf("resolve data dir: %w", err)
	}
	if err := os.MkdirAll(backupAbs, 0o700); err != nil {
		return "", "", fmt.Errorf("create backup dir: %w", err)
	}
	backupReal, err := filepath.EvalSymlinks(backupAbs)
	if err != nil {
		return "", "", fmt.Errorf("resolve backup dir: %w", err)
	}
	dataReal = filepath.Clean(dataReal)
	backupReal = filepath.Clean(backupReal)
	if backupReal == dataReal {
		return "", "", fmt.Errorf("backup dir must not equal data dir")
	}
	rel, err := filepath.Rel(dataReal, backupReal)
	if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && rel != "." {
		return "", "", fmt.Errorf("backup dir must not be under data dir")
	}
	test, err := os.CreateTemp(backupReal, ".write-test-*")
	if err != nil {
		return "", "", fmt.Errorf("backup dir is not writable: %w", err)
	}
	name := test.Name()
	if err := test.Close(); err != nil {
		_ = os.Remove(name)
		return "", "", fmt.Errorf("backup dir write test: %w", err)
	}
	_ = os.Remove(name)
	return dataReal, backupReal, nil
}

func stageSnapshot(ctx context.Context, dataDir string, stagingDir string, includeLogs bool) error {
	if err := os.MkdirAll(stagingDir, 0o700); err != nil {
		return err
	}
	return filepath.WalkDir(dataDir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rel, err := filepath.Rel(dataDir, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		if !includeLogs && isLogPath(rel) {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		dst := filepath.Join(stagingDir, rel)
		if entry.IsDir() {
			return os.MkdirAll(dst, info.Mode().Perm())
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		return copyFile(path, dst, info.Mode().Perm())
	})
}

func isLogPath(rel string) bool {
	first := rel
	if idx := strings.IndexRune(rel, filepath.Separator); idx >= 0 {
		first = rel[:idx]
	}
	return first == "log" || first == "logs"
}

func copyFile(src string, dst string, mode fs.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	if copyErr != nil {
		_ = os.Remove(dst)
		return copyErr
	}
	if closeErr != nil {
		_ = os.Remove(dst)
		return closeErr
	}
	return nil
}

func createZipArchive(ctx context.Context, sourceDir string, archivePath string) error {
	if err := os.MkdirAll(filepath.Dir(archivePath), 0o700); err != nil {
		return err
	}
	out, err := os.OpenFile(archivePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	zw := zip.NewWriter(out)
	walkErr := filepath.WalkDir(sourceDir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		rel, err := filepath.Rel(sourceDir, path)
		if err != nil {
			return err
		}
		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return err
		}
		header.Name = filepath.ToSlash(rel)
		header.Method = zip.Deflate
		writer, err := zw.CreateHeader(header)
		if err != nil {
			return err
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		_, err = io.Copy(writer, in)
		closeErr := in.Close()
		if err != nil {
			return err
		}
		return closeErr
	})
	closeZipErr := zw.Close()
	closeFileErr := out.Close()
	if walkErr != nil {
		_ = os.Remove(archivePath)
		return walkErr
	}
	if closeZipErr != nil {
		_ = os.Remove(archivePath)
		return closeZipErr
	}
	if closeFileErr != nil {
		_ = os.Remove(archivePath)
		return closeFileErr
	}
	return nil
}

func fileSHA256(path string) (string, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer file.Close()
	h := sha256.New()
	size, err := io.Copy(h, file)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), size, nil
}
