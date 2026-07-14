package backup

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/klauspost/compress/zstd"
)

func ArchiveExtension(format ArchiveFormat) (string, error) {
	switch format {
	case ArchiveFormatZip:
		return ".zip", nil
	case ArchiveFormatTar:
		return ".tar", nil
	case ArchiveFormatTarGz:
		return ".tar.gz", nil
	case ArchiveFormatTarZst:
		return ".tar.zst", nil
	default:
		return "", fmt.Errorf("unsupported backup archive_format %q", format)
	}
}

func IsSupportedArchiveFormat(format ArchiveFormat) bool {
	_, err := ArchiveExtension(format)
	return err == nil
}

func WriteArchive(ctx context.Context, format ArchiveFormat, sourceDir string, archivePath string) error {
	switch format {
	case ArchiveFormatZip:
		return createZipArchive(ctx, sourceDir, archivePath)
	case ArchiveFormatTar:
		return createTarArchive(ctx, sourceDir, archivePath, func(out io.Writer) (io.WriteCloser, error) {
			return nopWriteCloser{Writer: out}, nil
		})
	case ArchiveFormatTarGz:
		return createTarArchive(ctx, sourceDir, archivePath, func(out io.Writer) (io.WriteCloser, error) {
			return gzip.NewWriter(out), nil
		})
	case ArchiveFormatTarZst:
		return createTarArchive(ctx, sourceDir, archivePath, func(out io.Writer) (io.WriteCloser, error) {
			return zstd.NewWriter(out)
		})
	default:
		return fmt.Errorf("unsupported backup archive_format %q", format)
	}
}

type nopWriteCloser struct {
	io.Writer
}

func (n nopWriteCloser) Close() error { return nil }

func createTarArchive(ctx context.Context, sourceDir string, archivePath string, wrap func(io.Writer) (io.WriteCloser, error)) error {
	if err := os.MkdirAll(filepath.Dir(archivePath), 0o700); err != nil {
		return err
	}
	out, err := os.OpenFile(archivePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	wrapped, err := wrap(out)
	if err != nil {
		_ = out.Close()
		_ = os.Remove(archivePath)
		return err
	}
	tw := tar.NewWriter(wrapped)
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
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		header.Name = filepath.ToSlash(rel)
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		_, err = io.Copy(tw, in)
		closeErr := in.Close()
		if err != nil {
			return err
		}
		return closeErr
	})
	closeTarErr := tw.Close()
	closeWrappedErr := wrapped.Close()
	closeFileErr := out.Close()
	if walkErr != nil {
		_ = os.Remove(archivePath)
		return walkErr
	}
	if closeTarErr != nil {
		_ = os.Remove(archivePath)
		return closeTarErr
	}
	if closeWrappedErr != nil {
		_ = os.Remove(archivePath)
		return closeWrappedErr
	}
	if closeFileErr != nil {
		_ = os.Remove(archivePath)
		return closeFileErr
	}
	return nil
}
