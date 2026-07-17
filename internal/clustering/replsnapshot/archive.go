package replsnapshot

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const ManifestArchivePath = "resync_manifest.json"

func WriteZipSnapshot(ctx context.Context, sourceDir, archivePath string, manifest Manifest) error {
	if err := os.MkdirAll(filepath.Dir(archivePath), 0o700); err != nil {
		return err
	}
	out, err := os.OpenFile(archivePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	zw := zip.NewWriter(out)
	manifestJSON, err := manifest.JSON()
	if err != nil {
		_ = out.Close()
		return err
	}
	mw, err := zw.Create(ManifestArchivePath)
	if err != nil {
		_ = out.Close()
		return err
	}
	if _, err := mw.Write([]byte(manifestJSON)); err != nil {
		_ = out.Close()
		return err
	}
	for _, file := range manifest.Files {
		if err := ctx.Err(); err != nil {
			return err
		}
		src := filepath.Join(sourceDir, filepath.FromSlash(file.Path))
		info, err := os.Stat(src)
		if err != nil {
			return err
		}
		hdr, err := zip.FileInfoHeader(info)
		if err != nil {
			return err
		}
		hdr.Name = file.Path
		hdr.Method = zip.Deflate
		w, err := zw.CreateHeader(hdr)
		if err != nil {
			return err
		}
		in, err := os.Open(src)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(w, in)
		closeErr := in.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
	if err := zw.Close(); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

func ExtractZipSnapshot(ctx context.Context, archivePath, destDir string, policy SnapshotPathPolicy) (Manifest, error) {
	zr, err := zip.OpenReader(archivePath)
	if err != nil {
		return Manifest{}, err
	}
	defer zr.Close()
	var manifest Manifest
	for _, f := range zr.File {
		if f.Name == ManifestArchivePath {
			rc, err := f.Open()
			if err != nil {
				return Manifest{}, err
			}
			raw, err := io.ReadAll(rc)
			_ = rc.Close()
			if err != nil {
				return Manifest{}, err
			}
			manifest, err = ManifestFromJSON(string(raw))
			if err != nil {
				return Manifest{}, err
			}
			break
		}
	}
	if manifest.Version == 0 {
		return Manifest{}, fmt.Errorf("snapshot manifest missing")
	}
	if err := ValidateManifest(manifest, policy); err != nil {
		return Manifest{}, err
	}
	for _, mf := range manifest.Files {
		if err := ctx.Err(); err != nil {
			return Manifest{}, err
		}
		var zf *zip.File
		for _, f := range zr.File {
			if f.Name == mf.Path {
				zf = f
				break
			}
		}
		if zf == nil {
			return Manifest{}, fmt.Errorf("snapshot file missing: %s", mf.Path)
		}
		if zf.FileInfo().IsDir() {
			return Manifest{}, fmt.Errorf("unexpected directory entry: %s", mf.Path)
		}
		dst := filepath.Join(destDir, filepath.FromSlash(mf.Path))
		if err := os.MkdirAll(filepath.Dir(dst), 0700); err != nil {
			return Manifest{}, err
		}
		rc, err := zf.Open()
		if err != nil {
			return Manifest{}, err
		}
		out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mf.Mode.Perm())
		if err != nil {
			_ = rc.Close()
			return Manifest{}, err
		}
		_, copyErr := io.Copy(out, rc)
		closeOut := out.Close()
		closeIn := rc.Close()
		if copyErr != nil {
			return Manifest{}, copyErr
		}
		if closeOut != nil {
			return Manifest{}, closeOut
		}
		if closeIn != nil {
			return Manifest{}, closeIn
		}
		sum, size, err := FileSHA256(dst)
		if err != nil {
			return Manifest{}, err
		}
		if size != mf.Size || sum != mf.ChecksumSHA256 {
			return Manifest{}, fmt.Errorf("snapshot checksum mismatch for %s", mf.Path)
		}
	}
	return manifest, nil
}
