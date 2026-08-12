package archive

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
)

const (
	Format        = "mycel-user-backup-v1"
	ManifestPath  = "manifest.json"
	DefaultMethod = "zstd"
)

type Manifest struct {
	Format         string    `json:"format"`
	Version        int       `json:"version"`
	CreatedAt      time.Time `json:"created_at"`
	Source         Source    `json:"source"`
	SubjectUser    User      `json:"subject_user"`
	Options        Options   `json:"options"`
	Spaces         []Space   `json:"spaces"`
	Files          []File    `json:"files"`
	ChecksumSHA256 string    `json:"checksum_sha256,omitempty"`
}

type Source struct {
	Endpoint string `json:"endpoint,omitempty"`
	Label    string `json:"label,omitempty"`
}

type User struct {
	PrincipalID string `json:"principal_id,omitempty"`
	UserID      string `json:"user_id,omitempty"` // legacy mycel-user-backup-v1 compatibility
	Username    string `json:"username"`
	State       string `json:"state,omitempty"`
}

type Options struct {
	IncludeBlobs          bool `json:"include_blobs"`
	IncludeSemanticConfig bool `json:"include_semantic_config"`
	IncludeInactiveSpaces bool `json:"include_inactive_spaces"`
}

type Space struct {
	SourceSpaceID    string   `json:"source_space_id"`
	Name             string   `json:"name"`
	OwnerPrincipalID string   `json:"owner_principal_id,omitempty"`
	OwnerUserID      string   `json:"owner_user_id,omitempty"` // legacy mycel-user-backup-v1 compatibility
	Domains          []Domain `json:"domains"`
}

type Domain struct {
	SourceDomainID string `json:"source_domain_id"`
	Key            string `json:"key"`
	Name           string `json:"name"`
	Description    string `json:"description,omitempty"`
	Default        bool   `json:"default"`
	System         bool   `json:"system,omitempty"`
	DiscoveryMode  string `json:"discovery_mode,omitempty"`
	SearchMode     string `json:"search_mode,omitempty"`
	SemanticMode   string `json:"semantic_mode,omitempty"`
	ReadOnly       bool   `json:"read_only,omitempty"`
	DataPath       string `json:"data_path"`
	SchemaPath     string `json:"schema_path,omitempty"`
}

type File struct {
	Path        string `json:"path"`
	SizeBytes   int64  `json:"size_bytes"`
	SHA256      string `json:"sha256"`
	ContentType string `json:"content_type,omitempty"`
}

type Entry struct {
	Path        string
	Data        []byte
	ContentType string
}

type Archive struct {
	Manifest Manifest
	Entries  map[string][]byte
}

func Write(w io.Writer, method string, manifest Manifest, entries []Entry) error {
	if method == "" {
		method = DefaultMethod
	}
	manifest.Format = Format
	if manifest.Version == 0 {
		manifest.Version = 1
	}
	if manifest.CreatedAt.IsZero() {
		manifest.CreatedAt = time.Now().UTC()
	}
	manifest.Files = make([]File, 0, len(entries))
	for _, entry := range entries {
		clean, err := cleanEntryPath(entry.Path)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(entry.Data)
		manifest.Files = append(manifest.Files, File{Path: clean, SizeBytes: int64(len(entry.Data)), SHA256: hex.EncodeToString(sum[:]), ContentType: entry.ContentType})
	}
	sort.Slice(manifest.Files, func(i, j int) bool { return manifest.Files[i].Path < manifest.Files[j].Path })
	manifest.ChecksumSHA256 = ""
	manifestRaw, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	manifestHash := sha256.Sum256(manifestRaw)
	manifest.ChecksumSHA256 = hex.EncodeToString(manifestHash[:])
	manifestRaw, err = json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	manifestRaw = append(manifestRaw, '\n')

	cw, closeFn, err := compressedWriter(w, method)
	if err != nil {
		return err
	}
	tw := tar.NewWriter(cw)
	if err := writeTarFile(tw, ManifestPath, manifestRaw); err != nil {
		return err
	}
	sorted := append([]Entry(nil), entries...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Path < sorted[j].Path })
	seen := map[string]struct{}{ManifestPath: {}}
	for _, entry := range sorted {
		clean, err := cleanEntryPath(entry.Path)
		if err != nil {
			return err
		}
		if _, ok := seen[clean]; ok {
			return fmt.Errorf("duplicate archive entry %q", clean)
		}
		seen[clean] = struct{}{}
		if err := writeTarFile(tw, clean, entry.Data); err != nil {
			return err
		}
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return closeFn()
}

func Read(r io.Reader, method string) (*Archive, error) {
	if method == "" {
		method = DefaultMethod
	}
	cr, closeFn, err := compressedReader(r, method)
	if err != nil {
		return nil, err
	}
	defer closeFn()
	tr := tar.NewReader(cr)
	entries := map[string][]byte{}
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read tar: %w", err)
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		clean, err := cleanEntryPath(hdr.Name)
		if err != nil {
			return nil, err
		}
		if _, ok := entries[clean]; ok {
			return nil, fmt.Errorf("duplicate archive entry %q", clean)
		}
		data, err := io.ReadAll(tr)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", clean, err)
		}
		entries[clean] = data
	}
	raw, ok := entries[ManifestPath]
	if !ok {
		return nil, fmt.Errorf("missing %s", ManifestPath)
	}
	var manifest Manifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return nil, fmt.Errorf("invalid manifest: %w", err)
	}
	delete(entries, ManifestPath)
	if err := Validate(manifest, entries); err != nil {
		return nil, err
	}
	return &Archive{Manifest: manifest, Entries: entries}, nil
}

func Validate(manifest Manifest, entries map[string][]byte) error {
	if manifest.Format != Format {
		return fmt.Errorf("unsupported backup format %q", manifest.Format)
	}
	if manifest.Version != 1 {
		return fmt.Errorf("unsupported backup version %d", manifest.Version)
	}
	if manifest.ChecksumSHA256 != "" {
		copy := manifest
		copy.ChecksumSHA256 = ""
		raw, err := json.MarshalIndent(copy, "", "  ")
		if err != nil {
			return fmt.Errorf("marshal manifest for checksum: %w", err)
		}
		sum := sha256.Sum256(raw)
		if !strings.EqualFold(manifest.ChecksumSHA256, hex.EncodeToString(sum[:])) {
			return fmt.Errorf("manifest checksum mismatch")
		}
	}
	if manifest.SubjectUser.PrincipalID == "" && manifest.SubjectUser.UserID == "" && manifest.SubjectUser.Username == "" {
		return fmt.Errorf("manifest subject_user requires principal_id, legacy user_id, or username")
	}
	wantFiles := map[string]File{}
	for _, file := range manifest.Files {
		clean, err := cleanEntryPath(file.Path)
		if err != nil {
			return err
		}
		wantFiles[clean] = file
	}
	for name, file := range wantFiles {
		data, ok := entries[name]
		if !ok {
			return fmt.Errorf("manifest file %q missing from archive", name)
		}
		if file.SizeBytes != int64(len(data)) {
			return fmt.Errorf("archive file %q size mismatch", name)
		}
		sum := sha256.Sum256(data)
		if !strings.EqualFold(file.SHA256, hex.EncodeToString(sum[:])) {
			return fmt.Errorf("archive file %q checksum mismatch", name)
		}
	}
	for name := range entries {
		if _, ok := wantFiles[name]; !ok {
			return fmt.Errorf("archive file %q missing from manifest", name)
		}
	}
	for _, sp := range manifest.Spaces {
		if strings.TrimSpace(sp.Name) == "" {
			return fmt.Errorf("space %q has empty name", sp.SourceSpaceID)
		}
		for _, d := range sp.Domains {
			if d.DataPath == "" {
				return fmt.Errorf("domain %q has empty data_path", d.SourceDomainID)
			}
			if _, ok := entries[d.DataPath]; !ok {
				return fmt.Errorf("domain data %q missing", d.DataPath)
			}
			if d.SchemaPath != "" {
				if _, ok := entries[d.SchemaPath]; !ok {
					return fmt.Errorf("domain schema %q missing", d.SchemaPath)
				}
			}
		}
	}
	return nil
}

func cleanEntryPath(p string) (string, error) {
	p = strings.TrimSpace(strings.ReplaceAll(p, "\\", "/"))
	if p == "" || strings.HasPrefix(p, "/") {
		return "", fmt.Errorf("invalid archive path %q", p)
	}
	clean := path.Clean(p)
	if clean == "." || strings.HasPrefix(clean, "../") || clean == ".." {
		return "", fmt.Errorf("invalid archive path %q", p)
	}
	return clean, nil
}

func writeTarFile(tw *tar.Writer, name string, data []byte) error {
	hdr := &tar.Header{Name: name, Mode: 0o600, Size: int64(len(data)), ModTime: time.Unix(0, 0).UTC(), Typeflag: tar.TypeReg}
	if err := tw.WriteHeader(hdr); err != nil {
		return fmt.Errorf("write tar header %s: %w", name, err)
	}
	if _, err := tw.Write(data); err != nil {
		return fmt.Errorf("write tar file %s: %w", name, err)
	}
	return nil
}

func compressedWriter(w io.Writer, method string) (io.Writer, func() error, error) {
	switch strings.ToLower(method) {
	case "zstd", "zst":
		zw, err := zstd.NewWriter(w)
		if err != nil {
			return nil, nil, err
		}
		return zw, zw.Close, nil
	case "gzip", "gz":
		gw := gzip.NewWriter(w)
		return gw, gw.Close, nil
	case "none", "tar":
		return w, func() error { return nil }, nil
	default:
		return nil, nil, fmt.Errorf("unsupported compression method %q", method)
	}
}

func compressedReader(r io.Reader, method string) (io.Reader, func() error, error) {
	if method == "auto" {
		buf, err := io.ReadAll(r)
		if err != nil {
			return nil, nil, err
		}
		if zr, err := zstd.NewReader(bytes.NewReader(buf)); err == nil {
			return zr, func() error { zr.Close(); return nil }, nil
		}
		if gr, err := gzip.NewReader(bytes.NewReader(buf)); err == nil {
			return gr, gr.Close, nil
		}
		return bytes.NewReader(buf), func() error { return nil }, nil
	}
	switch strings.ToLower(method) {
	case "zstd", "zst":
		zr, err := zstd.NewReader(r)
		if err != nil {
			return nil, nil, err
		}
		return zr, func() error { zr.Close(); return nil }, nil
	case "gzip", "gz":
		gr, err := gzip.NewReader(r)
		if err != nil {
			return nil, nil, err
		}
		return gr, gr.Close, nil
	case "none", "tar":
		return r, func() error { return nil }, nil
	default:
		return nil, nil, fmt.Errorf("unsupported compression method %q", method)
	}
}
