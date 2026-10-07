package cluster

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	backupcore "github.com/myceldb/mycel/internal/backup"
	"github.com/myceldb/mycel/internal/fsperm"
)

const BackupSetManifestName = "backup-set.json"

// RestorePlan describes the deterministic offline restore mapping for one
// validated cluster backup set.
type RestorePlan struct {
	BackupSetID   string                   `json:"backup_set_id"`
	ClusterID     string                   `json:"cluster_id"`
	State         string                   `json:"state"`
	CreatedAt     time.Time                `json:"created_at"`
	CompletedAt   time.Time                `json:"completed_at,omitempty"`
	ExpectedNodes int                      `json:"expected_nodes"`
	ArchiveFormat backupcore.ArchiveFormat `json:"archive_format"`
	ManifestPath  string                   `json:"manifest_path"`
	Warnings      []string                 `json:"warnings,omitempty"`
	Nodes         []RestorePlanNode        `json:"nodes"`
}

// RestorePlanNode maps one backup-set ordinal to its pod archive artifacts.
type RestorePlanNode struct {
	Ordinal        int    `json:"ordinal"`
	PodName        string `json:"pod_name"`
	NodeID         string `json:"node_id"`
	RaftNodeID     uint64 `json:"raft_node_id,omitempty"`
	ArchiveName    string `json:"archive_name"`
	ArchivePath    string `json:"archive_path,omitempty"`
	ArchiveURI     string `json:"archive_uri,omitempty"`
	ManifestName   string `json:"manifest_name"`
	ManifestPath   string `json:"manifest_path,omitempty"`
	ManifestURI    string `json:"manifest_uri,omitempty"`
	SizeBytes      int64  `json:"size_bytes"`
	ChecksumSHA256 string `json:"checksum_sha256"`
}

// RestoreLocalInput configures offline extraction of one ordinal archive.
type RestoreLocalInput struct {
	BackupSet string
	Ordinal   int
	DataDir   string
	Force     bool
}

// RestoreLocalResult summarizes a completed local ordinal restore.
type RestoreLocalResult struct {
	BackupSetID string `json:"backup_set_id"`
	ClusterID   string `json:"cluster_id"`
	Ordinal     int    `json:"ordinal"`
	PodName     string `json:"pod_name"`
	ArchivePath string `json:"archive_path"`
	DataDir     string `json:"data_dir"`
}

// BuildRestorePlan loads and validates a backup set for restore and verifies
// local archive/manifests/checksums where the backup set uses local paths.
func BuildRestorePlan(ctx context.Context, backupSet string) (RestorePlan, Manifest, error) {
	manifest, manifestPath, err := LoadManifestPath(ctx, backupSet)
	if err != nil {
		return RestorePlan{}, Manifest{}, err
	}
	if err := ValidateArchiveFiles(ctx, manifest); err != nil {
		return RestorePlan{}, Manifest{}, err
	}
	baseDir := filepath.Dir(manifestPath)
	plan := RestorePlan{BackupSetID: manifest.BackupSetID, ClusterID: manifest.ClusterID, State: manifest.State, CreatedAt: manifest.CreatedAt, CompletedAt: manifest.CompletedAt, ExpectedNodes: manifest.ExpectedNodes, ArchiveFormat: manifest.ArchiveFormat, ManifestPath: manifestPath, Warnings: restoreWarnings(manifest)}
	for _, node := range sortedNodes(manifest.Nodes) {
		archivePath := resolveArtifactPath(baseDir, node.ArchiveURI, node.ArchiveName, node.PodName)
		manifestPath := resolveArtifactPath(baseDir, node.ManifestURI, node.ManifestName, node.PodName)
		if archivePath == "" && node.ArchiveURI == "" {
			return RestorePlan{}, Manifest{}, fmt.Errorf("node ordinal %d archive %s is not locally resolvable", node.Ordinal, node.ArchiveName)
		}
		if manifestPath == "" && node.ManifestURI == "" {
			return RestorePlan{}, Manifest{}, fmt.Errorf("node ordinal %d manifest %s is not locally resolvable", node.Ordinal, node.ManifestName)
		}
		if archivePath != "" {
			if err := verifyNodeArchiveLocal(ctx, node, archivePath); err != nil {
				return RestorePlan{}, Manifest{}, err
			}
		}
		if manifestPath != "" {
			if err := verifyNodeManifestLocal(node, manifestPath); err != nil {
				return RestorePlan{}, Manifest{}, err
			}
		}
		plan.Nodes = append(plan.Nodes, RestorePlanNode{Ordinal: node.Ordinal, PodName: node.PodName, NodeID: node.NodeID, RaftNodeID: node.RaftNodeID, ArchiveName: node.ArchiveName, ArchivePath: archivePath, ArchiveURI: node.ArchiveURI, ManifestName: node.ManifestName, ManifestPath: manifestPath, ManifestURI: node.ManifestURI, SizeBytes: node.SizeBytes, ChecksumSHA256: node.ChecksumSHA256})
	}
	return plan, manifest, nil
}

func LoadManifestPath(ctx context.Context, backupSet string) (Manifest, string, error) {
	if err := ctx.Err(); err != nil {
		return Manifest{}, "", err
	}
	path := strings.TrimSpace(backupSet)
	if path == "" {
		return Manifest{}, "", fmt.Errorf("backup set path is required")
	}
	if strings.HasPrefix(path, "file://") {
		p, ok := localArtifactPath(path)
		if !ok {
			return Manifest{}, "", fmt.Errorf("invalid file backup-set URI %q", backupSet)
		}
		path = p
	} else if strings.Contains(path, "://") {
		return Manifest{}, "", fmt.Errorf("backup-set must be a local path or file:// URI")
	}
	info, err := os.Stat(path)
	if err != nil {
		return Manifest{}, "", err
	}
	if info.IsDir() {
		path = filepath.Join(path, BackupSetManifestName)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return Manifest{}, "", err
	}
	manifest, err := Parse(raw)
	if err != nil {
		return Manifest{}, "", err
	}
	if err := Validate(manifest, ValidationModeRestore); err != nil {
		return Manifest{}, "", err
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return Manifest{}, "", err
	}
	return manifest, abs, nil
}

func RestoreLocal(ctx context.Context, in RestoreLocalInput) (RestoreLocalResult, error) {
	if in.Ordinal < 0 {
		return RestoreLocalResult{}, fmt.Errorf("ordinal must be non-negative")
	}
	dataDir := strings.TrimSpace(in.DataDir)
	if dataDir == "" {
		return RestoreLocalResult{}, fmt.Errorf("target data directory is required")
	}
	plan, _, err := BuildRestorePlan(ctx, in.BackupSet)
	if err != nil {
		return RestoreLocalResult{}, err
	}
	var node RestorePlanNode
	found := false
	for _, candidate := range plan.Nodes {
		if candidate.Ordinal == in.Ordinal {
			node = candidate
			found = true
			break
		}
	}
	if !found {
		return RestoreLocalResult{}, fmt.Errorf("backup set %s has no ordinal %d", plan.BackupSetID, in.Ordinal)
	}
	if node.ArchivePath == "" {
		return RestoreLocalResult{}, fmt.Errorf("archive for ordinal %d is not a local file path", in.Ordinal)
	}
	absDataDir, err := filepath.Abs(dataDir)
	if err != nil {
		return RestoreLocalResult{}, err
	}
	if err := ensureRestoreTarget(absDataDir, in.Force); err != nil {
		return RestoreLocalResult{}, err
	}
	if err := ExtractArchive(ctx, plan.ArchiveFormat, node.ArchivePath, absDataDir); err != nil {
		return RestoreLocalResult{}, fmt.Errorf("extract ordinal %d archive: %w", in.Ordinal, err)
	}
	if err := verifyRestoredDataDirForNode(absDataDir, node); err != nil {
		return RestoreLocalResult{}, err
	}
	return RestoreLocalResult{BackupSetID: plan.BackupSetID, ClusterID: plan.ClusterID, Ordinal: in.Ordinal, PodName: node.PodName, ArchivePath: node.ArchivePath, DataDir: absDataDir}, nil
}

func ExtractArchive(ctx context.Context, format backupcore.ArchiveFormat, archivePath, dataDir string) error {
	switch format {
	case backupcore.ArchiveFormatZip:
		return extractZip(ctx, archivePath, dataDir)
	case backupcore.ArchiveFormatTar:
		file, err := os.Open(archivePath)
		if err != nil {
			return err
		}
		defer file.Close()
		return extractTar(ctx, tar.NewReader(file), dataDir)
	case backupcore.ArchiveFormatTarGz:
		file, err := os.Open(archivePath)
		if err != nil {
			return err
		}
		defer file.Close()
		gz, err := gzip.NewReader(file)
		if err != nil {
			return err
		}
		defer gz.Close()
		return extractTar(ctx, tar.NewReader(gz), dataDir)
	case backupcore.ArchiveFormatTarZst:
		file, err := os.Open(archivePath)
		if err != nil {
			return err
		}
		defer file.Close()
		zr, err := zstd.NewReader(file)
		if err != nil {
			return err
		}
		defer zr.Close()
		return extractTar(ctx, tar.NewReader(zr), dataDir)
	default:
		return fmt.Errorf("unsupported backup archive_format %q", format)
	}
}

func VerifyRestoredDataDir(dataDir string) error {
	return verifyRestoredDataDirForNode(dataDir, RestorePlanNode{})
}

func verifyRestoredDataDirForNode(dataDir string, node RestorePlanNode) error {
	nodePath := filepath.Join(dataDir, "meta", "clustering", "node.json")
	if err := requireRegularFile(nodePath); err != nil {
		return fmt.Errorf("restored clustering metadata missing: %w", err)
	}
	if node.NodeID != "" {
		raw, err := os.ReadFile(nodePath)
		if err != nil {
			return fmt.Errorf("read restored clustering metadata: %w", err)
		}
		var identity struct {
			NodeID string `json:"node_id"`
		}
		if err := json.Unmarshal(raw, &identity); err != nil {
			return fmt.Errorf("parse restored clustering metadata: %w", err)
		}
		if identity.NodeID != node.NodeID {
			return fmt.Errorf("restored clustering node_id=%s does not match ordinal %d manifest node_id=%s", identity.NodeID, node.Ordinal, node.NodeID)
		}
	}
	raftDir := filepath.Join(dataDir, "meta", "raft")
	if info, err := os.Stat(raftDir); err != nil {
		return fmt.Errorf("restored raft metadata missing: %w", err)
	} else if !info.IsDir() {
		return fmt.Errorf("restored raft metadata path is not a directory: %s", raftDir)
	}
	foundRaftFile := false
	if err := filepath.WalkDir(raftDir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		switch entry.Name() {
		case "hard_state.pb", "entries.pb", "entries.log", "conf_state.pb", "snapshot.pb":
			foundRaftFile = true
			return filepath.SkipAll
		}
		return nil
	}); err != nil {
		return err
	}
	if !foundRaftFile {
		return fmt.Errorf("restored raft metadata contains no raft storage files under %s", raftDir)
	}
	return nil
}

func ensureRestoreTarget(dataDir string, force bool) error {
	info, err := os.Stat(dataDir)
	if os.IsNotExist(err) {
		return os.MkdirAll(dataDir, fsperm.PrivateDir)
	}
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("data-dir is not a directory: %s", dataDir)
	}
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return err
	}
	if len(entries) > 0 && !force {
		return fmt.Errorf("data-dir %s is not empty; refusing offline restore; use an empty target data dir", dataDir)
	}
	return nil
}

func resolveArtifactPath(baseDir, uri, name, podName string) string {
	if path, ok := localArtifactPath(uri); ok && path != "" {
		if filepath.IsAbs(path) {
			return path
		}
		return filepath.Join(baseDir, path)
	}
	if strings.TrimSpace(name) == "" {
		return ""
	}
	candidates := []string{filepath.Join(baseDir, strings.TrimSpace(name))}
	if strings.TrimSpace(podName) != "" {
		candidates = append(candidates, filepath.Join(baseDir, strings.TrimSpace(podName), strings.TrimSpace(name)))
	}
	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	return ""
}

func verifyNodeArchiveLocal(ctx context.Context, node NodeArtifact, path string) error {
	if filepath.Base(path) != node.ArchiveName {
		return fmt.Errorf("archive path %s does not match archive_name %s", path, node.ArchiveName)
	}
	checksum, size, err := fileSHA256(ctx, path)
	if err != nil {
		return fmt.Errorf("verify archive ordinal %d: %w", node.Ordinal, err)
	}
	if checksum != node.ChecksumSHA256 {
		return fmt.Errorf("verify archive ordinal %d: checksum mismatch got %s want %s", node.Ordinal, checksum, node.ChecksumSHA256)
	}
	if node.SizeBytes >= 0 && size != node.SizeBytes {
		return fmt.Errorf("verify archive ordinal %d: size mismatch got %d want %d", node.Ordinal, size, node.SizeBytes)
	}
	return nil
}

func verifyNodeManifestLocal(node NodeArtifact, path string) error {
	if filepath.Base(path) != node.ManifestName {
		return fmt.Errorf("manifest path %s does not match manifest_name %s", path, node.ManifestName)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read node manifest ordinal %d: %w", node.Ordinal, err)
	}
	var manifest backupcore.Manifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return fmt.Errorf("parse node manifest ordinal %d: %w", node.Ordinal, err)
	}
	if manifest.ArchiveName != node.ArchiveName {
		return fmt.Errorf("node manifest ordinal %d archive_name=%s want %s", node.Ordinal, manifest.ArchiveName, node.ArchiveName)
	}
	if manifest.ChecksumSHA256 != node.ChecksumSHA256 {
		return fmt.Errorf("node manifest ordinal %d checksum=%s want %s", node.Ordinal, manifest.ChecksumSHA256, node.ChecksumSHA256)
	}
	if node.SizeBytes >= 0 && manifest.SizeBytes != node.SizeBytes {
		return fmt.Errorf("node manifest ordinal %d size=%d want %d", node.Ordinal, manifest.SizeBytes, node.SizeBytes)
	}
	return nil
}

func restoreWarnings(manifest Manifest) []string {
	warnings := []string{
		"Restore is offline: stop all myceld pods before extracting archives.",
		"Restore each archive only to the matching ordinal/PVC recorded in this plan.",
		"Recreate required Kubernetes secrets, TLS/mTLS material, backend auth tokens, and encryption-at-rest KEK provider material before starting restored pods.",
		"External blob/object-store payloads are not contained in local daemon data archives and must be restored separately when configured.",
	}
	if strings.TrimSpace(manifest.Image) != "" {
		warnings = append(warnings, "Use a compatible myceld image for the restored cluster: "+strings.TrimSpace(manifest.Image))
	}
	return warnings
}

func extractZip(ctx context.Context, archivePath, dataDir string) error {
	reader, err := zip.OpenReader(archivePath)
	if err != nil {
		return err
	}
	defer reader.Close()
	for _, file := range reader.File {
		if err := ctx.Err(); err != nil {
			return err
		}
		name, ok := safeArchiveName(file.Name)
		if !ok {
			return fmt.Errorf("unsafe archive entry %q", file.Name)
		}
		if name == "" {
			continue
		}
		target := filepath.Join(dataDir, filepath.FromSlash(name))
		if file.FileInfo().IsDir() {
			if err := os.MkdirAll(target, file.Mode().Perm()); err != nil {
				return err
			}
			continue
		}
		if !file.FileInfo().Mode().IsRegular() {
			continue
		}
		in, err := file.Open()
		if err != nil {
			return err
		}
		if err := writeExtractedFile(target, in, file.Mode().Perm()); err != nil {
			_ = in.Close()
			return err
		}
		if err := in.Close(); err != nil {
			return err
		}
	}
	return nil
}

func extractTar(ctx context.Context, reader *tar.Reader, dataDir string) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		header, err := reader.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		name, ok := safeArchiveName(header.Name)
		if !ok {
			return fmt.Errorf("unsafe archive entry %q", header.Name)
		}
		if name == "" {
			continue
		}
		target := filepath.Join(dataDir, filepath.FromSlash(name))
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, fs.FileMode(header.Mode).Perm()); err != nil {
				return err
			}
		case tar.TypeReg, tar.TypeRegA:
			if err := writeExtractedFile(target, reader, fs.FileMode(header.Mode).Perm()); err != nil {
				return err
			}
		default:
			continue
		}
	}
}

func safeArchiveName(name string) (string, bool) {
	name = strings.TrimSpace(filepath.ToSlash(name))
	if name == "" || name == "." {
		return "", true
	}
	if strings.HasPrefix(name, "/") || strings.Contains(name, "\x00") {
		return "", false
	}
	for _, part := range strings.Split(name, "/") {
		if part == ".." {
			return "", false
		}
	}
	clean := pathCleanSlash(name)
	if clean == "." {
		return "", true
	}
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return "", false
	}
	return clean, true
}

func pathCleanSlash(name string) string {
	return strings.TrimPrefix(filepath.ToSlash(filepath.Clean(filepath.FromSlash(name))), "./")
}

func writeExtractedFile(path string, in io.Reader, mode fs.FileMode) error {
	if mode == 0 {
		mode = fsperm.PrivateFile
	}
	if err := os.MkdirAll(filepath.Dir(path), fsperm.PrivateDir); err != nil {
		return err
	}
	out, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode.Perm())
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

func requireRegularFile(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("not a regular file: %s", path)
	}
	return nil
}

func SortRestorePlan(plan RestorePlan) RestorePlan {
	sort.SliceStable(plan.Nodes, func(i, j int) bool { return plan.Nodes[i].Ordinal < plan.Nodes[j].Ordinal })
	return plan
}
