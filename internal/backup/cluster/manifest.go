package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	backupcore "github.com/myceldb/mycel/internal/backup"
)

const ManifestVersion = 1

const StateSucceeded = "succeeded"

// Manifest describes one complete logical full-cluster backup set. Archive
// artifacts may live on different per-pod backup mounts or object-store paths;
// URI fields record the physical locations while names preserve portable
// pod/ordinal identity after copying.
type Manifest struct {
	Version       int                      `json:"version"`
	BackupSetID   string                   `json:"backup_set_id"`
	CreatedAt     time.Time                `json:"created_at"`
	CompletedAt   time.Time                `json:"completed_at,omitempty"`
	ClusterID     string                   `json:"cluster_id"`
	Complete      bool                     `json:"complete"`
	State         string                   `json:"state"`
	Reason        string                   `json:"reason,omitempty"`
	ManifestURI   string                   `json:"manifest_uri,omitempty"`
	ExpectedNodes int                      `json:"expected_nodes"`
	Image         string                   `json:"image,omitempty"`
	Namespace     string                   `json:"namespace,omitempty"`
	StatefulSet   string                   `json:"statefulset,omitempty"`
	DataDir       string                   `json:"data_dir,omitempty"`
	ArchiveFormat backupcore.ArchiveFormat `json:"archive_format"`
	RaftBarriers  map[string]uint64        `json:"raft_barriers,omitempty"`
	Nodes         []NodeArtifact           `json:"nodes"`
}

// NodeArtifact describes one pod/PVC archive in a backup set.
type NodeArtifact struct {
	PodName        string             `json:"pod_name"`
	NodeID         string             `json:"node_id"`
	Ordinal        int                `json:"ordinal"`
	RaftNodeID     uint64             `json:"raft_node_id,omitempty"`
	ArchiveName    string             `json:"archive_name"`
	ArchiveURI     string             `json:"archive_uri,omitempty"`
	ManifestName   string             `json:"manifest_name"`
	ManifestURI    string             `json:"manifest_uri,omitempty"`
	SizeBytes      int64              `json:"size_bytes"`
	ChecksumSHA256 string             `json:"checksum_sha256"`
	AppliedIndexes map[string]uint64  `json:"applied_indexes,omitempty"`
	RaftFreeze     RaftFreezeEvidence `json:"raft_freeze,omitempty"`
}

type RaftFreezeEvidence struct {
	LeaseID    string                     `json:"lease_id,omitempty"`
	AcquiredAt time.Time                  `json:"acquired_at,omitempty"`
	ReleasedAt time.Time                  `json:"released_at,omitempty"`
	ExpiresAt  time.Time                  `json:"expires_at,omitempty"`
	Groups     map[string]RaftFreezeGroup `json:"groups,omitempty"`
}

type RaftFreezeGroup struct {
	GroupID       string `json:"group_id,omitempty"`
	BarrierIndex  uint64 `json:"barrier_index"`
	AppliedIndex  uint64 `json:"applied_index"`
	CommitIndex   uint64 `json:"commit_index,omitempty"`
	Term          uint64 `json:"term,omitempty"`
	LastIndex     uint64 `json:"last_index,omitempty"`
	SnapshotIndex uint64 `json:"snapshot_index,omitempty"`
	Leader        uint64 `json:"leader,omitempty"`
}

type ValidationMode int

const (
	ValidationModeBackupSet ValidationMode = iota
	ValidationModeRestore
)

// NewArchiveName returns the standard portable cluster-system backup archive
// filename for one pod.
func NewArchiveName(ts time.Time, podName string, backupSetID string, format backupcore.ArchiveFormat) (string, error) {
	podName = strings.TrimSpace(podName)
	backupSetID = strings.TrimSpace(backupSetID)
	if podName == "" {
		return "", fmt.Errorf("pod_name is required")
	}
	if backupSetID == "" {
		return "", fmt.Errorf("backup_set_id is required")
	}
	ext, err := backupcore.ArchiveExtension(format)
	if err != nil {
		return "", err
	}
	stamp := ts.UTC().Format("20060102T150405Z")
	return fmt.Sprintf("mycel-system-%s-%s-%s%s", stamp, podName, backupSetID, ext), nil
}

func NewNodeManifestName(archiveName string) (string, error) {
	archiveName = strings.TrimSpace(archiveName)
	if archiveName == "" {
		return "", fmt.Errorf("archive_name is required")
	}
	for _, suffix := range []string{".tar.zst", ".tar.gz", ".tar", ".zip"} {
		if strings.HasSuffix(archiveName, suffix) {
			return strings.TrimSuffix(archiveName, suffix) + ".manifest.json", nil
		}
	}
	return "", fmt.Errorf("archive_name %q has unsupported extension", archiveName)
}

// MarshalDeterministic emits stable JSON by sorting node artifacts by ordinal,
// pod name, and node ID before marshaling.
func (m Manifest) MarshalDeterministic() ([]byte, error) {
	m.Nodes = sortedNodes(m.Nodes)
	if m.RaftBarriers != nil && len(m.RaftBarriers) == 0 {
		m.RaftBarriers = nil
	}
	return json.MarshalIndent(m, "", "  ")
}

func Parse(data []byte) (Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return Manifest{}, err
	}
	m.Nodes = sortedNodes(m.Nodes)
	return m, nil
}

func Validate(m Manifest, mode ValidationMode) error {
	var problems []string
	if m.Version != ManifestVersion {
		problems = append(problems, fmt.Sprintf("version must be %d", ManifestVersion))
	}
	if strings.TrimSpace(m.BackupSetID) == "" {
		problems = append(problems, "backup_set_id is required")
	}
	if strings.TrimSpace(m.ClusterID) == "" {
		problems = append(problems, "cluster_id is required")
	}
	if strings.TrimSpace(m.State) == "" {
		problems = append(problems, "state is required")
	}
	if mode == ValidationModeRestore {
		if !m.Complete {
			problems = append(problems, "backup set must be complete for restore")
		}
		if m.State != StateSucceeded {
			problems = append(problems, "backup set state must be succeeded for restore")
		}
	}
	if m.ExpectedNodes <= 0 {
		problems = append(problems, "expected_nodes must be positive")
	}
	if len(m.Nodes) == 0 {
		problems = append(problems, "nodes are required")
	}
	if m.ExpectedNodes > 0 && len(m.Nodes) != m.ExpectedNodes {
		problems = append(problems, fmt.Sprintf("expected_nodes=%d but nodes has %d entries", m.ExpectedNodes, len(m.Nodes)))
	}
	if !backupcore.IsSupportedArchiveFormat(m.ArchiveFormat) {
		problems = append(problems, fmt.Sprintf("unsupported archive_format %q", m.ArchiveFormat))
	}
	ext := ""
	if backupcore.IsSupportedArchiveFormat(m.ArchiveFormat) {
		var err error
		ext, err = backupcore.ArchiveExtension(m.ArchiveFormat)
		if err != nil {
			problems = append(problems, err.Error())
		}
	}
	seenOrdinals := map[int]struct{}{}
	seenPods := map[string]struct{}{}
	seenArchives := map[string]struct{}{}
	for i, node := range m.Nodes {
		prefix := fmt.Sprintf("nodes[%d]", i)
		if strings.TrimSpace(node.PodName) == "" {
			problems = append(problems, prefix+": pod_name is required")
		}
		if strings.TrimSpace(node.NodeID) == "" {
			problems = append(problems, prefix+": node_id is required")
		}
		if node.Ordinal < 0 {
			problems = append(problems, prefix+": ordinal must be non-negative")
		}
		if _, ok := seenOrdinals[node.Ordinal]; ok {
			problems = append(problems, fmt.Sprintf("duplicate ordinal %d", node.Ordinal))
		}
		seenOrdinals[node.Ordinal] = struct{}{}
		podKey := strings.TrimSpace(node.PodName)
		if podKey != "" {
			if _, ok := seenPods[podKey]; ok {
				problems = append(problems, fmt.Sprintf("duplicate pod_name %s", podKey))
			}
			seenPods[podKey] = struct{}{}
		}
		archiveName := strings.TrimSpace(node.ArchiveName)
		if archiveName == "" {
			problems = append(problems, prefix+": archive_name is required")
		} else {
			if filepath.Base(archiveName) != archiveName {
				problems = append(problems, prefix+": archive_name must be a base name")
			}
			if ext != "" && !strings.HasSuffix(archiveName, ext) {
				problems = append(problems, prefix+": archive_name extension does not match archive_format")
			}
			if podKey != "" && m.BackupSetID != "" && ext != "" && !matchesArchiveName(archiveName, podKey, m.BackupSetID, ext) {
				problems = append(problems, prefix+": archive_name must match mycel-system-<utc_timestamp>-<pod_name>-<backup_set_id><archive_ext>")
			}
			if _, ok := seenArchives[archiveName]; ok {
				problems = append(problems, fmt.Sprintf("duplicate archive_name %s", archiveName))
			}
			seenArchives[archiveName] = struct{}{}
		}
		manifestName := strings.TrimSpace(node.ManifestName)
		if manifestName == "" {
			problems = append(problems, prefix+": manifest_name is required")
		} else {
			if filepath.Base(manifestName) != manifestName {
				problems = append(problems, prefix+": manifest_name must be a base name")
			}
			if !strings.HasSuffix(manifestName, ".manifest.json") {
				problems = append(problems, prefix+": manifest_name must end with .manifest.json")
			}
			if archiveName != "" {
				expectedManifest, err := NewNodeManifestName(archiveName)
				if err != nil {
					problems = append(problems, prefix+": "+err.Error())
				} else if manifestName != expectedManifest {
					problems = append(problems, prefix+": manifest_name must match archive_name")
				}
			}
		}
		if node.SizeBytes < 0 {
			problems = append(problems, prefix+": size_bytes must not be negative")
		}
		if !validSHA256(node.ChecksumSHA256) {
			problems = append(problems, prefix+": checksum_sha256 must be a lowercase hex SHA-256")
		}
		if node.ArchiveURI != "" && !validArtifactURI(node.ArchiveURI) {
			problems = append(problems, prefix+": archive_uri is invalid")
		}
		if node.ManifestURI != "" && !validArtifactURI(node.ManifestURI) {
			problems = append(problems, prefix+": manifest_uri is invalid")
		}
		if mode == ValidationModeRestore && len(m.RaftBarriers) > 0 {
			if strings.TrimSpace(node.RaftFreeze.LeaseID) == "" {
				problems = append(problems, prefix+": raft_freeze.lease_id is required for restore")
			}
			if node.RaftFreeze.AcquiredAt.IsZero() {
				problems = append(problems, prefix+": raft_freeze.acquired_at is required for restore")
			}
			if node.RaftFreeze.ExpiresAt.IsZero() {
				problems = append(problems, prefix+": raft_freeze.expires_at is required for restore")
			}
			if !node.RaftFreeze.AcquiredAt.IsZero() && !node.RaftFreeze.ExpiresAt.IsZero() && !node.RaftFreeze.ExpiresAt.After(node.RaftFreeze.AcquiredAt) {
				problems = append(problems, prefix+": raft_freeze.expires_at must be after acquired_at")
			}
			if node.RaftFreeze.ReleasedAt.IsZero() {
				problems = append(problems, prefix+": raft_freeze.released_at is required for restore")
			}
			if len(node.RaftFreeze.Groups) != len(m.RaftBarriers) {
				problems = append(problems, prefix+": raft_freeze.groups must cover all raft barriers")
			}
			for groupID, barrier := range m.RaftBarriers {
				freezeGroup, ok := node.RaftFreeze.Groups[groupID]
				if !ok {
					problems = append(problems, prefix+": raft_freeze missing group "+groupID)
					continue
				}
				if freezeGroup.GroupID != "" && freezeGroup.GroupID != groupID {
					problems = append(problems, prefix+": raft_freeze group key does not match group_id for "+groupID)
				}
				if freezeGroup.BarrierIndex != barrier {
					problems = append(problems, fmt.Sprintf("%s: raft_freeze group %s barrier_index=%d want %d", prefix, groupID, freezeGroup.BarrierIndex, barrier))
				}
				if freezeGroup.AppliedIndex < barrier {
					problems = append(problems, fmt.Sprintf("%s: raft_freeze group %s applied_index=%d below barrier %d", prefix, groupID, freezeGroup.AppliedIndex, barrier))
				}
				if freezeGroup.CommitIndex != 0 && freezeGroup.CommitIndex < barrier {
					problems = append(problems, fmt.Sprintf("%s: raft_freeze group %s commit_index=%d below barrier %d", prefix, groupID, freezeGroup.CommitIndex, barrier))
				}
				if freezeGroup.LastIndex != 0 && freezeGroup.LastIndex < barrier {
					problems = append(problems, fmt.Sprintf("%s: raft_freeze group %s last_index=%d below barrier %d", prefix, groupID, freezeGroup.LastIndex, barrier))
				}
			}
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("invalid cluster backup manifest: %s", strings.Join(problems, "; "))
	}
	return nil
}

// ValidateArchiveFiles verifies archive checksums for local filesystem paths or
// file:// URIs in the manifest. Non-file URIs are ignored because their bytes
// are resolved by the deployment-specific storage layer.
func ValidateArchiveFiles(ctx context.Context, m Manifest) error {
	if err := Validate(m, ValidationModeRestore); err != nil {
		return err
	}
	for _, node := range m.Nodes {
		path, ok := localArtifactPath(node.ArchiveURI)
		if !ok {
			continue
		}
		checksum, size, err := fileSHA256(ctx, path)
		if err != nil {
			return fmt.Errorf("validate archive %s: %w", node.PodName, err)
		}
		if checksum != node.ChecksumSHA256 {
			return fmt.Errorf("validate archive %s: checksum mismatch got %s want %s", node.PodName, checksum, node.ChecksumSHA256)
		}
		if node.SizeBytes >= 0 && size != node.SizeBytes {
			return fmt.Errorf("validate archive %s: size mismatch got %d want %d", node.PodName, size, node.SizeBytes)
		}
	}
	return nil
}

func sortedNodes(nodes []NodeArtifact) []NodeArtifact {
	out := append([]NodeArtifact(nil), nodes...)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Ordinal != out[j].Ordinal {
			return out[i].Ordinal < out[j].Ordinal
		}
		if out[i].PodName != out[j].PodName {
			return out[i].PodName < out[j].PodName
		}
		return out[i].NodeID < out[j].NodeID
	})
	return out
}

func matchesArchiveName(archiveName string, podName string, backupSetID string, ext string) bool {
	pattern := `^mycel-system-\d{8}T\d{6}Z-` + regexp.QuoteMeta(podName) + `-` + regexp.QuoteMeta(backupSetID) + regexp.QuoteMeta(ext) + `$`
	return regexp.MustCompile(pattern).MatchString(archiveName)
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && strings.ToLower(value) == value
}

func validArtifactURI(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return true
	}
	if strings.Contains(value, "\x00") {
		return false
	}
	if strings.Contains(value, "://") {
		u, err := url.Parse(value)
		return err == nil && u.Scheme != "" && (u.Host != "" || u.Scheme == "file")
	}
	return filepath.IsAbs(value) || strings.HasPrefix(value, ".")
}

func localArtifactPath(value string) (string, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", false
	}
	if strings.HasPrefix(value, "file://") {
		u, err := url.Parse(value)
		if err != nil || u.Path == "" {
			return "", false
		}
		return u.Path, true
	}
	if strings.Contains(value, "://") {
		return "", false
	}
	return value, true
}

func fileSHA256(ctx context.Context, path string) (string, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer file.Close()
	h := sha256.New()
	buf := make([]byte, 1024*1024)
	var size int64
	for {
		if err := ctx.Err(); err != nil {
			return "", 0, err
		}
		n, readErr := file.Read(buf)
		if n > 0 {
			written, err := h.Write(buf[:n])
			if err != nil {
				return "", 0, err
			}
			size += int64(written)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return "", 0, readErr
		}
	}
	return hex.EncodeToString(h.Sum(nil)), size, nil
}
