package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	clusterbackup "github.com/myceldb/mycel/internal/backup/cluster"
	"github.com/myceldb/mycel/internal/wal"
)

type clusterBackupPhase string

const (
	clusterBackupPhaseRequested          clusterBackupPhase = "requested"
	clusterBackupPhasePrechecking        clusterBackupPhase = "prechecking"
	clusterBackupPhaseQuiescing          clusterBackupPhase = "quiescing"
	clusterBackupPhaseBarrierWait        clusterBackupPhase = "barrier_wait"
	clusterBackupPhaseArchiving          clusterBackupPhase = "archiving"
	clusterBackupPhaseValidating         clusterBackupPhase = "validating"
	clusterBackupPhaseCommittingManifest clusterBackupPhase = "committing_manifest"
	clusterBackupPhaseSucceeded          clusterBackupPhase = "succeeded"
	clusterBackupPhaseFailed             clusterBackupPhase = "failed"
	clusterBackupPhaseAborted            clusterBackupPhase = "aborted"
)

func (p clusterBackupPhase) terminal() bool {
	switch p {
	case clusterBackupPhaseSucceeded, clusterBackupPhaseFailed, clusterBackupPhaseAborted:
		return true
	default:
		return false
	}
}

type clusterBackupExpectedNode struct {
	PodName    string `json:"pod_name"`
	NodeID     string `json:"node_id"`
	Ordinal    int    `json:"ordinal"`
	RaftNodeID uint64 `json:"raft_node_id,omitempty"`
}

type clusterBackupFailure struct {
	Phase   clusterBackupPhase `json:"phase"`
	Message string             `json:"message"`
}

type clusterBackupRun struct {
	BackupSetID string                                `json:"backup_set_id"`
	ClusterID   string                                `json:"cluster_id,omitempty"`
	Reason      string                                `json:"reason,omitempty"`
	Phase       clusterBackupPhase                    `json:"phase"`
	CreatedAt   time.Time                             `json:"created_at"`
	UpdatedAt   time.Time                             `json:"updated_at"`
	Expected    []clusterBackupExpectedNode           `json:"expected_nodes,omitempty"`
	Barriers    map[string]uint64                     `json:"barriers,omitempty"`
	NodeResults map[string]clusterbackup.NodeArtifact `json:"node_results,omitempty"`
	Manifest    *clusterbackup.Manifest               `json:"manifest,omitempty"`
	Failure     *clusterBackupFailure                 `json:"failure,omitempty"`
}

type clusterBackupRequestRecord struct {
	BackupSetID string                      `json:"backup_set_id"`
	ClusterID   string                      `json:"cluster_id,omitempty"`
	Reason      string                      `json:"reason,omitempty"`
	CreatedAt   time.Time                   `json:"created_at"`
	Expected    []clusterBackupExpectedNode `json:"expected_nodes,omitempty"`
}

type clusterBackupPhaseRecord struct {
	BackupSetID string             `json:"backup_set_id"`
	Phase       clusterBackupPhase `json:"phase"`
	UpdatedAt   time.Time          `json:"updated_at"`
}

type clusterBackupBarrierRecord struct {
	BackupSetID string            `json:"backup_set_id"`
	Barriers    map[string]uint64 `json:"barriers"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

type clusterBackupNodeResultRecord struct {
	BackupSetID string                     `json:"backup_set_id"`
	Node        clusterbackup.NodeArtifact `json:"node"`
	UpdatedAt   time.Time                  `json:"updated_at"`
}

type clusterBackupCompleteRecord struct {
	BackupSetID string                 `json:"backup_set_id"`
	Manifest    clusterbackup.Manifest `json:"manifest"`
	CompletedAt time.Time              `json:"completed_at"`
}

type clusterBackupFailureRecord struct {
	BackupSetID string             `json:"backup_set_id"`
	Phase       clusterBackupPhase `json:"phase"`
	Message     string             `json:"message"`
	UpdatedAt   time.Time          `json:"updated_at"`
}

func (m *Module) applyClusterBackupRequest(ctx context.Context, rec wal.Record) error {
	var payload clusterBackupRequestRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	_ = ctx
	backupSetID := strings.TrimSpace(payload.BackupSetID)
	if backupSetID == "" {
		return fmt.Errorf("backup_set_id is required")
	}
	createdAt := payload.CreatedAt.UTC()
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.activeClusterBackupID != "" && m.activeClusterBackupID != backupSetID {
		return fmt.Errorf("cluster backup %s is already active", m.activeClusterBackupID)
	}
	m.ensureClusterBackupStateLocked()
	if _, ok := m.clusterBackups[backupSetID]; ok {
		return fmt.Errorf("cluster backup %s already exists", backupSetID)
	}
	run := clusterBackupRun{BackupSetID: backupSetID, ClusterID: strings.TrimSpace(payload.ClusterID), Reason: strings.TrimSpace(payload.Reason), Phase: clusterBackupPhaseRequested, CreatedAt: createdAt, UpdatedAt: createdAt, Expected: append([]clusterBackupExpectedNode(nil), payload.Expected...), NodeResults: map[string]clusterbackup.NodeArtifact{}}
	m.clusterBackups[backupSetID] = run
	m.activeClusterBackupID = backupSetID
	return nil
}

func (m *Module) applyClusterBackupPhase(ctx context.Context, rec wal.Record) error {
	var payload clusterBackupPhaseRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	_ = ctx
	if strings.TrimSpace(payload.BackupSetID) == "" {
		return fmt.Errorf("backup_set_id is required")
	}
	if !validClusterBackupPhase(payload.Phase) || payload.Phase.terminal() {
		return fmt.Errorf("invalid non-terminal cluster backup phase %q", payload.Phase)
	}
	updatedAt := payload.UpdatedAt.UTC()
	if updatedAt.IsZero() {
		updatedAt = time.Now().UTC()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	run, err := m.clusterBackupRunLocked(payload.BackupSetID)
	if err != nil {
		return err
	}
	run.Phase = payload.Phase
	run.UpdatedAt = updatedAt
	m.clusterBackups[payload.BackupSetID] = run
	m.activeClusterBackupID = payload.BackupSetID
	return nil
}

func (m *Module) applyClusterBackupBarrier(ctx context.Context, rec wal.Record) error {
	var payload clusterBackupBarrierRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	_ = ctx
	if strings.TrimSpace(payload.BackupSetID) == "" {
		return fmt.Errorf("backup_set_id is required")
	}
	if len(payload.Barriers) == 0 {
		return fmt.Errorf("barriers are required")
	}
	updatedAt := payload.UpdatedAt.UTC()
	if updatedAt.IsZero() {
		updatedAt = time.Now().UTC()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	run, err := m.clusterBackupRunLocked(payload.BackupSetID)
	if err != nil {
		return err
	}
	run.Phase = clusterBackupPhaseBarrierWait
	run.Barriers = cloneStringUint64Map(payload.Barriers)
	run.UpdatedAt = updatedAt
	m.clusterBackups[payload.BackupSetID] = run
	m.activeClusterBackupID = payload.BackupSetID
	return nil
}

func (m *Module) applyClusterBackupNodeResult(ctx context.Context, rec wal.Record) error {
	var payload clusterBackupNodeResultRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	_ = ctx
	backupSetID := strings.TrimSpace(payload.BackupSetID)
	if backupSetID == "" {
		return fmt.Errorf("backup_set_id is required")
	}
	if strings.TrimSpace(payload.Node.PodName) == "" {
		return fmt.Errorf("node pod_name is required")
	}
	updatedAt := payload.UpdatedAt.UTC()
	if updatedAt.IsZero() {
		updatedAt = time.Now().UTC()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	run, err := m.clusterBackupRunLocked(backupSetID)
	if err != nil {
		return err
	}
	if err := validateNodeArtifactAgainstExpected(run, payload.Node); err != nil {
		return err
	}
	if run.NodeResults == nil {
		run.NodeResults = map[string]clusterbackup.NodeArtifact{}
	}
	run.Phase = clusterBackupPhaseArchiving
	run.NodeResults[payload.Node.PodName] = payload.Node
	run.UpdatedAt = updatedAt
	m.clusterBackups[backupSetID] = run
	m.activeClusterBackupID = backupSetID
	return nil
}

func (m *Module) applyClusterBackupComplete(ctx context.Context, rec wal.Record) error {
	var payload clusterBackupCompleteRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	_ = ctx
	backupSetID := strings.TrimSpace(payload.BackupSetID)
	if backupSetID == "" {
		backupSetID = strings.TrimSpace(payload.Manifest.BackupSetID)
	}
	if backupSetID == "" {
		return fmt.Errorf("backup_set_id is required")
	}
	completedAt := payload.CompletedAt.UTC()
	if completedAt.IsZero() {
		completedAt = time.Now().UTC()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	run, err := m.clusterBackupRunLocked(backupSetID)
	if err != nil {
		return err
	}
	manifest := payload.Manifest
	manifest.BackupSetID = firstNonEmptyString(manifest.BackupSetID, backupSetID)
	manifest.State = clusterbackup.StateSucceeded
	manifest.Complete = true
	if manifest.CompletedAt.IsZero() {
		manifest.CompletedAt = completedAt
	}
	if err := validateCompletionManifest(run, manifest); err != nil {
		return err
	}
	run.Phase = clusterBackupPhaseSucceeded
	run.Manifest = &manifest
	run.UpdatedAt = completedAt
	run.Failure = nil
	m.clusterBackups[backupSetID] = run
	if m.activeClusterBackupID == backupSetID {
		m.activeClusterBackupID = ""
	}
	m.releaseClusterBackupLeaseLocked(backupSetID)
	return nil
}

func (m *Module) applyClusterBackupFailure(ctx context.Context, rec wal.Record, terminal clusterBackupPhase) error {
	var payload clusterBackupFailureRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	_ = ctx
	backupSetID := strings.TrimSpace(payload.BackupSetID)
	if backupSetID == "" {
		return fmt.Errorf("backup_set_id is required")
	}
	if terminal != clusterBackupPhaseFailed && terminal != clusterBackupPhaseAborted {
		return fmt.Errorf("invalid cluster backup terminal phase %q", terminal)
	}
	updatedAt := payload.UpdatedAt.UTC()
	if updatedAt.IsZero() {
		updatedAt = time.Now().UTC()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	run, err := m.clusterBackupRunLocked(backupSetID)
	if err != nil {
		return err
	}
	failurePhase := payload.Phase
	if !validClusterBackupPhase(failurePhase) {
		failurePhase = run.Phase
	}
	run.Phase = terminal
	run.Failure = &clusterBackupFailure{Phase: failurePhase, Message: strings.TrimSpace(payload.Message)}
	run.UpdatedAt = updatedAt
	m.clusterBackups[backupSetID] = run
	if m.activeClusterBackupID == backupSetID {
		m.activeClusterBackupID = ""
	}
	m.releaseClusterBackupLeaseLocked(backupSetID)
	return nil
}

func (m *Module) releaseClusterBackupLeaseLocked(backupSetID string) {
	lease := m.clusterBackupLeases[backupSetID]
	delete(m.clusterBackupLeases, backupSetID)
	if lease != nil {
		go func() { _ = lease.Release(context.Background()) }()
	}
}

func validateNodeArtifactAgainstExpected(run clusterBackupRun, artifact clusterbackup.NodeArtifact) error {
	if len(run.Expected) == 0 {
		return fmt.Errorf("cluster backup %s has no expected node set", run.BackupSetID)
	}
	for _, expected := range run.Expected {
		if expected.PodName != artifact.PodName {
			continue
		}
		if expected.NodeID != "" && artifact.NodeID != expected.NodeID {
			return fmt.Errorf("node result for %s has node_id %s, expected %s", artifact.PodName, artifact.NodeID, expected.NodeID)
		}
		if artifact.Ordinal != expected.Ordinal {
			return fmt.Errorf("node result for %s has ordinal %d, expected %d", artifact.PodName, artifact.Ordinal, expected.Ordinal)
		}
		if expected.RaftNodeID != 0 && artifact.RaftNodeID != expected.RaftNodeID {
			return fmt.Errorf("node result for %s has raft_node_id %d, expected %d", artifact.PodName, artifact.RaftNodeID, expected.RaftNodeID)
		}
		return nil
	}
	return fmt.Errorf("node result for unexpected pod %s", artifact.PodName)
}

func validateCompletionManifest(run clusterBackupRun, manifest clusterbackup.Manifest) error {
	if err := clusterbackup.Validate(manifest, clusterbackup.ValidationModeRestore); err != nil {
		return err
	}
	if manifest.BackupSetID != run.BackupSetID {
		return fmt.Errorf("manifest backup_set_id %s does not match run %s", manifest.BackupSetID, run.BackupSetID)
	}
	if run.ClusterID != "" && manifest.ClusterID != run.ClusterID {
		return fmt.Errorf("manifest cluster_id %s does not match run %s", manifest.ClusterID, run.ClusterID)
	}
	if len(manifest.Nodes) != len(run.Expected) {
		return fmt.Errorf("manifest has %d nodes, expected %d", len(manifest.Nodes), len(run.Expected))
	}
	for _, node := range manifest.Nodes {
		if err := validateNodeArtifactAgainstExpected(run, node); err != nil {
			return err
		}
		result, ok := run.NodeResults[node.PodName]
		if !ok {
			return fmt.Errorf("manifest includes pod %s without node result", node.PodName)
		}
		if result.ArchiveName != node.ArchiveName || result.ChecksumSHA256 != node.ChecksumSHA256 || result.SizeBytes != node.SizeBytes {
			return fmt.Errorf("manifest node %s does not match recorded node result", node.PodName)
		}
		for groupID, barrier := range run.Barriers {
			applied, ok := node.AppliedIndexes[groupID]
			if !ok {
				return fmt.Errorf("manifest node %s missing applied index for raft barrier %s", node.PodName, groupID)
			}
			if applied < barrier {
				return fmt.Errorf("manifest node %s applied index for raft barrier %s is %d, want at least %d", node.PodName, groupID, applied, barrier)
			}
		}
	}
	return nil
}

func (m *Module) clusterBackupRunLocked(backupSetID string) (clusterBackupRun, error) {
	m.ensureClusterBackupStateLocked()
	run, ok := m.clusterBackups[backupSetID]
	if !ok {
		return clusterBackupRun{}, fmt.Errorf("cluster backup %s is not active or known", backupSetID)
	}
	if run.Phase.terminal() {
		return clusterBackupRun{}, fmt.Errorf("cluster backup %s is already terminal (%s)", backupSetID, run.Phase)
	}
	return run, nil
}

func (m *Module) ensureClusterBackupStateLocked() {
	if m.clusterBackups == nil {
		m.clusterBackups = map[string]clusterBackupRun{}
	}
}

func (m *Module) clusterBackupSnapshot() (string, map[string]clusterBackupRun) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.activeClusterBackupID, cloneClusterBackupRuns(m.clusterBackups)
}

func (m *Module) restoreClusterBackupState(activeID string, runs map[string]clusterBackupRun) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.clusterBackups = cloneClusterBackupRuns(runs)
	if activeID != "" {
		if run, ok := m.clusterBackups[activeID]; ok && !run.Phase.terminal() {
			m.activeClusterBackupID = activeID
			return
		}
	}
	m.activeClusterBackupID = ""
	for id, run := range m.clusterBackups {
		if !run.Phase.terminal() {
			m.activeClusterBackupID = id
			return
		}
	}
}

func cloneClusterBackupRuns(in map[string]clusterBackupRun) map[string]clusterBackupRun {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]clusterBackupRun, len(in))
	for k, v := range in {
		v.Expected = append([]clusterBackupExpectedNode(nil), v.Expected...)
		v.Barriers = cloneStringUint64Map(v.Barriers)
		if v.NodeResults != nil {
			v.NodeResults = make(map[string]clusterbackup.NodeArtifact, len(v.NodeResults))
			for nk, nv := range in[k].NodeResults {
				nv.AppliedIndexes = cloneStringUint64Map(nv.AppliedIndexes)
				nv.RaftFreeze.Groups = cloneRaftFreezeGroups(nv.RaftFreeze.Groups)
				v.NodeResults[nk] = nv
			}
		}
		if v.Manifest != nil {
			manifest := *v.Manifest
			manifest.RaftBarriers = cloneStringUint64Map(manifest.RaftBarriers)
			manifest.Nodes = append([]clusterbackup.NodeArtifact(nil), manifest.Nodes...)
			for i := range manifest.Nodes {
				manifest.Nodes[i].AppliedIndexes = cloneStringUint64Map(manifest.Nodes[i].AppliedIndexes)
				manifest.Nodes[i].RaftFreeze.Groups = cloneRaftFreezeGroups(manifest.Nodes[i].RaftFreeze.Groups)
			}
			v.Manifest = &manifest
		}
		if v.Failure != nil {
			failure := *v.Failure
			v.Failure = &failure
		}
		out[k] = v
	}
	return out
}

func cloneRaftFreezeGroups(in map[string]clusterbackup.RaftFreezeGroup) map[string]clusterbackup.RaftFreezeGroup {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]clusterbackup.RaftFreezeGroup, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneStringUint64Map(in map[string]uint64) map[string]uint64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]uint64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func validClusterBackupPhase(phase clusterBackupPhase) bool {
	switch phase {
	case clusterBackupPhaseRequested, clusterBackupPhasePrechecking, clusterBackupPhaseQuiescing, clusterBackupPhaseBarrierWait, clusterBackupPhaseArchiving, clusterBackupPhaseValidating, clusterBackupPhaseCommittingManifest, clusterBackupPhaseSucceeded, clusterBackupPhaseFailed, clusterBackupPhaseAborted:
		return true
	default:
		return false
	}
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
