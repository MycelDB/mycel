package cluster

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	RestoredDerivedReadinessSchema       = "mycel.restored_derived_readiness"
	RestoredDerivedReadinessVersion      = 1
	RestoredDerivedReadinessRelativePath = "meta/backup/restored-derived-readiness.json"
)

type RestoredDerivedReadinessOptions struct {
	RestoreOperationID string
	RestorePlanID      string
	Ordinal            int
	NodeID             string
	PodName            string
	TargetPVCName      string
	RestoredAt         time.Time
	Evidence           map[string]string
}

type RestoredDerivedReadinessDocument struct {
	Schema             string                              `json:"schema"`
	Version            int                                 `json:"version"`
	BackupSetID        string                              `json:"backup_set_id"`
	RestoreOperationID string                              `json:"restore_operation_id,omitempty"`
	RestorePlanID      string                              `json:"restore_plan_id,omitempty"`
	ClusterID          string                              `json:"cluster_id"`
	NodeID             string                              `json:"node_id,omitempty"`
	PodName            string                              `json:"pod_name,omitempty"`
	Ordinal            int                                 `json:"ordinal"`
	TargetPVCName      string                              `json:"target_pvc_name,omitempty"`
	RestoredAt         time.Time                           `json:"restored_at"`
	DerivedReady       bool                                `json:"derived_ready"`
	Subsystems         []RestoredDerivedSubsystemReadiness `json:"subsystems"`
}

type RestoredDerivedSubsystemReadiness struct {
	Subsystem           string            `json:"subsystem"`
	ArtifactID          string            `json:"artifact_id,omitempty"`
	State               string            `json:"state"`
	Ready               bool              `json:"ready"`
	RebuildRequired     bool              `json:"rebuild_required"`
	Reason              string            `json:"reason"`
	PolicyMode          string            `json:"policy_mode,omitempty"`
	RestorePolicy       string            `json:"restore_policy,omitempty"`
	ValidationState     string            `json:"validation_state,omitempty"`
	SourceFrontierKind  string            `json:"source_frontier_kind,omitempty"`
	SourceFrontierValue string            `json:"source_frontier_value,omitempty"`
	Evidence            map[string]string `json:"evidence,omitempty"`
	UpdatedAt           time.Time         `json:"updated_at"`
}

func RestoredDerivedReadinessPath(dataDir string) string {
	return filepath.Join(dataDir, RestoredDerivedReadinessRelativePath)
}

func BuildRestoredDerivedReadiness(manifest BackupSetManifestV1, opts RestoredDerivedReadinessOptions) RestoredDerivedReadinessDocument {
	restoredAt := opts.RestoredAt.UTC()
	if restoredAt.IsZero() {
		restoredAt = time.Now().UTC()
	}
	doc := RestoredDerivedReadinessDocument{Schema: RestoredDerivedReadinessSchema, Version: RestoredDerivedReadinessVersion, BackupSetID: manifest.BackupSetID, RestoreOperationID: strings.TrimSpace(opts.RestoreOperationID), RestorePlanID: strings.TrimSpace(opts.RestorePlanID), ClusterID: manifest.Source.ClusterID, NodeID: strings.TrimSpace(opts.NodeID), PodName: strings.TrimSpace(opts.PodName), Ordinal: opts.Ordinal, TargetPVCName: strings.TrimSpace(opts.TargetPVCName), RestoredAt: restoredAt, DerivedReady: true}
	baseEvidence := copyStringMap(opts.Evidence)
	if len(baseEvidence) == 0 {
		baseEvidence = map[string]string{}
	}
	baseEvidence["backup_set_id"] = manifest.BackupSetID
	if doc.RestoreOperationID != "" {
		baseEvidence["restore_operation_id"] = doc.RestoreOperationID
	}
	if doc.RestorePlanID != "" {
		baseEvidence["restore_plan_id"] = doc.RestorePlanID
	}
	for _, row := range manifest.DerivedState {
		status := restoredDerivedSubsystemStatus(row, restoredAt, baseEvidence)
		if !status.Ready || status.RebuildRequired {
			doc.DerivedReady = false
		}
		doc.Subsystems = append(doc.Subsystems, status)
	}
	sort.Slice(doc.Subsystems, func(i, j int) bool {
		if doc.Subsystems[i].Subsystem == doc.Subsystems[j].Subsystem {
			return doc.Subsystems[i].ArtifactID < doc.Subsystems[j].ArtifactID
		}
		return doc.Subsystems[i].Subsystem < doc.Subsystems[j].Subsystem
	})
	return doc
}

func restoredDerivedSubsystemStatus(row BackupSetDerivedState, updatedAt time.Time, baseEvidence map[string]string) RestoredDerivedSubsystemReadiness {
	mode := strings.ToUpper(strings.TrimSpace(row.PolicyMode))
	restorePolicy := strings.ToUpper(strings.TrimSpace(row.RestorePolicy))
	validation := strings.ToUpper(strings.TrimSpace(row.ValidationState))
	status := RestoredDerivedSubsystemReadiness{Subsystem: strings.ToUpper(strings.TrimSpace(row.Subsystem)), ArtifactID: strings.TrimSpace(row.ArtifactID), PolicyMode: mode, RestorePolicy: restorePolicy, ValidationState: validation, SourceFrontierKind: strings.TrimSpace(row.SourceFrontierKind), SourceFrontierValue: strings.TrimSpace(row.SourceFrontierValue), UpdatedAt: updatedAt, Evidence: copyStringMap(baseEvidence)}
	if status.Evidence == nil {
		status.Evidence = map[string]string{}
	}
	status.Evidence["policy_mode"] = mode
	status.Evidence["restore_policy"] = restorePolicy
	status.Evidence["validation_state"] = validation
	status.Evidence["included"] = fmt.Sprintf("%t", row.Included)
	switch {
	case restorePolicy == "REBUILD_REQUIRED" || validation == "REBUILD_REQUIRED" || ((mode == "INCLUDE_OPTIONAL" || mode == "REBUILD_ALLOWED") && !row.Included):
		status.State = "rebuild_required"
		status.RebuildRequired = true
		status.Reason = "derived_artifact_omitted_rebuild_required"
	case restorePolicy == "PRESERVED" && row.Included && validation == "INCLUDED_AND_VALIDATED":
		status.State = "ready"
		status.Ready = true
		status.Reason = "preserved_from_backup"
	case restorePolicy == "DISCARD" || validation == "DISCARDED":
		status.State = "discarded"
		status.Ready = true
		status.Reason = "discarded_by_policy"
	default:
		status.State = "unknown"
		status.Reason = "derived_artifact_state_unrecognized"
	}
	return status
}

func WriteRestoredDerivedReadiness(dataDir string, doc RestoredDerivedReadinessDocument) error {
	path := RestoredDerivedReadinessPath(dataDir)
	if strings.TrimSpace(dataDir) == "" {
		return fmt.Errorf("data dir is required")
	}
	if doc.Schema == "" {
		doc.Schema = RestoredDerivedReadinessSchema
	}
	if doc.Version == 0 {
		doc.Version = RestoredDerivedReadinessVersion
	}
	if doc.RestoredAt.IsZero() {
		doc.RestoredAt = time.Now().UTC()
	}
	raw, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp := path + ".tmp-" + fmt.Sprintf("%d", time.Now().UnixNano())
	if err := os.WriteFile(tmp, append(raw, '\n'), 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func ReadRestoredDerivedReadiness(dataDir string) (RestoredDerivedReadinessDocument, bool, error) {
	path := RestoredDerivedReadinessPath(dataDir)
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return RestoredDerivedReadinessDocument{}, false, nil
		}
		return RestoredDerivedReadinessDocument{}, false, err
	}
	var doc RestoredDerivedReadinessDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return RestoredDerivedReadinessDocument{}, true, err
	}
	if doc.Schema != RestoredDerivedReadinessSchema {
		return RestoredDerivedReadinessDocument{}, true, fmt.Errorf("unexpected restored derived readiness schema %q", doc.Schema)
	}
	if doc.Version != RestoredDerivedReadinessVersion {
		return RestoredDerivedReadinessDocument{}, true, fmt.Errorf("unsupported restored derived readiness version %d", doc.Version)
	}
	return doc, true, nil
}

func copyStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
