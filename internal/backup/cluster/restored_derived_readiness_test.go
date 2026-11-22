package cluster

import (
	"testing"
	"time"
)

func TestBuildRestoredDerivedReadinessMarksOptionalOmissionsRebuildRequired(t *testing.T) {
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	manifest := BackupSetManifestV1{
		BackupSetID: "backup-set-1",
		Source:      BackupSetSource{ClusterID: "cluster-a"},
		DerivedState: []BackupSetDerivedState{
			{Subsystem: "LEXICAL_SEARCH", ArtifactID: "lexical:phase-6", PolicyMode: "INCLUDE_OPTIONAL", Included: false, RestorePolicy: "REBUILD_REQUIRED", ValidationState: "REBUILD_REQUIRED", SourceFrontierKind: "graph_revision", SourceFrontierValue: "42"},
			{Subsystem: "SEMANTIC_VECTOR_SEARCH", ArtifactID: "semantic:phase-6", PolicyMode: "INCLUDE_REQUIRED", Included: true, RestorePolicy: "PRESERVED", ValidationState: "INCLUDED_AND_VALIDATED"},
			{Subsystem: "METADATA_ADJACENCY_MEMORY_INDEX", ArtifactID: "memory:phase-6", PolicyMode: "DISCARD", Included: false, RestorePolicy: "DISCARD", ValidationState: "DISCARDED"},
		},
	}
	doc := BuildRestoredDerivedReadiness(manifest, RestoredDerivedReadinessOptions{RestoreOperationID: "restore-1", RestorePlanID: "plan-1", Ordinal: 0, NodeID: "node-1", RestoredAt: now})
	if doc.DerivedReady {
		t.Fatalf("DerivedReady=true, want false when optional artifact requires rebuild")
	}
	bySubsystem := map[string]RestoredDerivedSubsystemReadiness{}
	for _, row := range doc.Subsystems {
		bySubsystem[row.Subsystem] = row
	}
	lexical := bySubsystem["LEXICAL_SEARCH"]
	if lexical.State != "rebuild_required" || !lexical.RebuildRequired || lexical.Ready || lexical.Reason != "derived_artifact_omitted_rebuild_required" {
		t.Fatalf("lexical readiness=%#v, want rebuild_required", lexical)
	}
	if lexical.SourceFrontierValue != "42" || lexical.Evidence["included"] != "false" || lexical.Evidence["backup_set_id"] != "backup-set-1" {
		t.Fatalf("lexical evidence/frontier not preserved: %#v", lexical)
	}
	semantic := bySubsystem["SEMANTIC_VECTOR_SEARCH"]
	if semantic.State != "ready" || !semantic.Ready || semantic.RebuildRequired {
		t.Fatalf("semantic readiness=%#v, want ready preserved", semantic)
	}
	discarded := bySubsystem["METADATA_ADJACENCY_MEMORY_INDEX"]
	if discarded.State != "discarded" || !discarded.Ready || discarded.RebuildRequired {
		t.Fatalf("discarded readiness=%#v, want ready discarded", discarded)
	}
}

func TestRestoredDerivedReadinessRoundTrip(t *testing.T) {
	dir := t.TempDir()
	doc := BuildRestoredDerivedReadiness(BackupSetManifestV1{BackupSetID: "backup-set-1", Source: BackupSetSource{ClusterID: "cluster-a"}, DerivedState: []BackupSetDerivedState{{Subsystem: "GRAPH_CHECKPOINT", ArtifactID: "graph:phase-6", PolicyMode: "INCLUDE_OPTIONAL", RestorePolicy: "REBUILD_REQUIRED", ValidationState: "REBUILD_REQUIRED"}}}, RestoredDerivedReadinessOptions{Ordinal: 1, NodeID: "node-1"})
	if err := WriteRestoredDerivedReadiness(dir, doc); err != nil {
		t.Fatalf("WriteRestoredDerivedReadiness() error = %v", err)
	}
	got, ok, err := ReadRestoredDerivedReadiness(dir)
	if err != nil || !ok {
		t.Fatalf("ReadRestoredDerivedReadiness() = ok=%t err=%v", ok, err)
	}
	if got.BackupSetID != doc.BackupSetID || got.Subsystems[0].State != "rebuild_required" || got.Subsystems[0].Evidence["included"] != "false" {
		t.Fatalf("round-trip mismatch: %#v", got)
	}
}
