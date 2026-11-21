package service

import "testing"

func TestRestoreCleanTargetProofMappingBlockers(t *testing.T) {
	target := ClusterRestoreTarget{Nodes: []ClusterRestoreNodeMapping{
		{Ordinal: 0, TargetPVCName: "data-myceld-0", TargetDataDir: "/mnt/restore/0"},
		{Ordinal: 1, TargetPVCName: "data-myceld-1", TargetDataDir: "/mnt/restore/1"},
	}}
	proof := ClusterRestoreCleanTargetProof{TargetEmpty: true, CheckedNodes: []ClusterRestoreNodeMapping{
		{Ordinal: 0, TargetPVCName: "data-myceld-0", TargetDataDir: "/mnt/restore/0"},
		{Ordinal: 1, TargetPVCName: "data-myceld-other", TargetDataDir: "/mnt/restore/other"},
	}}
	blockers := restoreCleanTargetProofMappingBlockers(target, proof)
	if !hasBlockerReason(blockers, "clean_target_proof_mismatch") {
		t.Fatalf("blockers=%#v, want clean_target_proof_mismatch", blockers)
	}

	proof.CheckedNodes = proof.CheckedNodes[:1]
	blockers = restoreCleanTargetProofMappingBlockers(target, proof)
	if !hasBlockerReason(blockers, "clean_target_proof_incomplete") {
		t.Fatalf("blockers=%#v, want clean_target_proof_incomplete", blockers)
	}

	proof.CheckedNodes = []ClusterRestoreNodeMapping{
		{Ordinal: 0, TargetPVCName: "data-myceld-0", TargetDataDir: "/mnt/restore/0"},
		{Ordinal: 1, TargetPVCName: "data-myceld-1", TargetDataDir: "/mnt/restore/1"},
	}
	if blockers := restoreCleanTargetProofMappingBlockers(target, proof); len(blockers) != 0 {
		t.Fatalf("blockers=%#v, want none", blockers)
	}
}
