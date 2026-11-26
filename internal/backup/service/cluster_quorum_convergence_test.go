package service

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/myceldb/mycel/internal/clustering/backend"
)

const (
	quorumBackupSetID = "backup-quorum-test"
	quorumPartition   = "space-partition-24"
)

func TestClusterBackupQuorumEvidenceConvergence(t *testing.T) {
	for _, tc := range []struct {
		name            string
		missingPolls    int
		blockOnPoll     int
		wantPolls       int
		wantElapsed     time.Duration
		wantFailedPhase clusterBackupPhase
		wantError       string
	}{
		{"recovers before quiesce", 1, 0, 3, 2 * time.Second, clusterBackupPhaseBarrierWait, "post-quiesce"},
		{"times out if evidence never arrives", 10, 0, 3, 5 * time.Second, clusterBackupPhasePrechecking, "convergence timed out"},
		{"deadline bounds an in-flight follow-up probe", 1, 2, 2, 5 * time.Second, clusterBackupPhasePrechecking, "convergence timed out"},
		{"fails closed if proof is lost after quiesce", 0, 0, 2, 0, clusterBackupPhaseBarrierWait, "post-quiesce"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			outputDir := t.TempDir()
			synctest.Test(t, func(t *testing.T) {
				m, peer, nodes := newQuorumBackupFixture(tc.missingPolls)
				peer.blockOnPoll = tc.blockOnPoll
				startedAt := time.Now()
				m.runClusterBackupOperation(t.Context(), StartClusterBackupInput{OutputDir: outputDir, ConvergenceTimeout: 5 * time.Second}, quorumBackupSetID, "cluster", startedAt, nodes)
				got, err := m.ClusterBackupStatus(quorumBackupSetID)
				if err != nil {
					t.Fatal(err)
				}
				if elapsed := time.Since(startedAt); elapsed != tc.wantElapsed || peer.polls != tc.wantPolls {
					t.Fatalf("elapsed=%s polls=%d, want %s and %d; status=%+v", elapsed, peer.polls, tc.wantElapsed, tc.wantPolls, got)
				}
				if got.Phase != string(clusterBackupPhaseFailed) || got.FailurePhase != string(tc.wantFailedPhase) || !strings.Contains(got.Error, tc.wantError) || !strings.Contains(got.Error, quorumPartition) {
					t.Fatalf("got phase=%s failed phase=%s error=%q; want failed/%s and %q for %s", got.Phase, got.FailurePhase, got.Error, tc.wantFailedPhase, tc.wantError, quorumPartition)
				}
				if len(got.Barriers) != 0 || len(got.Nodes) != 0 || got.ManifestURI != "" {
					t.Fatalf("backup saved evidence from an unproven barrier: %+v", got)
				}
				if tc.missingPolls > 0 && !quorumBackupBlockerPresent(peer.secondPollBlockers) {
					t.Fatalf("second poll did not expose the missing partition blocker: %+v", peer.secondPollBlockers)
				}
				if tc.wantFailedPhase == clusterBackupPhasePrechecking {
					if len(peer.acquired) != 0 || len(peer.released) != 0 || !quorumBackupBlockerPresent(got.Blockers) {
						t.Fatalf("persistent missing quorum must retain the blocker without quiescing: status=%+v acquired=%v released=%v", got, peer.acquired, peer.released)
					}
				} else if !reflect.DeepEqual(peer.acquired, []string{"node-a", "node-b", "node-c"}) || !reflect.DeepEqual(peer.released, []string{"node-c", "node-b", "node-a"}) || len(peer.acquireBlockers) != 0 {
					t.Fatalf("post-quiesce failure must release all leases; acquired=%v released=%v blockers at acquisition=%+v", peer.acquired, peer.released, peer.acquireBlockers)
				}
			})
		})
	}
}

func TestClusterBackupConvergenceDoesNotMaskPeerValidationFailure(t *testing.T) {
	outputDir := t.TempDir()
	synctest.Test(t, func(t *testing.T) {
		m, peer, nodes := newQuorumBackupFixture(0)
		peer.probeErr = errors.New("backup destination is not writable")
		startedAt := time.Now()
		const budget = 2 * time.Minute
		m.runClusterBackupOperation(t.Context(), StartClusterBackupInput{OutputDir: outputDir, ConvergenceTimeout: budget}, quorumBackupSetID, "cluster", startedAt, nodes)
		got, err := m.ClusterBackupStatus(quorumBackupSetID)
		if err != nil {
			t.Fatal(err)
		}
		if time.Since(startedAt) >= budget || got.FailurePhase != string(clusterBackupPhasePrechecking) || got.Error != "precheck myceld-0: backup destination is not writable" || len(got.Blockers) != 0 || len(peer.acquired) != 0 {
			t.Fatalf("non-convergence error must remain terminal: elapsed=%s status=%+v acquired=%v", time.Since(startedAt), got, peer.acquired)
		}
	})
}

func quorumBackupBlockerPresent(blockers []ClusterBackupBlocker) bool {
	return len(blockers) == 1 && blockers[0].RaftGroup == quorumPartition && blockers[0].Reason == "raft_quorum_read_unverified"
}

func newQuorumBackupFixture(missingPolls int) (*Module, *quorumBackupPeer, []ClusterBackupNode) {
	// Use in-process operation record application and script the peer responses
	// independently so this tests convergence deterministically, not elections.
	m := NewModule()
	m.restoreClusterBackupState(quorumBackupSetID, map[string]clusterBackupRun{
		quorumBackupSetID: {BackupSetID: quorumBackupSetID, ClusterID: "cluster", Phase: clusterBackupPhaseRequested},
	})
	peer := &quorumBackupPeer{module: m, missingPolls: missingPolls}
	m.clusterBackendClient = peer
	nodes := []ClusterBackupNode{
		{PodName: "myceld-0", NodeID: "node-a", Ordinal: 0, RaftNodeID: 1, BackendAdvertiseAddr: "node-a"},
		{PodName: "myceld-1", NodeID: "node-b", Ordinal: 1, RaftNodeID: 2, BackendAdvertiseAddr: "node-b"},
		{PodName: "myceld-2", NodeID: "node-c", Ordinal: 2, RaftNodeID: 3, BackendAdvertiseAddr: "node-c"},
	}
	return m, peer, nodes
}

type quorumBackupPeer struct {
	backendClient      // Capturing or freezing an unproven group is a test failure.
	module             *Module
	probeErr           error
	missingPolls       int
	blockOnPoll        int
	polls              int
	secondPollBlockers []ClusterBackupBlocker
	acquireBlockers    []ClusterBackupBlocker
	acquired, released []string
}

func (p *quorumBackupPeer) CheckLocalBackupReadiness(ctx context.Context, addr string, _ backend.CreateLocalBackupArchiveInput) (map[string]uint64, map[string]uint64, error) {
	if p.probeErr != nil {
		return nil, nil, p.probeErr
	}
	applied := map[string]uint64{"system": 10, quorumPartition: 10}
	if addr != "node-a" {
		return applied, nil, nil
	}
	p.polls++
	if p.polls == 2 && len(p.acquired) == 0 {
		status, _ := p.module.ClusterBackupStatus(quorumBackupSetID)
		p.secondPollBlockers = status.Blockers
	}
	if p.polls == p.blockOnPoll {
		<-ctx.Done()
		return nil, nil, ctx.Err()
	}
	commits := map[string]uint64{"system": 10}
	// Only a leader supplies a quorum read. Always withdraw the partition
	// proof after quiesce to ensure precheck evidence can't be reused.
	if p.polls > p.missingPolls && len(p.acquired) == 0 {
		commits[quorumPartition] = 10
	}
	return applied, commits, nil
}

func (p *quorumBackupPeer) AcquireLocalBackupQuiesce(_ context.Context, addr string, _ backend.CreateLocalBackupArchiveInput) error {
	status, _ := p.module.ClusterBackupStatus(quorumBackupSetID)
	p.acquireBlockers = append(p.acquireBlockers, status.Blockers...)
	p.acquired = append(p.acquired, addr)
	return nil
}

func (p *quorumBackupPeer) ReleaseLocalBackupQuiesce(_ context.Context, addr string, _ backend.CreateLocalBackupArchiveInput) error {
	p.released = append(p.released, addr)
	return nil
}
