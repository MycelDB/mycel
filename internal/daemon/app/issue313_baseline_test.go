package app

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	graphmodel "github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/identity/model"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	spaceservice "github.com/myceldb/mycel/internal/space/service"
)

type issue313TimingRecord struct {
	Event                   string  `json:"event"`
	TransactionID           string  `json:"transaction_id"`
	OperationCount          int     `json:"operation_count"`
	PutNodes                int     `json:"put_nodes"`
	TotalMS                 float64 `json:"total_ms"`
	RaftCommandBytes        int     `json:"raft_command_bytes"`
	EnterWriteMS            float64 `json:"enter_write_ms"`
	RaftBuildMS             float64 `json:"raft_build_ms"`
	RaftProposeMS           float64 `json:"raft_propose_ms"`
	RaftStateMachineApplyMS float64 `json:"raft_state_machine_apply_ms"`
	StorageCommitTotalMS    float64 `json:"storage_commit_total_ms"`
	StorageWaitLockMS       float64 `json:"storage_wait_lock_ms"`
	StorageNodeAppendMS     float64 `json:"storage_node_append_ms"`
	StorageEdgeAppendMS     float64 `json:"storage_edge_append_ms"`
	StorageTxnSyncMS        float64 `json:"storage_txn_sync_ms"`
	StorageInMemoryApplyMS  float64 `json:"storage_in_memory_apply_ms"`
}

// TestIssue313LargeTransactionBaseline is an opt-in, in-process three-node
// baseline for issue #313. It intentionally makes no pass/fail claim about a
// production limit; it records the current latency and contention boundary
// before admission or execution changes are introduced.
func TestIssue313LargeTransactionBaseline(t *testing.T) {
	if os.Getenv("MYCELD_RUN_ISSUE313_BASELINE") != "1" {
		t.Skip("set MYCELD_RUN_ISSUE313_BASELINE=1 to run the opt-in baseline")
	}
	t.Setenv("MYCELD_GRAPH_WRITE_TRACE", "1")
	t.Setenv("MYCELD_GRAPH_WRITE_TRACE_THRESHOLD", "0")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cluster := newPhaseDIntegrationCluster(t, 4)
	cluster.start(ctx, t)
	defer cluster.close()
	cluster.waitForLeaders(ctx, t)

	created, err := cluster.nodes[1].space.CreateSpaceWithResult(ctx, spaceservice.CreateSpaceInput{
		Name:              "issue-313-baseline",
		OwnerPrincipalID:  identity.PrincipalID(uuid.NewString()),
		DefaultDomainKey:  "baseline",
		DefaultDomainName: "Baseline",
		CommandID:         "issue-313-baseline-space",
	})
	if err != nil {
		t.Fatalf("CreateSpaceWithResult() error = %v", err)
	}
	spaceID := created.Space.SpaceID.String()
	domainID := created.Domain.ID
	cluster.waitForSpace(ctx, t, spaceID)
	schema := phaseDTestSchema(domainID)
	schema.NodeTypes[0].Payload = []schemamodel.FieldSpec{{Name: "body", Type: schemamodel.FieldTypeString}}
	if err := cluster.nodes[1].schema.PutDomainSchema(ctx, schema); err != nil {
		t.Fatalf("PutDomainSchema() error = %v", err)
	}
	cluster.waitForSchema(ctx, t, domainID)

	leader := cluster.leaderForSpace(ctx, t, spaceID)
	graphNode := cluster.nodes[leader]
	seedTx := phaseDGraphTx(spaceID, domainID.String(), 0)
	seed, err := graphNode.graph.CreateNode(ctx, seedTx, graphmodel.NodeInput{
		Labels:     []string{"Person"},
		Properties: map[string]any{"firstName": "issue-313-seed"},
	})
	if err != nil {
		t.Fatalf("CreateNode(seed) error = %v", err)
	}
	seedCommit, err := graphNode.graph.CommitTransactionGraph(ctx, seedTx)
	if err != nil {
		t.Fatalf("CommitTransactionGraph(seed) error = %v", err)
	}
	cluster.waitForGraphNode(ctx, t, spaceID, domainID.String(), seed.ID.String(), seedCommit.CommittedRevision, "issue-313-seed")

	revision := seedCommit.CommittedRevision
	const payloadBytes = 768
	for _, nodeCount := range []int{16, 64, 256, 512, 1024} {
		leader = cluster.leaderForSpace(ctx, t, spaceID)
		graphNode = cluster.nodes[leader]
		tx := phaseDGraphTx(spaceID, domainID.String(), revision)
		var last graphmodel.Node
		for i := 0; i < nodeCount; i++ {
			last, err = graphNode.graph.CreateNode(ctx, tx, graphmodel.NodeInput{
				Labels:     []string{"Person"},
				Properties: map[string]any{"firstName": fmt.Sprintf("issue-313-%d-%d", nodeCount, i)},
				Payload:    map[string]any{"body": strings.Repeat("x", payloadBytes)},
			})
			if err != nil {
				t.Fatalf("CreateNode(size=%d,index=%d) error = %v", nodeCount, i, err)
			}
		}
		start := time.Now()
		commit, err := graphNode.graph.CommitTransactionGraph(ctx, tx)
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("CommitTransactionGraph(size=%d) error = %v", nodeCount, err)
		}
		if commit.OperationCount != int32(nodeCount) {
			t.Fatalf("size=%d operation_count=%d, want %d", nodeCount, commit.OperationCount, nodeCount)
		}
		revision = commit.CommittedRevision
		cluster.waitForGraphNode(ctx, t, spaceID, domainID.String(), last.ID.String(), revision, fmt.Sprintf("issue-313-%d-%d", nodeCount, nodeCount-1))
		t.Logf("baseline size=%d payload_bytes_per_node=%d operation_count=%d wall_commit_ms=%.3f committed_revision=%d", nodeCount, payloadBytes, commit.OperationCount, float64(elapsed.Microseconds())/1000, revision)
	}

	// Measure a read issued while the largest transaction is being proposed and
	// applied. The result is diagnostic: this test records the observed latency
	// and does not set a production SLO.
	leader = cluster.leaderForSpace(ctx, t, spaceID)
	graphNode = cluster.nodes[leader]
	largeTx := phaseDGraphTx(spaceID, domainID.String(), revision)
	var last graphmodel.Node
	for i := 0; i < 1024; i++ {
		last, err = graphNode.graph.CreateNode(ctx, largeTx, graphmodel.NodeInput{
			Labels:     []string{"Person"},
			Properties: map[string]any{"firstName": fmt.Sprintf("issue-313-probe-%d", i)},
			Payload:    map[string]any{"body": strings.Repeat("x", payloadBytes)},
		})
		if err != nil {
			t.Fatalf("CreateNode(probe,index=%d) error = %v", i, err)
		}
	}
	commitDone := make(chan error, 1)
	commitStart := time.Now()
	go func() {
		_, commitErr := graphNode.graph.CommitTransactionGraph(ctx, largeTx)
		commitDone <- commitErr
	}()
	time.Sleep(2 * time.Millisecond)
	readStart := time.Now()
	readTx := phaseDGraphTx(spaceID, domainID.String(), revision)
	_, readErr := graphNode.graph.GetNode(ctx, readTx, seed.ID.String())
	readElapsed := time.Since(readStart)
	if readErr != nil {
		t.Fatalf("GetNode(concurrent probe) error = %v", readErr)
	}
	if err := <-commitDone; err != nil {
		t.Fatalf("CommitTransactionGraph(concurrent probe) error = %v", err)
	}
	t.Logf("concurrent read probe wall_read_ms=%.3f large_commit_elapsed_ms=%.3f last_node=%s", float64(readElapsed.Microseconds())/1000, float64(time.Since(commitStart).Microseconds())/1000, last.ID)

	for _, line := range issue313TimingLines(cluster.logs) {
		t.Log(line)
	}
}

func issue313TimingLines(logs map[consensus.NodeID]*phaseDLogBuffer) []string {
	lines := make([]string, 0)
	for nodeID, buffer := range logs {
		scanner := bufio.NewScanner(strings.NewReader(buffer.String()))
		for scanner.Scan() {
			var record issue313TimingRecord
			if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
				continue
			}
			if record.Event != "graph_write_commit_timing" && record.Event != "graph_write_apply_timing" {
				continue
			}
			lines = append(lines, fmt.Sprintf("trace node=%d event=%s operations=%d put_nodes=%d total_ms=%.3f raft_command_bytes=%d raft_propose_ms=%.3f raft_apply_ms=%.3f storage_commit_ms=%.3f storage_lock_wait_ms=%.3f storage_node_append_ms=%.3f storage_txn_sync_ms=%.3f storage_memory_apply_ms=%.3f", nodeID, record.Event, record.OperationCount, record.PutNodes, record.TotalMS, record.RaftCommandBytes, record.RaftProposeMS, record.RaftStateMachineApplyMS, record.StorageCommitTotalMS, record.StorageWaitLockMS, record.StorageNodeAppendMS, record.StorageTxnSyncMS, record.StorageInMemoryApplyMS))
		}
	}
	sort.Strings(lines)
	return lines
}
