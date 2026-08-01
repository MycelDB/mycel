package backend

import (
	"context"
	"sort"

	"github.com/myceldb/mycel/internal/clustering/consensus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type PeerLocalGraphConsistencyResult struct {
	TargetNode consensus.NodeID
	Addr       string
	Result     LocalGraphConsistencyResult
	Err        error
}

// CollectLocalGraphConsistency queries each supplied peer for local graph
// consistency diagnostics. It intentionally performs no cluster-level
// consistency classification; G4 compares the returned evidence. Missing or
// failed peers are returned with Err so callers cannot accidentally treat an
// incomplete collection as a successful consistency proof.
func (c Client) CollectLocalGraphConsistency(ctx context.Context, addrs map[consensus.NodeID]string, in LocalGraphConsistencyInput) []PeerLocalGraphConsistencyResult {
	nodes := make([]int, 0, len(addrs))
	for nodeID := range addrs {
		nodes = append(nodes, int(nodeID))
	}
	sort.Ints(nodes)
	out := make([]PeerLocalGraphConsistencyResult, 0, len(nodes))
	for _, rawNodeID := range nodes {
		nodeID := consensus.NodeID(rawNodeID)
		addr := addrs[nodeID]
		res, err := c.GetLocalGraphConsistency(ctx, addr, in)
		if err == nil && res.RaftNodeID != 0 && res.RaftNodeID != nodeID {
			err = status.Errorf(codes.FailedPrecondition, "backend response raft_node_id %d does not match target node %d", res.RaftNodeID, nodeID)
		}
		out = append(out, PeerLocalGraphConsistencyResult{TargetNode: nodeID, Addr: addr, Result: res, Err: err})
	}
	return out
}
