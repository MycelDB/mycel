package client

import (
	"context"

	clusterbackend "github.com/myceldb/mycel/internal/clustering/backend"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type ForwardedClientHandler struct {
	LocalNode    consensus.NodeID
	Sessions     *SessionService
	Transactions *TransactionService
	Graphs       *GraphService
	Queries      *QueryService
	Metadata     *MetadataCatalogService
}

func (h ForwardedClientHandler) HandleForwardedClientRequest(ctx context.Context, req clusterbackend.ForwardedClientRequest) (clusterbackend.ForwardedClientResponse, error) {
	if h.LocalNode != 0 && req.TargetNode != 0 && req.TargetNode != h.LocalNode {
		return clusterbackend.ForwardedClientResponse{}, status.Errorf(codes.FailedPrecondition, "forwarded request target node %d does not match local node %d", req.TargetNode, h.LocalNode)
	}
	ctx = forwardedContext(ctx, req.Principal)
	switch req.Operation {
	case clientv1.SessionService_OpenSession_FullMethodName:
		return h.handleSessionOpen(ctx, req)
	case clientv1.SessionService_GetSession_FullMethodName:
		return h.handleSessionGet(ctx, req)
	case clientv1.SessionService_HeartbeatSession_FullMethodName:
		return h.handleSessionHeartbeat(ctx, req)
	case clientv1.SessionService_CloseSession_FullMethodName:
		return h.handleSessionClose(ctx, req)
	case clientv1.TransactionService_BeginTransaction_FullMethodName:
		return h.handleTransactionBegin(ctx, req)
	case clientv1.TransactionService_GetTransaction_FullMethodName:
		return h.handleTransactionGet(ctx, req)
	case clientv1.TransactionService_CommitTransaction_FullMethodName:
		return h.handleTransactionCommit(ctx, req)
	case clientv1.TransactionService_RollbackTransaction_FullMethodName:
		return h.handleTransactionRollback(ctx, req)
	case clientv1.TransactionService_CloseTransaction_FullMethodName:
		return h.handleTransactionClose(ctx, req)
	case clientv1.GraphService_GetNode_FullMethodName:
		return h.handleGraphGetNode(ctx, req)
	case clientv1.GraphService_ListNodes_FullMethodName:
		return h.handleGraphListNodes(ctx, req)
	case clientv1.GraphService_CreateNode_FullMethodName:
		return h.handleGraphCreateNode(ctx, req)
	case clientv1.GraphService_UpdateNode_FullMethodName:
		return h.handleGraphUpdateNode(ctx, req)
	case clientv1.GraphService_UpsertNode_FullMethodName:
		return h.handleGraphUpsertNode(ctx, req)
	case clientv1.GraphService_DeleteNode_FullMethodName:
		return h.handleGraphDeleteNode(ctx, req)
	case clientv1.GraphService_GetEdge_FullMethodName:
		return h.handleGraphGetEdge(ctx, req)
	case clientv1.GraphService_ListEdges_FullMethodName:
		return h.handleGraphListEdges(ctx, req)
	case clientv1.GraphService_CreateEdge_FullMethodName:
		return h.handleGraphCreateEdge(ctx, req)
	case clientv1.GraphService_UpdateEdge_FullMethodName:
		return h.handleGraphUpdateEdge(ctx, req)
	case clientv1.GraphService_DeleteEdge_FullMethodName:
		return h.handleGraphDeleteEdge(ctx, req)
	case clientv1.GraphService_ListChildren_FullMethodName:
		return h.handleGraphListChildren(ctx, req)
	case clientv1.GraphService_GetParent_FullMethodName:
		return h.handleGraphGetParent(ctx, req)
	case clientv1.GraphService_MoveSubtree_FullMethodName:
		return h.handleGraphMoveSubtree(ctx, req)
	case clientv1.GraphService_ReorderChildren_FullMethodName:
		return h.handleGraphReorderChildren(ctx, req)
	case clientv1.GraphService_ApplyGraphOperations_FullMethodName:
		return h.handleGraphApplyOperations(ctx, req)
	case clientv1.QueryService_ExecuteQuery_FullMethodName:
		return h.handleQueryExecute(ctx, req)
	case clientv1.QueryService_ExecuteGQL_FullMethodName:
		return h.handleQueryGQL(ctx, req)
	case clientv1.QueryService_ExecuteGQLScript_FullMethodName:
		return h.handleQueryGQLScript(ctx, req)
	case clientv1.MetadataCatalogService_ListTags_FullMethodName:
		return h.handleMetadataListTags(ctx, req)
	case clientv1.MetadataCatalogService_ListPropertyNames_FullMethodName:
		return h.handleMetadataListPropertyNames(ctx, req)
	default:
		return clusterbackend.ForwardedClientResponse{}, status.Errorf(codes.Unimplemented, "forwarded client operation %q is not supported", req.Operation)
	}
}

func decodeForwarded(payload []byte, msg proto.Message) error {
	if err := proto.Unmarshal(payload, msg); err != nil {
		return status.Errorf(codes.InvalidArgument, "decode forwarded request: %v", err)
	}
	return nil
}

func encodeForwarded(msg proto.Message) (clusterbackend.ForwardedClientResponse, error) {
	payload, err := proto.Marshal(msg)
	if err != nil {
		return clusterbackend.ForwardedClientResponse{}, status.Errorf(codes.Internal, "encode forwarded response: %v", err)
	}
	return clusterbackend.ForwardedClientResponse{PayloadType: clusterbackend.PayloadTypeProto, Payload: payload}, nil
}

func (h ForwardedClientHandler) handleSessionOpen(ctx context.Context, in clusterbackend.ForwardedClientRequest) (clusterbackend.ForwardedClientResponse, error) {
	if h.Sessions == nil {
		return clusterbackend.ForwardedClientResponse{}, status.Error(codes.FailedPrecondition, "session service is not configured")
	}
	req := &clientv1.OpenSessionRequest{}
	if err := decodeForwarded(in.Payload, req); err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	res, err := h.Sessions.OpenSession(ctx, req)
	if err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	return encodeForwarded(res)
}

func (h ForwardedClientHandler) handleSessionGet(ctx context.Context, in clusterbackend.ForwardedClientRequest) (clusterbackend.ForwardedClientResponse, error) {
	if h.Sessions == nil {
		return clusterbackend.ForwardedClientResponse{}, status.Error(codes.FailedPrecondition, "session service is not configured")
	}
	req := &clientv1.GetSessionRequest{}
	if err := decodeForwarded(in.Payload, req); err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	res, err := h.Sessions.GetSession(ctx, req)
	if err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	return encodeForwarded(res)
}
func (h ForwardedClientHandler) handleSessionHeartbeat(ctx context.Context, in clusterbackend.ForwardedClientRequest) (clusterbackend.ForwardedClientResponse, error) {
	if h.Sessions == nil {
		return clusterbackend.ForwardedClientResponse{}, status.Error(codes.FailedPrecondition, "session service is not configured")
	}
	req := &clientv1.HeartbeatSessionRequest{}
	if err := decodeForwarded(in.Payload, req); err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	res, err := h.Sessions.HeartbeatSession(ctx, req)
	if err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	return encodeForwarded(res)
}
func (h ForwardedClientHandler) handleSessionClose(ctx context.Context, in clusterbackend.ForwardedClientRequest) (clusterbackend.ForwardedClientResponse, error) {
	if h.Sessions == nil {
		return clusterbackend.ForwardedClientResponse{}, status.Error(codes.FailedPrecondition, "session service is not configured")
	}
	req := &clientv1.CloseSessionRequest{}
	if err := decodeForwarded(in.Payload, req); err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	res, err := h.Sessions.CloseSession(ctx, req)
	if err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	return encodeForwarded(res)
}
func (h ForwardedClientHandler) handleTransactionBegin(ctx context.Context, in clusterbackend.ForwardedClientRequest) (clusterbackend.ForwardedClientResponse, error) {
	if h.Transactions == nil {
		return clusterbackend.ForwardedClientResponse{}, status.Error(codes.FailedPrecondition, "transaction service is not configured")
	}
	req := &clientv1.BeginTransactionRequest{}
	if err := decodeForwarded(in.Payload, req); err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	res, err := h.Transactions.BeginTransaction(ctx, req)
	if err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	return encodeForwarded(res)
}
func (h ForwardedClientHandler) handleTransactionGet(ctx context.Context, in clusterbackend.ForwardedClientRequest) (clusterbackend.ForwardedClientResponse, error) {
	if h.Transactions == nil {
		return clusterbackend.ForwardedClientResponse{}, status.Error(codes.FailedPrecondition, "transaction service is not configured")
	}
	req := &clientv1.GetTransactionRequest{}
	if err := decodeForwarded(in.Payload, req); err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	res, err := h.Transactions.GetTransaction(ctx, req)
	if err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	return encodeForwarded(res)
}
func (h ForwardedClientHandler) handleTransactionCommit(ctx context.Context, in clusterbackend.ForwardedClientRequest) (clusterbackend.ForwardedClientResponse, error) {
	if h.Transactions == nil {
		return clusterbackend.ForwardedClientResponse{}, status.Error(codes.FailedPrecondition, "transaction service is not configured")
	}
	req := &clientv1.CommitTransactionRequest{}
	if err := decodeForwarded(in.Payload, req); err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	res, err := h.Transactions.CommitTransaction(ctx, req)
	if err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	return encodeForwarded(res)
}
func (h ForwardedClientHandler) handleTransactionRollback(ctx context.Context, in clusterbackend.ForwardedClientRequest) (clusterbackend.ForwardedClientResponse, error) {
	if h.Transactions == nil {
		return clusterbackend.ForwardedClientResponse{}, status.Error(codes.FailedPrecondition, "transaction service is not configured")
	}
	req := &clientv1.RollbackTransactionRequest{}
	if err := decodeForwarded(in.Payload, req); err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	res, err := h.Transactions.RollbackTransaction(ctx, req)
	if err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	return encodeForwarded(res)
}
func (h ForwardedClientHandler) handleTransactionClose(ctx context.Context, in clusterbackend.ForwardedClientRequest) (clusterbackend.ForwardedClientResponse, error) {
	if h.Transactions == nil {
		return clusterbackend.ForwardedClientResponse{}, status.Error(codes.FailedPrecondition, "transaction service is not configured")
	}
	req := &clientv1.CloseTransactionRequest{}
	if err := decodeForwarded(in.Payload, req); err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	res, err := h.Transactions.CloseTransaction(ctx, req)
	if err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	return encodeForwarded(res)
}

func (h ForwardedClientHandler) handleGraphGetNode(ctx context.Context, in clusterbackend.ForwardedClientRequest) (clusterbackend.ForwardedClientResponse, error) {
	if h.Graphs == nil {
		return clusterbackend.ForwardedClientResponse{}, status.Error(codes.FailedPrecondition, "graph service is not configured")
	}
	req := &clientv1.GetNodeRequest{}
	if err := decodeForwarded(in.Payload, req); err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	res, err := h.Graphs.GetNode(ctx, req)
	if err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	return encodeForwarded(res)
}
func (h ForwardedClientHandler) handleGraphListNodes(ctx context.Context, in clusterbackend.ForwardedClientRequest) (clusterbackend.ForwardedClientResponse, error) {
	if h.Graphs == nil {
		return clusterbackend.ForwardedClientResponse{}, status.Error(codes.FailedPrecondition, "graph service is not configured")
	}
	req := &clientv1.ListNodesRequest{}
	if err := decodeForwarded(in.Payload, req); err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	res, err := h.Graphs.ListNodes(ctx, req)
	if err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	return encodeForwarded(res)
}
func (h ForwardedClientHandler) handleGraphCreateNode(ctx context.Context, in clusterbackend.ForwardedClientRequest) (clusterbackend.ForwardedClientResponse, error) {
	if h.Graphs == nil {
		return clusterbackend.ForwardedClientResponse{}, status.Error(codes.FailedPrecondition, "graph service is not configured")
	}
	req := &clientv1.CreateNodeRequest{}
	if err := decodeForwarded(in.Payload, req); err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	res, err := h.Graphs.CreateNode(ctx, req)
	if err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	return encodeForwarded(res)
}
func (h ForwardedClientHandler) handleGraphUpdateNode(ctx context.Context, in clusterbackend.ForwardedClientRequest) (clusterbackend.ForwardedClientResponse, error) {
	if h.Graphs == nil {
		return clusterbackend.ForwardedClientResponse{}, status.Error(codes.FailedPrecondition, "graph service is not configured")
	}
	req := &clientv1.UpdateNodeRequest{}
	if err := decodeForwarded(in.Payload, req); err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	res, err := h.Graphs.UpdateNode(ctx, req)
	if err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	return encodeForwarded(res)
}
func (h ForwardedClientHandler) handleGraphUpsertNode(ctx context.Context, in clusterbackend.ForwardedClientRequest) (clusterbackend.ForwardedClientResponse, error) {
	if h.Graphs == nil {
		return clusterbackend.ForwardedClientResponse{}, status.Error(codes.FailedPrecondition, "graph service is not configured")
	}
	req := &clientv1.UpsertNodeRequest{}
	if err := decodeForwarded(in.Payload, req); err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	res, err := h.Graphs.UpsertNode(ctx, req)
	if err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	return encodeForwarded(res)
}
func (h ForwardedClientHandler) handleGraphDeleteNode(ctx context.Context, in clusterbackend.ForwardedClientRequest) (clusterbackend.ForwardedClientResponse, error) {
	if h.Graphs == nil {
		return clusterbackend.ForwardedClientResponse{}, status.Error(codes.FailedPrecondition, "graph service is not configured")
	}
	req := &clientv1.DeleteNodeRequest{}
	if err := decodeForwarded(in.Payload, req); err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	res, err := h.Graphs.DeleteNode(ctx, req)
	if err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	return encodeForwarded(res)
}
func (h ForwardedClientHandler) handleGraphGetEdge(ctx context.Context, in clusterbackend.ForwardedClientRequest) (clusterbackend.ForwardedClientResponse, error) {
	if h.Graphs == nil {
		return clusterbackend.ForwardedClientResponse{}, status.Error(codes.FailedPrecondition, "graph service is not configured")
	}
	req := &clientv1.GetEdgeRequest{}
	if err := decodeForwarded(in.Payload, req); err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	res, err := h.Graphs.GetEdge(ctx, req)
	if err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	return encodeForwarded(res)
}
func (h ForwardedClientHandler) handleGraphListEdges(ctx context.Context, in clusterbackend.ForwardedClientRequest) (clusterbackend.ForwardedClientResponse, error) {
	if h.Graphs == nil {
		return clusterbackend.ForwardedClientResponse{}, status.Error(codes.FailedPrecondition, "graph service is not configured")
	}
	req := &clientv1.ListEdgesRequest{}
	if err := decodeForwarded(in.Payload, req); err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	res, err := h.Graphs.ListEdges(ctx, req)
	if err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	return encodeForwarded(res)
}
func (h ForwardedClientHandler) handleGraphCreateEdge(ctx context.Context, in clusterbackend.ForwardedClientRequest) (clusterbackend.ForwardedClientResponse, error) {
	if h.Graphs == nil {
		return clusterbackend.ForwardedClientResponse{}, status.Error(codes.FailedPrecondition, "graph service is not configured")
	}
	req := &clientv1.CreateEdgeRequest{}
	if err := decodeForwarded(in.Payload, req); err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	res, err := h.Graphs.CreateEdge(ctx, req)
	if err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	return encodeForwarded(res)
}
func (h ForwardedClientHandler) handleGraphUpdateEdge(ctx context.Context, in clusterbackend.ForwardedClientRequest) (clusterbackend.ForwardedClientResponse, error) {
	if h.Graphs == nil {
		return clusterbackend.ForwardedClientResponse{}, status.Error(codes.FailedPrecondition, "graph service is not configured")
	}
	req := &clientv1.UpdateEdgeRequest{}
	if err := decodeForwarded(in.Payload, req); err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	res, err := h.Graphs.UpdateEdge(ctx, req)
	if err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	return encodeForwarded(res)
}
func (h ForwardedClientHandler) handleGraphDeleteEdge(ctx context.Context, in clusterbackend.ForwardedClientRequest) (clusterbackend.ForwardedClientResponse, error) {
	if h.Graphs == nil {
		return clusterbackend.ForwardedClientResponse{}, status.Error(codes.FailedPrecondition, "graph service is not configured")
	}
	req := &clientv1.DeleteEdgeRequest{}
	if err := decodeForwarded(in.Payload, req); err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	res, err := h.Graphs.DeleteEdge(ctx, req)
	if err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	return encodeForwarded(res)
}
func (h ForwardedClientHandler) handleGraphListChildren(ctx context.Context, in clusterbackend.ForwardedClientRequest) (clusterbackend.ForwardedClientResponse, error) {
	if h.Graphs == nil {
		return clusterbackend.ForwardedClientResponse{}, status.Error(codes.FailedPrecondition, "graph service is not configured")
	}
	req := &clientv1.ListChildrenRequest{}
	if err := decodeForwarded(in.Payload, req); err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	res, err := h.Graphs.ListChildren(ctx, req)
	if err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	return encodeForwarded(res)
}
func (h ForwardedClientHandler) handleGraphGetParent(ctx context.Context, in clusterbackend.ForwardedClientRequest) (clusterbackend.ForwardedClientResponse, error) {
	if h.Graphs == nil {
		return clusterbackend.ForwardedClientResponse{}, status.Error(codes.FailedPrecondition, "graph service is not configured")
	}
	req := &clientv1.GetParentRequest{}
	if err := decodeForwarded(in.Payload, req); err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	res, err := h.Graphs.GetParent(ctx, req)
	if err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	return encodeForwarded(res)
}
func (h ForwardedClientHandler) handleGraphMoveSubtree(ctx context.Context, in clusterbackend.ForwardedClientRequest) (clusterbackend.ForwardedClientResponse, error) {
	if h.Graphs == nil {
		return clusterbackend.ForwardedClientResponse{}, status.Error(codes.FailedPrecondition, "graph service is not configured")
	}
	req := &clientv1.MoveSubtreeRequest{}
	if err := decodeForwarded(in.Payload, req); err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	res, err := h.Graphs.MoveSubtree(ctx, req)
	if err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	return encodeForwarded(res)
}
func (h ForwardedClientHandler) handleGraphReorderChildren(ctx context.Context, in clusterbackend.ForwardedClientRequest) (clusterbackend.ForwardedClientResponse, error) {
	if h.Graphs == nil {
		return clusterbackend.ForwardedClientResponse{}, status.Error(codes.FailedPrecondition, "graph service is not configured")
	}
	req := &clientv1.ReorderChildrenRequest{}
	if err := decodeForwarded(in.Payload, req); err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	res, err := h.Graphs.ReorderChildren(ctx, req)
	if err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	return encodeForwarded(res)
}
func (h ForwardedClientHandler) handleGraphApplyOperations(ctx context.Context, in clusterbackend.ForwardedClientRequest) (clusterbackend.ForwardedClientResponse, error) {
	if h.Graphs == nil {
		return clusterbackend.ForwardedClientResponse{}, status.Error(codes.FailedPrecondition, "graph service is not configured")
	}
	req := &clientv1.ApplyGraphOperationsRequest{}
	if err := decodeForwarded(in.Payload, req); err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	res, err := h.Graphs.ApplyGraphOperations(ctx, req)
	if err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	return encodeForwarded(res)
}
func (h ForwardedClientHandler) handleQueryExecute(ctx context.Context, in clusterbackend.ForwardedClientRequest) (clusterbackend.ForwardedClientResponse, error) {
	if h.Queries == nil {
		return clusterbackend.ForwardedClientResponse{}, status.Error(codes.FailedPrecondition, "query service is not configured")
	}
	req := &clientv1.ExecuteQueryRequest{}
	if err := decodeForwarded(in.Payload, req); err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	res, err := h.Queries.ExecuteQuery(ctx, req)
	if err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	return encodeForwarded(res)
}
func (h ForwardedClientHandler) handleQueryGQL(ctx context.Context, in clusterbackend.ForwardedClientRequest) (clusterbackend.ForwardedClientResponse, error) {
	if h.Queries == nil {
		return clusterbackend.ForwardedClientResponse{}, status.Error(codes.FailedPrecondition, "query service is not configured")
	}
	req := &clientv1.ExecuteGQLRequest{}
	if err := decodeForwarded(in.Payload, req); err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	res, err := h.Queries.ExecuteGQL(ctx, req)
	if err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	return encodeForwarded(res)
}
func (h ForwardedClientHandler) handleQueryGQLScript(ctx context.Context, in clusterbackend.ForwardedClientRequest) (clusterbackend.ForwardedClientResponse, error) {
	if h.Queries == nil {
		return clusterbackend.ForwardedClientResponse{}, status.Error(codes.FailedPrecondition, "query service is not configured")
	}
	req := &clientv1.ExecuteGQLScriptRequest{}
	if err := decodeForwarded(in.Payload, req); err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	res, err := h.Queries.ExecuteGQLScript(ctx, req)
	if err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	return encodeForwarded(res)
}
func (h ForwardedClientHandler) handleMetadataListTags(ctx context.Context, in clusterbackend.ForwardedClientRequest) (clusterbackend.ForwardedClientResponse, error) {
	if h.Metadata == nil {
		return clusterbackend.ForwardedClientResponse{}, status.Error(codes.FailedPrecondition, "metadata catalog service is not configured")
	}
	req := &clientv1.ListTagsRequest{}
	if err := decodeForwarded(in.Payload, req); err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	res, err := h.Metadata.ListTags(ctx, req)
	if err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	return encodeForwarded(res)
}
func (h ForwardedClientHandler) handleMetadataListPropertyNames(ctx context.Context, in clusterbackend.ForwardedClientRequest) (clusterbackend.ForwardedClientResponse, error) {
	if h.Metadata == nil {
		return clusterbackend.ForwardedClientResponse{}, status.Error(codes.FailedPrecondition, "metadata catalog service is not configured")
	}
	req := &clientv1.ListPropertyNamesRequest{}
	if err := decodeForwarded(in.Payload, req); err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	res, err := h.Metadata.ListPropertyNames(ctx, req)
	if err != nil {
		return clusterbackend.ForwardedClientResponse{}, err
	}
	return encodeForwarded(res)
}
