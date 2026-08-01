package client

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"

	daemonblob "github.com/myceldb/mycel/internal/blob/service"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	daegraph "github.com/myceldb/mycel/internal/graph/service"
	daemonsession "github.com/myceldb/mycel/internal/session/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type GraphService struct {
	clientv1.UnimplementedGraphServiceServer
	sessions daemonsession.Manager
	graphs   daegraph.Manager
	blobs    daemonblob.Manager
	router   ClientRequestRouter
}

func NewGraphService(sessions daemonsession.Manager, graphs daegraph.Manager, blobs ...daemonblob.Manager) *GraphService {
	var blobManager daemonblob.Manager
	if len(blobs) > 0 {
		blobManager = blobs[0]
	}
	return &GraphService{sessions: sessions, graphs: graphs, blobs: blobManager}
}

func (s *GraphService) WithClientRequestRouter(router ClientRequestRouter) *GraphService {
	s.router = router
	return s
}

func (s *GraphService) GetNode(ctx context.Context, req *clientv1.GetNodeRequest) (*clientv1.GetNodeResponse, error) {
	if err := rejectUnsupportedStaleRead(req.GetReadOptions()); err != nil {
		return nil, err
	}
	if s.router != nil {
		res := &clientv1.GetNodeResponse{}
		forwarded, err := s.router.ForwardUnary(ctx, clientv1.GraphService_GetNode_FullMethodName, "", req.GetTransactionId(), req, res)
		if forwarded || err != nil {
			return res, err
		}
	}
	readCtx, recorder := daegraph.WithReadMetadataRecorder(ctx)
	tx, err := s.transaction(readCtx, req.GetTransactionId())
	if err != nil {
		return nil, err
	}
	node, err := s.graphs.GetNode(readCtx, tx, req.GetNodeId())
	if err != nil {
		return nil, mapGraphError(err, "get node")
	}
	return &clientv1.GetNodeResponse{Node: mapProtoNode(node), ReadMetadata: protoReadMetadata(recorder.Summary())}, nil
}

func (s *GraphService) ListNodes(ctx context.Context, req *clientv1.ListNodesRequest) (*clientv1.ListNodesResponse, error) {
	if err := rejectUnsupportedStaleRead(req.GetReadOptions()); err != nil {
		return nil, err
	}
	if s.router != nil {
		res := &clientv1.ListNodesResponse{}
		forwarded, err := s.router.ForwardUnary(ctx, clientv1.GraphService_ListNodes_FullMethodName, "", req.GetTransactionId(), req, res)
		if forwarded || err != nil {
			return res, err
		}
	}
	readCtx, recorder := daegraph.WithReadMetadataRecorder(ctx)
	tx, err := s.transaction(readCtx, req.GetTransactionId())
	if err != nil {
		return nil, err
	}
	nodes, next, err := s.graphs.ListNodes(readCtx, tx, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapGraphError(err, "list nodes")
	}
	out := make([]*clientv1.Node, 0, len(nodes))
	for _, node := range nodes {
		out = append(out, mapProtoNode(node))
	}
	return &clientv1.ListNodesResponse{Nodes: out, NextPageToken: next, ReadMetadata: protoReadMetadata(recorder.Summary())}, nil
}

func (s *GraphService) CreateNode(ctx context.Context, req *clientv1.CreateNodeRequest) (*clientv1.CreateNodeResponse, error) {
	if s.router != nil {
		res := &clientv1.CreateNodeResponse{}
		forwarded, err := s.router.ForwardUnary(ctx, clientv1.GraphService_CreateNode_FullMethodName, "", req.GetTransactionId(), req, res)
		if forwarded || err != nil {
			return res, err
		}
	}
	tx, err := s.transaction(ctx, req.GetTransactionId())
	if err != nil {
		return nil, err
	}
	node, err := s.graphs.CreateNode(ctx, tx, nodeInputFromProto(req.GetNode()))
	if err != nil {
		return nil, mapGraphError(err, "create node")
	}
	return &clientv1.CreateNodeResponse{Node: mapProtoNode(node)}, nil
}

func (s *GraphService) CreateBlobNode(stream clientv1.GraphService_CreateBlobNodeServer) error {
	if s.blobs == nil {
		return status.Error(codes.Unimplemented, "blob node creation is not configured")
	}
	ctx := stream.Context()
	var meta *clientv1.CreateBlobNodeMetadata
	var buf bytes.Buffer
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if m := req.GetMetadata(); m != nil {
			if meta != nil {
				return status.Error(codes.InvalidArgument, "blob node metadata must be sent once")
			}
			if s.router != nil {
				if err := s.router.EnsureLocalTransaction(ctx, m.GetTransactionId()); err != nil {
					return err
				}
			}
			meta = m
			continue
		}
		chunk := req.GetChunk()
		if meta == nil {
			return status.Error(codes.InvalidArgument, "blob node metadata must be sent before chunks")
		}
		if len(chunk) > 0 {
			if _, err := buf.Write(chunk); err != nil {
				return err
			}
		}
	}
	if meta == nil {
		return status.Error(codes.InvalidArgument, "blob node metadata is required")
	}
	tx, err := s.transaction(ctx, meta.GetTransactionId())
	if err != nil {
		return err
	}
	if tx.Mode != daemonsession.TransactionModeReadWrite {
		return mapGraphError(daegraph.ErrReadOnly, "create blob node")
	}
	blob, err := s.blobs.UploadBlob(ctx, daemonblob.UploadInput{SpaceID: tx.SpaceID, DeclaredMimeType: meta.GetDeclaredMimeType(), OriginalFilename: meta.GetOriginalFilename(), Reader: bytes.NewReader(buf.Bytes())})
	if err != nil {
		return mapBlobError(err, "upload blob node content")
	}
	properties := structMap(meta.GetProperties())
	payload := structMap(meta.GetPayload())
	if payload == nil {
		payload = map[string]any{}
	}
	payload["blob_id"] = blob.BlobID
	payload["mime_type"] = blob.MimeType
	payload["size_bytes"] = blob.SizeBytes
	if blob.DeclaredMimeType != "" {
		payload["declared_mime_type"] = blob.DeclaredMimeType
	}
	if blob.OriginalFilename != "" {
		payload["original_filename"] = filepath.Base(blob.OriginalFilename)
	}
	node, err := s.graphs.CreateNode(ctx, tx, daegraph.NodeInput{NodeID: meta.GetNodeId(), Labels: meta.GetLabels(), BlobID: blob.BlobID, Properties: properties, Payload: payload, Meta: structMap(meta.GetMeta())})
	if err != nil {
		return mapGraphError(err, "create blob node")
	}
	return stream.SendAndClose(&clientv1.CreateBlobNodeResponse{Node: mapProtoNode(node), Blob: mapProtoBlob(blob)})
}

func (s *GraphService) UpdateNode(ctx context.Context, req *clientv1.UpdateNodeRequest) (*clientv1.UpdateNodeResponse, error) {
	if s.router != nil {
		res := &clientv1.UpdateNodeResponse{}
		forwarded, err := s.router.ForwardUnary(ctx, clientv1.GraphService_UpdateNode_FullMethodName, "", req.GetTransactionId(), req, res)
		if forwarded || err != nil {
			return res, err
		}
	}
	tx, err := s.transaction(ctx, req.GetTransactionId())
	if err != nil {
		return nil, err
	}
	input := updateNodeInputFromProto(req.GetNode())
	if req.GetUpdateMask() != nil {
		input.UpdateMask = req.GetUpdateMask().GetPaths()
	}
	node, err := s.graphs.UpdateNode(ctx, tx, input)
	if err != nil {
		return nil, mapGraphError(err, "update node")
	}
	return &clientv1.UpdateNodeResponse{Node: mapProtoNode(node)}, nil
}

func (s *GraphService) UpsertNode(ctx context.Context, req *clientv1.UpsertNodeRequest) (*clientv1.UpsertNodeResponse, error) {
	if s.router != nil {
		res := &clientv1.UpsertNodeResponse{}
		forwarded, err := s.router.ForwardUnary(ctx, clientv1.GraphService_UpsertNode_FullMethodName, "", req.GetTransactionId(), req, res)
		if forwarded || err != nil {
			return res, err
		}
	}
	tx, err := s.transaction(ctx, req.GetTransactionId())
	if err != nil {
		return nil, err
	}
	node, err := s.graphs.UpsertNode(ctx, tx, nodeInputFromProto(req.GetNode()))
	if err != nil {
		return nil, mapGraphError(err, "upsert node")
	}
	return &clientv1.UpsertNodeResponse{Node: mapProtoNode(node)}, nil
}

func (s *GraphService) DeleteNode(ctx context.Context, req *clientv1.DeleteNodeRequest) (*clientv1.DeleteNodeResponse, error) {
	if s.router != nil {
		res := &clientv1.DeleteNodeResponse{}
		forwarded, err := s.router.ForwardUnary(ctx, clientv1.GraphService_DeleteNode_FullMethodName, "", req.GetTransactionId(), req, res)
		if forwarded || err != nil {
			return res, err
		}
	}
	tx, err := s.transaction(ctx, req.GetTransactionId())
	if err != nil {
		return nil, err
	}
	nodes, edges, err := s.graphs.DeleteNode(ctx, tx, req.GetNodeId(), req.GetRecursive())
	if err != nil {
		return nil, mapGraphError(err, "delete node")
	}
	return &clientv1.DeleteNodeResponse{DeletedNodeIds: nodes, DeletedEdgeIds: edges}, nil
}

func (s *GraphService) GetEdge(ctx context.Context, req *clientv1.GetEdgeRequest) (*clientv1.GetEdgeResponse, error) {
	if err := rejectUnsupportedStaleRead(req.GetReadOptions()); err != nil {
		return nil, err
	}
	if s.router != nil {
		res := &clientv1.GetEdgeResponse{}
		forwarded, err := s.router.ForwardUnary(ctx, clientv1.GraphService_GetEdge_FullMethodName, "", req.GetTransactionId(), req, res)
		if forwarded || err != nil {
			return res, err
		}
	}
	readCtx, recorder := daegraph.WithReadMetadataRecorder(ctx)
	tx, err := s.transaction(readCtx, req.GetTransactionId())
	if err != nil {
		return nil, err
	}
	edge, err := s.graphs.GetEdge(readCtx, tx, req.GetEdgeId())
	if err != nil {
		return nil, mapGraphError(err, "get edge")
	}
	return &clientv1.GetEdgeResponse{Edge: mapProtoEdge(edge), ReadMetadata: protoReadMetadata(recorder.Summary())}, nil
}

func (s *GraphService) ListEdges(ctx context.Context, req *clientv1.ListEdgesRequest) (*clientv1.ListEdgesResponse, error) {
	if err := rejectUnsupportedStaleRead(req.GetReadOptions()); err != nil {
		return nil, err
	}
	if s.router != nil {
		res := &clientv1.ListEdgesResponse{}
		forwarded, err := s.router.ForwardUnary(ctx, clientv1.GraphService_ListEdges_FullMethodName, "", req.GetTransactionId(), req, res)
		if forwarded || err != nil {
			return res, err
		}
	}
	readCtx, recorder := daegraph.WithReadMetadataRecorder(ctx)
	tx, err := s.transaction(readCtx, req.GetTransactionId())
	if err != nil {
		return nil, err
	}
	edges, next, err := s.graphs.ListEdges(readCtx, tx, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapGraphError(err, "list edges")
	}
	out := make([]*clientv1.Edge, 0, len(edges))
	for _, edge := range edges {
		out = append(out, mapProtoEdge(edge))
	}
	return &clientv1.ListEdgesResponse{Edges: out, NextPageToken: next, ReadMetadata: protoReadMetadata(recorder.Summary())}, nil
}

func (s *GraphService) CreateEdge(ctx context.Context, req *clientv1.CreateEdgeRequest) (*clientv1.CreateEdgeResponse, error) {
	if s.router != nil {
		res := &clientv1.CreateEdgeResponse{}
		forwarded, err := s.router.ForwardUnary(ctx, clientv1.GraphService_CreateEdge_FullMethodName, "", req.GetTransactionId(), req, res)
		if forwarded || err != nil {
			return res, err
		}
	}
	tx, err := s.transaction(ctx, req.GetTransactionId())
	if err != nil {
		return nil, err
	}
	edge, err := s.graphs.CreateEdge(ctx, tx, edgeInputFromProto(req.GetEdge()))
	if err != nil {
		return nil, mapGraphError(err, "create edge")
	}
	return &clientv1.CreateEdgeResponse{Edge: mapProtoEdge(edge)}, nil
}

func (s *GraphService) UpdateEdge(ctx context.Context, req *clientv1.UpdateEdgeRequest) (*clientv1.UpdateEdgeResponse, error) {
	if s.router != nil {
		res := &clientv1.UpdateEdgeResponse{}
		forwarded, err := s.router.ForwardUnary(ctx, clientv1.GraphService_UpdateEdge_FullMethodName, "", req.GetTransactionId(), req, res)
		if forwarded || err != nil {
			return res, err
		}
	}
	tx, err := s.transaction(ctx, req.GetTransactionId())
	if err != nil {
		return nil, err
	}
	input := updateEdgeInputFromProto(req.GetEdge())
	if req.GetUpdateMask() != nil {
		input.UpdateMask = req.GetUpdateMask().GetPaths()
	}
	edge, err := s.graphs.UpdateEdge(ctx, tx, input)
	if err != nil {
		return nil, mapGraphError(err, "update edge")
	}
	return &clientv1.UpdateEdgeResponse{Edge: mapProtoEdge(edge)}, nil
}

func (s *GraphService) DeleteEdge(ctx context.Context, req *clientv1.DeleteEdgeRequest) (*clientv1.DeleteEdgeResponse, error) {
	if s.router != nil {
		res := &clientv1.DeleteEdgeResponse{}
		forwarded, err := s.router.ForwardUnary(ctx, clientv1.GraphService_DeleteEdge_FullMethodName, "", req.GetTransactionId(), req, res)
		if forwarded || err != nil {
			return res, err
		}
	}
	tx, err := s.transaction(ctx, req.GetTransactionId())
	if err != nil {
		return nil, err
	}
	id, err := s.graphs.DeleteEdge(ctx, tx, req.GetEdgeId())
	if err != nil {
		return nil, mapGraphError(err, "delete edge")
	}
	return &clientv1.DeleteEdgeResponse{DeletedEdgeId: id}, nil
}

func (s *GraphService) ListChildren(ctx context.Context, req *clientv1.ListChildrenRequest) (*clientv1.ListChildrenResponse, error) {
	if err := rejectUnsupportedStaleRead(req.GetReadOptions()); err != nil {
		return nil, err
	}
	if s.router != nil {
		res := &clientv1.ListChildrenResponse{}
		forwarded, err := s.router.ForwardUnary(ctx, clientv1.GraphService_ListChildren_FullMethodName, "", req.GetTransactionId(), req, res)
		if forwarded || err != nil {
			return res, err
		}
	}
	readCtx, recorder := daegraph.WithReadMetadataRecorder(ctx)
	tx, err := s.transaction(readCtx, req.GetTransactionId())
	if err != nil {
		return nil, err
	}
	edges, err := s.graphs.ListChildren(readCtx, tx, req.GetParentNodeId())
	if err != nil {
		return nil, mapGraphError(err, "list children")
	}
	out := make([]*clientv1.Edge, 0, len(edges))
	for _, edge := range edges {
		out = append(out, mapProtoEdge(edge))
	}
	return &clientv1.ListChildrenResponse{ContainsEdges: out, ReadMetadata: protoReadMetadata(recorder.Summary())}, nil
}

func (s *GraphService) GetParent(ctx context.Context, req *clientv1.GetParentRequest) (*clientv1.GetParentResponse, error) {
	if err := rejectUnsupportedStaleRead(req.GetReadOptions()); err != nil {
		return nil, err
	}
	if s.router != nil {
		res := &clientv1.GetParentResponse{}
		forwarded, err := s.router.ForwardUnary(ctx, clientv1.GraphService_GetParent_FullMethodName, "", req.GetTransactionId(), req, res)
		if forwarded || err != nil {
			return res, err
		}
	}
	readCtx, recorder := daegraph.WithReadMetadataRecorder(ctx)
	tx, err := s.transaction(readCtx, req.GetTransactionId())
	if err != nil {
		return nil, err
	}
	edge, err := s.graphs.GetParent(readCtx, tx, req.GetChildNodeId())
	if err != nil {
		return nil, mapGraphError(err, "get parent")
	}
	res := &clientv1.GetParentResponse{ReadMetadata: protoReadMetadata(recorder.Summary())}
	if edge != nil {
		res.ContainsEdge = mapProtoEdge(*edge)
	}
	return res, nil
}

func (s *GraphService) MoveSubtree(ctx context.Context, req *clientv1.MoveSubtreeRequest) (*clientv1.MoveSubtreeResponse, error) {
	if s.router != nil {
		res := &clientv1.MoveSubtreeResponse{}
		forwarded, err := s.router.ForwardUnary(ctx, clientv1.GraphService_MoveSubtree_FullMethodName, "", req.GetTransactionId(), req, res)
		if forwarded || err != nil {
			return res, err
		}
	}
	tx, err := s.transaction(ctx, req.GetTransactionId())
	if err != nil {
		return nil, err
	}
	var order *int32
	if req.Order != nil {
		order = req.Order
	}
	edge, err := s.graphs.MoveSubtree(ctx, tx, req.GetNodeId(), req.GetNewParentNodeId(), order)
	if err != nil {
		return nil, mapGraphError(err, "move subtree")
	}
	return &clientv1.MoveSubtreeResponse{ContainsEdge: mapProtoEdge(edge)}, nil
}

func (s *GraphService) ReorderChildren(ctx context.Context, req *clientv1.ReorderChildrenRequest) (*clientv1.ReorderChildrenResponse, error) {
	if s.router != nil {
		res := &clientv1.ReorderChildrenResponse{}
		forwarded, err := s.router.ForwardUnary(ctx, clientv1.GraphService_ReorderChildren_FullMethodName, "", req.GetTransactionId(), req, res)
		if forwarded || err != nil {
			return res, err
		}
	}
	tx, err := s.transaction(ctx, req.GetTransactionId())
	if err != nil {
		return nil, err
	}
	edges, err := s.graphs.ReorderChildren(ctx, tx, req.GetParentNodeId(), req.GetChildNodeIds())
	if err != nil {
		return nil, mapGraphError(err, "reorder children")
	}
	out := make([]*clientv1.Edge, 0, len(edges))
	for _, edge := range edges {
		out = append(out, mapProtoEdge(edge))
	}
	return &clientv1.ReorderChildrenResponse{ContainsEdges: out}, nil
}

func (s *GraphService) ApplyGraphOperations(ctx context.Context, req *clientv1.ApplyGraphOperationsRequest) (*clientv1.ApplyGraphOperationsResponse, error) {
	if s.router != nil {
		res := &clientv1.ApplyGraphOperationsResponse{}
		forwarded, err := s.router.ForwardUnary(ctx, clientv1.GraphService_ApplyGraphOperations_FullMethodName, "", req.GetTransactionId(), req, res)
		if forwarded || err != nil {
			return res, err
		}
	}
	tx, err := s.transaction(ctx, req.GetTransactionId())
	if err != nil {
		return nil, err
	}
	results := make([]*clientv1.GraphOperationResult, 0, len(req.GetOperations()))
	for _, op := range req.GetOperations() {
		result, err := s.applyOperation(ctx, tx, op)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return &clientv1.ApplyGraphOperationsResponse{Results: results}, nil
}

func (s *GraphService) applyOperation(ctx context.Context, tx daemonsession.GraphTransaction, op *clientv1.GraphOperation) (*clientv1.GraphOperationResult, error) {
	switch value := op.GetOperation().(type) {
	case *clientv1.GraphOperation_CreateNode:
		node, err := s.graphs.CreateNode(ctx, tx, nodeInputFromProto(value.CreateNode))
		if err != nil {
			return nil, mapGraphError(err, "apply create node")
		}
		return &clientv1.GraphOperationResult{Result: &clientv1.GraphOperationResult_CreatedNode{CreatedNode: mapProtoNode(node)}}, nil
	case *clientv1.GraphOperation_UpdateNode:
		input := updateNodeInputFromProto(value.UpdateNode.GetNode())
		if value.UpdateNode.GetUpdateMask() != nil {
			input.UpdateMask = value.UpdateNode.GetUpdateMask().GetPaths()
		}
		node, err := s.graphs.UpdateNode(ctx, tx, input)
		if err != nil {
			return nil, mapGraphError(err, "apply update node")
		}
		return &clientv1.GraphOperationResult{Result: &clientv1.GraphOperationResult_UpdatedNode{UpdatedNode: mapProtoNode(node)}}, nil
	case *clientv1.GraphOperation_UpsertNode:
		node, err := s.graphs.UpsertNode(ctx, tx, nodeInputFromProto(value.UpsertNode.GetNode()))
		if err != nil {
			return nil, mapGraphError(err, "apply upsert node")
		}
		return &clientv1.GraphOperationResult{Result: &clientv1.GraphOperationResult_UpsertedNode{UpsertedNode: mapProtoNode(node)}}, nil
	case *clientv1.GraphOperation_DeleteNode:
		nodes, edges, err := s.graphs.DeleteNode(ctx, tx, value.DeleteNode.GetNodeId(), value.DeleteNode.GetRecursive())
		if err != nil {
			return nil, mapGraphError(err, "apply delete node")
		}
		return &clientv1.GraphOperationResult{Result: &clientv1.GraphOperationResult_DeletedNode{DeletedNode: &clientv1.NodeDeleteResult{DeletedNodeIds: nodes, DeletedEdgeIds: edges}}}, nil
	case *clientv1.GraphOperation_CreateEdge:
		edge, err := s.graphs.CreateEdge(ctx, tx, edgeInputFromProto(value.CreateEdge))
		if err != nil {
			return nil, mapGraphError(err, "apply create edge")
		}
		return &clientv1.GraphOperationResult{Result: &clientv1.GraphOperationResult_CreatedEdge{CreatedEdge: mapProtoEdge(edge)}}, nil
	case *clientv1.GraphOperation_UpdateEdge:
		input := updateEdgeInputFromProto(value.UpdateEdge.GetEdge())
		if value.UpdateEdge.GetUpdateMask() != nil {
			input.UpdateMask = value.UpdateEdge.GetUpdateMask().GetPaths()
		}
		edge, err := s.graphs.UpdateEdge(ctx, tx, input)
		if err != nil {
			return nil, mapGraphError(err, "apply update edge")
		}
		return &clientv1.GraphOperationResult{Result: &clientv1.GraphOperationResult_UpdatedEdge{UpdatedEdge: mapProtoEdge(edge)}}, nil
	case *clientv1.GraphOperation_DeleteEdge:
		id, err := s.graphs.DeleteEdge(ctx, tx, value.DeleteEdge.GetEdgeId())
		if err != nil {
			return nil, mapGraphError(err, "apply delete edge")
		}
		return &clientv1.GraphOperationResult{Result: &clientv1.GraphOperationResult_DeletedEdge{DeletedEdge: &clientv1.EdgeDeleteResult{DeletedEdgeId: id}}}, nil
	case *clientv1.GraphOperation_MoveSubtree:
		edge, err := s.graphs.MoveSubtree(ctx, tx, value.MoveSubtree.GetNodeId(), value.MoveSubtree.GetNewParentNodeId(), value.MoveSubtree.Order)
		if err != nil {
			return nil, mapGraphError(err, "apply move subtree")
		}
		return &clientv1.GraphOperationResult{Result: &clientv1.GraphOperationResult_MovedSubtreeEdge{MovedSubtreeEdge: mapProtoEdge(edge)}}, nil
	case *clientv1.GraphOperation_ReorderChildren:
		edges, err := s.graphs.ReorderChildren(ctx, tx, value.ReorderChildren.GetParentNodeId(), value.ReorderChildren.GetChildNodeIds())
		if err != nil {
			return nil, mapGraphError(err, "apply reorder children")
		}
		out := make([]*clientv1.Edge, 0, len(edges))
		for _, edge := range edges {
			out = append(out, mapProtoEdge(edge))
		}
		return &clientv1.GraphOperationResult{Result: &clientv1.GraphOperationResult_ReorderedChildren{ReorderedChildren: &clientv1.ChildrenReorderResult{ContainsEdges: out}}}, nil
	default:
		return nil, status.Error(codes.InvalidArgument, "graph operation is required")
	}
}

func (s *GraphService) transaction(ctx context.Context, transactionID string) (daemonsession.GraphTransaction, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return daemonsession.GraphTransaction{}, err
	}
	tx, err := s.sessions.GetTransaction(ctx, principal.UserID, transactionID)
	if err != nil {
		return daemonsession.GraphTransaction{}, mapSessionError(err, "get transaction")
	}
	return tx, nil
}

func nodeInputFromProto(node *clientv1.NodeCreate) daegraph.NodeInput {
	if node == nil {
		return daegraph.NodeInput{}
	}
	return daegraph.NodeInput{NodeID: node.GetNodeId(), Labels: node.GetLabels(), Properties: structMap(node.GetProperties()), Payload: structMap(node.GetPayload()), Meta: structMap(node.GetMeta())}
}

func updateNodeInputFromProto(node *clientv1.Node) daegraph.UpdateNodeInput {
	if node == nil {
		return daegraph.UpdateNodeInput{}
	}
	return daegraph.UpdateNodeInput{NodeID: node.GetNodeId(), Labels: node.GetLabels(), Properties: structMap(node.GetProperties()), Payload: structMap(node.GetPayload()), Meta: structMap(node.GetMeta())}
}

func edgeInputFromProto(edge *clientv1.EdgeCreate) daegraph.EdgeInput {
	if edge == nil {
		return daegraph.EdgeInput{}
	}
	return daegraph.EdgeInput{EdgeID: edge.GetEdgeId(), FromNodeID: edge.GetFromNodeId(), ToNodeID: edge.GetToNodeId(), Labels: edge.GetLabels(), Properties: structMap(edge.GetProperties()), Payload: structMap(edge.GetPayload()), Meta: structMap(edge.GetMeta())}
}

func updateEdgeInputFromProto(edge *clientv1.Edge) daegraph.UpdateEdgeInput {
	if edge == nil {
		return daegraph.UpdateEdgeInput{}
	}
	return daegraph.UpdateEdgeInput{EdgeID: edge.GetEdgeId(), Labels: edge.GetLabels(), Properties: structMap(edge.GetProperties()), Payload: structMap(edge.GetPayload()), Meta: structMap(edge.GetMeta())}
}

func mapProtoNode(node domaingraph.Node) *clientv1.Node {
	properties := node.Properties
	if properties == nil {
		properties = node.Props
	}
	payload := node.Payload
	if payload == nil {
		payload = map[string]any{}
		if node.Content != "" {
			payload["text"] = node.Content
		}
		if node.BlobRef != nil {
			payload["blob_id"] = string(*node.BlobRef)
		}
	}
	out := &clientv1.Node{NodeId: node.ID.String(), DomainId: node.DomainID.String(), Labels: append([]string(nil), node.Labels...), Properties: protoStruct(properties), Payload: protoStruct(payload), Meta: protoStruct(node.Meta), CreateTime: timestamppb.New(node.CreatedAt), UpdateTime: timestamppb.New(node.UpdatedAt)}
	return out
}

func mapProtoEdge(edge domaingraph.Edge) *clientv1.Edge {
	return &clientv1.Edge{EdgeId: edge.ID.String(), DomainId: edge.DomainID.String(), FromNodeId: edge.FromID.String(), ToNodeId: edge.ToID.String(), Labels: append([]string(nil), edge.Labels...), Properties: protoStruct(edge.Properties), Payload: protoStruct(edge.Payload), Meta: protoStruct(edge.Meta), CreateTime: timestamppb.New(edge.CreatedAt), UpdateTime: timestamppb.New(edge.UpdatedAt)}
}

func structMap(value *structpb.Struct) map[string]any {
	if value == nil {
		return nil
	}
	return value.AsMap()
}

func protoStruct(value map[string]any) *structpb.Struct {
	if value == nil {
		value = map[string]any{}
	}
	out, err := structpb.NewStruct(value)
	if err != nil {
		return &structpb.Struct{}
	}
	return out
}

func mapGraphError(err error, action string) error {
	if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
		return err
	}
	if errors.Is(err, daegraph.ErrInvalidInput) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if errors.Is(err, daegraph.ErrNotFound) {
		return status.Error(codes.NotFound, "graph entity not found")
	}
	if errors.Is(err, daegraph.ErrUnauthorized) {
		return status.Error(codes.PermissionDenied, "graph access denied")
	}
	if errors.Is(err, daegraph.ErrReadOnly) || errors.Is(err, daegraph.ErrInvalidState) {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	if errors.Is(err, daegraph.ErrConflict) {
		return status.Error(codes.Aborted, "graph transaction conflict")
	}
	return status.Errorf(codes.Internal, "%s: %v", action, err)
}
