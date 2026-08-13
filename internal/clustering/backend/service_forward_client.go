package backend

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/clustering/routing"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const PayloadTypeProto = "application/x-protobuf"

type ForwardClientDiagnostics struct {
	RequestsReceived         uint64
	RequestsDispatched       uint64
	RequestFailures          uint64
	ClusterRejections        uint64
	RouteLoopRejections      uint64
	LastFailureAt            time.Time
	LastFailureReason        string
	LastFailureOperation     string
	LastFailureRequesterNode consensus.NodeID
	LastFailureTargetNode    consensus.NodeID
	LastOperation            string
	LastRequesterNode        consensus.NodeID
	LastTargetNode           consensus.NodeID
}

type ForwardedPrincipal struct {
	Kind          string
	PrincipalID   string
	AuthSessionID string
	Username      string
	CreatedAt     string
}

type ForwardedClientRequest struct {
	ClusterID     string
	Operation     string
	SessionID     string
	TransactionID string
	RequesterNode consensus.NodeID
	TargetNode    consensus.NodeID
	Principal     ForwardedPrincipal
	PayloadType   string
	Payload       []byte
	RequestID     string
}

type ForwardedClientResponse struct {
	PayloadType string
	Payload     []byte
}

type ForwardedClientRequestHandler interface {
	HandleForwardedClientRequest(ctx context.Context, req ForwardedClientRequest) (ForwardedClientResponse, error)
}

func (s *Service) WithClientRequestForwarder(handler ForwardedClientRequestHandler) *Service {
	s.ClientRequestForwarder = handler
	return s
}

func (s *Service) ForwardClientRequest(ctx context.Context, req *clusterpb.ForwardClientRequestRequest) (*clusterpb.ForwardClientRequestResponse, error) {
	s.recordForwardClientReceived(req)
	if err := validateProtocol(req.GetProtocolVersion()); err != nil {
		s.recordForwardClientFailure(req, err)
		return nil, err
	}
	if !s.Identity.ClusterAdmitted || strings.TrimSpace(s.Identity.ClusterID) == "" || strings.TrimSpace(req.GetClusterId()) != s.Identity.ClusterID {
		err := status.Error(codes.PermissionDenied, "local node is not admitted to requested cluster")
		s.recordForwardClientClusterRejection(req, err)
		return nil, err
	}
	if strings.TrimSpace(req.GetOperation()) == "" {
		err := status.Error(codes.InvalidArgument, "operation is required")
		s.recordForwardClientFailure(req, err)
		return nil, err
	}
	if req.GetRequesterNodeId() == 0 {
		err := status.Error(codes.InvalidArgument, "requester_node_id is required")
		s.recordForwardClientFailure(req, err)
		return nil, err
	}
	if req.GetTargetNodeId() == 0 {
		err := status.Error(codes.InvalidArgument, "target_node_id is required")
		s.recordForwardClientFailure(req, err)
		return nil, err
	}
	if req.GetSessionId() == "" && req.GetTransactionId() == "" && !strings.HasSuffix(req.GetOperation(), "/OpenSession") {
		err := status.Error(codes.InvalidArgument, "session_id or transaction_id is required")
		s.recordForwardClientFailure(req, err)
		return nil, err
	}
	depth := (routing.ForwardingGuard{}).IncomingDepth(ctx)
	if depth == 0 {
		err := status.Error(codes.FailedPrecondition, "forwarded client request is missing route metadata")
		s.recordForwardClientFailure(req, err)
		return nil, err
	}
	if depth > routing.DefaultMaxRouteDepth {
		err := routing.NewForwardingLoop("forwarded client request exceeded route depth")
		s.recordForwardClientLoopRejection(req, err)
		return nil, err
	}
	handler := s.ClientRequestForwarder
	if handler == nil {
		err := status.Error(codes.FailedPrecondition, "client request forwarder is not configured")
		s.recordForwardClientFailure(req, err)
		return nil, err
	}
	out, err := handler.HandleForwardedClientRequest(ctx, ForwardedClientRequest{
		ClusterID:     strings.TrimSpace(req.GetClusterId()),
		Operation:     strings.TrimSpace(req.GetOperation()),
		SessionID:     strings.TrimSpace(req.GetSessionId()),
		TransactionID: strings.TrimSpace(req.GetTransactionId()),
		RequesterNode: consensus.NodeID(req.GetRequesterNodeId()),
		TargetNode:    consensus.NodeID(req.GetTargetNodeId()),
		Principal:     principalFromProto(req.GetPrincipal()),
		PayloadType:   strings.TrimSpace(req.GetPayloadType()),
		Payload:       append([]byte(nil), req.GetPayload()...),
		RequestID:     strings.TrimSpace(req.GetRequestId()),
	})
	if err != nil {
		s.recordForwardClientFailure(req, err)
		return nil, err
	}
	s.recordForwardClientDispatched(req)
	return &clusterpb.ForwardClientRequestResponse{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, PayloadType: out.PayloadType, Payload: append([]byte(nil), out.Payload...)}, nil
}

func (s *Service) ForwardClientDiagnostics() ForwardClientDiagnostics {
	if s == nil {
		return ForwardClientDiagnostics{}
	}
	s.forwardMu.Lock()
	defer s.forwardMu.Unlock()
	return s.forwardDiagnostics
}

func (s *Service) recordForwardClientReceived(req *clusterpb.ForwardClientRequestRequest) {
	s.forwardMu.Lock()
	defer s.forwardMu.Unlock()
	s.forwardDiagnostics.RequestsReceived++
	s.recordForwardClientContextLocked(req)
}

func (s *Service) recordForwardClientDispatched(req *clusterpb.ForwardClientRequestRequest) {
	s.forwardMu.Lock()
	defer s.forwardMu.Unlock()
	s.forwardDiagnostics.RequestsDispatched++
	s.recordForwardClientContextLocked(req)
}

func (s *Service) recordForwardClientFailure(req *clusterpb.ForwardClientRequestRequest, err error) {
	s.forwardMu.Lock()
	defer s.forwardMu.Unlock()
	s.forwardDiagnostics.RequestFailures++
	s.recordForwardClientFailureLocked(req, err)
}

func (s *Service) recordForwardClientClusterRejection(req *clusterpb.ForwardClientRequestRequest, err error) {
	s.forwardMu.Lock()
	defer s.forwardMu.Unlock()
	s.forwardDiagnostics.RequestFailures++
	s.forwardDiagnostics.ClusterRejections++
	s.recordForwardClientFailureLocked(req, err)
}

func (s *Service) recordForwardClientLoopRejection(req *clusterpb.ForwardClientRequestRequest, err error) {
	s.forwardMu.Lock()
	defer s.forwardMu.Unlock()
	s.forwardDiagnostics.RequestFailures++
	s.forwardDiagnostics.RouteLoopRejections++
	s.recordForwardClientFailureLocked(req, err)
}

func (s *Service) recordForwardClientFailureLocked(req *clusterpb.ForwardClientRequestRequest, err error) {
	s.forwardDiagnostics.LastFailureAt = time.Now().UTC()
	s.forwardDiagnostics.LastFailureReason = forwardClientFailureReason(err)
	if req != nil {
		s.forwardDiagnostics.LastFailureOperation = strings.TrimSpace(req.GetOperation())
		s.forwardDiagnostics.LastFailureRequesterNode = consensus.NodeID(req.GetRequesterNodeId())
		s.forwardDiagnostics.LastFailureTargetNode = consensus.NodeID(req.GetTargetNodeId())
	}
}

func (s *Service) recordForwardClientContextLocked(req *clusterpb.ForwardClientRequestRequest) {
	if req == nil {
		return
	}
	s.forwardDiagnostics.LastOperation = strings.TrimSpace(req.GetOperation())
	s.forwardDiagnostics.LastRequesterNode = consensus.NodeID(req.GetRequesterNodeId())
	s.forwardDiagnostics.LastTargetNode = consensus.NodeID(req.GetTargetNodeId())
}

func forwardClientFailureReason(err error) string {
	if errors.Is(err, routing.ErrForwardingLoop) {
		return string(routing.ReasonForwardingLoop)
	}
	if st, ok := status.FromError(err); ok {
		return st.Code().String()
	}
	return "error"
}

func principalFromProto(in *clusterpb.ForwardedPrincipal) ForwardedPrincipal {
	if in == nil {
		return ForwardedPrincipal{}
	}
	return ForwardedPrincipal{Kind: strings.TrimSpace(in.GetKind()), PrincipalID: strings.TrimSpace(in.GetPrincipalId()), AuthSessionID: strings.TrimSpace(in.GetAuthSessionId()), Username: strings.TrimSpace(in.GetUsername()), CreatedAt: strings.TrimSpace(in.GetCreatedAt())}
}

func principalToProto(in ForwardedPrincipal) *clusterpb.ForwardedPrincipal {
	return &clusterpb.ForwardedPrincipal{Kind: strings.TrimSpace(in.Kind), PrincipalId: strings.TrimSpace(in.PrincipalID), AuthSessionId: strings.TrimSpace(in.AuthSessionID), Username: strings.TrimSpace(in.Username), CreatedAt: strings.TrimSpace(in.CreatedAt)}
}
