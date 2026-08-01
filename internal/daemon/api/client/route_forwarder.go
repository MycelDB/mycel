package client

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	clusterbackend "github.com/myceldb/mycel/internal/clustering/backend"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/clustering/routing"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ClientRequestRouter routes Phase E session/transaction scoped client API
// requests to the node that owns the in-flight session/transaction state.
type ClientRequestRouter interface {
	ForwardUnary(ctx context.Context, operation string, sessionID string, transactionID string, req proto.Message, res proto.Message) (bool, error)
	ForwardUnaryToNode(ctx context.Context, operation string, target consensus.NodeID, sessionID string, transactionID string, req proto.Message, res proto.Message) (bool, error)
	EnsureLocalSession(ctx context.Context, sessionID string) error
	EnsureLocalTransaction(ctx context.Context, transactionID string) error
}

type ClientRouteDiagnostics struct {
	Enabled                  bool
	LocalNode                consensus.NodeID
	ForwardAttempts          uint64
	ForwardSuccesses         uint64
	ForwardFailures          uint64
	LocalDecisions           uint64
	UnknownHomeFailures      uint64
	HomeMismatchFailures     uint64
	RouteUnavailableFailures uint64
	RouteLoopRejections      uint64
	LastFailureAt            time.Time
	LastFailureReason        string
	LastFailureOperation     string
	LastFailureTargetNode    consensus.NodeID
	LastFailureSessionID     string
	LastFailureTransactionID string
}

type BackendClientRequestRouter struct {
	Enabled   bool
	ClusterID string
	LocalNode consensus.NodeID
	NodeAddrs []string
	Client    clusterbackend.Client

	mu          sync.Mutex
	diagnostics ClientRouteDiagnostics
}

func NewBackendClientRequestRouter(enabled bool, clusterID string, localNode consensus.NodeID, nodeAddrs []string, authToken string) *BackendClientRequestRouter {
	return &BackendClientRequestRouter{Enabled: enabled, ClusterID: strings.TrimSpace(clusterID), LocalNode: localNode, NodeAddrs: append([]string(nil), nodeAddrs...), Client: clusterbackend.Client{AuthToken: authToken}}
}

func (r *BackendClientRequestRouter) ForwardUnary(ctx context.Context, operation string, sessionID string, transactionID string, req proto.Message, res proto.Message) (bool, error) {
	if r == nil || !r.Enabled {
		return false, nil
	}
	target, err := r.targetFor(sessionID, transactionID)
	if err != nil {
		r.recordRouteFailure(operation, sessionID, transactionID, target, err)
		return false, err
	}
	return r.ForwardUnaryToNode(ctx, operation, target, sessionID, transactionID, req, res)
}

func (r *BackendClientRequestRouter) ForwardUnaryToNode(ctx context.Context, operation string, target consensus.NodeID, sessionID string, transactionID string, req proto.Message, res proto.Message) (bool, error) {
	if r == nil || !r.Enabled {
		return false, nil
	}
	if target == 0 || target == r.LocalNode {
		r.recordLocalDecision()
		return false, nil
	}
	addr, err := r.addrFor(target)
	if err != nil {
		r.recordRouteFailure(operation, sessionID, transactionID, target, err)
		return false, err
	}
	if req == nil || res == nil {
		err := status.Error(codes.Internal, "forwarded request and response messages are required")
		r.recordForwardFailure(operation, sessionID, transactionID, target, err)
		return true, err
	}
	payload, err := proto.Marshal(req)
	if err != nil {
		err = status.Errorf(codes.Internal, "marshal forwarded request: %v", err)
		r.recordForwardFailure(operation, sessionID, transactionID, target, err)
		return true, err
	}
	principal, _ := spaceUserPrincipalFromContext(ctx)
	r.recordForwardAttempt()
	out, err := r.Client.ForwardClientRequest(ctx, addr, clusterbackend.ForwardClientRequestInput{ClusterID: r.ClusterID, Operation: operation, SessionID: sessionID, TransactionID: transactionID, RequesterNode: r.LocalNode, TargetNode: target, Principal: principalToForwarded(principal), PayloadType: clusterbackend.PayloadTypeProto, Payload: payload})
	if err != nil {
		r.recordForwardFailure(operation, sessionID, transactionID, target, err)
		return true, err
	}
	if out.PayloadType != "" && out.PayloadType != clusterbackend.PayloadTypeProto {
		err := status.Errorf(codes.Internal, "unsupported forwarded response payload type %q", out.PayloadType)
		r.recordForwardFailure(operation, sessionID, transactionID, target, err)
		return true, err
	}
	if err := proto.Unmarshal(out.Payload, res); err != nil {
		err = status.Errorf(codes.Internal, "unmarshal forwarded response: %v", err)
		r.recordForwardFailure(operation, sessionID, transactionID, target, err)
		return true, err
	}
	r.recordForwardSuccess()
	return true, nil
}

func (r *BackendClientRequestRouter) EnsureLocalSession(ctx context.Context, sessionID string) error {
	if r == nil || !r.Enabled {
		return nil
	}
	target, err := r.targetFor(sessionID, "")
	if err != nil {
		r.recordRouteFailure("stream.session", sessionID, "", target, err)
		return err
	}
	if target != 0 && target != r.LocalNode {
		err := routing.NewRouteUnavailable("stream forwarding for remote-home session requests is not yet supported", routing.WithHomeNode(target), routing.WithTargetNode(target), routing.WithLocalNode(r.LocalNode))
		r.recordRouteFailure("stream.session", sessionID, "", target, err)
		return err
	}
	return nil
}

func (r *BackendClientRequestRouter) EnsureLocalTransaction(ctx context.Context, transactionID string) error {
	if r == nil || !r.Enabled {
		return nil
	}
	target, err := r.targetFor("", transactionID)
	if err != nil {
		r.recordRouteFailure("stream.transaction", "", transactionID, target, err)
		return err
	}
	if target != 0 && target != r.LocalNode {
		err := routing.NewRouteUnavailable("stream forwarding for remote-home transaction requests is not yet supported", routing.WithHomeNode(target), routing.WithTargetNode(target), routing.WithLocalNode(r.LocalNode))
		r.recordRouteFailure("stream.transaction", "", transactionID, target, err)
		return err
	}
	return nil
}

func (r *BackendClientRequestRouter) targetFor(sessionID string, transactionID string) (consensus.NodeID, error) {
	if strings.TrimSpace(transactionID) != "" {
		home, ok, err := routing.ParseTransactionHomeNode(transactionID)
		if err != nil {
			return 0, status.Error(codes.InvalidArgument, err.Error())
		}
		if !ok {
			return 0, routing.NewUnknownSessionHome("transaction id does not encode home node", routing.WithLocalNode(r.LocalNode))
		}
		return home, nil
	}
	if strings.TrimSpace(sessionID) != "" {
		home, ok, err := routing.ParseSessionHomeNode(sessionID)
		if err != nil {
			return 0, status.Error(codes.InvalidArgument, err.Error())
		}
		if !ok {
			return 0, routing.NewUnknownSessionHome("session id does not encode home node", routing.WithLocalNode(r.LocalNode))
		}
		return home, nil
	}
	return 0, status.Error(codes.InvalidArgument, "session_id or transaction_id is required for routing")
}

func (r *BackendClientRequestRouter) addrFor(node consensus.NodeID) (string, error) {
	if node == 0 {
		return "", routing.NewRouteUnavailable("target node is unknown", routing.WithLocalNode(r.LocalNode))
	}
	idx := int(node) - 1
	if idx < 0 || idx >= len(r.NodeAddrs) || strings.TrimSpace(r.NodeAddrs[idx]) == "" {
		return "", routing.NewRouteUnavailable(fmt.Sprintf("backend address for node %d is unavailable", node), routing.WithTargetNode(node), routing.WithLocalNode(r.LocalNode))
	}
	return strings.TrimSpace(r.NodeAddrs[idx]), nil
}

func (r *BackendClientRequestRouter) Diagnostics() ClientRouteDiagnostics {
	if r == nil {
		return ClientRouteDiagnostics{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.diagnostics
	out.Enabled = r.Enabled
	out.LocalNode = r.LocalNode
	return out
}

func (r *BackendClientRequestRouter) recordLocalDecision() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.diagnostics.LocalDecisions++
}

func (r *BackendClientRequestRouter) recordForwardAttempt() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.diagnostics.ForwardAttempts++
}

func (r *BackendClientRequestRouter) recordForwardSuccess() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.diagnostics.ForwardSuccesses++
}

func (r *BackendClientRequestRouter) recordForwardFailure(operation string, sessionID string, transactionID string, target consensus.NodeID, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.diagnostics.ForwardFailures++
	r.recordFailureLocked(operation, sessionID, transactionID, target, err)
}

func (r *BackendClientRequestRouter) recordRouteFailure(operation string, sessionID string, transactionID string, target consensus.NodeID, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recordFailureLocked(operation, sessionID, transactionID, target, err)
}

func (r *BackendClientRequestRouter) recordFailureLocked(operation string, sessionID string, transactionID string, target consensus.NodeID, err error) {
	if errors.Is(err, routing.ErrUnknownSessionHome) || grpcFailureLooksLike(err, codes.NotFound, "does not encode home node") {
		r.diagnostics.UnknownHomeFailures++
	}
	if errors.Is(err, routing.ErrSessionHomeMismatch) || grpcFailureLooksLike(err, codes.FailedPrecondition, "belongs to another home node") || grpcFailureLooksLike(err, codes.FailedPrecondition, "home mismatch") {
		r.diagnostics.HomeMismatchFailures++
	}
	if errors.Is(err, routing.ErrRouteUnavailable) || status.Code(err) == codes.Unavailable {
		r.diagnostics.RouteUnavailableFailures++
	}
	if errors.Is(err, routing.ErrForwardingLoop) || grpcFailureLooksLike(err, codes.FailedPrecondition, string(routing.ReasonForwardingLoop)) || grpcFailureLooksLike(err, codes.FailedPrecondition, "route depth") {
		r.diagnostics.RouteLoopRejections++
	}
	r.diagnostics.LastFailureAt = time.Now().UTC()
	r.diagnostics.LastFailureReason = sanitizedRouteFailureReason(err)
	r.diagnostics.LastFailureOperation = strings.TrimSpace(operation)
	r.diagnostics.LastFailureTargetNode = target
	r.diagnostics.LastFailureSessionID = sanitizeRouteID(sessionID)
	r.diagnostics.LastFailureTransactionID = sanitizeRouteID(transactionID)
}

func sanitizedRouteFailureReason(err error) string {
	switch {
	case errors.Is(err, routing.ErrUnknownSessionHome) || grpcFailureLooksLike(err, codes.NotFound, "does not encode home node"):
		return string(routing.ReasonUnknownHome)
	case errors.Is(err, routing.ErrSessionHomeMismatch) || grpcFailureLooksLike(err, codes.FailedPrecondition, "belongs to another home node") || grpcFailureLooksLike(err, codes.FailedPrecondition, "home mismatch"):
		return string(routing.ReasonHomeMismatch)
	case errors.Is(err, routing.ErrRouteUnavailable) || status.Code(err) == codes.Unavailable:
		return string(routing.ReasonUnavailable)
	case errors.Is(err, routing.ErrForwardingLoop) || grpcFailureLooksLike(err, codes.FailedPrecondition, string(routing.ReasonForwardingLoop)) || grpcFailureLooksLike(err, codes.FailedPrecondition, "route depth"):
		return string(routing.ReasonForwardingLoop)
	}
	if st, ok := status.FromError(err); ok {
		return st.Code().String()
	}
	return "error"
}

func grpcFailureLooksLike(err error, code codes.Code, fragment string) bool {
	st, ok := status.FromError(err)
	return ok && st.Code() == code && strings.Contains(strings.ToLower(st.Message()), strings.ToLower(fragment))
}

func sanitizeRouteID(id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return ""
	}
	parts := strings.Split(id, ".")
	if len(parts) >= 2 {
		return strings.Join(parts[:2], ".") + ".*"
	}
	if len(id) <= 8 {
		return id
	}
	return id[:8] + "…"
}

func principalToForwarded(p daemonauth.Principal) clusterbackend.ForwardedPrincipal {
	created := ""
	if !p.CreatedAt.IsZero() {
		created = p.CreatedAt.UTC().Format(time.RFC3339Nano)
	}
	return clusterbackend.ForwardedPrincipal{Kind: string(p.Kind), OperatorID: p.OperatorID, UserID: p.UserID, AuthSessionID: p.AuthSessionID, Username: p.Username, CreatedAt: created}
}

func principalFromForwarded(p clusterbackend.ForwardedPrincipal) daemonauth.Principal {
	created := time.Time{}
	if strings.TrimSpace(p.CreatedAt) != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(p.CreatedAt)); err == nil {
			created = parsed
		}
	}
	return daemonauth.Principal{Kind: daemonauth.PrincipalKind(strings.TrimSpace(p.Kind)), OperatorID: strings.TrimSpace(p.OperatorID), UserID: strings.TrimSpace(p.UserID), AuthSessionID: strings.TrimSpace(p.AuthSessionID), Username: strings.TrimSpace(p.Username), CreatedAt: created}
}

func forwardedContext(ctx context.Context, principal clusterbackend.ForwardedPrincipal) context.Context {
	return daemonauth.ContextWithPrincipal(ctx, principalFromForwarded(principal))
}
