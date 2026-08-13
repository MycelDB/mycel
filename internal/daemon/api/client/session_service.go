package client

import (
	"context"
	"errors"
	"time"

	"github.com/myceldb/mycel/internal/clustering/consensus"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	daegraph "github.com/myceldb/mycel/internal/graph/service"
	daemonsession "github.com/myceldb/mycel/internal/session/service"
	daemonspace "github.com/myceldb/mycel/internal/space/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type SessionService struct {
	clientv1.UnimplementedSessionServiceServer
	sessions         daemonsession.Manager
	spaces           daemonspace.Manager
	graphWriteRouter GraphWriteRouteProvider
	router           ClientRequestRouter
}

func NewSessionService(sessions daemonsession.Manager, spaces daemonspace.Manager) *SessionService {
	return &SessionService{sessions: sessions, spaces: spaces}
}

type GraphWriteRouteProvider interface {
	GraphWriteRoute(ctx context.Context, spaceID string) (leader consensus.NodeID, local consensus.NodeID, err error)
}

func (s *SessionService) WithClientRequestRouter(router ClientRequestRouter) *SessionService {
	s.router = router
	return s
}

func (s *SessionService) WithGraphWriteRouteProvider(provider GraphWriteRouteProvider) *SessionService {
	s.graphWriteRouter = provider
	return s
}

func (s *SessionService) OpenSession(ctx context.Context, req *clientv1.OpenSessionRequest) (*clientv1.OpenSessionResponse, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if s.router != nil && s.graphWriteRouter != nil {
		leader, local, err := s.graphWriteRouter.GraphWriteRoute(ctx, req.GetSpaceId())
		if err != nil {
			return nil, mapGraphError(err, "open session graph write route")
		}
		if leader != 0 && local != 0 && leader != local {
			res := &clientv1.OpenSessionResponse{}
			forwarded, err := s.router.ForwardUnaryToNode(ctx, clientv1.SessionService_OpenSession_FullMethodName, leader, "", "", req, res)
			if forwarded || err != nil {
				return res, err
			}
		}
	}
	// Validate that the caller can see the requested domain before minting a daemon session handle.
	if _, err := s.spaces.GetVisibleDomain(ctx, principal.PrincipalID, req.GetSpaceId(), req.GetDomainId(), ""); err != nil {
		return nil, mapSessionError(err, "open session")
	}
	var idle time.Duration
	if req.GetRequestedIdleTimeout() != nil {
		idle = req.GetRequestedIdleTimeout().AsDuration()
	}
	session, err := s.sessions.OpenSession(ctx, daemonsession.OpenSessionInput{PrincipalID: principal.PrincipalID, SpaceID: req.GetSpaceId(), DomainID: req.GetDomainId(), IdleTimeout: idle})
	if err != nil {
		return nil, mapSessionError(err, "open session")
	}
	return &clientv1.OpenSessionResponse{Session: mapGraphSession(session)}, nil
}

func (s *SessionService) GetSession(ctx context.Context, req *clientv1.GetSessionRequest) (*clientv1.GetSessionResponse, error) {
	if s.router != nil {
		res := &clientv1.GetSessionResponse{}
		forwarded, err := s.router.ForwardUnary(ctx, clientv1.SessionService_GetSession_FullMethodName, req.GetSessionId(), "", req, res)
		if forwarded || err != nil {
			return res, err
		}
	}
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	session, err := s.sessions.GetSession(ctx, principal.PrincipalID, req.GetSessionId())
	if err != nil {
		return nil, mapSessionError(err, "get session")
	}
	return &clientv1.GetSessionResponse{Session: mapGraphSession(session)}, nil
}

func (s *SessionService) HeartbeatSession(ctx context.Context, req *clientv1.HeartbeatSessionRequest) (*clientv1.HeartbeatSessionResponse, error) {
	if s.router != nil {
		res := &clientv1.HeartbeatSessionResponse{}
		forwarded, err := s.router.ForwardUnary(ctx, clientv1.SessionService_HeartbeatSession_FullMethodName, req.GetSessionId(), "", req, res)
		if forwarded || err != nil {
			return res, err
		}
	}
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	var extension time.Duration
	if req.GetRequestedExtension() != nil {
		extension = req.GetRequestedExtension().AsDuration()
	}
	session, err := s.sessions.HeartbeatSession(ctx, principal.PrincipalID, req.GetSessionId(), extension)
	if err != nil {
		return nil, mapSessionError(err, "heartbeat session")
	}
	return &clientv1.HeartbeatSessionResponse{Session: mapGraphSession(session)}, nil
}

func (s *SessionService) CloseSession(ctx context.Context, req *clientv1.CloseSessionRequest) (*clientv1.CloseSessionResponse, error) {
	if s.router != nil {
		res := &clientv1.CloseSessionResponse{}
		forwarded, err := s.router.ForwardUnary(ctx, clientv1.SessionService_CloseSession_FullMethodName, req.GetSessionId(), "", req, res)
		if forwarded || err != nil {
			return res, err
		}
	}
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	session, err := s.sessions.CloseSession(ctx, principal.PrincipalID, req.GetSessionId())
	if err != nil {
		return nil, mapSessionError(err, "close session")
	}
	return &clientv1.CloseSessionResponse{Session: mapGraphSession(session)}, nil
}

type TransactionGraphCommitter interface {
	CurrentRevision(ctx context.Context, spaceID string) (int64, error)
	CommitTransactionGraph(ctx context.Context, tx daemonsession.GraphTransaction) (daegraph.CommitResult, error)
	DiscardTransactionGraph(ctx context.Context, transactionID string)
}

type TransactionGraphWriteLeaderChecker interface {
	RequireLocalGraphWriteLeader(ctx context.Context, spaceID string) error
}

type TransactionService struct {
	clientv1.UnimplementedTransactionServiceServer
	sessions daemonsession.Manager
	spaces   daemonspace.Manager
	graphs   TransactionGraphCommitter
	router   ClientRequestRouter
}

func NewTransactionService(sessions daemonsession.Manager, args ...any) *TransactionService {
	service := &TransactionService{sessions: sessions}
	for _, arg := range args {
		if graphCommitter, ok := arg.(TransactionGraphCommitter); ok {
			service.graphs = graphCommitter
		}
		if spaces, ok := arg.(daemonspace.Manager); ok {
			service.spaces = spaces
		}
	}
	return service
}

func (s *TransactionService) WithClientRequestRouter(router ClientRequestRouter) *TransactionService {
	s.router = router
	return s
}

func (s *TransactionService) BeginTransaction(ctx context.Context, req *clientv1.BeginTransactionRequest) (*clientv1.BeginTransactionResponse, error) {
	if s.router != nil {
		res := &clientv1.BeginTransactionResponse{}
		forwarded, err := s.router.ForwardUnary(ctx, clientv1.TransactionService_BeginTransaction_FullMethodName, req.GetSessionId(), "", req, res)
		if forwarded || err != nil {
			return res, err
		}
	}
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	input := daemonsession.BeginTransactionInput{PrincipalID: principal.PrincipalID, SessionID: req.GetSessionId(), Mode: transactionModeFromProto(req.GetMode()), Origin: graphchange.OriginMetadata{OperationID: req.GetOperationId()}}
	var session daemonsession.GraphSession
	if s.graphs != nil || s.spaces != nil {
		var err error
		session, err = s.sessions.GetSession(ctx, principal.PrincipalID, req.GetSessionId())
		if err != nil {
			return nil, mapSessionError(err, "begin transaction")
		}
	}
	if s.spaces != nil {
		domain, err := s.spaces.GetVisibleDomain(ctx, principal.PrincipalID, session.SpaceID, session.DomainID, "")
		if err != nil {
			return nil, mapDomainError(err, "begin transaction domain")
		}
		if domain.ReadOnly && input.Mode != daemonsession.TransactionModeReadOnly {
			return nil, status.Error(codes.FailedPrecondition, "domain is read-only")
		}
	}
	if input.Mode == daemonsession.TransactionModeReadWrite && s.graphs != nil {
		if router, ok := s.graphs.(GraphWriteRouteProvider); ok {
			if _, _, err := router.GraphWriteRoute(ctx, session.SpaceID); err != nil {
				return nil, mapGraphError(err, "begin transaction write route")
			}
		} else if checker, ok := s.graphs.(TransactionGraphWriteLeaderChecker); ok {
			if err := checker.RequireLocalGraphWriteLeader(ctx, session.SpaceID); err != nil {
				return nil, mapGraphError(err, "begin transaction write route")
			}
		}
	}
	if s.graphs != nil {
		baseRevision, err := s.graphs.CurrentRevision(ctx, session.SpaceID)
		if err != nil {
			return nil, mapGraphError(err, "begin transaction revision")
		}
		input.BaseRevision = &baseRevision
	}
	tx, err := s.sessions.BeginTransaction(ctx, input)
	if err != nil {
		return nil, mapSessionError(err, "begin transaction")
	}
	return &clientv1.BeginTransactionResponse{Transaction: mapGraphTransaction(tx)}, nil
}

func (s *TransactionService) GetTransaction(ctx context.Context, req *clientv1.GetTransactionRequest) (*clientv1.GetTransactionResponse, error) {
	if s.router != nil {
		res := &clientv1.GetTransactionResponse{}
		forwarded, err := s.router.ForwardUnary(ctx, clientv1.TransactionService_GetTransaction_FullMethodName, "", req.GetTransactionId(), req, res)
		if forwarded || err != nil {
			return res, err
		}
	}
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := s.sessions.GetTransaction(ctx, principal.PrincipalID, req.GetTransactionId())
	if err != nil {
		return nil, mapSessionError(err, "get transaction")
	}
	return &clientv1.GetTransactionResponse{Transaction: mapGraphTransaction(tx)}, nil
}

func (s *TransactionService) CommitTransaction(ctx context.Context, req *clientv1.CommitTransactionRequest) (*clientv1.CommitTransactionResponse, error) {
	if s.router != nil {
		res := &clientv1.CommitTransactionResponse{}
		forwarded, err := s.router.ForwardUnary(ctx, clientv1.TransactionService_CommitTransaction_FullMethodName, "", req.GetTransactionId(), req, res)
		if forwarded || err != nil {
			return res, err
		}
	}
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := s.sessions.GetTransaction(ctx, principal.PrincipalID, req.GetTransactionId())
	if err != nil {
		return nil, mapSessionError(err, "commit transaction")
	}
	var graphCommit daegraph.CommitResult
	if s.graphs != nil {
		graphCommit, err = s.graphs.CommitTransactionGraph(ctx, tx)
		if err != nil {
			return nil, mapGraphError(err, "commit graph transaction")
		}
	}
	commitCtx := context.WithoutCancel(ctx)
	var commit daemonsession.TransactionCommit
	if graphCommit.CommittedRevision > 0 {
		commit, err = s.sessions.CommitTransactionAtRevision(commitCtx, principal.PrincipalID, req.GetTransactionId(), graphCommit.OperationCount, graphCommit.CommittedRevision)
	} else {
		commit, err = s.sessions.CommitTransaction(commitCtx, principal.PrincipalID, req.GetTransactionId(), graphCommit.OperationCount)
	}
	if err != nil {
		return nil, mapSessionError(err, "commit transaction")
	}
	return &clientv1.CommitTransactionResponse{Commit: mapTransactionCommit(commit)}, nil
}

func (s *TransactionService) RollbackTransaction(ctx context.Context, req *clientv1.RollbackTransactionRequest) (*clientv1.RollbackTransactionResponse, error) {
	if s.router != nil {
		res := &clientv1.RollbackTransactionResponse{}
		forwarded, err := s.router.ForwardUnary(ctx, clientv1.TransactionService_RollbackTransaction_FullMethodName, "", req.GetTransactionId(), req, res)
		if forwarded || err != nil {
			return res, err
		}
	}
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := s.sessions.RollbackTransaction(ctx, principal.PrincipalID, req.GetTransactionId())
	if err == nil && s.graphs != nil {
		s.graphs.DiscardTransactionGraph(ctx, req.GetTransactionId())
	}
	if err != nil {
		return nil, mapSessionError(err, "rollback transaction")
	}
	return &clientv1.RollbackTransactionResponse{Transaction: mapGraphTransaction(tx)}, nil
}

func (s *TransactionService) CloseTransaction(ctx context.Context, req *clientv1.CloseTransactionRequest) (*clientv1.CloseTransactionResponse, error) {
	if s.router != nil {
		res := &clientv1.CloseTransactionResponse{}
		forwarded, err := s.router.ForwardUnary(ctx, clientv1.TransactionService_CloseTransaction_FullMethodName, "", req.GetTransactionId(), req, res)
		if forwarded || err != nil {
			return res, err
		}
	}
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := s.sessions.CloseTransaction(ctx, principal.PrincipalID, req.GetTransactionId())
	if err == nil && s.graphs != nil {
		s.graphs.DiscardTransactionGraph(ctx, req.GetTransactionId())
	}
	if err != nil {
		return nil, mapSessionError(err, "close transaction")
	}
	return &clientv1.CloseTransactionResponse{Transaction: mapGraphTransaction(tx)}, nil
}

func mapGraphSession(session daemonsession.GraphSession) *clientv1.GraphSession {
	return &clientv1.GraphSession{SessionId: session.ID, SpaceId: session.SpaceID, DomainId: session.DomainID, State: sessionStateToProto(session.State), CreateTime: timestamppb.New(session.CreatedAt), LastSeenTime: timestamppb.New(session.LastSeen), ExpireTime: timestamppb.New(session.ExpiresAt)}
}

func mapGraphTransaction(tx daemonsession.GraphTransaction) *clientv1.GraphTransaction {
	return &clientv1.GraphTransaction{TransactionId: tx.ID, SessionId: tx.SessionID, SpaceId: tx.SpaceID, DomainId: tx.DomainID, Mode: transactionModeToProto(tx.Mode), State: transactionStateToProto(tx.State), BaseRevision: tx.BaseRevision, CreateTime: timestamppb.New(tx.CreatedAt), LastSeenTime: timestamppb.New(tx.LastSeen), ExpireTime: timestamppb.New(tx.ExpiresAt), OperationId: tx.Origin.OperationID}
}

func mapTransactionCommit(commit daemonsession.TransactionCommit) *clientv1.TransactionCommit {
	return &clientv1.TransactionCommit{CommitId: commit.ID, TransactionId: commit.TransactionID, SessionId: commit.SessionID, SpaceId: commit.SpaceID, DomainId: commit.DomainID, BaseRevision: commit.BaseRevision, CommittedRevision: commit.CommittedRevision, OperationCount: commit.OperationCount, CommitTime: timestamppb.New(commit.CommittedAt), OperationId: commit.Origin.OperationID}
}

func sessionStateToProto(state daemonsession.SessionState) clientv1.SessionState {
	switch state {
	case daemonsession.SessionStateClosed:
		return clientv1.SessionState_SESSION_STATE_CLOSED
	case daemonsession.SessionStateExpired:
		return clientv1.SessionState_SESSION_STATE_EXPIRED
	default:
		return clientv1.SessionState_SESSION_STATE_ACTIVE
	}
}

func transactionModeFromProto(mode clientv1.TransactionMode) daemonsession.TransactionMode {
	if mode == clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE {
		return daemonsession.TransactionModeReadWrite
	}
	if mode == clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY {
		return daemonsession.TransactionModeReadOnly
	}
	return ""
}

func transactionModeToProto(mode daemonsession.TransactionMode) clientv1.TransactionMode {
	if mode == daemonsession.TransactionModeReadWrite {
		return clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE
	}
	if mode == daemonsession.TransactionModeReadOnly {
		return clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY
	}
	return clientv1.TransactionMode_TRANSACTION_MODE_UNSPECIFIED
}

func transactionStateToProto(state daemonsession.TransactionState) clientv1.TransactionState {
	switch state {
	case daemonsession.TransactionStateCommitted:
		return clientv1.TransactionState_TRANSACTION_STATE_COMMITTED
	case daemonsession.TransactionStateRolledBack:
		return clientv1.TransactionState_TRANSACTION_STATE_ROLLED_BACK
	case daemonsession.TransactionStateClosed:
		return clientv1.TransactionState_TRANSACTION_STATE_CLOSED
	case daemonsession.TransactionStateExpired:
		return clientv1.TransactionState_TRANSACTION_STATE_EXPIRED
	case daemonsession.TransactionStateAborted:
		return clientv1.TransactionState_TRANSACTION_STATE_ABORTED
	default:
		return clientv1.TransactionState_TRANSACTION_STATE_ACTIVE
	}
}

func mapSessionError(err error, action string) error {
	if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
		return err
	}
	if errors.Is(err, daemonsession.ErrInvalidInput) || errors.Is(err, daemonspace.ErrInvalidInput) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if errors.Is(err, daemonsession.ErrSessionNotFound) || errors.Is(err, daemonsession.ErrTransactionNotFound) || errors.Is(err, daemonspace.ErrSpaceNotFound) {
		return status.Error(codes.NotFound, "session, transaction, space, or domain not found")
	}
	if errors.Is(err, daemonsession.ErrUnauthorized) || errors.Is(err, daemonspace.ErrUnauthorized) {
		return status.Error(codes.PermissionDenied, "session access denied")
	}
	if errors.Is(err, daemonsession.ErrClosed) || errors.Is(err, daemonsession.ErrInvalidState) {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	return status.Errorf(codes.Internal, "%s: %v", action, err)
}
