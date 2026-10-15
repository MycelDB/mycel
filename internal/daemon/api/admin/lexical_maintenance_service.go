package admin

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	graph "github.com/myceldb/mycel/internal/graph/model"
	daegraph "github.com/myceldb/mycel/internal/graph/service"
	principalservice "github.com/myceldb/mycel/internal/identity/service/principal"
	"github.com/myceldb/mycel/internal/search/lexical/analyzer"
	lexicalindex "github.com/myceldb/mycel/internal/search/lexical/index"
	lexicalservice "github.com/myceldb/mycel/internal/search/lexical/service"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	daemonspace "github.com/myceldb/mycel/internal/space/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type AdminLexicalMaintenanceService struct {
	adminv1.UnimplementedAdminLexicalMaintenanceServiceServer
	lexical    lexicalservice.Manager
	graphs     daegraph.Manager
	spaces     daemonspace.Manager
	authorizer OperatorAuthorizer
}

type lexicalSnapshotRebuilder interface {
	RebuildFromSnapshot(ctx context.Context, spaceID string, domainID string, snapshot lexicalservice.RebuildSnapshot) error
}

func NewAdminLexicalMaintenanceService(lexical lexicalservice.Manager, graphs daegraph.Manager, spaces daemonspace.Manager, authorizer OperatorAuthorizer) *AdminLexicalMaintenanceService {
	return &AdminLexicalMaintenanceService{lexical: lexical, graphs: graphs, spaces: spaces, authorizer: authorizer}
}

func (s *AdminLexicalMaintenanceService) GetLexicalMaintenanceStatus(ctx context.Context, req *adminv1.GetLexicalMaintenanceStatusRequest) (*adminv1.GetLexicalMaintenanceStatusResponse, error) {
	spaceID, domainID, err := s.requireMaintenance(ctx, req.GetSpaceId(), req.GetDomainId())
	if err != nil {
		return nil, err
	}
	st, err := s.lexicalStatus(ctx, spaceID, domainID)
	if err != nil {
		return nil, mapAdminLexicalError(err, "read graph revision for lexical status")
	}
	return &adminv1.GetLexicalMaintenanceStatusResponse{Status: mapAdminLexicalStatus(st), Owner: true, CursorSource: "local", Warnings: maintenanceWarnings(st)}, nil
}

func (s *AdminLexicalMaintenanceService) RebuildLexicalIndex(ctx context.Context, req *adminv1.RebuildLexicalIndexRequest) (*adminv1.RebuildLexicalIndexResponse, error) {
	spaceID, domainID, err := s.requireMaintenance(ctx, req.GetSpaceId(), req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if req.GetDryRun() {
		return &adminv1.RebuildLexicalIndexResponse{SpaceId: spaceID.String(), DomainId: domainID.String(), State: adminv1.LexicalRebuildState_LEXICAL_REBUILD_STATE_QUEUED, Accepted: true, DryRun: true, Warnings: []string{"dry run; rebuild not started"}}, nil
	}
	rebuilder, ok := s.lexical.(lexicalSnapshotRebuilder)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "serialized lexical rebuild is not configured")
	}
	if err := rebuilder.RebuildFromSnapshot(ctx, spaceID.String(), domainID.String(), func() ([]lexicalindex.IndexedDocument, uint64, error) {
		docs, latest, err := s.collectDocuments(ctx, spaceID, domainID)
		if err != nil {
			return nil, 0, err
		}
		lexicalservice.SortDocumentsByNodeID(docs)
		return docs, latest, nil
	}); err != nil {
		return nil, mapAdminLexicalError(err, "rebuild lexical index")
	}
	return &adminv1.RebuildLexicalIndexResponse{SpaceId: spaceID.String(), DomainId: domainID.String(), State: adminv1.LexicalRebuildState_LEXICAL_REBUILD_STATE_RUNNING, Accepted: true, RebuildId: uuid.NewString()}, nil
}

func (s *AdminLexicalMaintenanceService) requireMaintenance(ctx context.Context, spaceIDText string, domainIDText string) (domainspace.SpaceID, graph.DomainID, error) {
	principal, err := principalFromContext(ctx)
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](spaceIDText, "space_id")
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	domainID, err := parseSemanticUUID[graph.DomainID](domainIDText, "domain_id")
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	if s.spaces != nil {
		if _, err := s.spaces.GetVisibleDomain(ctx, principal.PrincipalID, spaceID.String(), domainID.String(), ""); err != nil {
			return uuid.Nil, uuid.Nil, mapAdminLexicalError(err, "authorize lexical domain")
		}
	}
	scope := principalservice.AccessScope{Type: "domain", SpaceID: spaceID.String(), DomainID: domainID.String()}
	capability := commonv1.Capability_CAPABILITY_SYSTEM_MAINTAIN_SPACE.String()
	if scoped, ok := s.authorizer.(ScopedOperatorAuthorizer); ok {
		if err := scoped.Authorize(ctx, principal.PrincipalID, capability, scope); err != nil {
			return uuid.Nil, uuid.Nil, err
		}
		return spaceID, domainID, nil
	}
	ok, err := s.authorizer.HasCapability(ctx, principal.PrincipalID, capability)
	if err != nil {
		return uuid.Nil, uuid.Nil, status.Errorf(codes.Internal, "authorize operator: %v", err)
	}
	if !ok {
		return uuid.Nil, uuid.Nil, status.Error(codes.PermissionDenied, "operator lacks required lexical maintenance capability")
	}
	return spaceID, domainID, nil
}

func (s *AdminLexicalMaintenanceService) collectDocuments(ctx context.Context, spaceID domainspace.SpaceID, domainID graph.DomainID) ([]lexicalindex.IndexedDocument, uint64, error) {
	if s.graphs == nil {
		return nil, 0, status.Error(codes.FailedPrecondition, "graph service is not configured")
	}
	principal, _ := principalFromContext(ctx)
	latest, err := s.latestGraphRevision(ctx, spaceID, domainID)
	if err != nil {
		return nil, 0, mapAdminLexicalError(err, "read graph revision for lexical rebuild")
	}
	tx := sessionmodel.GraphTransaction{ID: "lexical-rebuild-" + uuid.NewString(), PrincipalID: principal.PrincipalID, SpaceID: spaceID.String(), DomainID: domainID.String(), Mode: sessionmodel.TransactionModeReadOnly, State: sessionmodel.TransactionStateActive, BaseRevision: int64(latest), CreatedAt: time.Now().UTC(), LastSeen: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Minute)}
	var out []lexicalindex.IndexedDocument
	pageToken := ""
	for {
		nodes, next, err := s.graphs.ListNodes(ctx, tx, 500, pageToken)
		if err != nil {
			return nil, 0, mapAdminLexicalError(err, "list graph nodes for lexical rebuild")
		}
		for _, node := range nodes {
			out = append(out, lexicalindex.IndexedDocument{Document: analyzer.ExtractNodeDocument(node), GraphRevision: latest})
		}
		if next == "" {
			break
		}
		pageToken = next
	}
	return out, latest, nil
}

type lexicalGraphDomainRevisionReader interface {
	CurrentDomainRevision(ctx context.Context, spaceID string, domainID string) (int64, error)
}

func (s *AdminLexicalMaintenanceService) lexicalStatus(ctx context.Context, spaceID domainspace.SpaceID, domainID graph.DomainID) (lexicalservice.Status, error) {
	latest, err := s.latestGraphRevision(ctx, spaceID, domainID)
	if err != nil {
		return lexicalservice.Status{}, err
	}
	maintenance := s.lexical.Status(ctx, spaceID.String(), domainID.String(), latest)
	if maintenance.IndexedGraphRevision <= latest {
		return maintenance, nil
	}
	latestAfterCursor, err := s.latestGraphRevision(ctx, spaceID, domainID)
	if err != nil {
		return lexicalservice.Status{}, err
	}
	return lexicalservice.ReconcileStatusLatest(maintenance, latestAfterCursor), nil
}

func (s *AdminLexicalMaintenanceService) latestGraphRevision(ctx context.Context, spaceID domainspace.SpaceID, domainID graph.DomainID) (uint64, error) {
	if s.graphs == nil {
		return 0, nil
	}
	var (
		rev int64
		err error
	)
	if revisions, ok := s.graphs.(lexicalGraphDomainRevisionReader); ok {
		rev, err = revisions.CurrentDomainRevision(ctx, spaceID.String(), domainID.String())
	} else {
		rev, err = s.graphs.CurrentRevision(ctx, spaceID.String())
	}
	if err != nil {
		return 0, err
	}
	if rev < 0 {
		return 0, fmt.Errorf("graph revision is negative: %d", rev)
	}
	return uint64(rev), nil
}

func mapAdminLexicalStatus(in lexicalservice.Status) *clientv1.LexicalIndexStatus {
	return &clientv1.LexicalIndexStatus{SpaceId: in.SpaceID, DomainId: in.DomainID, State: mapAdminLexicalState(in.State), IndexedGraphRevision: int64(in.IndexedGraphRevision), LatestKnownGraphRevision: int64(in.LatestKnownGraphRevision), RevisionLag: int64(in.RevisionLag), LiveDocumentCount: int64(in.LiveDocumentCount), DeletedDocumentCount: int64(in.DeletedDocumentCount), SegmentCount: int32(in.SegmentCount), AnalyzerVersion: in.AnalyzerVersion, IndexFormatVersion: in.IndexFormatVersion, LastError: in.LastError}
}

func mapAdminLexicalState(state lexicalservice.State) clientv1.SearchFreshnessState {
	switch state {
	case lexicalservice.StateFresh:
		return clientv1.SearchFreshnessState_SEARCH_FRESHNESS_STATE_FRESH
	case lexicalservice.StateStale:
		return clientv1.SearchFreshnessState_SEARCH_FRESHNESS_STATE_STALE
	case lexicalservice.StateRebuilding:
		return clientv1.SearchFreshnessState_SEARCH_FRESHNESS_STATE_REBUILDING
	case lexicalservice.StateUnavailable:
		return clientv1.SearchFreshnessState_SEARCH_FRESHNESS_STATE_UNAVAILABLE
	case lexicalservice.StateError:
		return clientv1.SearchFreshnessState_SEARCH_FRESHNESS_STATE_ERROR
	default:
		return clientv1.SearchFreshnessState_SEARCH_FRESHNESS_STATE_UNSPECIFIED
	}
}

func maintenanceWarnings(st lexicalservice.Status) []string {
	if st.State == lexicalservice.StateStale {
		return []string{fmt.Sprintf("lexical index is stale by %d graph revisions", st.RevisionLag)}
	}
	if st.State == lexicalservice.StateUnavailable {
		return []string{"lexical index is unavailable; run rebuild"}
	}
	return nil
}

func mapAdminLexicalError(err error, action string) error {
	if err == nil {
		return nil
	}
	if status.Code(err) != codes.OK {
		return err
	}
	return status.Errorf(codes.Internal, "%s: %v", action, err)
}
