package admin

import (
	"context"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/domain/graph"
	"github.com/myceldb/mycel/domain/identity"
	domainspace "github.com/myceldb/mycel/domain/space"
	adminv1 "github.com/myceldb/mycel/gen/go/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel/gen/go/mycel/common/v1"
	daemonsemantic "github.com/myceldb/mycel/internal/daemon/modules/semantic"
	daemonspace "github.com/myceldb/mycel/internal/daemon/modules/space"
	semanticmigration "github.com/myceldb/mycel/internal/semantic/migration"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type AdminSemanticMigrationService struct {
	adminv1.UnimplementedAdminSemanticMigrationServiceServer
	semantic   daemonsemantic.Manager
	spaces     daemonspace.Manager
	authorizer OperatorAuthorizer
}

func NewAdminSemanticMigrationService(semantic daemonsemantic.Manager, spaces daemonspace.Manager, authorizer OperatorAuthorizer) *AdminSemanticMigrationService {
	return &AdminSemanticMigrationService{semantic: semantic, spaces: spaces, authorizer: authorizer}
}

func (s *AdminSemanticMigrationService) MigrateLegacyEmbeddings(ctx context.Context, req *adminv1.MigrateLegacyEmbeddingsRequest) (*adminv1.MigrateLegacyEmbeddingsResponse, error) {
	if err := s.requireMigration(ctx); err != nil {
		return nil, err
	}
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](req.GetSpaceId(), "space_id")
	if err != nil {
		return nil, err
	}
	domainID, err := parseSemanticUUID[graph.DomainID](req.GetDomainId(), "domain_id")
	if err != nil {
		return nil, err
	}
	ownerID := req.GetOwnerUserId()
	if ownerID == "" {
		sp, err := s.spaces.GetSpace(ctx, spaceID.String())
		if err != nil {
			return nil, mapAdminDomainError(err, "get space")
		}
		ownerID = sp.OwnerID.String()
	}
	if _, err := parseSemanticUUID[identity.UserID](ownerID, "owner_user_id"); err != nil {
		return nil, err
	}
	res, err := s.semantic.MigrateLegacyEmbeddings(ctx, daemonsemantic.LegacyMigrationInput{OwnerUserID: ownerID, SpaceID: spaceID, DomainID: domainID, ProfileRef: req.GetProfileRef(), AllowBackgroundUse: req.GetAllowBackgroundUse(), AddAllowPolicy: req.GetAddAllowPolicy(), Strict: req.GetStrict(), DryRun: req.GetDryRun(), Limit: int(req.GetLimit())})
	if err != nil {
		return nil, mapAdminSemanticMigrationError(err, "migrate legacy embeddings")
	}
	return mapLegacyMigrationResponse(res), nil
}

func (s *AdminSemanticMigrationService) requireMigration(ctx context.Context) error {
	principal, err := principalFromContext(ctx)
	if err != nil {
		return err
	}
	ok, err := s.authorizer.HasCapability(ctx, principal.OperatorID, commonv1.Capability_CAPABILITY_SEMANTIC_SEARCH.String())
	if err != nil {
		return status.Errorf(codes.Internal, "authorize operator: %v", err)
	}
	if !ok {
		return status.Error(codes.PermissionDenied, "operator lacks required semantic migration capability")
	}
	return nil
}

func mapLegacyMigrationResponse(res semanticmigration.LegacyEmbeddingResult) *adminv1.MigrateLegacyEmbeddingsResponse {
	out := &adminv1.MigrateLegacyEmbeddingsResponse{ProfilesSeen: int32(res.ProfilesSeen), ProfilesMigrated: int32(res.ProfilesMigrated), ProfilesSkipped: int32(res.ProfilesSkipped), DryRun: res.DryRun, Warnings: append([]string(nil), res.Warnings...)}
	for _, id := range res.EndpointIDs {
		out.EndpointIds = append(out.EndpointIds, uuid.UUID(id).String())
	}
	for _, id := range res.ModelIDs {
		out.ModelIds = append(out.ModelIds, uuid.UUID(id).String())
	}
	for _, id := range res.CredentialIDs {
		out.CredentialIds = append(out.CredentialIds, uuid.UUID(id).String())
	}
	for _, id := range res.SemanticIndexIDs {
		out.SemanticIndexIds = append(out.SemanticIndexIds, uuid.UUID(id).String())
	}
	for _, id := range res.CredentialGrantIDs {
		out.CredentialGrantIds = append(out.CredentialGrantIds, uuid.UUID(id).String())
	}
	for _, id := range res.PolicyIDs {
		out.PolicyIds = append(out.PolicyIds, uuid.UUID(id).String())
	}
	return out
}

func mapAdminSemanticMigrationError(err error, action string) error {
	if err == nil {
		return nil
	}
	return mapAdminInferenceError(err, action)
}
