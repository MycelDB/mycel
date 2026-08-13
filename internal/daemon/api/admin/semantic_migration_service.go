package admin

import (
	"context"

	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	daemonsemantic "github.com/myceldb/mycel/internal/semantic/service"
	daemonspace "github.com/myceldb/mycel/internal/space/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const legacyEmbeddingMigrationClosedMessage = "legacy embedding migration window is closed; configure inference credentials, grants, policies, and semantic indexes directly"

type AdminSemanticMigrationService struct {
	adminv1.UnimplementedAdminSemanticMigrationServiceServer
	authorizer OperatorAuthorizer
}

func NewAdminSemanticMigrationService(_ daemonsemantic.Manager, _ daemonspace.Manager, authorizer OperatorAuthorizer) *AdminSemanticMigrationService {
	return &AdminSemanticMigrationService{authorizer: authorizer}
}

func (s *AdminSemanticMigrationService) MigrateLegacyEmbeddings(ctx context.Context, _ *adminv1.MigrateLegacyEmbeddingsRequest) (*adminv1.MigrateLegacyEmbeddingsResponse, error) {
	if err := s.requireMigration(ctx); err != nil {
		return nil, err
	}
	return nil, status.Error(codes.FailedPrecondition, legacyEmbeddingMigrationClosedMessage)
}

func (s *AdminSemanticMigrationService) requireMigration(ctx context.Context) error {
	principal, err := principalFromContext(ctx)
	if err != nil {
		return err
	}
	ok, err := s.authorizer.HasCapability(ctx, principal.PrincipalID, commonv1.Capability_CAPABILITY_SEMANTIC_SEARCH.String())
	if err != nil {
		return status.Errorf(codes.Internal, "authorize operator: %v", err)
	}
	if !ok {
		return status.Error(codes.PermissionDenied, "operator lacks required semantic migration capability")
	}
	return nil
}
