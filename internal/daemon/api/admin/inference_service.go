package admin

import (
	"context"

	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	daemoninference "github.com/myceldb/mycel/internal/inference/service"
	daemonsemantic "github.com/myceldb/mycel/internal/semantic/service"
)

const adminInferenceMaxPageSize = 500

type AdminInferenceService struct {
	adminv1.UnimplementedAdminInferenceCatalogServiceServer
	adminv1.UnimplementedAdminIntelligenceAccessProfileServiceServer
	adminv1.UnimplementedAdminIntelligenceAccessCredentialServiceServer
	adminv1.UnimplementedAdminIntelligenceAccessGrantServiceServer
	adminv1.UnimplementedAdminIntelligenceAccessPolicyServiceServer
	adminv1.UnimplementedAdminIntelligenceAccessUsageServiceServer
	semantic   daemonsemantic.Manager
	inference  daemoninference.Manager
	authorizer OperatorAuthorizer
}

func NewAdminInferenceService(semantic daemonsemantic.Manager, inference daemoninference.Manager, authorizer OperatorAuthorizer) *AdminInferenceService {
	return &AdminInferenceService{semantic: semantic, inference: inference, authorizer: authorizer}
}

func (s *AdminInferenceService) beginSemanticMutation(ctx context.Context) (context.Context, func(), error) {
	ctx, release, err := s.semantic.BeginMutation(ctx)
	if err != nil {
		return ctx, nil, mapAdminInferenceError(err, "begin semantic mutation")
	}
	return ctx, release, nil
}
