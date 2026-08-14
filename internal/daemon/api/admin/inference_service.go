package admin

import (
	"context"

	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	daemonsemantic "github.com/myceldb/mycel/internal/semantic/service"
)

const adminInferenceMaxPageSize = 500

type AdminInferenceService struct {
	adminv1.UnimplementedAdminInferenceCatalogServiceServer
	adminv1.UnimplementedAdminInferenceProfileServiceServer
	adminv1.UnimplementedAdminInferenceCredentialServiceServer
	adminv1.UnimplementedAdminInferenceGrantServiceServer
	adminv1.UnimplementedAdminInferencePolicyServiceServer
	adminv1.UnimplementedAdminInferenceUsageServiceServer
	semantic   daemonsemantic.Manager
	authorizer OperatorAuthorizer
}

func NewAdminInferenceService(semantic daemonsemantic.Manager, authorizer OperatorAuthorizer) *AdminInferenceService {
	return &AdminInferenceService{semantic: semantic, authorizer: authorizer}
}

func (s *AdminInferenceService) beginSemanticMutation(ctx context.Context) (context.Context, func(), error) {
	ctx, release, err := s.semantic.BeginMutation(ctx)
	if err != nil {
		return ctx, nil, mapAdminInferenceError(err, "begin semantic mutation")
	}
	return ctx, release, nil
}
