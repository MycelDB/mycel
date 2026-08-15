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
	adminv1.UnimplementedAdminInferenceProfileServiceServer
	adminv1.UnimplementedAdminInferenceCredentialServiceServer
	adminv1.UnimplementedAdminInferenceGrantServiceServer
	adminv1.UnimplementedAdminInferencePolicyServiceServer
	adminv1.UnimplementedAdminInferenceUsageServiceServer
	semantic   daemonsemantic.Manager
	inference  daemoninference.Manager
	authorizer OperatorAuthorizer
}

func NewAdminInferenceService(semantic daemonsemantic.Manager, inference daemoninference.Manager, authorizer OperatorAuthorizer) *AdminInferenceService {
	return &AdminInferenceService{semantic: semantic, inference: inference, authorizer: authorizer}
}

func (s *AdminInferenceService) beginSemanticMutation(ctx context.Context) (context.Context, func(), error) {
	if s.inference != nil {
		if err := s.inference.RequireLocalWriteAllowed(); err != nil {
			return ctx, nil, mapAdminInferenceError(err, "preflight inference mutation")
		}
	}
	ctx, release, err := s.semantic.BeginMutation(ctx)
	if err != nil {
		return ctx, nil, mapAdminInferenceError(err, "begin semantic mutation")
	}
	return ctx, release, nil
}
