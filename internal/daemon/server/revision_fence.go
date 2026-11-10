package server

import (
	"context"

	daegraph "github.com/myceldb/mycel/internal/graph/service"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
	daemonspace "github.com/myceldb/mycel/internal/space/service"
)

type revisionFenceDependencyValidator struct {
	spaces daemonspace.Manager
}

func (v revisionFenceDependencyValidator) ValidateRevisionFenceDependency(ctx context.Context, tx sessionmodel.GraphTransaction, dependency daegraph.RevisionDependency) error {
	provider, ok := v.spaces.(daemonspace.ReadRevisionProvider)
	if !ok {
		return daegraph.ErrInvalidInput
	}
	authorizationRevision, visibilityRevision, err := provider.ReadVisibilityRevisions(ctx, tx.PrincipalID, tx.SpaceID, tx.DomainID)
	if err != nil {
		return err
	}
	switch dependency.Kind {
	case "authorization":
		if dependency.ID != tx.PrincipalID {
			return daegraph.ErrInvalidInput
		}
		if authorizationRevision != dependency.Revision {
			return daegraph.ErrRevisionFenceConflict
		}
	case "visibility":
		if dependency.ID != tx.DomainID {
			return daegraph.ErrInvalidInput
		}
		if visibilityRevision != dependency.Revision {
			return daegraph.ErrRevisionFenceConflict
		}
	default:
		return daegraph.ErrInvalidInput
	}
	return nil
}
