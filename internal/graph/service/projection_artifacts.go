package service

import (
	"context"
	"sort"

	"github.com/myceldb/mycel/internal/projection"
)

// ProjectionArtifactManifests returns validated local graph artifact evidence
// for backup catalog construction. It deliberately returns manifests only;
// registry checkpoints remain the authoritative publication record.
func (m *Module) ProjectionArtifactManifests(ctx context.Context) ([]projection.ArtifactManifest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	stores := make([]struct {
		key   domainStoreKey
		store projectionArtifactStore
	}, 0, len(m.stores))
	for key, store := range m.stores {
		stores = append(stores, struct {
			key   domainStoreKey
			store projectionArtifactStore
		}{key: key, store: store})
	}
	m.mu.Unlock()
	sort.Slice(stores, func(i, j int) bool {
		if stores[i].key.SpaceID != stores[j].key.SpaceID {
			return stores[i].key.SpaceID < stores[j].key.SpaceID
		}
		return stores[i].key.DomainID < stores[j].key.DomainID
	})
	out := make([]projection.ArtifactManifest, 0)
	for _, item := range stores {
		manifests, err := item.store.ProjectionArtifactManifests(ctx)
		if err != nil {
			return nil, err
		}
		out = append(out, manifests...)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ProjectionID < out[j].ProjectionID })
	return out, nil
}

// projectionArtifactStore is the narrow storage surface needed by backup
// evidence collection. It keeps this service method independent of storage
// implementation details while preserving validation at the storage boundary.
type projectionArtifactStore interface {
	ProjectionArtifactManifests(context.Context) ([]projection.ArtifactManifest, error)
}
