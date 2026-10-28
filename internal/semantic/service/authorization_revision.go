package service

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

// GlobalAuthorizationRevision derives a deterministic revision exclusively from
// authoritative system-Raft semantic state. Derived inference projections are
// intentionally not consulted.
func (m *Module) GlobalAuthorizationRevision(ctx context.Context) (uint64, error) {
	if m == nil || m.globalBase == nil {
		return 0, fmt.Errorf("semantic global authorization state is unavailable")
	}
	endpoints, err := m.globalBase.ListModelEndpoints(ctx)
	if err != nil {
		return 0, err
	}
	models, err := m.globalBase.ListModels(ctx)
	if err != nil {
		return 0, err
	}
	capabilities, err := m.globalBase.ListModelEndpointCapabilities(ctx)
	if err != nil {
		return 0, err
	}
	credentials, err := m.globalBase.ListCredentials(ctx)
	if err != nil {
		return 0, err
	}
	sort.Slice(endpoints, func(i, j int) bool { return endpoints[i].ID.String() < endpoints[j].ID.String() })
	sort.Slice(models, func(i, j int) bool { return models[i].ID.String() < models[j].ID.String() })
	sort.Slice(capabilities, func(i, j int) bool { return capabilities[i].ID.String() < capabilities[j].ID.String() })
	sort.Slice(credentials, func(i, j int) bool { return credentials[i].ID.String() < credentials[j].ID.String() })
	encoded, err := json.Marshal(struct {
		Endpoints    []domainsemantic.ModelEndpoint
		Models       []domainsemantic.InferenceModel
		Capabilities []domainsemantic.ModelEndpointCapability
		Credentials  []domainsemantic.InferenceCredential
	}{endpoints, models, capabilities, credentials})
	if err != nil {
		return 0, err
	}
	return semanticAuthorizationRevision(encoded), nil
}

// SpaceAuthorizationRevision derives a deterministic revision exclusively from
// authoritative partition-Raft semantic state.
func (m *Module) SpaceAuthorizationRevision(ctx context.Context, value string) (uint64, error) {
	parsed, err := uuid.Parse(strings.TrimSpace(value))
	if err != nil || parsed == uuid.Nil {
		return 0, fmt.Errorf("space_id must be a UUID")
	}
	spaceID := domainspace.SpaceID(parsed)
	manager, err := m.baseSpaceManager(ctx, spaceID)
	if err != nil {
		return 0, err
	}
	return spaceAuthorizationRevisionFromManager(ctx, manager)
}

func spaceAuthorizationRevisionFromManager(ctx context.Context, manager interface {
	ListIntelligenceProfiles(context.Context) ([]domainsemantic.IntelligenceProfile, error)
	ListCredentialGrants(context.Context) ([]domainsemantic.CredentialGrant, error)
	ListInferencePolicies(context.Context) ([]domainsemantic.InferencePolicy, error)
}) (uint64, error) {
	profiles, err := manager.ListIntelligenceProfiles(ctx)
	if err != nil {
		return 0, err
	}
	grants, err := manager.ListCredentialGrants(ctx)
	if err != nil {
		return 0, err
	}
	policies, err := manager.ListInferencePolicies(ctx)
	if err != nil {
		return 0, err
	}
	sort.Slice(profiles, func(i, j int) bool { return profiles[i].ID.String() < profiles[j].ID.String() })
	sort.Slice(grants, func(i, j int) bool { return grants[i].ID.String() < grants[j].ID.String() })
	sort.Slice(policies, func(i, j int) bool { return policies[i].ID.String() < policies[j].ID.String() })
	encoded, err := json.Marshal(struct {
		Profiles []domainsemantic.IntelligenceProfile
		Grants   []domainsemantic.CredentialGrant
		Policies []domainsemantic.InferencePolicy
	}{profiles, grants, policies})
	if err != nil {
		return 0, err
	}
	return semanticAuthorizationRevision(encoded), nil
}

func semanticAuthorizationRevision(encoded []byte) uint64 {
	sum := sha256.Sum256(encoded)
	revision := binary.BigEndian.Uint64(sum[:8])
	if revision == 0 {
		return 1
	}
	return revision
}
