package admin

import (
	"context"
	"sort"
	"strings"

	"github.com/google/uuid"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Profile RPC handlers. Intelligence profiles are semantic-authoritative,
// space-scoped configuration so clustered mode commits them through semantic
// partition Raft. The standalone inference store is a derived runtime
// projection used by request resolution.

func (s *AdminInferenceService) CreateIntelligenceProfile(ctx context.Context, req *adminv1.AdminIntelligenceAccessProfileServiceCreateIntelligenceProfileRequest) (*adminv1.AdminIntelligenceAccessProfileServiceCreateIntelligenceProfileResponse, error) {
	principal, err := s.requireInferenceCapability(ctx, capIntelligenceProfileManage, inferenceScope(req.GetSpaceId(), ""))
	if err != nil {
		return nil, err
	}
	profile, err := semanticProfileFromProto(req, principal.PrincipalID)
	if err != nil {
		return nil, err
	}
	if s.semantic == nil {
		return nil, status.Error(codes.FailedPrecondition, "semantic subsystem is not configured")
	}
	ctx, release, err := s.beginSemanticMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	mgr, err := s.semantic.SpaceManager(ctx, profile.SpaceID)
	if err != nil {
		return nil, mapAdminInferenceError(err, "open semantic space manager")
	}
	stored, err := mgr.UpsertIntelligenceProfile(ctx, profile)
	if err != nil {
		return nil, mapAdminInferenceError(err, "upsert intelligence profile")
	}
	if err := s.syncIntelligenceProfile(ctx, stored); err != nil {
		return nil, mapAdminInferenceError(err, "sync intelligence profile")
	}
	return &adminv1.AdminIntelligenceAccessProfileServiceCreateIntelligenceProfileResponse{IntelligenceProfile: mapSemanticIntelligenceProfile(stored)}, nil
}

func (s *AdminInferenceService) ListIntelligenceProfiles(ctx context.Context, req *adminv1.AdminIntelligenceAccessProfileServiceListIntelligenceProfilesRequest) (*adminv1.AdminIntelligenceAccessProfileServiceListIntelligenceProfilesResponse, error) {
	spaceIDText := strings.TrimSpace(req.GetSpaceId())
	if _, err := s.requireInferenceCapability(ctx, capIntelligenceProfileRead, inferenceScope(spaceIDText, req.GetDomainId())); err != nil {
		return nil, err
	}
	if spaceIDText == "" {
		return nil, status.Error(codes.InvalidArgument, "space_id is required")
	}
	if s.semantic == nil {
		return s.listStandaloneIntelligenceProfiles(ctx, req)
	}
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](spaceIDText, "space_id")
	if err != nil {
		return nil, err
	}
	mgr, err := s.semantic.SpaceManager(ctx, spaceID)
	if err != nil {
		return nil, mapAdminInferenceError(err, "open semantic space manager")
	}
	items, err := mgr.ListIntelligenceProfiles(ctx)
	if err != nil {
		return nil, mapAdminInferenceError(err, "list intelligence profiles")
	}
	if strings.TrimSpace(req.GetDomainId()) != "" {
		items = filterSemanticProfilesByDomain(items, req.GetDomainId())
	}
	if op := semanticOperationFromProto(req.GetOperation()); op != "" {
		items = filterSemanticProfilesByOperation(items, op)
	}
	if strings.TrimSpace(req.GetPurpose()) != "" {
		items = filterSemanticProfilesByPurpose(items, req.GetPurpose())
	}
	if !req.GetIncludeDisabled() {
		items = filterSemanticProfilesEnabled(items)
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].Key < items[j].Key })
	page, next, err := paginateAdminInference(items, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &adminv1.AdminIntelligenceAccessProfileServiceListIntelligenceProfilesResponse{IntelligenceProfiles: mapSemanticIntelligenceProfiles(page), NextPageToken: next}, nil
}

func (s *AdminInferenceService) GetIntelligenceProfile(ctx context.Context, req *adminv1.AdminIntelligenceAccessProfileServiceGetIntelligenceProfileRequest) (*adminv1.AdminIntelligenceAccessProfileServiceGetIntelligenceProfileResponse, error) {
	if _, err := s.requireInferenceCapability(ctx, capIntelligenceProfileRead, inferenceScope(req.GetSpaceId(), "")); err != nil {
		return nil, err
	}
	profile, err := s.resolveSemanticIntelligenceProfile(ctx, req.GetSpaceId(), firstNonEmptyAdmin(req.GetIntelligenceProfileId(), req.GetIntelligenceProfile()))
	if err != nil {
		return nil, err
	}
	return &adminv1.AdminIntelligenceAccessProfileServiceGetIntelligenceProfileResponse{IntelligenceProfile: mapSemanticIntelligenceProfile(profile)}, nil
}

func (s *AdminInferenceService) SetIntelligenceProfileEnabled(ctx context.Context, req *adminv1.AdminIntelligenceAccessProfileServiceSetIntelligenceProfileEnabledRequest) (*adminv1.AdminIntelligenceAccessProfileServiceSetIntelligenceProfileEnabledResponse, error) {
	if _, err := s.requireInferenceCapability(ctx, capIntelligenceProfileManage, inferenceScope(req.GetSpaceId(), "")); err != nil {
		return nil, err
	}
	profile, err := s.resolveSemanticIntelligenceProfile(ctx, req.GetSpaceId(), firstNonEmptyAdmin(req.GetIntelligenceProfileId(), req.GetIntelligenceProfile()))
	if err != nil {
		return nil, err
	}
	if s.semantic == nil {
		return nil, status.Error(codes.FailedPrecondition, "semantic subsystem is not configured")
	}
	ctx, release, err := s.beginSemanticMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	mgr, err := s.semantic.SpaceManager(ctx, profile.SpaceID)
	if err != nil {
		return nil, mapAdminInferenceError(err, "open semantic space manager")
	}
	profile.Enabled = req.GetEnabled()
	stored, err := mgr.UpsertIntelligenceProfile(ctx, profile)
	if err != nil {
		return nil, mapAdminInferenceError(err, "update intelligence profile")
	}
	if err := s.syncIntelligenceProfile(ctx, stored); err != nil {
		return nil, mapAdminInferenceError(err, "sync intelligence profile")
	}
	return &adminv1.AdminIntelligenceAccessProfileServiceSetIntelligenceProfileEnabledResponse{IntelligenceProfile: mapSemanticIntelligenceProfile(stored)}, nil
}

func (s *AdminInferenceService) DeleteIntelligenceProfile(ctx context.Context, req *adminv1.AdminIntelligenceAccessProfileServiceDeleteIntelligenceProfileRequest) (*adminv1.AdminIntelligenceAccessProfileServiceDeleteIntelligenceProfileResponse, error) {
	if _, err := s.requireInferenceCapability(ctx, capIntelligenceProfileManage, inferenceScope(req.GetSpaceId(), "")); err != nil {
		return nil, err
	}
	profile, err := s.resolveSemanticIntelligenceProfile(ctx, req.GetSpaceId(), firstNonEmptyAdmin(req.GetIntelligenceProfileId(), req.GetIntelligenceProfile()))
	if err != nil {
		return nil, err
	}
	refs, err := s.profileReferences(ctx, semanticProfileToInference(profile))
	if err != nil {
		return nil, err
	}
	if len(refs) > 0 {
		return nil, referencedPrecondition("inference profile", refs)
	}
	if s.semantic == nil {
		mgr, err := s.inference.SpaceManager(ctx, profile.SpaceID.String())
		if err != nil {
			return nil, mapAdminInferenceError(err, "open inference space manager")
		}
		if err := mgr.DeleteProfile(ctx, domaininference.ProfileID(profile.ID)); err != nil {
			return nil, mapAdminInferenceError(err, "delete inference profile")
		}
		return &adminv1.AdminIntelligenceAccessProfileServiceDeleteIntelligenceProfileResponse{IntelligenceProfileId: profile.ID.String()}, nil
	}
	ctx, release, err := s.beginSemanticMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	mgr, err := s.semantic.SpaceManager(ctx, profile.SpaceID)
	if err != nil {
		return nil, mapAdminInferenceError(err, "open semantic space manager")
	}
	if err := mgr.DeleteIntelligenceProfile(ctx, profile.ID); err != nil {
		return nil, mapAdminInferenceError(err, "delete intelligence profile")
	}
	if err := s.deleteSyncedIntelligenceProfile(ctx, profile.SpaceID.String(), profile.ID); err != nil {
		return nil, mapAdminInferenceError(err, "delete synced intelligence profile")
	}
	return &adminv1.AdminIntelligenceAccessProfileServiceDeleteIntelligenceProfileResponse{IntelligenceProfileId: profile.ID.String()}, nil
}

func (s *AdminInferenceService) resolveIntelligenceProfile(ctx context.Context, spaceID string, ref string) (domaininference.Profile, error) {
	profile, err := s.resolveSemanticIntelligenceProfile(ctx, spaceID, ref)
	if err != nil {
		return domaininference.Profile{}, err
	}
	return semanticProfileToInference(profile), nil
}

func (s *AdminInferenceService) resolveSemanticIntelligenceProfile(ctx context.Context, spaceID string, ref string) (domainsemantic.IntelligenceProfile, error) {
	if s.semantic == nil {
		profile, err := s.resolveStandaloneIntelligenceProfile(ctx, spaceID, ref)
		if err != nil {
			return domainsemantic.IntelligenceProfile{}, err
		}
		return semanticProfileFromInference(profile), nil
	}
	spaceID = strings.TrimSpace(spaceID)
	if spaceID == "" {
		return domainsemantic.IntelligenceProfile{}, status.Error(codes.InvalidArgument, "space_id is required")
	}
	parsed, err := parseSemanticUUID[domainspace.SpaceID](spaceID, "space_id")
	if err != nil {
		return domainsemantic.IntelligenceProfile{}, err
	}
	mgr, err := s.semantic.SpaceManager(ctx, parsed)
	if err != nil {
		return domainsemantic.IntelligenceProfile{}, mapAdminInferenceError(err, "open semantic space manager")
	}
	items, err := mgr.ListIntelligenceProfiles(ctx)
	if err != nil {
		return domainsemantic.IntelligenceProfile{}, mapAdminInferenceError(err, "list intelligence profiles")
	}
	if id, err := uuid.Parse(strings.TrimSpace(ref)); err == nil && id != uuid.Nil {
		for _, item := range items {
			if item.ID == id {
				return item, nil
			}
		}
		return domainsemantic.IntelligenceProfile{}, status.Error(codes.NotFound, "inference profile not found")
	}
	key := strings.ToLower(strings.TrimSpace(ref))
	for _, item := range items {
		if strings.ToLower(strings.TrimSpace(item.Key)) == key {
			return item, nil
		}
	}
	return domainsemantic.IntelligenceProfile{}, status.Errorf(codes.NotFound, "inference profile %q not found", ref)
}

func (s *AdminInferenceService) listStandaloneIntelligenceProfiles(ctx context.Context, req *adminv1.AdminIntelligenceAccessProfileServiceListIntelligenceProfilesRequest) (*adminv1.AdminIntelligenceAccessProfileServiceListIntelligenceProfilesResponse, error) {
	if s.inference == nil {
		return nil, status.Error(codes.FailedPrecondition, "inference subsystem is not configured")
	}
	mgr, err := s.inference.SpaceManager(ctx, strings.TrimSpace(req.GetSpaceId()))
	if err != nil {
		return nil, mapAdminInferenceError(err, "open inference space manager")
	}
	items, err := mgr.ListProfiles(ctx)
	if err != nil {
		return nil, mapAdminInferenceError(err, "list inference profiles")
	}
	if strings.TrimSpace(req.GetDomainId()) != "" {
		items = filterProfilesByDomain(items, req.GetDomainId())
	}
	if op := inferenceOperationFromProto(req.GetOperation()); op != "" {
		items = filterProfilesByOperation(items, op)
	}
	if strings.TrimSpace(req.GetPurpose()) != "" {
		items = filterProfilesByPurpose(items, req.GetPurpose())
	}
	if !req.GetIncludeDisabled() {
		items = filterProfilesEnabled(items)
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].Key < items[j].Key })
	page, next, err := paginateAdminInference(items, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &adminv1.AdminIntelligenceAccessProfileServiceListIntelligenceProfilesResponse{IntelligenceProfiles: mapIntelligenceProfiles(page), NextPageToken: next}, nil
}

func (s *AdminInferenceService) resolveStandaloneIntelligenceProfile(ctx context.Context, spaceID string, ref string) (domaininference.Profile, error) {
	if s.inference == nil {
		return domaininference.Profile{}, status.Error(codes.FailedPrecondition, "inference subsystem is not configured")
	}
	spaceID = strings.TrimSpace(spaceID)
	if spaceID == "" {
		return domaininference.Profile{}, status.Error(codes.InvalidArgument, "space_id is required")
	}
	mgr, err := s.inference.SpaceManager(ctx, spaceID)
	if err != nil {
		return domaininference.Profile{}, mapAdminInferenceError(err, "open inference space manager")
	}
	items, err := mgr.ListProfiles(ctx)
	if err != nil {
		return domaininference.Profile{}, mapAdminInferenceError(err, "list inference profiles")
	}
	if id, err := uuid.Parse(strings.TrimSpace(ref)); err == nil && id != uuid.Nil {
		for _, item := range items {
			if item.ID == id {
				return item, nil
			}
		}
		return domaininference.Profile{}, status.Error(codes.NotFound, "inference profile not found")
	}
	key := strings.ToLower(strings.TrimSpace(ref))
	for _, item := range items {
		if strings.ToLower(strings.TrimSpace(item.Key)) == key {
			return item, nil
		}
	}
	return domaininference.Profile{}, status.Errorf(codes.NotFound, "inference profile %q not found", ref)
}

func filterProfilesByDomain(items []domaininference.Profile, domainID string) []domaininference.Profile {
	out := items[:0]
	for _, item := range items {
		if len(item.DomainIDs) == 0 || stringInSliceAdmin(domainID, item.DomainIDs) {
			out = append(out, item)
		}
	}
	return out
}

func filterProfilesByOperation(items []domaininference.Profile, op domaininference.Operation) []domaininference.Profile {
	out := items[:0]
	for _, item := range items {
		if item.Operation == op {
			out = append(out, item)
		}
	}
	return out
}

func filterProfilesByPurpose(items []domaininference.Profile, purpose string) []domaininference.Profile {
	purpose = strings.ToLower(strings.TrimSpace(purpose))
	out := items[:0]
	for _, item := range items {
		if strings.ToLower(strings.TrimSpace(item.Purpose)) == purpose {
			out = append(out, item)
		}
	}
	return out
}

func filterProfilesEnabled(items []domaininference.Profile) []domaininference.Profile {
	out := items[:0]
	for _, item := range items {
		if item.Enabled {
			out = append(out, item)
		}
	}
	return out
}

func filterSemanticProfilesByDomain(items []domainsemantic.IntelligenceProfile, domainID string) []domainsemantic.IntelligenceProfile {
	out := items[:0]
	for _, item := range items {
		if len(item.DomainIDs) == 0 || stringInSliceAdmin(domainID, item.DomainIDs) {
			out = append(out, item)
		}
	}
	return out
}

func filterSemanticProfilesByOperation(items []domainsemantic.IntelligenceProfile, op domainsemantic.Operation) []domainsemantic.IntelligenceProfile {
	out := items[:0]
	for _, item := range items {
		if item.Operation == op {
			out = append(out, item)
		}
	}
	return out
}

func filterSemanticProfilesByPurpose(items []domainsemantic.IntelligenceProfile, purpose string) []domainsemantic.IntelligenceProfile {
	purpose = strings.ToLower(strings.TrimSpace(purpose))
	out := items[:0]
	for _, item := range items {
		if strings.ToLower(strings.TrimSpace(item.Purpose)) == purpose {
			out = append(out, item)
		}
	}
	return out
}

func filterSemanticProfilesEnabled(items []domainsemantic.IntelligenceProfile) []domainsemantic.IntelligenceProfile {
	out := items[:0]
	for _, item := range items {
		if item.Enabled {
			out = append(out, item)
		}
	}
	return out
}

func stringInSliceAdmin(value string, values []string) bool {
	for _, item := range values {
		if strings.TrimSpace(item) == strings.TrimSpace(value) {
			return true
		}
	}
	return false
}
