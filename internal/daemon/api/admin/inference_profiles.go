package admin

import (
	"context"
	"sort"
	"strings"

	"github.com/google/uuid"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Profile RPC handlers for the standalone inference subsystem.

func (s *AdminInferenceService) CreateIntelligenceProfile(ctx context.Context, req *adminv1.AdminIntelligenceAccessProfileServiceCreateIntelligenceProfileRequest) (*adminv1.AdminIntelligenceAccessProfileServiceCreateIntelligenceProfileResponse, error) {
	principal, err := s.requireInferenceCapability(ctx, capIntelligenceProfileManage, inferenceScope(req.GetSpaceId(), ""))
	if err != nil {
		return nil, err
	}
	if s.inference == nil {
		return nil, status.Error(codes.FailedPrecondition, "inference subsystem is not configured")
	}
	profile, err := inferenceProfileFromProto(req, principal.PrincipalID)
	if err != nil {
		return nil, err
	}
	mgr, err := s.inference.SpaceManager(ctx, profile.SpaceID)
	if err != nil {
		return nil, mapAdminInferenceError(err, "open inference space manager")
	}
	stored, err := mgr.UpsertProfile(ctx, profile)
	if err != nil {
		return nil, mapAdminInferenceError(err, "upsert inference profile")
	}
	return &adminv1.AdminIntelligenceAccessProfileServiceCreateIntelligenceProfileResponse{IntelligenceProfile: mapIntelligenceProfile(stored)}, nil
}

func (s *AdminInferenceService) ListIntelligenceProfiles(ctx context.Context, req *adminv1.AdminIntelligenceAccessProfileServiceListIntelligenceProfilesRequest) (*adminv1.AdminIntelligenceAccessProfileServiceListIntelligenceProfilesResponse, error) {
	spaceID := strings.TrimSpace(req.GetSpaceId())
	if _, err := s.requireInferenceCapability(ctx, capIntelligenceProfileRead, inferenceScope(spaceID, req.GetDomainId())); err != nil {
		return nil, err
	}
	if s.inference == nil {
		return nil, status.Error(codes.FailedPrecondition, "inference subsystem is not configured")
	}
	if spaceID == "" {
		return nil, status.Error(codes.InvalidArgument, "space_id is required")
	}
	mgr, err := s.inference.SpaceManager(ctx, spaceID)
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

func (s *AdminInferenceService) GetIntelligenceProfile(ctx context.Context, req *adminv1.AdminIntelligenceAccessProfileServiceGetIntelligenceProfileRequest) (*adminv1.AdminIntelligenceAccessProfileServiceGetIntelligenceProfileResponse, error) {
	if _, err := s.requireInferenceCapability(ctx, capIntelligenceProfileRead, inferenceScope(req.GetSpaceId(), "")); err != nil {
		return nil, err
	}
	profile, err := s.resolveIntelligenceProfile(ctx, req.GetSpaceId(), firstNonEmptyAdmin(req.GetIntelligenceProfileId(), req.GetIntelligenceProfile()))
	if err != nil {
		return nil, err
	}
	return &adminv1.AdminIntelligenceAccessProfileServiceGetIntelligenceProfileResponse{IntelligenceProfile: mapIntelligenceProfile(profile)}, nil
}

func (s *AdminInferenceService) SetIntelligenceProfileEnabled(ctx context.Context, req *adminv1.AdminIntelligenceAccessProfileServiceSetIntelligenceProfileEnabledRequest) (*adminv1.AdminIntelligenceAccessProfileServiceSetIntelligenceProfileEnabledResponse, error) {
	if _, err := s.requireInferenceCapability(ctx, capIntelligenceProfileManage, inferenceScope(req.GetSpaceId(), "")); err != nil {
		return nil, err
	}
	profile, err := s.resolveIntelligenceProfile(ctx, req.GetSpaceId(), firstNonEmptyAdmin(req.GetIntelligenceProfileId(), req.GetIntelligenceProfile()))
	if err != nil {
		return nil, err
	}
	mgr, err := s.inference.SpaceManager(ctx, profile.SpaceID)
	if err != nil {
		return nil, mapAdminInferenceError(err, "open inference space manager")
	}
	profile.Enabled = req.GetEnabled()
	stored, err := mgr.UpsertProfile(ctx, profile)
	if err != nil {
		return nil, mapAdminInferenceError(err, "update inference profile")
	}
	return &adminv1.AdminIntelligenceAccessProfileServiceSetIntelligenceProfileEnabledResponse{IntelligenceProfile: mapIntelligenceProfile(stored)}, nil
}

func (s *AdminInferenceService) DeleteIntelligenceProfile(ctx context.Context, req *adminv1.AdminIntelligenceAccessProfileServiceDeleteIntelligenceProfileRequest) (*adminv1.AdminIntelligenceAccessProfileServiceDeleteIntelligenceProfileResponse, error) {
	if _, err := s.requireInferenceCapability(ctx, capIntelligenceProfileManage, inferenceScope(req.GetSpaceId(), "")); err != nil {
		return nil, err
	}
	profile, err := s.resolveIntelligenceProfile(ctx, req.GetSpaceId(), firstNonEmptyAdmin(req.GetIntelligenceProfileId(), req.GetIntelligenceProfile()))
	if err != nil {
		return nil, err
	}
	refs, err := s.profileReferences(ctx, profile)
	if err != nil {
		return nil, err
	}
	if len(refs) > 0 {
		return nil, referencedPrecondition("inference profile", refs)
	}
	mgr, err := s.inference.SpaceManager(ctx, profile.SpaceID)
	if err != nil {
		return nil, mapAdminInferenceError(err, "open inference space manager")
	}
	if err := mgr.DeleteProfile(ctx, profile.ID); err != nil {
		return nil, mapAdminInferenceError(err, "delete inference profile")
	}
	return &adminv1.AdminIntelligenceAccessProfileServiceDeleteIntelligenceProfileResponse{IntelligenceProfileId: profile.ID.String()}, nil
}

func (s *AdminInferenceService) resolveIntelligenceProfile(ctx context.Context, spaceID string, ref string) (domaininference.Profile, error) {
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

func stringInSliceAdmin(value string, values []string) bool {
	for _, item := range values {
		if strings.TrimSpace(item) == strings.TrimSpace(value) {
			return true
		}
	}
	return false
}
