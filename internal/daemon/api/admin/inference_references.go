package admin

import (
	"context"
	"fmt"
	"strings"

	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Reference-safety helpers for AdminInferenceService lifecycle operations.

func (s *AdminInferenceService) modelEndpointReferences(ctx context.Context, id domainsemantic.ModelEndpointID) ([]string, error) {
	refs := []string{}
	global := s.semantic.GlobalManager()
	caps, err := global.ListModelEndpointCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	for _, cap := range caps {
		if cap.ModelEndpointID == id {
			refs = append(refs, "capability:"+cap.ID.String())
		}
	}
	credentials, err := global.ListCredentials(ctx)
	if err != nil {
		return nil, err
	}
	for _, credential := range credentials {
		if credential.ModelEndpointID == id {
			refs = append(refs, "credential:"+credential.ID.String())
		}
	}
	spaces, err := s.semantic.ListSpaceManagers(ctx)
	if err != nil {
		return nil, err
	}
	for _, space := range spaces {
		indexes, err := space.Manager.ListSemanticIndexes(ctx)
		if err != nil {
			return nil, err
		}
		for _, index := range indexes {
			if index.ModelEndpointID == id {
				refs = append(refs, "semantic_index:"+index.ID.String())
			}
		}
		grants, err := space.Manager.ListCredentialGrants(ctx)
		if err != nil {
			return nil, err
		}
		for _, grant := range grants {
			if grant.ModelEndpointID != nil && *grant.ModelEndpointID == id {
				refs = append(refs, "credential_grant:"+grant.ID.String())
			}
		}
		decisions, err := space.Manager.ListPolicyDecisions(ctx)
		if err != nil {
			return nil, err
		}
		for _, decision := range decisions {
			if decision.ModelEndpointID == id {
				refs = append(refs, "policy_decision:"+decision.ID.String())
			}
		}
	}
	return refs, nil
}

func (s *AdminInferenceService) modelReferences(ctx context.Context, id domainsemantic.InferenceModelID) ([]string, error) {
	refs := []string{}
	caps, err := s.semantic.GlobalManager().ListModelEndpointCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	for _, cap := range caps {
		if cap.ModelID == id {
			refs = append(refs, "capability:"+cap.ID.String())
		}
	}
	spaces, err := s.semantic.ListSpaceManagers(ctx)
	if err != nil {
		return nil, err
	}
	for _, space := range spaces {
		indexes, err := space.Manager.ListSemanticIndexes(ctx)
		if err != nil {
			return nil, err
		}
		for _, index := range indexes {
			if index.ModelID == id {
				refs = append(refs, "semantic_index:"+index.ID.String())
			}
		}
		grants, err := space.Manager.ListCredentialGrants(ctx)
		if err != nil {
			return nil, err
		}
		for _, grant := range grants {
			if grant.ModelID != nil && *grant.ModelID == id {
				refs = append(refs, "credential_grant:"+grant.ID.String())
			}
		}
		decisions, err := space.Manager.ListPolicyDecisions(ctx)
		if err != nil {
			return nil, err
		}
		for _, decision := range decisions {
			if decision.ModelID == id {
				refs = append(refs, "policy_decision:"+decision.ID.String())
			}
		}
	}
	return refs, nil
}

func (s *AdminInferenceService) vectorStoreReferences(ctx context.Context, id domainsemantic.VectorStoreID) ([]string, error) {
	refs := []string{}
	spaces, err := s.semantic.ListSpaceManagers(ctx)
	if err != nil {
		return nil, err
	}
	for _, space := range spaces {
		indexes, err := space.Manager.ListSemanticIndexes(ctx)
		if err != nil {
			return nil, err
		}
		for _, index := range indexes {
			if index.VectorStoreID == id {
				refs = append(refs, "semantic_index:"+index.ID.String())
			}
		}
	}
	return refs, nil
}

func (s *AdminInferenceService) capabilityReferences(ctx context.Context, id domainsemantic.ModelEndpointCapabilityID) ([]string, error) {
	refs := []string{}
	spaces, err := s.semantic.ListSpaceManagers(ctx)
	if err != nil {
		return nil, err
	}
	for _, space := range spaces {
		indexes, err := space.Manager.ListSemanticIndexes(ctx)
		if err != nil {
			return nil, err
		}
		for _, index := range indexes {
			if index.ModelEndpointCapabilityID == id {
				refs = append(refs, "semantic_index:"+index.ID.String())
			}
		}
	}
	return refs, nil
}

func (s *AdminInferenceService) credentialGrantReferences(ctx context.Context, id domainsemantic.InferenceCredentialID) ([]string, error) {
	refs := []string{}
	spaces, err := s.semantic.ListSpaceManagers(ctx)
	if err != nil {
		return nil, err
	}
	for _, space := range spaces {
		grants, err := space.Manager.ListCredentialGrants(ctx)
		if err != nil {
			return nil, err
		}
		for _, grant := range grants {
			if grant.CredentialID == id {
				refs = append(refs, "credential_grant:"+grant.ID.String())
			}
		}
	}
	return refs, nil
}

func (s *AdminInferenceService) credentialVectorReferences(ctx context.Context, id domainsemantic.InferenceCredentialID) ([]string, error) {
	return s.vectorRecordReferences(ctx, func(rec domainsemantic.AdvancedEmbeddingRecord) bool { return rec.CredentialID == id })
}

func (s *AdminInferenceService) credentialGrantVectorReferences(ctx context.Context, id domainsemantic.CredentialGrantID) ([]string, error) {
	return s.vectorRecordReferences(ctx, func(rec domainsemantic.AdvancedEmbeddingRecord) bool { return rec.CredentialGrantID == id })
}

func (s *AdminInferenceService) vectorRecordReferences(ctx context.Context, match func(domainsemantic.AdvancedEmbeddingRecord) bool) ([]string, error) {
	refs := []string{}
	spaces, err := s.semantic.ListSpaceManagers(ctx)
	if err != nil {
		return nil, mapAdminInferenceError(err, "list semantic spaces")
	}
	for _, space := range spaces {
		indexes, err := space.Manager.ListSemanticIndexes(ctx)
		if err != nil {
			return nil, mapAdminInferenceError(err, "list semantic indexes")
		}
		for _, index := range indexes {
			records, err := s.semantic.ListVectorRecords(ctx, space.SpaceID, index.ID)
			if err != nil {
				return nil, mapAdminInferenceError(err, "list vector records")
			}
			for _, rec := range records {
				if !rec.Tombstone && match(rec) {
					refs = append(refs, "vector_record:"+rec.ID.String()+":index:"+index.ID.String())
				}
			}
		}
	}
	return refs, nil
}

func (s *AdminInferenceService) secretCredentialReferences(ctx context.Context, id domainsemantic.SecretID, excluding domainsemantic.InferenceCredentialID) ([]string, error) {
	refs := []string{}
	credentials, err := s.semantic.GlobalManager().ListCredentials(ctx)
	if err != nil {
		return nil, mapAdminInferenceError(err, "list credentials")
	}
	for _, credential := range credentials {
		if credential.ID != excluding && credential.SecretRef == id {
			refs = append(refs, "credential:"+credential.ID.String())
		}
	}
	return refs, nil
}

func (s *AdminInferenceService) credentialByID(ctx context.Context, id domainsemantic.InferenceCredentialID) (domainsemantic.InferenceCredential, error) {
	credentials, err := s.semantic.GlobalManager().ListCredentials(ctx)
	if err != nil {
		return domainsemantic.InferenceCredential{}, mapAdminInferenceError(err, "list credentials")
	}
	for _, credential := range credentials {
		if credential.ID == id {
			return credential, nil
		}
	}
	return domainsemantic.InferenceCredential{}, status.Error(codes.NotFound, "credential not found")
}

func referencedPrecondition(resource string, refs []string) error {
	if len(refs) > 20 {
		refs = append(append([]string{}, refs[:20]...), fmt.Sprintf("...%d more", len(refs)-20))
	}
	return status.Errorf(codes.FailedPrecondition, "%s is referenced by %s", resource, strings.Join(refs, ", "))
}
