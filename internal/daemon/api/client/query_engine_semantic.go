package client

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	identity "github.com/myceldb/mycel/internal/identity/model"
	queryengine "github.com/myceldb/mycel/internal/query/engine"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	semanticsearch "github.com/myceldb/mycel/internal/semantic/search"
	daemonsemantic "github.com/myceldb/mycel/internal/semantic/service"
)

type querySemanticCandidateReader struct {
	searcher semanticQuerySearcher
}

// NewQuerySemanticCandidateReader adapts the daemon semantic capability to the
// query engine's consumer-owned candidate port.
func NewQuerySemanticCandidateReader(searcher semanticQuerySearcher) queryengine.SemanticCandidateReader {
	if searcher == nil {
		return nil
	}
	return querySemanticCandidateReader{searcher: searcher}
}

func (r querySemanticCandidateReader) SearchCandidates(ctx context.Context, request queryengine.SemanticCandidateRequest) (queryengine.SemanticCandidateBatch, error) {
	spaceID, err := parseQueryEngineUUID(request.SpaceID, "space ID")
	if err != nil {
		return queryengine.SemanticCandidateBatch{}, err
	}
	domainID, err := parseQueryEngineUUID(request.DomainID, "domain ID")
	if err != nil {
		return queryengine.SemanticCandidateBatch{}, err
	}
	actorID, err := parseQueryEngineUUID(request.ActorPrincipalID, "actor principal ID")
	if err != nil {
		return queryengine.SemanticCandidateBatch{}, err
	}
	ruleIDs := make([]domainsemantic.SemanticRuleID, 0, len(request.SemanticRuleIDs))
	for _, value := range request.SemanticRuleIDs {
		id, err := parseQueryEngineUUID(value, "semantic rule ID")
		if err != nil {
			return queryengine.SemanticCandidateBatch{}, err
		}
		ruleIDs = append(ruleIDs, id)
	}
	result, err := r.searcher.Search(ctx, daemonsemantic.SearchInput{
		SpaceID:             spaceID,
		DomainID:            domainID,
		SemanticRuleIDs:     ruleIDs,
		EmbeddingBindingKey: request.EmbeddingBindingKey,
		Text:                request.Text,
		Limit:               request.Limit,
		MinScore:            request.MinScore,
		ActorPrincipalID:    identity.PrincipalID(actorID.String()),
	})
	if err != nil {
		return queryengine.SemanticCandidateBatch{}, err
	}
	return querySemanticCandidateBatch(result), nil
}

func querySemanticCandidateBatch(result semanticsearch.Result) queryengine.SemanticCandidateBatch {
	batch := queryengine.SemanticCandidateBatch{
		Candidates: make([]queryengine.SemanticCandidate, 0, len(result.Results)),
		Warnings:   make([]queryengine.SemanticWarning, 0, len(result.WarningDetails)),
	}
	for _, item := range result.Results {
		ruleID := item.SemanticRuleID
		if ruleID == uuid.Nil {
			ruleID = item.SemanticIndexID
		}
		batch.Candidates = append(batch.Candidates, queryengine.SemanticCandidate{
			NodeID:              uuidString(item.NodeID),
			TargetNodeID:        uuidString(item.TargetNodeID),
			SemanticRuleID:      uuidString(ruleID),
			EmbeddingBindingKey: item.EmbeddingBindingKey,
			Score:               item.Score,
		})
	}
	for _, warning := range result.WarningDetails {
		batch.Warnings = append(batch.Warnings, queryengine.SemanticWarning{
			Code:                warning.Code,
			Message:             warning.Message,
			SemanticRuleID:      uuidString(warning.SemanticRuleID),
			EmbeddingBindingKey: warning.EmbeddingBindingKey,
			Retryable:           warning.Retryable,
		})
	}
	return batch
}

func parseQueryEngineUUID(value string, name string) (uuid.UUID, error) {
	id, err := uuid.Parse(strings.TrimSpace(value))
	if err != nil {
		return uuid.Nil, fmt.Errorf("invalid %s: %w", name, err)
	}
	if id == uuid.Nil {
		return uuid.Nil, fmt.Errorf("invalid %s: UUID must not be nil", name)
	}
	return id, nil
}
