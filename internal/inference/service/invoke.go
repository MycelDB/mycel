package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/myceldb/mycel/internal/inference/connectors"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
)

var ErrDenied = errors.New("inference request denied")

type InvokeRequest struct {
	Resolve         ResolveRequest
	Input           string
	Prompt          string
	Messages        []connectors.Message
	RequestID       string
	AutomationID    string
	AutomationRunID string
	SemanticIndexID string
	Metadata        map[string]any
}

type InvokeResponse struct {
	Allowed           bool
	Decision          domaininference.PolicyDecision
	ProviderRequestID string
	Text              string
	JSON              map[string]any
	Embedding         []float64
	Usage             connectors.Usage
}

func (m *Module) Invoke(ctx context.Context, req InvokeRequest) (InvokeResponse, error) {
	started := time.Now().UTC()
	if req.Resolve.SemanticIndexID == "" {
		req.Resolve.SemanticIndexID = req.SemanticIndexID
	}
	if req.Resolve.Metadata == nil {
		req.Resolve.Metadata = req.Metadata
	}
	resolved, err := m.Resolve(ctx, req.Resolve)
	if err != nil {
		return InvokeResponse{}, err
	}
	if !resolved.Allowed {
		_, appendErr := m.appendUsage(ctx, req, resolved, connectors.Usage{}, "", domaininference.UsageStatusDenied, "inference_denied", resolved.Decision.Reason, started)
		resp := InvokeResponse{Allowed: false, Decision: resolved.Decision}
		if appendErr != nil {
			return resp, appendErr
		}
		return resp, ErrDenied
	}
	if err := ctx.Err(); err != nil {
		_, appendErr := m.appendUsage(context.Background(), req, resolved, connectors.Usage{}, "", domaininference.UsageStatusCanceled, "canceled", err.Error(), started)
		if appendErr != nil {
			return InvokeResponse{Allowed: true, Decision: resolved.Decision}, appendErr
		}
		return InvokeResponse{Allowed: true, Decision: resolved.Decision}, err
	}
	secretValue := ""
	if resolved.Credential.AuthType != domaininference.CredentialAuthNone {
		secretValue, err = m.resolveSecret(ctx, resolved.Secret)
		if err != nil {
			_, appendErr := m.appendUsage(ctx, req, resolved, connectors.Usage{}, "", domaininference.UsageStatusFailed, "secret_resolution_failed", err.Error(), started)
			if appendErr != nil {
				return InvokeResponse{Allowed: true, Decision: resolved.Decision}, fmt.Errorf("%w; additionally failed to append usage event: %v", err, appendErr)
			}
			return InvokeResponse{Allowed: true, Decision: resolved.Decision}, err
		}
	}
	connector := m.connector(resolved.Endpoint.ConnectorType)
	if connector == nil {
		err := fmt.Errorf("connector %q is not registered", resolved.Endpoint.ConnectorType)
		_, appendErr := m.appendUsage(ctx, req, resolved, connectors.Usage{}, "", domaininference.UsageStatusFailed, "connector_unavailable", err.Error(), started)
		if appendErr != nil {
			return InvokeResponse{Allowed: true, Decision: resolved.Decision}, fmt.Errorf("%w; additionally failed to append usage event: %v", err, appendErr)
		}
		return InvokeResponse{Allowed: true, Decision: resolved.Decision}, err
	}
	resp, err := m.invokeConnector(ctx, connector, req, resolved, secretValue)
	status := domaininference.UsageStatusSucceeded
	errorCode := ""
	errorMessage := ""
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			status = domaininference.UsageStatusCanceled
			errorCode = "canceled"
		} else {
			status = domaininference.UsageStatusFailed
			errorCode = connectors.ErrorCode(err)
		}
		errorMessage = err.Error()
	}
	appendCtx := ctx
	if status == domaininference.UsageStatusCanceled {
		appendCtx = context.Background()
	}
	_, appendErr := m.appendUsage(appendCtx, req, resolved, resp.Usage, resp.ProviderRequestID, status, errorCode, errorMessage, started)
	out := InvokeResponse{Allowed: true, Decision: resolved.Decision, ProviderRequestID: resp.ProviderRequestID, Text: resp.Text, JSON: resp.JSON, Embedding: resp.Embedding, Usage: resp.Usage}
	if appendErr != nil {
		if err != nil {
			return out, fmt.Errorf("%w; additionally failed to append usage event: %v", err, appendErr)
		}
		return out, appendErr
	}
	return out, err
}

type connectorResponse struct {
	ProviderRequestID string
	Text              string
	JSON              map[string]any
	Embedding         []float64
	Usage             connectors.Usage
}

func (m *Module) invokeConnector(ctx context.Context, connector connectors.Connector, req InvokeRequest, resolved ResolveResult, secretValue string) (connectorResponse, error) {
	switch resolved.Profile.Operation {
	case domaininference.OperationEmbeddings:
		resp, err := connector.Embed(ctx, connectors.EmbeddingRequest{Endpoint: resolved.Endpoint, Model: resolved.Model, Capability: resolved.Capability, Credential: resolved.Credential, Secret: secretValue, Input: req.Input})
		return connectorResponse{ProviderRequestID: resp.ProviderRequestID, Embedding: resp.Vector, Usage: resp.Usage}, err
	case domaininference.OperationChat, domaininference.OperationSummarize, domaininference.OperationClassify:
		messages := normalizedMessages(req)
		resp, err := connector.Chat(ctx, connectors.ChatRequest{Endpoint: resolved.Endpoint, Model: resolved.Model, Capability: resolved.Capability, Credential: resolved.Credential, Secret: secretValue, Messages: messages, Parameters: mergeParameters(resolved.Profile.DefaultParameters, req.Resolve.Parameters)})
		return connectorResponse{ProviderRequestID: resp.ProviderRequestID, Text: resp.Text, JSON: resp.JSON, Usage: resp.Usage}, err
	default:
		return connectorResponse{}, fmt.Errorf("unsupported inference operation %q", resolved.Profile.Operation)
	}
}

func normalizedMessages(req InvokeRequest) []connectors.Message {
	if len(req.Messages) > 0 {
		out := make([]connectors.Message, 0, len(req.Messages))
		for _, msg := range req.Messages {
			if strings.TrimSpace(msg.Content) != "" {
				role := strings.TrimSpace(msg.Role)
				if role == "" {
					role = "user"
				}
				out = append(out, connectors.Message{Role: role, Content: msg.Content})
			}
		}
		return out
	}
	out := []connectors.Message{}
	if strings.TrimSpace(req.Prompt) != "" {
		out = append(out, connectors.Message{Role: "system", Content: req.Prompt})
	}
	if strings.TrimSpace(req.Input) != "" {
		out = append(out, connectors.Message{Role: "user", Content: req.Input})
	}
	return out
}

func (m *Module) appendUsage(ctx context.Context, req InvokeRequest, resolved ResolveResult, usage connectors.Usage, providerRequestID string, status domaininference.UsageStatus, errorCode string, errorMessage string, started time.Time) (domaininference.UsageEvent, error) {
	completed := time.Now().UTC()
	metadata := copyMap(req.Metadata)
	if metadata == nil {
		metadata = copyMap(req.Resolve.Metadata)
	}
	if usage.TokenCountSource != "" {
		if metadata == nil {
			metadata = map[string]any{}
		}
		metadata["token_count_source"] = usage.TokenCountSource
	}
	event := domaininference.UsageEvent{RequestID: req.RequestID, Operation: req.Resolve.Operation, UsageMode: req.Resolve.UsageMode, Status: status, SpaceID: req.Resolve.SpaceID, DomainID: req.Resolve.DomainID, NodeID: req.Resolve.NodeID, AutomationID: req.AutomationID, AutomationRunID: req.AutomationRunID, SemanticIndexID: firstNonEmpty(req.Resolve.SemanticIndexID, req.SemanticIndexID), ActorPrincipalID: req.Resolve.ActorPrincipalID, OnBehalfOfPrincipalID: req.Resolve.OnBehalfOfPrincipalID, ProfileID: resolved.Profile.ID, EndpointID: resolved.Endpoint.ID, ModelID: resolved.Model.ID, CapabilityID: resolved.Capability.ID, CredentialID: resolved.Credential.ID, CredentialGrantID: resolved.CredentialGrant.ID, PolicyDecisionID: resolved.Decision.ID, ProviderRequestID: providerRequestID, InputTokens: usage.InputTokens, OutputTokens: usage.OutputTokens, TotalTokens: usage.TotalTokens, LatencyMillis: completed.Sub(started).Milliseconds(), ErrorCode: errorCode, ErrorMessage: errorMessage, StartedAt: started, CompletedAt: completed, Metadata: metadata}
	return m.UsageLedger().AppendUsageEvent(ctx, event)
}
