package service

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/myceldb/mycel/internal/inference/connectors"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	inferenceruntime "github.com/myceldb/mycel/internal/inference/runtime"
)

func (m *Module) Invoke(ctx context.Context, req inferenceruntime.InvokeRequest) (inferenceruntime.InvokeResponse, error) {
	started := time.Now().UTC()
	if req.Resolve.SemanticIndexID == "" {
		req.Resolve.SemanticIndexID = req.SemanticIndexID
	}
	if req.Resolve.Metadata == nil {
		req.Resolve.Metadata = req.Metadata
	}
	resolved, err := m.Resolve(ctx, req.Resolve)
	if err != nil {
		return inferenceruntime.InvokeResponse{}, err
	}
	if !resolved.Allowed {
		_, appendErr := m.appendUsage(ctx, req, resolved, domaininference.Usage{}, "", domaininference.UsageStatusDenied, "inference_denied", resolved.Decision.Reason, started)
		resp := inferenceruntime.InvokeResponse{Allowed: false, Decision: resolved.Decision, Status: domaininference.UsageStatusDenied}
		if appendErr != nil {
			return resp, appendErr
		}
		return resp, inferenceruntime.ErrDenied
	}
	if req.AfterAuthorization != nil {
		if err := req.AfterAuthorization(ctx, resolved); err != nil {
			_, appendErr := m.appendUsage(context.WithoutCancel(ctx), req, resolved, domaininference.Usage{}, "", domaininference.UsageStatusFailed, "authorization_fence_failed", "authorization_fence_failed", started)
			if appendErr != nil {
				return inferenceruntime.InvokeResponse{Allowed: true, Decision: resolved.Decision, Status: domaininference.UsageStatusFailed}, errors.Join(err, appendErr)
			}
			return inferenceruntime.InvokeResponse{Allowed: true, Decision: resolved.Decision, Status: domaininference.UsageStatusFailed}, err
		}
	}
	if err := ctx.Err(); err != nil {
		_, appendErr := m.appendUsage(context.Background(), req, resolved, domaininference.Usage{}, "", domaininference.UsageStatusCanceled, "canceled", err.Error(), started)
		if appendErr != nil {
			return inferenceruntime.InvokeResponse{Allowed: true, Decision: resolved.Decision, Status: domaininference.UsageStatusCanceled}, appendErr
		}
		return inferenceruntime.InvokeResponse{Allowed: true, Decision: resolved.Decision, Status: domaininference.UsageStatusCanceled}, err
	}
	connector := m.connector(resolved.Endpoint.ConnectorType)
	if connector == nil {
		err := fmt.Errorf("connector %q is not registered", resolved.Endpoint.ConnectorType)
		_, appendErr := m.appendUsage(ctx, req, resolved, domaininference.Usage{}, "", domaininference.UsageStatusFailed, "connector_unavailable", err.Error(), started)
		if appendErr != nil {
			return inferenceruntime.InvokeResponse{Allowed: true, Decision: resolved.Decision, Status: domaininference.UsageStatusFailed}, fmt.Errorf("%w; additionally failed to append usage event: %v", err, appendErr)
		}
		return inferenceruntime.InvokeResponse{Allowed: true, Decision: resolved.Decision, Status: domaininference.UsageStatusFailed}, err
	}
	providerCtx := ctx
	providerCancel := func() {}
	var budgetAttempt domaininference.BudgetAttempt
	if req.BudgetScopeID != "" {
		budgetAttempt, err = m.reserveInvocationBudget(ctx, req, resolved, connector)
		if err != nil {
			reason := budgetDenialReason(err)
			_, appendErr := m.appendUsage(ctx, req, resolved, domaininference.Usage{}, "", domaininference.UsageStatusDenied, "budget_denied", reason, started)
			resp := inferenceruntime.InvokeResponse{Allowed: true, Decision: resolved.Decision, Status: domaininference.UsageStatusDenied}
			if appendErr != nil {
				return resp, appendErr
			}
			return resp, err
		}
		providerCtx, providerCancel = context.WithDeadline(ctx, budgetAttempt.ReservedUntil)
		if req.Resolve.Operation != domaininference.OperationEmbeddings && budgetAttempt.ReservedOutputTokens > 0 {
			// Send the exact reserved output ceiling to the provider. Without this,
			// a per-request token budget would account for a cap it never enforced.
			req.Resolve.Parameters.MaxOutputTokens = int(budgetAttempt.ReservedOutputTokens)
		}
	}
	secretValue := ""
	if resolved.Credential.AuthType != domaininference.CredentialAuthNone && resolved.Endpoint.ConnectorType != domaininference.ConnectorFake {
		secretValue, err = m.resolveSecret(providerCtx, resolved.Secret)
		if err != nil {
			providerCancel()
			if req.BudgetScopeID != "" {
				if completionErr := m.failReservedInvocationBudget(req, "secret_resolution_failed"); completionErr != nil {
					return inferenceruntime.InvokeResponse{Allowed: true, Decision: resolved.Decision, Status: domaininference.UsageStatusFailed}, fmt.Errorf("%w; budget completion failed: %v", err, completionErr)
				}
			}
			_, appendErr := m.appendUsage(ctx, req, resolved, domaininference.Usage{}, "", domaininference.UsageStatusFailed, "secret_resolution_failed", "secret_resolution_failed", started)
			if appendErr != nil {
				return inferenceruntime.InvokeResponse{Allowed: true, Decision: resolved.Decision, Status: domaininference.UsageStatusFailed}, fmt.Errorf("%w; additionally failed to append usage event: %v", err, appendErr)
			}
			return inferenceruntime.InvokeResponse{Allowed: true, Decision: resolved.Decision, Status: domaininference.UsageStatusFailed}, err
		}
	}
	if err := providerCtx.Err(); err != nil {
		providerCancel()
		if req.BudgetScopeID != "" {
			if completionErr := m.failReservedInvocationBudget(req, "provider_deadline_exhausted"); completionErr != nil {
				return inferenceruntime.InvokeResponse{Allowed: true, Decision: resolved.Decision, Status: domaininference.UsageStatusCanceled}, fmt.Errorf("%w; budget completion failed: %v", err, completionErr)
			}
		}
		_, appendErr := m.appendUsage(context.Background(), req, resolved, domaininference.Usage{}, "", domaininference.UsageStatusCanceled, "canceled", "provider_deadline_exhausted", started)
		if appendErr != nil {
			return inferenceruntime.InvokeResponse{Allowed: true, Decision: resolved.Decision, Status: domaininference.UsageStatusCanceled}, appendErr
		}
		return inferenceruntime.InvokeResponse{Allowed: true, Decision: resolved.Decision, Status: domaininference.UsageStatusCanceled}, err
	}
	if req.BeforeProviderAttempt != nil {
		if err := req.BeforeProviderAttempt(providerCtx, resolved); err != nil {
			providerCancel()
			if req.BudgetScopeID != "" {
				if completionErr := m.failReservedInvocationBudget(req, "provider_start_fence_failed"); completionErr != nil {
					return inferenceruntime.InvokeResponse{Allowed: true, Decision: resolved.Decision, Status: domaininference.UsageStatusFailed}, fmt.Errorf("%w; budget completion failed: %v", err, completionErr)
				}
			}
			_, appendErr := m.appendUsage(context.WithoutCancel(ctx), req, resolved, domaininference.Usage{}, "", domaininference.UsageStatusFailed, "provider_start_fence_failed", "provider_start_fence_failed", started)
			if appendErr != nil {
				return inferenceruntime.InvokeResponse{Allowed: true, Decision: resolved.Decision, Status: domaininference.UsageStatusFailed}, errors.Join(err, appendErr)
			}
			return inferenceruntime.InvokeResponse{Allowed: true, Decision: resolved.Decision, Status: domaininference.UsageStatusFailed}, err
		}
	}
	providerStarted := time.Now().UTC()
	resp, err := m.invokeConnector(providerCtx, connector, req, resolved, secretValue)
	providerCancel()
	providerCompleted := time.Now().UTC()
	if req.BudgetScopeID != "" {
		completionErr := m.completeInvocationBudget(req, resolved.Endpoint.ConnectorType, resp, err, providerStarted, providerCompleted)
		if completionErr != nil {
			out := inferenceruntime.InvokeResponse{Allowed: true, Decision: resolved.Decision, ProviderRequestID: resp.ProviderRequestID, Usage: resp.Usage, Status: domaininference.UsageStatusFailed}
			if errors.Is(completionErr, ErrBudgetAccounting) {
				_, appendErr := m.appendUsage(context.Background(), req, resolved, resp.Usage, resp.ProviderRequestID, domaininference.UsageStatusFailed, "reservation_exceeded", "reservation_exceeded", started)
				if appendErr != nil {
					return out, errors.Join(err, completionErr, appendErr)
				}
			}
			if err != nil {
				return out, fmt.Errorf("%w; budget completion failed: %v", err, completionErr)
			}
			return out, completionErr
		}
	}
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
	out := inferenceruntime.InvokeResponse{Allowed: true, Decision: resolved.Decision, ProviderRequestID: resp.ProviderRequestID, Text: resp.Text, JSON: resp.JSON, Embedding: resp.Embedding, Usage: resp.Usage, Status: status}
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
	Usage             domaininference.Usage
}

func (m *Module) invokeConnector(ctx context.Context, connector connectors.Connector, req inferenceruntime.InvokeRequest, resolved inferenceruntime.ResolveResult, secretValue string) (connectorResponse, error) {
	switch resolved.Profile.Operation {
	case domaininference.OperationEmbeddings:
		resp, err := connector.Embed(ctx, connectors.EmbeddingRequest{Endpoint: resolved.Endpoint, Model: resolved.Model, Capability: resolved.Capability, Credential: resolved.Credential, Secret: secretValue, Input: req.Input})
		if err == nil {
			err = validateEmbeddingResponse(resp.Vector, resolved.Model.EmbeddingDims)
		}
		return connectorResponse{ProviderRequestID: resp.ProviderRequestID, Embedding: resp.Vector, Usage: resp.Usage}, err
	case domaininference.OperationChat, domaininference.OperationSummarize, domaininference.OperationClassify, domaininference.OperationImageAnalysis:
		messages := normalizedMessages(req)
		resp, err := connector.Chat(ctx, connectors.ChatRequest{Endpoint: resolved.Endpoint, Model: resolved.Model, Capability: resolved.Capability, Credential: resolved.Credential, Secret: secretValue, Messages: messages, Parameters: mergeParameters(resolved.Profile.DefaultParameters, req.Resolve.Parameters)})
		return connectorResponse{ProviderRequestID: resp.ProviderRequestID, Text: resp.Text, JSON: resp.JSON, Usage: resp.Usage}, err
	default:
		return connectorResponse{}, fmt.Errorf("unsupported inference operation %q", resolved.Profile.Operation)
	}
}

func validateEmbeddingResponse(vector []float64, expectedDimensions int) error {
	if len(vector) == 0 {
		return connectors.ConnectorError{Code: "invalid_embedding", Err: errors.New("embedding response is empty")}
	}
	if expectedDimensions > 0 && len(vector) != expectedDimensions {
		return connectors.ConnectorError{Code: "invalid_embedding_dimensions", Err: fmt.Errorf("embedding response dimensions %d do not match declared dimensions %d", len(vector), expectedDimensions)}
	}
	for _, value := range vector {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return connectors.ConnectorError{Code: "invalid_embedding", Err: errors.New("embedding response contains a non-finite component")}
		}
	}
	return nil
}

func normalizedMessages(req inferenceruntime.InvokeRequest) []domaininference.Message {
	if len(req.Messages) > 0 {
		out := make([]domaininference.Message, 0, len(req.Messages)+1)
		for _, msg := range req.Messages {
			if strings.TrimSpace(msg.Content) != "" || len(msg.Parts) > 0 {
				role := strings.TrimSpace(msg.Role)
				if role == "" {
					role = "user"
				}
				out = append(out, domaininference.Message{Role: role, Content: msg.Content, Parts: cloneMessageParts(msg.Parts)})
			}
		}
		if len(req.ImageInputs) > 0 {
			out = appendImagesToUserMessage(out, req.ImageInputs)
		}
		return out
	}
	out := []domaininference.Message{}
	if strings.TrimSpace(req.Prompt) != "" {
		out = append(out, domaininference.Message{Role: "system", Content: req.Prompt})
	}
	if strings.TrimSpace(req.Input) != "" || len(req.ImageInputs) > 0 {
		msg := domaininference.Message{Role: "user"}
		if len(req.ImageInputs) == 0 {
			msg.Content = req.Input
		} else {
			if strings.TrimSpace(req.Input) != "" {
				msg.Parts = append(msg.Parts, domaininference.MessagePart{Type: "text", Text: req.Input})
			}
			msg.Parts = append(msg.Parts, imageMessageParts(req.ImageInputs)...)
		}
		out = append(out, msg)
	}
	return out
}

func appendImagesToUserMessage(messages []domaininference.Message, images []domaininference.ImageInput) []domaininference.Message {
	parts := imageMessageParts(images)
	if len(parts) == 0 {
		return messages
	}
	for i := len(messages) - 1; i >= 0; i-- {
		if strings.EqualFold(strings.TrimSpace(messages[i].Role), "user") {
			messages[i].Parts = append(messages[i].Parts, parts...)
			return messages
		}
	}
	return append(messages, domaininference.Message{Role: "user", Parts: parts})
}

func imageMessageParts(images []domaininference.ImageInput) []domaininference.MessagePart {
	parts := make([]domaininference.MessagePart, 0, len(images))
	for _, image := range images {
		if len(image.Data) == 0 {
			continue
		}
		cloned := image
		cloned.Data = append([]byte(nil), image.Data...)
		parts = append(parts, domaininference.MessagePart{Type: "image", Image: &cloned})
	}
	return parts
}

func cloneMessageParts(parts []domaininference.MessagePart) []domaininference.MessagePart {
	out := make([]domaininference.MessagePart, 0, len(parts))
	for _, part := range parts {
		cloned := domaininference.MessagePart{Type: part.Type, Text: part.Text}
		if part.Image != nil {
			image := *part.Image
			image.Data = append([]byte(nil), part.Image.Data...)
			cloned.Image = &image
		}
		out = append(out, cloned)
	}
	return out
}

func (m *Module) appendUsage(ctx context.Context, req inferenceruntime.InvokeRequest, resolved inferenceruntime.ResolveResult, usage domaininference.Usage, providerRequestID string, status domaininference.UsageStatus, errorCode string, errorMessage string, started time.Time) (domaininference.UsageEvent, error) {
	completed := time.Now().UTC()
	metadata := map[string]any(nil)
	if req.BudgetScopeID == "" {
		metadata = copyMap(req.Metadata)
		if metadata == nil {
			metadata = copyMap(req.Resolve.Metadata)
		}
	}
	if source := safeTokenCountSource(usage.TokenCountSource); source != "" {
		if metadata == nil {
			metadata = map[string]any{}
		}
		metadata["token_count_source"] = source
	}
	rawErrorMessage := errorMessage
	errorCode = safeUsageCategory(errorCode)
	errorMessage = safeUsageErrorMessage(errorCode, rawErrorMessage)
	event := domaininference.UsageEvent{RequestID: req.RequestID, BudgetScopeID: req.BudgetScopeID, BudgetAttemptID: req.BudgetAttemptID, Operation: req.Resolve.Operation, UsageMode: req.Resolve.UsageMode, Status: status, SpaceID: req.Resolve.SpaceID, DomainID: req.Resolve.DomainID, NodeID: req.Resolve.NodeID, AutomationID: req.AutomationID, AutomationRunID: req.AutomationRunID, SemanticRuleID: firstNonEmpty(req.Resolve.SemanticRuleID, req.SemanticRuleID), EmbeddingBindingKey: firstNonEmpty(req.Resolve.EmbeddingBindingKey, req.EmbeddingBindingKey), SemanticIndexID: firstNonEmpty(req.Resolve.SemanticIndexID, req.SemanticIndexID), ActorPrincipalID: req.Resolve.ActorPrincipalID, OnBehalfOfPrincipalID: req.Resolve.OnBehalfOfPrincipalID, ProfileID: resolved.Profile.ID, EndpointID: resolved.Endpoint.ID, ModelID: resolved.Model.ID, CapabilityID: resolved.Capability.ID, CredentialID: resolved.Credential.ID, CredentialGrantID: resolved.CredentialGrant.ID, PolicyDecisionID: resolved.Decision.ID, ProviderRequestID: providerRequestID, InputTokens: usage.InputTokens, OutputTokens: usage.OutputTokens, TotalTokens: usage.TotalTokens, LatencyMillis: completed.Sub(started).Milliseconds(), ErrorCode: errorCode, ErrorMessage: errorMessage, StartedAt: started, CompletedAt: completed, Metadata: metadata}
	return m.UsageLedger().AppendUsageEvent(ctx, event)
}

func safeTokenCountSource(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "provider_reported", "estimated", "unavailable":
		return strings.ToLower(strings.TrimSpace(value))
	default:
		return ""
	}
}

func safeUsageCategory(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	switch value {
	case "", "budget_denied", "canceled", "connector_unavailable", "inference_denied", "secret_resolution_failed", "provider_deadline_exhausted",
		"capability_mismatch", "decode_error", "empty_chat_response", "empty_embedding", "empty_input", "empty_messages", "endpoint_disabled",
		"fake_blocked", "fake_failure", "fake_fixture_input_unknown", "fake_fixture_invalid", "fake_fixture_reset", "fake_fixture_script_exhausted",
		"fake_fixture_state_capacity", "fake_refused", "invalid_embedding_dimensions", "missing_endpoint_url", "missing_secret", "network_error",
		"unsupported_connector", "unsupported_operation", "rate_limited", "provider_error", "provider_rejected", "reservation_exceeded":
		return value
	default:
		return "provider_error"
	}
}

func safeUsageErrorMessage(errorCode, value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if errorCode == "budget_denied" {
		switch value {
		case "scope_not_found", "scope_not_active", "scope_expired", "route_mismatch", "duplicate_attempt", "attempt_capacity_exhausted",
			"request_limit_exhausted", "attempt_limit_exhausted", "retry_classification_invalid", "retry_limit_exhausted", "concurrency_limit_exhausted",
			"batch_limit_exceeded", "connector_batching_unsupported", "invalid_reservation", "input_tokens_per_request_exceeded",
			"output_tokens_per_request_exceeded", "input_token_limit_exhausted", "output_token_limit_exhausted", "provider_time_per_request_exceeded",
			"provider_time_limit_exhausted", "price_unavailable", "cost_overflow", "cost_estimate_mismatch", "cost_limit_exhausted",
			"deadline_exhausted", "input_token_reservation_unavailable", "output_token_reservation_unavailable", "budget_denied":
			return value
		}
	}
	return errorCode
}
