package connectors

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	domaininference "github.com/myceldb/mycel/internal/inference/model"
)

type OpenAICompatible struct{ HTTPClient *http.Client }

func (c OpenAICompatible) Embed(ctx context.Context, req EmbeddingRequest) (EmbeddingResponse, error) {
	if err := validateEmbeddingRequest(req); err != nil {
		return EmbeddingResponse{}, err
	}
	modelName := providerModelName(req.Model, req.Capability)
	body := map[string]any{"model": modelName, "input": req.Input}
	raw, err := json.Marshal(body)
	if err != nil {
		return EmbeddingResponse{}, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpointURL(req.Endpoint.BaseURL, "/embeddings"), bytes.NewReader(raw))
	if err != nil {
		return EmbeddingResponse{}, err
	}
	setHeaders(httpReq, req.Credential, req.Secret)
	res, err := c.httpClient().Do(httpReq)
	if err != nil {
		return EmbeddingResponse{}, ConnectorError{Code: "network_error", Retryable: true, Err: err}
	}
	defer res.Body.Close()
	requestID := firstHeader(res, "X-Request-Id", "X-Request-ID", "OpenAI-Request-ID")
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return EmbeddingResponse{ProviderRequestID: requestID}, decodeProviderError(res, "embedding provider")
	}
	var decoded struct {
		Data []struct {
			Embedding []float64 `json:"embedding"`
		} `json:"data"`
		Usage struct {
			PromptTokens int64 `json:"prompt_tokens"`
			TotalTokens  int64 `json:"total_tokens"`
		} `json:"usage"`
	}
	if err := json.NewDecoder(res.Body).Decode(&decoded); err != nil {
		return EmbeddingResponse{}, ConnectorError{Code: "decode_error", Err: err}
	}
	if len(decoded.Data) == 0 || len(decoded.Data[0].Embedding) == 0 {
		return EmbeddingResponse{}, ConnectorError{Code: "empty_embedding", Err: fmt.Errorf("embedding provider returned empty vector")}
	}
	usage := Usage{InputTokens: decoded.Usage.PromptTokens, TotalTokens: decoded.Usage.TotalTokens, TokenCountSource: "provider_reported"}
	if usage.TotalTokens == 0 && usage.InputTokens > 0 {
		usage.TotalTokens = usage.InputTokens
	}
	if usage.TotalTokens == 0 {
		usage.TokenCountSource = "unavailable"
	}
	return EmbeddingResponse{Vector: decoded.Data[0].Embedding, ProviderRequestID: requestID, Usage: usage}, nil
}

func (c OpenAICompatible) Chat(ctx context.Context, req ChatRequest) (ChatResponse, error) {
	if err := validateChatRequest(req); err != nil {
		return ChatResponse{}, err
	}
	body := map[string]any{"model": providerModelName(req.Model, req.Capability), "messages": req.Messages}
	if req.Parameters.Temperature != nil {
		body["temperature"] = *req.Parameters.Temperature
	}
	if req.Parameters.MaxOutputTokens > 0 {
		body["max_tokens"] = req.Parameters.MaxOutputTokens
	}
	if responseFormat := strings.ToLower(strings.TrimSpace(req.Parameters.ResponseFormat)); responseFormat == "json" || responseFormat == "json_object" {
		body["response_format"] = map[string]string{"type": "json_object"}
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return ChatResponse{}, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpointURL(req.Endpoint.BaseURL, "/chat/completions"), bytes.NewReader(raw))
	if err != nil {
		return ChatResponse{}, err
	}
	setHeaders(httpReq, req.Credential, req.Secret)
	res, err := c.httpClient().Do(httpReq)
	if err != nil {
		return ChatResponse{}, ConnectorError{Code: "network_error", Retryable: true, Err: err}
	}
	defer res.Body.Close()
	requestID := firstHeader(res, "X-Request-Id", "X-Request-ID", "OpenAI-Request-ID")
	var decoded struct {
		ID      string `json:"id"`
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
		Usage struct {
			PromptTokens     int64 `json:"prompt_tokens"`
			CompletionTokens int64 `json:"completion_tokens"`
			TotalTokens      int64 `json:"total_tokens"`
		} `json:"usage"`
		Error *struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    string `json:"code"`
		} `json:"error"`
	}
	if err := json.NewDecoder(res.Body).Decode(&decoded); err != nil {
		return ChatResponse{ProviderRequestID: requestID}, ConnectorError{Code: "decode_error", Err: err}
	}
	if requestID == "" {
		requestID = decoded.ID
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return ChatResponse{ProviderRequestID: requestID}, providerPayloadError(res.StatusCode, res.Status, decoded.Error, "chat provider")
	}
	if len(decoded.Choices) == 0 {
		return ChatResponse{ProviderRequestID: requestID}, ConnectorError{Code: "empty_chat_response", Err: fmt.Errorf("chat provider returned no choices")}
	}
	text := decoded.Choices[0].Message.Content
	usage := Usage{InputTokens: decoded.Usage.PromptTokens, OutputTokens: decoded.Usage.CompletionTokens, TotalTokens: decoded.Usage.TotalTokens, TokenCountSource: "provider_reported"}
	if usage.TotalTokens == 0 && (usage.InputTokens > 0 || usage.OutputTokens > 0) {
		usage.TotalTokens = usage.InputTokens + usage.OutputTokens
	}
	if usage.TotalTokens == 0 {
		usage.TokenCountSource = "unavailable"
	}
	out := ChatResponse{Text: text, ProviderRequestID: requestID, Usage: usage}
	if responseFormat := strings.ToLower(strings.TrimSpace(req.Parameters.ResponseFormat)); responseFormat == "json" || responseFormat == "json_object" {
		var payload map[string]any
		if err := json.Unmarshal([]byte(text), &payload); err == nil {
			out.JSON = payload
		}
	}
	return out, nil
}

func validateEmbeddingRequest(req EmbeddingRequest) error {
	if !req.Endpoint.Enabled {
		return ConnectorError{Code: "endpoint_disabled", Err: fmt.Errorf("endpoint %s is disabled", req.Endpoint.ID)}
	}
	if req.Endpoint.ConnectorType != domaininference.ConnectorOpenAICompatible {
		return ConnectorError{Code: "unsupported_connector", Err: fmt.Errorf("connector %q is not openai-compatible", req.Endpoint.ConnectorType)}
	}
	if req.Model.Kind != domaininference.ModelKindEmbedding || req.Capability.Operation != domaininference.OperationEmbeddings {
		return ConnectorError{Code: "unsupported_operation", Err: fmt.Errorf("model/capability does not support embeddings")}
	}
	if req.Capability.EndpointID != req.Endpoint.ID || req.Capability.ModelID != req.Model.ID || !req.Capability.Enabled {
		return ConnectorError{Code: "capability_mismatch", Err: fmt.Errorf("enabled capability not found for endpoint/model")}
	}
	if req.Credential.AuthType != domaininference.CredentialAuthNone && strings.TrimSpace(req.Secret) == "" {
		return ConnectorError{Code: "missing_secret", Err: fmt.Errorf("credential secret is required")}
	}
	if strings.TrimSpace(req.Input) == "" {
		return ConnectorError{Code: "empty_input", Err: fmt.Errorf("embedding input is required")}
	}
	if strings.TrimSpace(req.Endpoint.BaseURL) == "" {
		return ConnectorError{Code: "missing_endpoint_url", Err: fmt.Errorf("endpoint base_url is required")}
	}
	return nil
}

func validateChatRequest(req ChatRequest) error {
	if !req.Endpoint.Enabled {
		return ConnectorError{Code: "endpoint_disabled", Err: fmt.Errorf("endpoint %s is disabled", req.Endpoint.ID)}
	}
	if req.Endpoint.ConnectorType != domaininference.ConnectorOpenAICompatible {
		return ConnectorError{Code: "unsupported_connector", Err: fmt.Errorf("connector %q is not openai-compatible", req.Endpoint.ConnectorType)}
	}
	if req.Model.Kind != domaininference.ModelKindGenerative || !isChatLike(req.Capability.Operation) {
		return ConnectorError{Code: "unsupported_operation", Err: fmt.Errorf("model/capability does not support chat generation")}
	}
	if req.Capability.Operation == domaininference.OperationImageAnalysis && (!connectorModalityListContains(req.Model.InputModalities, "image") || (!connectorModalityListContains(req.Model.OutputModalities, "text") && !connectorModalityListContains(req.Model.OutputModalities, "json"))) {
		return ConnectorError{Code: "unsupported_operation", Err: fmt.Errorf("model/capability does not support image analysis modalities")}
	}
	if req.Capability.EndpointID != req.Endpoint.ID || req.Capability.ModelID != req.Model.ID || !req.Capability.Enabled {
		return ConnectorError{Code: "capability_mismatch", Err: fmt.Errorf("enabled capability not found for endpoint/model")}
	}
	if req.Credential.AuthType != domaininference.CredentialAuthNone && strings.TrimSpace(req.Secret) == "" {
		return ConnectorError{Code: "missing_secret", Err: fmt.Errorf("credential secret is required")}
	}
	if len(req.Messages) == 0 {
		return ConnectorError{Code: "empty_messages", Err: fmt.Errorf("chat messages are required")}
	}
	if strings.TrimSpace(req.Endpoint.BaseURL) == "" {
		return ConnectorError{Code: "missing_endpoint_url", Err: fmt.Errorf("endpoint base_url is required")}
	}
	return nil
}

func isChatLike(op domaininference.Operation) bool {
	switch op {
	case domaininference.OperationChat, domaininference.OperationSummarize, domaininference.OperationClassify, domaininference.OperationImageAnalysis:
		return true
	default:
		return false
	}
}

func connectorModalityListContains(values []string, value string) bool {
	for _, item := range values {
		if strings.EqualFold(strings.TrimSpace(item), value) {
			return true
		}
	}
	return false
}

func providerModelName(model domaininference.Model, capability domaininference.Capability) string {
	if strings.TrimSpace(capability.ProviderModelOverride) != "" {
		return capability.ProviderModelOverride
	}
	return model.ProviderModelName
}

func endpointURL(baseURL, suffix string) string {
	endpoint := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if strings.HasSuffix(endpoint, suffix) {
		return endpoint
	}
	return endpoint + suffix
}

func setHeaders(req *http.Request, credential domaininference.Credential, secret string) {
	req.Header.Set("Content-Type", "application/json")
	if strings.TrimSpace(secret) == "" {
		return
	}
	switch credential.AuthType {
	case domaininference.CredentialAuthBearer, domaininference.CredentialAuthAPIKey:
		req.Header.Set("Authorization", "Bearer "+secret)
	case domaininference.CredentialAuthBasic:
		req.Header.Set("Authorization", "Basic "+secret)
	}
}

func decodeProviderError(res *http.Response, prefix string) error {
	var payload struct {
		Error *struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    string `json:"code"`
		} `json:"error"`
	}
	_ = json.NewDecoder(res.Body).Decode(&payload)
	return providerPayloadError(res.StatusCode, res.Status, payload.Error, prefix)
}

func providerPayloadError(statusCode int, status string, payload *struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    string `json:"code"`
}, prefix string) error {
	msg := status
	code := fmt.Sprintf("provider_http_%d", statusCode)
	if payload != nil {
		if strings.TrimSpace(payload.Message) != "" {
			msg = payload.Message
		}
		if strings.TrimSpace(payload.Code) != "" {
			code = payload.Code
		} else if strings.TrimSpace(payload.Type) != "" {
			code = payload.Type
		}
	}
	return ConnectorError{Code: code, Retryable: statusCode == http.StatusTooManyRequests || statusCode >= 500, Err: fmt.Errorf("%s returned %s: %s", prefix, status, msg)}
}

func firstHeader(res *http.Response, names ...string) string {
	for _, name := range names {
		if value := strings.TrimSpace(res.Header.Get(name)); value != "" {
			return value
		}
	}
	return ""
}

func (c OpenAICompatible) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return &http.Client{Timeout: 90 * time.Second}
}
