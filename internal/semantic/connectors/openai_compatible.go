package connectors

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
)

type OpenAICompatible struct{ HTTPClient *http.Client }

func (c OpenAICompatible) Embed(ctx context.Context, in EmbeddingRequest) (EmbeddingResponse, error) {
	if !in.Endpoint.Enabled {
		return EmbeddingResponse{}, fmt.Errorf("model endpoint %s is disabled", in.Endpoint.ID)
	}
	if in.Endpoint.ConnectorType != domainsemantic.ConnectorOpenAICompatible {
		return EmbeddingResponse{}, fmt.Errorf("connector %q is not openai-compatible", in.Endpoint.ConnectorType)
	}
	if in.Model.Operation != domainsemantic.OperationEmbeddings {
		return EmbeddingResponse{}, fmt.Errorf("model %s does not support embeddings", in.Model.ID)
	}
	if !in.Capability.Enabled || in.Capability.ModelEndpointID != in.Endpoint.ID || in.Capability.ModelID != in.Model.ID || in.Capability.Operation != domainsemantic.OperationEmbeddings {
		return EmbeddingResponse{}, fmt.Errorf("enabled embedding capability not found for endpoint=%s model=%s", in.Endpoint.ID, in.Model.ID)
	}
	if in.Credential.Status != domainsemantic.CredentialStatusActive {
		return EmbeddingResponse{}, fmt.Errorf("credential %s is not active", in.Credential.ID)
	}
	if in.Credential.ModelEndpointID != in.Endpoint.ID {
		return EmbeddingResponse{}, fmt.Errorf("credential %s is not for endpoint %s", in.Credential.ID, in.Endpoint.ID)
	}
	if strings.TrimSpace(in.Input) == "" {
		return EmbeddingResponse{}, fmt.Errorf("embedding input is required")
	}
	modelName := in.Model.ModelName
	if strings.TrimSpace(in.Capability.ModelNameOverride) != "" {
		modelName = in.Capability.ModelNameOverride
	}
	endpoint := strings.TrimRight(in.Endpoint.EndpointURL, "/")
	if endpoint == "" {
		return EmbeddingResponse{}, fmt.Errorf("endpoint url is required")
	}
	if !strings.HasSuffix(endpoint, "/embeddings") {
		endpoint += "/embeddings"
	}
	body := map[string]any{"model": modelName, "input": in.Input}
	raw, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(raw))
	if err != nil {
		return EmbeddingResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if strings.TrimSpace(in.Secret) != "" {
		switch in.Credential.AuthType {
		case domainsemantic.AuthModeBearerToken, domainsemantic.AuthModeAPIKey:
			req.Header.Set("Authorization", "Bearer "+in.Secret)
		default:
			return EmbeddingResponse{}, fmt.Errorf("unsupported auth mode %q for openai-compatible connector", in.Credential.AuthType)
		}
	}
	res, err := c.httpClient().Do(req)
	if err != nil {
		return EmbeddingResponse{}, err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		var providerErr struct {
			Error struct {
				Message string `json:"message"`
				Type    string `json:"type"`
				Code    string `json:"code"`
			} `json:"error"`
		}
		_ = json.NewDecoder(res.Body).Decode(&providerErr)
		msg := strings.TrimSpace(providerErr.Error.Message)
		if msg == "" {
			msg = res.Status
		}
		return EmbeddingResponse{ProviderRequestID: res.Header.Get("X-Request-Id")}, fmt.Errorf("embedding provider returned %s: %s", res.Status, msg)
	}
	var decoded struct {
		Data []struct {
			Embedding []float64 `json:"embedding"`
		} `json:"data"`
		Usage struct {
			PromptTokens int `json:"prompt_tokens"`
			TotalTokens  int `json:"total_tokens"`
		} `json:"usage"`
	}
	if err := json.NewDecoder(res.Body).Decode(&decoded); err != nil {
		return EmbeddingResponse{}, err
	}
	if len(decoded.Data) == 0 || len(decoded.Data[0].Embedding) == 0 {
		return EmbeddingResponse{}, fmt.Errorf("embedding provider returned empty vector")
	}
	out := EmbeddingResponse{Vector: decoded.Data[0].Embedding, InputTokens: decoded.Usage.PromptTokens, TotalTokens: decoded.Usage.TotalTokens, TokenCountSource: "provider_reported", ProviderRequestID: res.Header.Get("X-Request-Id")}
	if out.TotalTokens == 0 && out.InputTokens > 0 {
		out.TotalTokens = out.InputTokens
	}
	if out.TotalTokens == 0 {
		out.TokenCountSource = "unavailable"
	}
	return out, nil
}

func (c OpenAICompatible) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return &http.Client{Timeout: 90 * time.Second}
}
