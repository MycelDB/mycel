package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	domainembedding "martinbeauvais.com/mbgit/knotbase/knotdb/domain/embedding"
)

type EmbedInput struct {
	Provider domainembedding.ProviderDefinition
	Model    domainembedding.ModelDefinition
	APIKey   string
	Text     string
}

type EmbedOutput struct {
	Vector []float64
}

type Client interface {
	Embed(ctx context.Context, in EmbedInput) (EmbedOutput, error)
}

type HTTPClient struct{ Client *http.Client }

func (c HTTPClient) Embed(ctx context.Context, in EmbedInput) (EmbedOutput, error) {
	switch in.Provider.Protocol {
	case "openai_embeddings":
		return c.embedOpenAI(ctx, in)
	case "ollama_embed":
		return c.embedOllama(ctx, in)
	default:
		return EmbedOutput{}, fmt.Errorf("unsupported embedding protocol %q", in.Provider.Protocol)
	}
}

func (c HTTPClient) httpClient() *http.Client {
	if c.Client != nil {
		return c.Client
	}
	return &http.Client{Timeout: 90 * time.Second}
}

func (c HTTPClient) embedOpenAI(ctx context.Context, in EmbedInput) (EmbedOutput, error) {
	endpoint := strings.TrimRight(in.Provider.DefaultEndpoint, "/") + "/embeddings"
	body := map[string]any{"model": in.Model.Model, "input": in.Text}
	raw, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(raw))
	if err != nil {
		return EmbedOutput{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if in.Provider.AuthStyle == "bearer" && strings.TrimSpace(in.APIKey) != "" {
		req.Header.Set("Authorization", "Bearer "+in.APIKey)
	}
	res, err := c.httpClient().Do(req)
	if err != nil {
		return EmbedOutput{}, err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return EmbedOutput{}, fmt.Errorf("embedding provider returned %s", res.Status)
	}
	var decoded struct {
		Data []struct {
			Embedding []float64 `json:"embedding"`
		} `json:"data"`
	}
	if err := json.NewDecoder(res.Body).Decode(&decoded); err != nil {
		return EmbedOutput{}, err
	}
	if len(decoded.Data) == 0 || len(decoded.Data[0].Embedding) == 0 {
		return EmbedOutput{}, fmt.Errorf("embedding provider returned empty vector")
	}
	return EmbedOutput{Vector: decoded.Data[0].Embedding}, nil
}

func (c HTTPClient) embedOllama(ctx context.Context, in EmbedInput) (EmbedOutput, error) {
	out, status, err := c.postOllamaEmbed(ctx, strings.TrimRight(in.Provider.DefaultEndpoint, "/")+"/api/embed", map[string]any{"model": in.Model.Model, "input": in.Text})
	if err == nil {
		return out, nil
	}
	// Older Ollama releases expose /api/embeddings with a "prompt" field.
	// Fall back on 404 so local installations can be used without catalog churn.
	if status == http.StatusNotFound {
		return c.postOllamaEmbedLegacy(ctx, in)
	}
	return EmbedOutput{}, err
}

func (c HTTPClient) postOllamaEmbedLegacy(ctx context.Context, in EmbedInput) (EmbedOutput, error) {
	out, _, err := c.postOllamaEmbed(ctx, strings.TrimRight(in.Provider.DefaultEndpoint, "/")+"/api/embeddings", map[string]any{"model": in.Model.Model, "prompt": in.Text})
	return out, err
}

func (c HTTPClient) postOllamaEmbed(ctx context.Context, endpoint string, body map[string]any) (EmbedOutput, int, error) {
	raw, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(raw))
	if err != nil {
		return EmbedOutput{}, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := c.httpClient().Do(req)
	if err != nil {
		return EmbedOutput{}, 0, err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return EmbedOutput{}, res.StatusCode, fmt.Errorf("embedding provider returned %s", res.Status)
	}
	var decoded struct {
		Embeddings [][]float64 `json:"embeddings"`
		Embedding  []float64   `json:"embedding"`
	}
	if err := json.NewDecoder(res.Body).Decode(&decoded); err != nil {
		return EmbedOutput{}, res.StatusCode, err
	}
	if len(decoded.Embeddings) > 0 && len(decoded.Embeddings[0]) > 0 {
		return EmbedOutput{Vector: decoded.Embeddings[0]}, res.StatusCode, nil
	}
	if len(decoded.Embedding) > 0 {
		return EmbedOutput{Vector: decoded.Embedding}, res.StatusCode, nil
	}
	return EmbedOutput{}, res.StatusCode, fmt.Errorf("embedding provider returned empty vector")
}
