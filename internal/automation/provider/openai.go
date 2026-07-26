package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type OpenAIConfig struct {
	BaseURL string
	APIKey  string
	Timeout time.Duration
}

type OpenAICompatibleProvider struct {
	cfg    OpenAIConfig
	client *http.Client
}

func NewOpenAICompatibleProvider(cfg OpenAIConfig) (*OpenAICompatibleProvider, error) {
	cfg.BaseURL = strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if cfg.BaseURL == "" {
		cfg.BaseURL = "https://api.openai.com/v1"
	}
	if strings.TrimSpace(cfg.APIKey) == "" {
		return nil, ErrUnavailable
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 60 * time.Second
	}
	return &OpenAICompatibleProvider{cfg: cfg, client: &http.Client{Timeout: cfg.Timeout}}, nil
}

func (p *OpenAICompatibleProvider) GenerateText(ctx context.Context, req Request) (Response, error) {
	model := strings.TrimSpace(req.Model)
	if model == "" {
		return Response{}, fmt.Errorf("automation model is required")
	}
	messages := []map[string]string{{"role": "system", "content": req.Prompt}, {"role": "user", "content": req.Input}}
	body := map[string]any{"model": model, "messages": messages}
	if req.Temperature != nil {
		body["temperature"] = *req.Temperature
	}
	if req.MaxOutputTokens > 0 {
		body["max_tokens"] = req.MaxOutputTokens
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return Response{}, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.cfg.BaseURL+"/chat/completions", bytes.NewReader(raw))
	if err != nil {
		return Response{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+p.cfg.APIKey)
	res, err := p.client.Do(httpReq)
	if err != nil {
		return Response{}, err
	}
	defer res.Body.Close()
	var payload struct {
		ID      string `json:"id"`
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
		Usage struct {
			PromptTokens        int64 `json:"prompt_tokens"`
			CompletionTokens    int64 `json:"completion_tokens"`
			TotalTokens         int64 `json:"total_tokens"`
			PromptTokensDetails struct {
				CachedTokens int64 `json:"cached_tokens"`
			} `json:"prompt_tokens_details"`
			CompletionTokensDetails struct {
				ReasoningTokens int64 `json:"reasoning_tokens"`
			} `json:"completion_tokens_details"`
		} `json:"usage"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		return Response{}, err
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		if payload.Error != nil && strings.TrimSpace(payload.Error.Message) != "" {
			return Response{}, fmt.Errorf("automation provider returned %s: %s", res.Status, payload.Error.Message)
		}
		return Response{}, fmt.Errorf("automation provider returned %s", res.Status)
	}
	if len(payload.Choices) == 0 {
		return Response{}, fmt.Errorf("automation provider returned no choices")
	}
	usage := Usage{InputTokens: payload.Usage.PromptTokens, OutputTokens: payload.Usage.CompletionTokens, TotalTokens: payload.Usage.TotalTokens, CachedInputTokens: payload.Usage.PromptTokensDetails.CachedTokens, ReasoningTokens: payload.Usage.CompletionTokensDetails.ReasoningTokens, Status: UsageStatusReported, Metadata: map[string]any{"protocol": "openai_chat_completions"}}
	if usage.InputTokens == 0 && usage.OutputTokens == 0 && usage.TotalTokens == 0 {
		usage.Status = UsageStatusUnavailable
	}
	return Response{Text: payload.Choices[0].Message.Content, ProviderRequestID: payload.ID, Usage: usage}, nil
}
