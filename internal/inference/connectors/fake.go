package connectors

import (
	"context"
	"errors"
	"strings"
	"sync"
)

type FakeConnector struct {
	mu        sync.Mutex
	Text      string
	Vector    []float64
	Err       error
	EmbedHits int
	ChatHits  int
}

func (f *FakeConnector) Embed(ctx context.Context, req EmbeddingRequest) (EmbeddingResponse, error) {
	if err := ctx.Err(); err != nil {
		return EmbeddingResponse{}, err
	}
	f.mu.Lock()
	f.EmbedHits++
	vector := append([]float64(nil), f.Vector...)
	err := f.Err
	f.mu.Unlock()
	if err != nil {
		return EmbeddingResponse{ProviderRequestID: "fake"}, err
	}
	if len(vector) == 0 {
		vector = []float64{1, 0, 0}
	}
	in := EstimateTokens(req.Input)
	return EmbeddingResponse{Vector: vector, ProviderRequestID: "fake", Usage: Usage{InputTokens: in, TotalTokens: in, TokenCountSource: "estimated"}}, nil
}

func (f *FakeConnector) Chat(ctx context.Context, req ChatRequest) (ChatResponse, error) {
	if err := ctx.Err(); err != nil {
		return ChatResponse{}, err
	}
	f.mu.Lock()
	f.ChatHits++
	text := strings.TrimSpace(f.Text)
	err := f.Err
	f.mu.Unlock()
	if err != nil {
		return ChatResponse{ProviderRequestID: "fake"}, err
	}
	if text == "" {
		parts := make([]string, 0, len(req.Messages))
		for _, msg := range req.Messages {
			parts = append(parts, strings.TrimSpace(msg.Content))
		}
		text = strings.TrimSpace(strings.Join(parts, "\n\n"))
	}
	if text == "" {
		text = "fake response"
	}
	in := int64(0)
	for _, msg := range req.Messages {
		in += EstimateTokens(msg.Content)
	}
	out := EstimateTokens(text)
	resp := ChatResponse{Text: text, ProviderRequestID: "fake", Usage: Usage{InputTokens: in, OutputTokens: out, TotalTokens: in + out, TokenCountSource: "estimated"}}
	if strings.EqualFold(req.Parameters.ResponseFormat, "json") || strings.EqualFold(req.Parameters.ResponseFormat, "json_object") {
		resp.JSON = map[string]any{"text": text}
	}
	return resp, nil
}

func (f *FakeConnector) Calls() (embed int, chat int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.EmbedHits, f.ChatHits
}

func NewFakeError(code string, retryable bool, message string) error {
	return ConnectorError{Code: code, Retryable: retryable, Err: errors.New(message)}
}
