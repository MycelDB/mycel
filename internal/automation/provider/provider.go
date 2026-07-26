package provider

import (
	"context"
	"errors"
	"strings"
)

var ErrUnavailable = errors.New("automation provider is not configured")

const (
	UsageStatusReported    = "reported"
	UsageStatusEstimated   = "estimated"
	UsageStatusUnavailable = "unavailable"
)

type Request struct {
	Provider        string
	Model           string
	Prompt          string
	Input           string
	Temperature     *float64
	MaxOutputTokens int
}

type Usage struct {
	InputTokens       int64          `json:"input_tokens,omitempty"`
	OutputTokens      int64          `json:"output_tokens,omitempty"`
	TotalTokens       int64          `json:"total_tokens,omitempty"`
	CachedInputTokens int64          `json:"cached_input_tokens,omitempty"`
	ReasoningTokens   int64          `json:"reasoning_tokens,omitempty"`
	Status            string         `json:"status,omitempty"`
	Metadata          map[string]any `json:"metadata,omitempty"`
}

type Response struct {
	Text              string
	ProviderRequestID string
	Usage             Usage
}

type Provider interface {
	GenerateText(context.Context, Request) (Response, error)
}

type FakeProvider struct {
	Text string
}

func (p FakeProvider) GenerateText(_ context.Context, req Request) (Response, error) {
	text := strings.TrimSpace(p.Text)
	if text == "" {
		text = strings.TrimSpace(req.Prompt + "\n\n" + req.Input)
	}
	in := estimateTokens(req.Prompt + "\n" + req.Input)
	out := estimateTokens(text)
	return Response{Text: text, ProviderRequestID: "fake", Usage: Usage{InputTokens: in, OutputTokens: out, TotalTokens: in + out, Status: UsageStatusEstimated, Metadata: map[string]any{"provider": "fake"}}}, nil
}

func estimateTokens(s string) int64 {
	words := strings.Fields(s)
	if len(words) == 0 {
		return 0
	}
	return int64(len(words))
}
