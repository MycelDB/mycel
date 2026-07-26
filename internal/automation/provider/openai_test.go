package provider

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestOpenAICompatibleProviderGenerateTextRecordsReportedUsage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Fatalf("authorization header = %q", r.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"req-1","choices":[{"message":{"content":"hello automation"}}],"usage":{"prompt_tokens":7,"completion_tokens":3,"total_tokens":10,"prompt_tokens_details":{"cached_tokens":2},"completion_tokens_details":{"reasoning_tokens":1}}}`))
	}))
	defer server.Close()
	p, err := NewOpenAICompatibleProvider(OpenAIConfig{BaseURL: server.URL, APIKey: "test-key"})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := p.GenerateText(context.Background(), Request{Model: "gpt-test", Prompt: "prompt", Input: "input"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Text != "hello automation" || resp.ProviderRequestID != "req-1" {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if resp.Usage.Status != UsageStatusReported || resp.Usage.InputTokens != 7 || resp.Usage.OutputTokens != 3 || resp.Usage.TotalTokens != 10 || resp.Usage.CachedInputTokens != 2 || resp.Usage.ReasoningTokens != 1 {
		t.Fatalf("unexpected usage: %+v", resp.Usage)
	}
}

func TestOpenAICompatibleProviderRequiresAPIKey(t *testing.T) {
	_, err := NewOpenAICompatibleProvider(OpenAIConfig{})
	if err != ErrUnavailable {
		t.Fatalf("error = %v, want ErrUnavailable", err)
	}
}
