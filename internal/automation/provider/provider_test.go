package provider

import (
	"context"
	"testing"
)

func TestFakeProviderReturnsUsage(t *testing.T) {
	resp, err := FakeProvider{}.GenerateText(context.Background(), Request{Prompt: "hello", Input: "world"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Text == "" {
		t.Fatal("expected text")
	}
	if resp.Usage.InputTokens == 0 || resp.Usage.OutputTokens == 0 || resp.Usage.TotalTokens != resp.Usage.InputTokens+resp.Usage.OutputTokens {
		t.Fatalf("unexpected usage: %+v", resp.Usage)
	}
	if resp.Usage.Status != UsageStatusEstimated {
		t.Fatalf("usage status = %q", resp.Usage.Status)
	}
}
