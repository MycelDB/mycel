package service

import (
	"context"
	"errors"
	"testing"

	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/provider"
)

func TestGenerateTextRequiresConfiguredProvider(t *testing.T) {
	mgr := NewManager(nil)
	run := automation.Run{}
	_, err := mgr.generateText(context.Background(), automation.Definition{Prompt: "prompt"}, "input", &run)
	if !errors.Is(err, provider.ErrUnavailable) {
		t.Fatalf("generateText() error = %v, want ErrUnavailable", err)
	}
	if run.Usage.Status != provider.UsageStatusUnavailable {
		t.Fatalf("usage status = %q", run.Usage.Status)
	}
}

func TestGenerateTextRecordsProviderUsage(t *testing.T) {
	mgr := NewManager(nil).WithProvider(provider.FakeProvider{Text: "result text"})
	run := automation.Run{}
	text, err := mgr.generateText(context.Background(), automation.Definition{Prompt: "prompt", Model: automation.Model{Provider: "fake", Model: "test"}}, "input words", &run)
	if err != nil {
		t.Fatal(err)
	}
	if text != "result text" {
		t.Fatalf("text = %q", text)
	}
	if run.ProviderRequestID != "fake" {
		t.Fatalf("provider request id = %q", run.ProviderRequestID)
	}
	if run.Usage.Status != provider.UsageStatusEstimated || run.Usage.InputTokens == 0 || run.Usage.OutputTokens == 0 || run.Usage.TotalTokens == 0 {
		t.Fatalf("unexpected usage: %+v", run.Usage)
	}
}

type noUsageProvider struct{}

func (noUsageProvider) GenerateText(context.Context, provider.Request) (provider.Response, error) {
	return provider.Response{Text: "ok"}, nil
}

func TestGenerateTextEnforcesTokenCeiling(t *testing.T) {
	mgr := NewManager(nil).WithProvider(provider.FakeProvider{Text: "many output tokens"}).WithRunCeilings(1, 0)
	run := automation.Run{}
	_, err := mgr.generateText(context.Background(), automation.Definition{Prompt: "prompt", Model: automation.Model{Provider: "fake", Model: "test"}}, "input", &run)
	if err == nil {
		t.Fatal("expected token ceiling error")
	}
}

func TestGenerateTextMarksMissingUsageUnavailable(t *testing.T) {
	mgr := NewManager(nil).WithProvider(noUsageProvider{})
	run := automation.Run{}
	_, err := mgr.generateText(context.Background(), automation.Definition{Prompt: "prompt"}, "input", &run)
	if err != nil {
		t.Fatal(err)
	}
	if run.Usage.Status != provider.UsageStatusUnavailable {
		t.Fatalf("usage status = %q", run.Usage.Status)
	}
}
