package service

import (
	"errors"
	"testing"

	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/provider"
)

func TestMaxAttemptsDefaultAndOverride(t *testing.T) {
	if got := maxAttempts(automation.Definition{}); got != 3 {
		t.Fatalf("default = %d", got)
	}
	if got := maxAttempts(automation.Definition{Safety: automation.Safety{MaxAttempts: 5}}); got != 5 {
		t.Fatalf("override = %d", got)
	}
}

func TestRetryableAutomationError(t *testing.T) {
	if !retryableAutomationError(provider.ErrUnavailable) {
		t.Fatal("expected unavailable to retry")
	}
	if !retryableAutomationError(errors.New("provider timeout")) {
		t.Fatal("expected timeout to retry")
	}
	if retryableAutomationError(errors.New("invalid json")) {
		t.Fatal("did not expect validation error to retry")
	}
}
