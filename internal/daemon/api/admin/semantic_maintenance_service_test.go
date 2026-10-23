package admin

import (
	"errors"
	"strings"
	"testing"

	semanticbackfill "github.com/myceldb/mycel/internal/semantic/backfill"
)

func TestMapBackfillResponseSanitizesProviderFailure(t *testing.T) {
	const secret = "provider-body-secret"
	response := mapBackfillResponse(semanticbackfill.Result{FailedCount: 1, Failures: []semanticbackfill.Failure{{Error: "upstream 500: " + secret}}})
	if len(response.GetFailures()) != 1 {
		t.Fatalf("failures = %d, want 1", len(response.GetFailures()))
	}
	got := response.GetFailures()[0].GetError()
	if strings.Contains(got, secret) || got != "semantic backfill failed" {
		t.Fatalf("unsafe backfill diagnostic %q", got)
	}
}

func TestMapBackfillResponsePreservesSafeRateLimitDiagnostic(t *testing.T) {
	response := mapBackfillResponse(semanticbackfill.Result{FailedCount: 1, Failures: []semanticbackfill.Failure{{Error: "semantic embedding failed (rate_limited)", Category: "rate_limited", Retryable: true}}})
	if got := response.GetFailures()[0].GetError(); got != "semantic provider rate limited" {
		t.Fatalf("rate-limit diagnostic=%q", got)
	}
}

func TestMapAdminSemanticMaintenanceErrorSanitizesProviderFailure(t *testing.T) {
	const secret = "provider-body-secret"
	err := mapAdminSemanticMaintenanceError(errors.New("upstream 500: "+secret), "backfill semantic rule")
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("unsafe maintenance error %v", err)
	}
}
