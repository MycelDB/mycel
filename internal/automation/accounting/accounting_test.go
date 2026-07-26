package accounting

import (
	"testing"

	automation "github.com/myceldb/mycel/internal/automation/model"
)

func TestEstimatorEstimatesKnownModel(t *testing.T) {
	est := DefaultEstimator()
	cost := est.Estimate("openai", "gpt-4o-mini", automation.TokenUsage{InputTokens: 1_000_000, OutputTokens: 1_000_000, TotalTokens: 2_000_000})
	if cost.Status != CostStatusEstimated || cost.Currency != "USD" || cost.TotalCost <= 0 {
		t.Fatalf("unexpected cost: %+v", cost)
	}
}

func TestEstimatorUnknownModelUnavailable(t *testing.T) {
	cost := DefaultEstimator().Estimate("unknown", "model", automation.TokenUsage{InputTokens: 1, OutputTokens: 1, TotalTokens: 2})
	if cost.Status != CostStatusUnavailable {
		t.Fatalf("status = %q", cost.Status)
	}
}
