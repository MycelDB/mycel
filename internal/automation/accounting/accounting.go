package accounting

import (
	"strings"

	automation "github.com/myceldb/mycel/internal/automation/model"
)

const (
	CostStatusEstimated   = "estimated"
	CostStatusUnavailable = "unavailable"
)

type Price struct {
	InputPerMillion  float64
	OutputPerMillion float64
	Currency         string
	Version          string
}

type Estimator struct{ Prices map[string]Price }

func DefaultEstimator() Estimator {
	return Estimator{Prices: map[string]Price{
		"openai:gpt-4o-mini": {InputPerMillion: 0.15, OutputPerMillion: 0.60, Currency: "USD", Version: "2024-07"},
	}}
}

func (e Estimator) Estimate(provider, model string, usage automation.TokenUsage) automation.CostEstimate {
	key := strings.ToLower(strings.TrimSpace(provider)) + ":" + strings.ToLower(strings.TrimSpace(model))
	price, ok := e.Prices[key]
	if !ok || usage.TotalTokens == 0 {
		return automation.CostEstimate{Status: CostStatusUnavailable}
	}
	input := float64(usage.InputTokens) / 1_000_000 * price.InputPerMillion
	output := float64(usage.OutputTokens) / 1_000_000 * price.OutputPerMillion
	return automation.CostEstimate{InputCost: input, OutputCost: output, TotalCost: input + output, Currency: price.Currency, PricingVersion: price.Version, Status: CostStatusEstimated}
}
