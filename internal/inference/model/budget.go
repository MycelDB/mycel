package inference

import "time"

type BudgetScopeID string
type BudgetAttemptID string

type BatchingMode string

const (
	BatchingModeSingle BatchingMode = "single"
	BatchingModeBatch  BatchingMode = "batch"
)

type BudgetScopeState string

const (
	BudgetScopeActive    BudgetScopeState = "active"
	BudgetScopeCompleted BudgetScopeState = "completed"
	BudgetScopeExpired   BudgetScopeState = "expired"
	BudgetScopeExhausted BudgetScopeState = "exhausted"
)

type BudgetAttemptState string

const (
	BudgetAttemptReserved  BudgetAttemptState = "reserved"
	BudgetAttemptSucceeded BudgetAttemptState = "succeeded"
	BudgetAttemptFailed    BudgetAttemptState = "failed"
	BudgetAttemptCanceled  BudgetAttemptState = "canceled"
	BudgetAttemptAmbiguous BudgetAttemptState = "ambiguous"
)

type BudgetLimits struct {
	MaxRequests                  int64        `json:"max_requests,omitempty"`
	MaxAttempts                  int64        `json:"max_attempts,omitempty"`
	MaxRetryAttempts             int64        `json:"max_retry_attempts,omitempty"`
	MaxRetriesPerRequest         int64        `json:"max_retries_per_request,omitempty"`
	MaxConcurrentProviderCalls   int64        `json:"max_concurrent_provider_calls,omitempty"`
	BatchingMode                 BatchingMode `json:"batching_mode"`
	MaxBatchItems                int64        `json:"max_batch_items"`
	MaxInputTokensPerRequest     int64        `json:"max_input_tokens_per_request,omitempty"`
	MaxOutputTokensPerRequest    int64        `json:"max_output_tokens_per_request,omitempty"`
	MaxInputTokens               int64        `json:"max_input_tokens,omitempty"`
	MaxOutputTokens              int64        `json:"max_output_tokens,omitempty"`
	MaxProviderMillisPerRequest  int64        `json:"max_provider_millis_per_request,omitempty"`
	MaxProviderMillis            int64        `json:"max_provider_millis,omitempty"`
	Deadline                     time.Time    `json:"deadline,omitempty"`
	Currency                     string       `json:"currency,omitempty"`
	PriceDefinitionVersion       string       `json:"price_definition_version,omitempty"`
	InputMicrosPerMillionTokens  int64        `json:"input_micros_per_million_tokens,omitempty"`
	OutputMicrosPerMillionTokens int64        `json:"output_micros_per_million_tokens,omitempty"`
	MaxEstimatedCostMicros       int64        `json:"max_estimated_cost_micros,omitempty"`
}

type BudgetRoute struct {
	EndpointID   EndpointID   `json:"endpoint_id,omitempty"`
	ModelID      ModelID      `json:"model_id,omitempty"`
	CapabilityID CapabilityID `json:"capability_id,omitempty"`
	Operation    Operation    `json:"operation,omitempty"`
}

type BudgetUsage struct {
	Requests            int64 `json:"requests"`
	Attempts            int64 `json:"attempts"`
	RetryAttempts       int64 `json:"retry_attempts"`
	BatchItems          int64 `json:"batch_items"`
	ActiveProviderCalls int64 `json:"active_provider_calls"`
	InputTokens         int64 `json:"input_tokens"`
	OutputTokens        int64 `json:"output_tokens"`
	ProviderMillis      int64 `json:"provider_millis"`
	EstimatedCostMicros int64 `json:"estimated_cost_micros"`
	AmbiguousAttempts   int64 `json:"ambiguous_attempts"`
	AccountingOverflow  bool  `json:"accounting_overflow,omitempty"`
}

type BudgetScope struct {
	ID                  BudgetScopeID        `json:"id"`
	ScopeSequence       uint64               `json:"scope_sequence,omitempty"`
	SpaceID             string               `json:"space_id,omitempty"`
	SemanticRuleID      string               `json:"semantic_rule_id,omitempty"`
	EmbeddingBindingKey string               `json:"embedding_binding_key,omitempty"`
	CorrelationID       string               `json:"correlation_id,omitempty"`
	State               BudgetScopeState     `json:"state"`
	Route               BudgetRoute          `json:"route"`
	Limits              BudgetLimits         `json:"limits"`
	Usage               BudgetUsage          `json:"usage"`
	Attempts            []BudgetAttempt      `json:"attempts,omitempty"`
	AttemptEvents       []BudgetAttemptEvent `json:"attempt_events,omitempty"`
	CreatedAt           time.Time            `json:"created_at"`
	UpdatedAt           time.Time            `json:"updated_at"`
	ExpiresAt           time.Time            `json:"expires_at"`
	RetentionUntil      time.Time            `json:"retention_until"`
	DenialReason        string               `json:"denial_reason,omitempty"`
}

type BudgetAttempt struct {
	ID                     BudgetAttemptID    `json:"id"`
	RequestID              string             `json:"request_id"`
	State                  BudgetAttemptState `json:"state"`
	Retry                  bool               `json:"retry,omitempty"`
	RetryIndex             int64              `json:"retry_index,omitempty"`
	BatchItems             int64              `json:"batch_items"`
	ReservedInputTokens    int64              `json:"reserved_input_tokens"`
	ReservedOutputTokens   int64              `json:"reserved_output_tokens"`
	ChargedInputTokens     int64              `json:"charged_input_tokens"`
	ChargedOutputTokens    int64              `json:"charged_output_tokens"`
	ReservedProviderMillis int64              `json:"reserved_provider_millis"`
	ChargedProviderMillis  int64              `json:"charged_provider_millis"`
	ReservedCostMicros     int64              `json:"reserved_cost_micros"`
	ChargedCostMicros      int64              `json:"charged_cost_micros"`
	UsageKnown             bool               `json:"usage_known,omitempty"`
	OutcomeKnown           bool               `json:"outcome_known,omitempty"`
	ProviderDurationKnown  bool               `json:"provider_duration_known,omitempty"`
	ReservedAt             time.Time          `json:"reserved_at"`
	ReservedUntil          time.Time          `json:"reserved_until"`
	CompletedAt            time.Time          `json:"completed_at,omitempty"`
	Outcome                string             `json:"outcome,omitempty"`
	ChargedCostSaturated   bool               `json:"charged_cost_saturated,omitempty"`
}

type BudgetAttemptEvent struct {
	Sequence       uint64        `json:"sequence"`
	AttemptVersion uint32        `json:"attempt_version"`
	Attempt        BudgetAttempt `json:"attempt"`
	RecordedAt     time.Time     `json:"recorded_at"`
}

type BudgetReservationRequest struct {
	ScopeID             BudgetScopeID
	AttemptID           BudgetAttemptID
	RequestID           string
	Route               BudgetRoute
	Retry               bool
	RetryIndex          int64
	BatchItems          int64
	InputTokens         int64
	OutputTokens        int64
	ProviderMillis      int64
	EstimatedCostMicros int64
	Now                 time.Time
}

type BudgetCompletionRequest struct {
	ScopeID               BudgetScopeID
	AttemptID             BudgetAttemptID
	State                 BudgetAttemptState
	InputTokens           int64
	OutputTokens          int64
	ProviderMillis        int64
	CostMicros            int64
	CostSaturated         bool
	UsageKnown            bool
	OutcomeKnown          bool
	ProviderDurationKnown bool
	Outcome               string
	CompletedAt           time.Time
}
