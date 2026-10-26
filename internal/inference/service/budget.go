package service

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/inference/connectors"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	inferenceruntime "github.com/myceldb/mycel/internal/inference/runtime"
	inferencestorage "github.com/myceldb/mycel/internal/inference/storage"
	"github.com/tiktoken-go/tokenizer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	cl100kTokenizerOnce sync.Once
	cl100kTokenizer     tokenizer.Codec
	cl100kTokenizerErr  error
)

const (
	maxBudgetScopesRetained = 1024
	maxBudgetAttempts       = 10_000
	maxBudgetBatchItems     = 1_000
	maxBudgetIdentityLength = 128
	maxBudgetOutcomeLength  = 64
	maxBudgetRetention      = 30 * 24 * time.Hour
)

var budgetIdentityPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]*$`)

var allowedBudgetOutcomes = map[string]struct{}{
	"succeeded":                   {},
	"canceled":                    {},
	"deadline_exceeded":           {},
	"secret_resolution_failed":    {},
	"provider_deadline_exhausted": {},
	"provider_transport_error":    {},
	"provider_rate_limited":       {},
	"provider_unavailable":        {},
	"provider_rejected":           {},
	"connector_error":             {},
	"unknown":                     {},
}

var (
	ErrBudgetDenied           = errors.New("inference budget denied")
	ErrBudgetAttemptDuplicate = errors.New("inference budget attempt already reserved")
	ErrBudgetAccounting       = errors.New("inference budget accounting exceeded reservation")
	ErrBudgetPersistence      = errors.New("inference budget persistence requires recovery")
)

type BudgetError struct {
	Reason string
	Err    error
}

func (e BudgetError) Error() string {
	if e.Err == nil {
		return "inference budget denied: " + e.Reason
	}
	return "inference budget denied: " + e.Reason + ": " + e.Err.Error()
}

func (e BudgetError) Unwrap() error {
	if e.Err != nil {
		return e.Err
	}
	return ErrBudgetDenied
}

func (e BudgetError) Is(target error) bool {
	return target == ErrBudgetDenied || (e.Err != nil && errors.Is(e.Err, target))
}

func (m *Module) requireBudgetPersistence() error {
	if m.budgetPersistenceErr != nil {
		return ErrBudgetPersistence
	}
	return nil
}

func (m *Module) failBudgetPersistence(err error) error {
	if err != nil {
		m.budgetPersistenceErr = err
	}
	return ErrBudgetPersistence
}

func (m *Module) handleBudgetMutationError(err error) error {
	if status.Code(err) == codes.FailedPrecondition {
		return err
	}
	return m.failBudgetPersistence(err)
}

func (m *Module) CreateBudgetScope(ctx context.Context, scope domaininference.BudgetScope) (domaininference.BudgetScope, error) {
	m.budgetMu.Lock()
	defer m.budgetMu.Unlock()
	if err := m.requireBudgetPersistence(); err != nil {
		return domaininference.BudgetScope{}, err
	}
	if err := m.RequireBudgetWriteAllowed(); err != nil {
		return domaininference.BudgetScope{}, err
	}
	now := time.Now().UTC()
	if scope.CreatedAt.IsZero() {
		scope.CreatedAt = now
	}
	if scope.UpdatedAt.IsZero() {
		scope.UpdatedAt = scope.CreatedAt
	}
	if scope.State == "" {
		scope.State = domaininference.BudgetScopeActive
	}
	if scope.ExpiresAt.IsZero() {
		scope.ExpiresAt = scope.Limits.Deadline
	}
	if scope.RetentionUntil.IsZero() {
		scope.RetentionUntil = scope.ExpiresAt.Add(maxBudgetRetention)
	}
	if err := validateBudgetScope(scope, now); err != nil {
		return domaininference.BudgetScope{}, err
	}
	ledger := m.UsageLedger()
	if _, err := ledger.GetBudgetScope(ctx, scope.ID); err == nil {
		return domaininference.BudgetScope{}, fmt.Errorf("budget scope %q already exists", scope.ID)
	} else if !errors.Is(err, inferencestorage.ErrBudgetScopeNotFound) {
		return domaininference.BudgetScope{}, err
	}
	scopes, err := ledger.ListBudgetScopes(ctx)
	if err != nil {
		return domaininference.BudgetScope{}, err
	}
	deleted, err := pruneEligibleBudgetScopes(ctx, ledger, scopes, now, maxBudgetScopesRetained)
	if err != nil {
		return domaininference.BudgetScope{}, m.handleBudgetMutationError(err)
	}
	if deleted > 0 {
		scopes, err = ledger.ListBudgetScopes(ctx)
		if err != nil {
			return domaininference.BudgetScope{}, err
		}
	}
	if len(scopes) >= maxBudgetScopesRetained {
		return domaininference.BudgetScope{}, BudgetError{Reason: "scope_capacity_exhausted"}
	}
	persisted, err := ledger.UpsertBudgetScope(ctx, scope)
	if err != nil {
		return domaininference.BudgetScope{}, m.handleBudgetMutationError(err)
	}
	return persisted, nil
}

func (m *Module) GetBudgetScope(ctx context.Context, id domaininference.BudgetScopeID) (domaininference.BudgetScope, error) {
	scope, err := m.UsageLedger().GetBudgetScope(ctx, id)
	if err != nil {
		return domaininference.BudgetScope{}, err
	}
	now := time.Now().UTC()
	scope.Usage = calculateBudgetUsage(scope.Attempts, now)
	if scope.State == domaininference.BudgetScopeActive && (!scope.ExpiresAt.After(now) || !scope.Limits.Deadline.After(now)) {
		scope.State = domaininference.BudgetScopeExpired
		scope.DenialReason = "scope_expired"
	}
	return scope, nil
}

func (m *Module) ListBudgetScopes(ctx context.Context) ([]domaininference.BudgetScope, error) {
	scopes, err := m.UsageLedger().ListBudgetScopes(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]domaininference.BudgetScope, 0, len(scopes))
	for _, scope := range scopes {
		resolved, err := m.GetBudgetScope(ctx, scope.ID)
		if err != nil {
			return nil, err
		}
		out = append(out, resolved)
	}
	return out, nil
}

func (m *Module) ReserveProviderAttempt(ctx context.Context, request domaininference.BudgetReservationRequest) (domaininference.BudgetScope, domaininference.BudgetAttempt, error) {
	m.budgetMu.Lock()
	defer m.budgetMu.Unlock()
	if err := m.requireBudgetPersistence(); err != nil {
		return domaininference.BudgetScope{}, domaininference.BudgetAttempt{}, err
	}
	if err := m.RequireBudgetWriteAllowed(); err != nil {
		return domaininference.BudgetScope{}, domaininference.BudgetAttempt{}, err
	}
	if request.Now.IsZero() {
		request.Now = time.Now().UTC()
	}
	ledger := m.UsageLedger()
	scope, err := ledger.GetBudgetScope(ctx, request.ScopeID)
	if err != nil {
		if errors.Is(err, inferencestorage.ErrBudgetScopeNotFound) {
			return domaininference.BudgetScope{}, domaininference.BudgetAttempt{}, BudgetError{Reason: "scope_not_found"}
		}
		return domaininference.BudgetScope{}, domaininference.BudgetAttempt{}, err
	}
	next, attempt, err := reserveBudgetAttempt(scope, request)
	if err != nil {
		return scope, domaininference.BudgetAttempt{}, err
	}
	persisted, err := ledger.UpsertBudgetScope(ctx, next)
	if err != nil {
		return domaininference.BudgetScope{}, domaininference.BudgetAttempt{}, m.handleBudgetMutationError(err)
	}
	return persisted, attempt, nil
}

func (m *Module) CompleteProviderAttempt(ctx context.Context, request domaininference.BudgetCompletionRequest) (domaininference.BudgetScope, error) {
	m.budgetMu.Lock()
	defer m.budgetMu.Unlock()
	if err := m.requireBudgetPersistence(); err != nil {
		return domaininference.BudgetScope{}, err
	}
	if err := m.RequireBudgetWriteAllowed(); err != nil {
		return domaininference.BudgetScope{}, err
	}
	if request.CompletedAt.IsZero() {
		request.CompletedAt = time.Now().UTC()
	}
	ledger := m.UsageLedger()
	scope, err := ledger.GetBudgetScope(ctx, request.ScopeID)
	if err != nil {
		return domaininference.BudgetScope{}, err
	}
	next, accountingErr := completeBudgetAttempt(scope, request)
	persisted, err := ledger.UpsertBudgetScope(ctx, next)
	if err != nil {
		return domaininference.BudgetScope{}, m.handleBudgetMutationError(err)
	}
	return persisted, accountingErr
}

func (m *Module) ReactivateBudgetScope(ctx context.Context, id domaininference.BudgetScopeID, now time.Time) (domaininference.BudgetScope, error) {
	m.budgetMu.Lock()
	defer m.budgetMu.Unlock()
	if err := m.requireBudgetPersistence(); err != nil {
		return domaininference.BudgetScope{}, err
	}
	if err := m.RequireBudgetWriteAllowed(); err != nil {
		return domaininference.BudgetScope{}, err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	ledger := m.UsageLedger()
	scope, err := ledger.GetBudgetScope(ctx, id)
	if err != nil {
		return domaininference.BudgetScope{}, err
	}
	if scope.State == domaininference.BudgetScopeActive {
		return scope, nil
	}
	if scope.State != domaininference.BudgetScopeCompleted || !scope.ExpiresAt.After(now) || !scope.Limits.Deadline.After(now) {
		return domaininference.BudgetScope{}, BudgetError{Reason: "scope_cannot_be_reactivated"}
	}
	scope.State = domaininference.BudgetScopeActive
	scope.DenialReason = ""
	scope.UpdatedAt = now
	persisted, err := ledger.UpsertBudgetScope(ctx, scope)
	if err != nil {
		return domaininference.BudgetScope{}, m.handleBudgetMutationError(err)
	}
	return persisted, nil
}

func (m *Module) CompleteBudgetScope(ctx context.Context, id domaininference.BudgetScopeID, now time.Time) (domaininference.BudgetScope, error) {
	m.budgetMu.Lock()
	defer m.budgetMu.Unlock()
	if err := m.requireBudgetPersistence(); err != nil {
		return domaininference.BudgetScope{}, err
	}
	if err := m.RequireBudgetWriteAllowed(); err != nil {
		return domaininference.BudgetScope{}, err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	ledger := m.UsageLedger()
	scope, err := ledger.GetBudgetScope(ctx, id)
	if err != nil {
		return domaininference.BudgetScope{}, err
	}
	if scope.State == domaininference.BudgetScopeCompleted || scope.State == domaininference.BudgetScopeExpired || scope.State == domaininference.BudgetScopeExhausted {
		return scope, nil
	}
	scope.State = domaininference.BudgetScopeCompleted
	scope.UpdatedAt = now
	scope.Usage = calculateBudgetUsage(scope.Attempts, now)
	persisted, err := ledger.UpsertBudgetScope(ctx, scope)
	if err != nil {
		return domaininference.BudgetScope{}, m.handleBudgetMutationError(err)
	}
	return persisted, nil
}

func pruneEligibleBudgetScopes(ctx context.Context, ledger inferencestorage.UsageLedger, scopes []domaininference.BudgetScope, now time.Time, limit int) (int, error) {
	sort.Slice(scopes, func(i, j int) bool { return scopes[i].RetentionUntil.Before(scopes[j].RetentionUntil) })
	deleted := 0
	for _, scope := range scopes {
		if deleted >= limit {
			break
		}
		terminal := scope.State == domaininference.BudgetScopeCompleted || scope.State == domaininference.BudgetScopeExpired || scope.State == domaininference.BudgetScopeExhausted || !scope.ExpiresAt.After(now) || !scope.Limits.Deadline.After(now)
		if !terminal || scope.RetentionUntil.IsZero() || scope.RetentionUntil.After(now) || hasActiveBudgetLease(scope.Attempts, now) {
			continue
		}
		if err := ledger.DeleteBudgetScope(ctx, scope.ID); err != nil {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}

func (m *Module) PruneBudgetScopes(ctx context.Context, now time.Time, limit int) (int, error) {
	m.budgetMu.Lock()
	defer m.budgetMu.Unlock()
	if err := m.requireBudgetPersistence(); err != nil {
		return 0, err
	}
	if err := m.RequireBudgetWriteAllowed(); err != nil {
		return 0, err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if limit <= 0 || limit > maxBudgetScopesRetained {
		return 0, fmt.Errorf("budget prune limit must be between 1 and %d", maxBudgetScopesRetained)
	}
	ledger := m.UsageLedger()
	scopes, err := ledger.ListBudgetScopes(ctx)
	if err != nil {
		return 0, err
	}
	deleted, err := pruneEligibleBudgetScopes(ctx, ledger, scopes, now, limit)
	if err != nil {
		return deleted, m.handleBudgetMutationError(err)
	}
	return deleted, nil
}

func reserveBudgetAttempt(scope domaininference.BudgetScope, request domaininference.BudgetReservationRequest) (domaininference.BudgetScope, domaininference.BudgetAttempt, error) {
	if err := validateBudgetIdentity("attempt", string(request.AttemptID)); err != nil {
		return scope, domaininference.BudgetAttempt{}, err
	}
	if err := validateBudgetIdentity("request", request.RequestID); err != nil {
		return scope, domaininference.BudgetAttempt{}, err
	}
	if scope.State != domaininference.BudgetScopeActive {
		return scope, domaininference.BudgetAttempt{}, BudgetError{Reason: "scope_not_active"}
	}
	if !request.Now.Before(scope.ExpiresAt) || !request.Now.Before(scope.Limits.Deadline) {
		return scope, domaininference.BudgetAttempt{}, BudgetError{Reason: "deadline_exhausted"}
	}
	if request.Route != scope.Route {
		return scope, domaininference.BudgetAttempt{}, BudgetError{Reason: "route_mismatch"}
	}
	for _, existing := range scope.Attempts {
		if existing.ID == request.AttemptID {
			return scope, domaininference.BudgetAttempt{}, BudgetError{Reason: "duplicate_attempt", Err: ErrBudgetAttemptDuplicate}
		}
	}
	if len(scope.Attempts) >= maxBudgetAttempts {
		return scope, domaininference.BudgetAttempt{}, BudgetError{Reason: "attempt_capacity_exhausted"}
	}
	usage := calculateBudgetUsage(scope.Attempts, request.Now)
	newRequest := true
	retryIndex := int64(0)
	for _, existing := range scope.Attempts {
		if existing.RequestID == request.RequestID {
			newRequest = false
			if existing.RetryIndex >= retryIndex {
				retryIndex = existing.RetryIndex + 1
			}
		}
	}
	// The durable scope history, not a caller/work-claim counter, is authoritative
	// for retry classification. Failed reservations leave no attempt and therefore
	// cannot turn the next provider attempt into a retry.
	isRetry := !newRequest
	if newRequest && limitExceeded(usage.Requests, 1, scope.Limits.MaxRequests) {
		return scope, domaininference.BudgetAttempt{}, BudgetError{Reason: "request_limit_exhausted"}
	}
	if limitExceeded(usage.Attempts, 1, scope.Limits.MaxAttempts) {
		return scope, domaininference.BudgetAttempt{}, BudgetError{Reason: "attempt_limit_exhausted"}
	}
	if isRetry && limitExceeded(usage.RetryAttempts, 1, scope.Limits.MaxRetryAttempts) {
		return scope, domaininference.BudgetAttempt{}, BudgetError{Reason: "retry_limit_exhausted"}
	}
	if isRetry && retryIndex > scope.Limits.MaxRetriesPerRequest {
		return scope, domaininference.BudgetAttempt{}, BudgetError{Reason: "per_request_retry_limit_exhausted"}
	}
	if limitExceeded(usage.ActiveProviderCalls, 1, scope.Limits.MaxConcurrentProviderCalls) {
		return scope, domaininference.BudgetAttempt{}, BudgetError{Reason: "concurrency_limit_exhausted"}
	}
	if request.BatchItems <= 0 || request.BatchItems > scope.Limits.MaxBatchItems || (scope.Limits.BatchingMode == domaininference.BatchingModeSingle && request.BatchItems != 1) {
		return scope, domaininference.BudgetAttempt{}, BudgetError{Reason: "batch_limit_exceeded"}
	}
	if request.InputTokens < 0 || request.OutputTokens < 0 || request.ProviderMillis <= 0 {
		return scope, domaininference.BudgetAttempt{}, BudgetError{Reason: "invalid_reservation"}
	}
	if limitExceeded(0, request.InputTokens, scope.Limits.MaxInputTokensPerRequest) {
		return scope, domaininference.BudgetAttempt{}, BudgetError{Reason: "input_tokens_per_request_exceeded"}
	}
	if limitExceeded(0, request.OutputTokens, scope.Limits.MaxOutputTokensPerRequest) {
		return scope, domaininference.BudgetAttempt{}, BudgetError{Reason: "output_tokens_per_request_exceeded"}
	}
	if limitExceeded(usage.InputTokens, request.InputTokens, scope.Limits.MaxInputTokens) {
		return scope, domaininference.BudgetAttempt{}, BudgetError{Reason: "input_token_limit_exhausted"}
	}
	if limitExceeded(usage.OutputTokens, request.OutputTokens, scope.Limits.MaxOutputTokens) {
		return scope, domaininference.BudgetAttempt{}, BudgetError{Reason: "output_token_limit_exhausted"}
	}
	if limitExceeded(0, request.ProviderMillis, scope.Limits.MaxProviderMillisPerRequest) {
		return scope, domaininference.BudgetAttempt{}, BudgetError{Reason: "provider_time_per_request_exceeded"}
	}
	if limitExceeded(usage.ProviderMillis, request.ProviderMillis, scope.Limits.MaxProviderMillis) {
		return scope, domaininference.BudgetAttempt{}, BudgetError{Reason: "provider_time_limit_exhausted"}
	}
	cost, err := estimatedBudgetCost(scope.Limits, request.InputTokens, request.OutputTokens)
	if err != nil {
		return scope, domaininference.BudgetAttempt{}, err
	}
	if request.EstimatedCostMicros != 0 && request.EstimatedCostMicros != cost {
		return scope, domaininference.BudgetAttempt{}, BudgetError{Reason: "cost_estimate_mismatch"}
	}
	if limitExceeded(usage.EstimatedCostMicros, cost, scope.Limits.MaxEstimatedCostMicros) {
		return scope, domaininference.BudgetAttempt{}, BudgetError{Reason: "cost_limit_exhausted"}
	}
	reservedUntil := request.Now.Add(time.Duration(request.ProviderMillis) * time.Millisecond)
	if reservedUntil.After(scope.Limits.Deadline) {
		reservedUntil = scope.Limits.Deadline
	}
	if !reservedUntil.After(request.Now) {
		return scope, domaininference.BudgetAttempt{}, BudgetError{Reason: "deadline_exhausted"}
	}
	attempt := domaininference.BudgetAttempt{ID: request.AttemptID, RequestID: request.RequestID, State: domaininference.BudgetAttemptReserved, Retry: isRetry, RetryIndex: retryIndex, BatchItems: request.BatchItems, ReservedInputTokens: request.InputTokens, ReservedOutputTokens: request.OutputTokens, ChargedInputTokens: request.InputTokens, ChargedOutputTokens: request.OutputTokens, ReservedProviderMillis: request.ProviderMillis, ChargedProviderMillis: request.ProviderMillis, ReservedCostMicros: cost, ChargedCostMicros: cost, ReservedAt: request.Now, ReservedUntil: reservedUntil}
	scope.Attempts = append(scope.Attempts, attempt)
	scope.AttemptEvents = appendBudgetAttemptEvent(scope.AttemptEvents, attempt, 1, request.Now)
	scope.UpdatedAt = request.Now
	scope.Usage = calculateBudgetUsage(scope.Attempts, request.Now)
	return scope, attempt, nil
}

func completeBudgetAttempt(scope domaininference.BudgetScope, request domaininference.BudgetCompletionRequest) (domaininference.BudgetScope, error) {
	if err := validateBudgetIdentity("attempt", string(request.AttemptID)); err != nil {
		return scope, err
	}
	if request.InputTokens < 0 || request.OutputTokens < 0 || request.ProviderMillis < 0 || request.CostMicros < 0 {
		return scope, fmt.Errorf("budget completion counters must not be negative")
	}
	index := -1
	for i := range scope.Attempts {
		if scope.Attempts[i].ID == request.AttemptID {
			index = i
			break
		}
	}
	if index < 0 {
		return scope, fmt.Errorf("budget attempt %q not found", request.AttemptID)
	}
	attempt := scope.Attempts[index]
	if attempt.State != domaininference.BudgetAttemptReserved {
		if attempt.State == request.State {
			return scope, nil
		}
		return scope, fmt.Errorf("budget attempt %q is already terminal", request.AttemptID)
	}
	switch request.State {
	case domaininference.BudgetAttemptSucceeded, domaininference.BudgetAttemptFailed, domaininference.BudgetAttemptCanceled, domaininference.BudgetAttemptAmbiguous:
	default:
		return scope, fmt.Errorf("budget completion state %q is invalid", request.State)
	}
	attempt.State = request.State
	attempt.CompletedAt = request.CompletedAt
	attempt.Outcome = sanitizeBudgetOutcome(firstNonEmpty(strings.TrimSpace(request.Outcome), "unknown"))
	attempt.OutcomeKnown = request.OutcomeKnown
	attempt.UsageKnown = request.UsageKnown && request.OutcomeKnown
	attempt.ProviderDurationKnown = request.ProviderDurationKnown && request.OutcomeKnown
	var accountingErr error
	if attempt.UsageKnown {
		attempt.ChargedInputTokens = request.InputTokens
		attempt.ChargedOutputTokens = request.OutputTokens
		attempt.ChargedCostMicros = request.CostMicros
		attempt.ChargedCostSaturated = request.CostSaturated
	}
	if attempt.ProviderDurationKnown {
		attempt.ChargedProviderMillis = request.ProviderMillis
	}
	if attempt.ChargedCostSaturated || (attempt.UsageKnown && (request.InputTokens > attempt.ReservedInputTokens || request.OutputTokens > attempt.ReservedOutputTokens || request.CostMicros > attempt.ReservedCostMicros)) || (attempt.ProviderDurationKnown && request.ProviderMillis > attempt.ReservedProviderMillis) {
		scope.State = domaininference.BudgetScopeExhausted
		scope.DenialReason = "reservation_exceeded"
		accountingErr = ErrBudgetAccounting
	}
	scope.Attempts[index] = attempt
	scope.AttemptEvents = appendBudgetAttemptEvent(scope.AttemptEvents, attempt, 2, request.CompletedAt)
	scope.UpdatedAt = request.CompletedAt
	scope.Usage = calculateBudgetUsage(scope.Attempts, request.CompletedAt)
	if scope.Usage.AccountingOverflow {
		scope.State = domaininference.BudgetScopeExhausted
		scope.DenialReason = "reservation_exceeded"
		accountingErr = ErrBudgetAccounting
	}
	return scope, accountingErr
}

func validateBudgetScope(scope domaininference.BudgetScope, now time.Time) error {
	if err := validateBudgetIdentity("scope", string(scope.ID)); err != nil {
		return err
	}
	if scope.State != domaininference.BudgetScopeActive {
		return fmt.Errorf("new budget scope state must be active")
	}
	if scope.Route.EndpointID == uuid.Nil || scope.Route.ModelID == uuid.Nil || scope.Route.CapabilityID == uuid.Nil || strings.TrimSpace(string(scope.Route.Operation)) == "" {
		return fmt.Errorf("budget scope route identity is incomplete")
	}
	switch scope.Route.Operation {
	case domaininference.OperationEmbeddings, domaininference.OperationChat, domaininference.OperationRerank, domaininference.OperationSummarize, domaininference.OperationClassify, domaininference.OperationImageAnalysis:
	default:
		return fmt.Errorf("budget scope route operation is unsupported")
	}
	limits := scope.Limits
	for name, value := range map[string]int64{
		"max_requests": limits.MaxRequests, "max_attempts": limits.MaxAttempts, "max_retry_attempts": limits.MaxRetryAttempts, "max_concurrent_provider_calls": limits.MaxConcurrentProviderCalls,
		"max_batch_items": limits.MaxBatchItems, "max_input_tokens_per_request": limits.MaxInputTokensPerRequest, "max_output_tokens_per_request": limits.MaxOutputTokensPerRequest,
		"max_input_tokens": limits.MaxInputTokens, "max_output_tokens": limits.MaxOutputTokens, "max_provider_millis_per_request": limits.MaxProviderMillisPerRequest,
		"max_provider_millis": limits.MaxProviderMillis, "input_price": limits.InputMicrosPerMillionTokens, "output_price": limits.OutputMicrosPerMillionTokens, "max_cost": limits.MaxEstimatedCostMicros,
	} {
		if value < 0 {
			return fmt.Errorf("budget limit %s must not be negative", name)
		}
	}
	if limits.MaxRequests <= 0 || limits.MaxAttempts <= 0 || limits.MaxAttempts > maxBudgetAttempts || limits.MaxRequests > limits.MaxAttempts {
		return fmt.Errorf("budget request/attempt limits are invalid")
	}
	if limits.MaxRetryAttempts > limits.MaxAttempts || limits.MaxRetriesPerRequest < 0 || limits.MaxRetriesPerRequest > limits.MaxRetryAttempts || limits.MaxConcurrentProviderCalls <= 0 || limits.MaxConcurrentProviderCalls > limits.MaxAttempts {
		return fmt.Errorf("budget retry/concurrency limits are invalid")
	}
	if limits.BatchingMode != domaininference.BatchingModeSingle && limits.BatchingMode != domaininference.BatchingModeBatch {
		return fmt.Errorf("budget batching mode must be single or batch")
	}
	if limits.MaxBatchItems <= 0 || limits.MaxBatchItems > maxBudgetBatchItems || (limits.BatchingMode == domaininference.BatchingModeSingle && limits.MaxBatchItems != 1) {
		return fmt.Errorf("budget batch limit is invalid")
	}
	if limits.MaxInputTokens > 0 && (limits.MaxInputTokensPerRequest <= 0 || limits.MaxInputTokensPerRequest > limits.MaxInputTokens) {
		return fmt.Errorf("budget input token limits are inconsistent")
	}
	if limits.MaxOutputTokens > 0 && (limits.MaxOutputTokensPerRequest <= 0 || limits.MaxOutputTokensPerRequest > limits.MaxOutputTokens) {
		return fmt.Errorf("budget output token limits are inconsistent")
	}
	if limits.MaxOutputTokensPerRequest > int64(^uint(0)>>1) {
		return fmt.Errorf("budget output token limit exceeds connector parameter capacity")
	}
	if limits.MaxProviderMillis <= 0 || limits.MaxProviderMillisPerRequest <= 0 || limits.MaxProviderMillisPerRequest > limits.MaxProviderMillis {
		return fmt.Errorf("budget provider duration limits are inconsistent")
	}
	if limits.Deadline.IsZero() || !limits.Deadline.After(now) || scope.ExpiresAt.IsZero() || scope.ExpiresAt.After(limits.Deadline) || !scope.ExpiresAt.After(now) {
		return fmt.Errorf("budget deadline/expiry is invalid")
	}
	if scope.RetentionUntil.Before(limits.Deadline) || scope.RetentionUntil.After(scope.ExpiresAt.Add(maxBudgetRetention)) {
		return fmt.Errorf("budget retention deadline is invalid")
	}
	if limits.InputMicrosPerMillionTokens > 0 && limits.MaxInputTokensPerRequest <= 0 {
		return fmt.Errorf("budget input price requires a bounded per-request token limit")
	}
	if limits.OutputMicrosPerMillionTokens > 0 && limits.MaxOutputTokensPerRequest <= 0 {
		return fmt.Errorf("budget output price requires a bounded per-request token limit")
	}
	if _, err := estimatedBudgetCost(limits, limits.MaxInputTokensPerRequest, limits.MaxOutputTokensPerRequest); err != nil {
		return fmt.Errorf("budget per-request cost bounds overflow: %w", err)
	}
	if limits.MaxEstimatedCostMicros > 0 {
		if limits.MaxInputTokensPerRequest <= 0 {
			return fmt.Errorf("budget cost limit requires a conservative input token reservation")
		}
		if limits.Currency != "USD" || strings.TrimSpace(limits.PriceDefinitionVersion) == "" || len(limits.PriceDefinitionVersion) > maxBudgetIdentityLength {
			return fmt.Errorf("budget cost limit requires USD and a bounded price definition")
		}
		if limits.InputMicrosPerMillionTokens <= 0 {
			return fmt.Errorf("budget cost limit requires a positive input token price")
		}
		if scope.Route.Operation != domaininference.OperationEmbeddings && limits.OutputMicrosPerMillionTokens <= 0 {
			return fmt.Errorf("budget cost limit requires a positive output token price")
		}
	}
	semanticIdentitySet := strings.TrimSpace(scope.SpaceID) != "" || strings.TrimSpace(scope.SemanticRuleID) != "" || strings.TrimSpace(scope.EmbeddingBindingKey) != "" || strings.TrimSpace(scope.CorrelationID) != ""
	if semanticIdentitySet {
		for kind, value := range map[string]string{"space": scope.SpaceID, "semantic_rule": scope.SemanticRuleID, "embedding_binding": scope.EmbeddingBindingKey, "correlation": scope.CorrelationID} {
			if err := validateBudgetIdentity(kind, value); err != nil {
				return err
			}
		}
	}
	if len(scope.Attempts) != 0 || len(scope.AttemptEvents) != 0 {
		return fmt.Errorf("new budget scope must not contain attempts or attempt events")
	}
	return nil
}

func appendBudgetAttemptEvent(events []domaininference.BudgetAttemptEvent, attempt domaininference.BudgetAttempt, version uint32, recordedAt time.Time) []domaininference.BudgetAttemptEvent {
	sequence := uint64(len(events)) + 1
	if len(events) > 0 && events[len(events)-1].Sequence >= sequence {
		sequence = events[len(events)-1].Sequence + 1
	}
	return append(events, domaininference.BudgetAttemptEvent{Sequence: sequence, AttemptVersion: version, Attempt: attempt, RecordedAt: recordedAt})
}

func validateBudgetIdentity(kind, value string) error {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > maxBudgetIdentityLength || !budgetIdentityPattern.MatchString(value) {
		return fmt.Errorf("budget %s identity is invalid", kind)
	}
	return nil
}

func hasActiveBudgetLease(attempts []domaininference.BudgetAttempt, now time.Time) bool {
	for _, attempt := range attempts {
		if attempt.State == domaininference.BudgetAttemptReserved && now.Before(attempt.ReservedUntil) {
			return true
		}
	}
	return false
}

func calculateBudgetUsage(attempts []domaininference.BudgetAttempt, now time.Time) domaininference.BudgetUsage {
	requests := map[string]struct{}{}
	var usage domaininference.BudgetUsage
	for _, attempt := range attempts {
		requests[attempt.RequestID] = struct{}{}
		usage.Attempts++
		if attempt.Retry {
			usage.RetryAttempts++
		}
		usage.BatchItems = saturatingBudgetAdd(usage.BatchItems, attempt.BatchItems)
		if attempt.State == domaininference.BudgetAttemptReserved {
			usage.ActiveProviderCalls++
			if !now.Before(attempt.ReservedUntil) {
				usage.AmbiguousAttempts++
			}
		} else {
			unknownOutcome := !attempt.OutcomeKnown
			if unknownOutcome {
				usage.ActiveProviderCalls++
			}
			if attempt.State == domaininference.BudgetAttemptAmbiguous || unknownOutcome {
				usage.AmbiguousAttempts++
			}
		}
		usage.InputTokens, usage.AccountingOverflow = saturatingBudgetAddTracked(usage.InputTokens, attempt.ChargedInputTokens, usage.AccountingOverflow)
		usage.OutputTokens, usage.AccountingOverflow = saturatingBudgetAddTracked(usage.OutputTokens, attempt.ChargedOutputTokens, usage.AccountingOverflow)
		usage.ProviderMillis, usage.AccountingOverflow = saturatingBudgetAddTracked(usage.ProviderMillis, attempt.ChargedProviderMillis, usage.AccountingOverflow)
		usage.EstimatedCostMicros, usage.AccountingOverflow = saturatingBudgetAddTracked(usage.EstimatedCostMicros, attempt.ChargedCostMicros, usage.AccountingOverflow || attempt.ChargedCostSaturated)
	}
	usage.Requests = int64(len(requests))
	return usage
}

func saturatingBudgetAdd(current, additional int64) int64 {
	value, _ := saturatingBudgetAddTracked(current, additional, false)
	return value
}

func saturatingBudgetAddTracked(current, additional int64, overflow bool) (int64, bool) {
	maxInt := int64(^uint64(0) >> 1)
	if additional < 0 || current > maxInt-additional {
		return maxInt, true
	}
	return current + additional, overflow
}

func estimatedBudgetCost(limits domaininference.BudgetLimits, inputTokens, outputTokens int64) (int64, error) {
	if limits.MaxEstimatedCostMicros <= 0 {
		return 0, nil
	}
	if strings.TrimSpace(limits.PriceDefinitionVersion) == "" || len(limits.Currency) != 3 {
		return 0, BudgetError{Reason: "price_unavailable"}
	}
	input, err := ceilMulPerMillion(inputTokens, limits.InputMicrosPerMillionTokens)
	if err != nil {
		return 0, BudgetError{Reason: "cost_overflow"}
	}
	output, err := ceilMulPerMillion(outputTokens, limits.OutputMicrosPerMillionTokens)
	if err != nil || input > int64(^uint64(0)>>1)-output {
		return 0, BudgetError{Reason: "cost_overflow"}
	}
	return input + output, nil
}

func ceilMulPerMillion(tokens, micros int64) (int64, error) {
	if tokens < 0 || micros < 0 {
		return 0, fmt.Errorf("negative cost input")
	}
	if tokens == 0 || micros == 0 {
		return 0, nil
	}
	const million = int64(1_000_000)
	maxInt := int64(^uint64(0) >> 1)
	if tokens > maxInt/micros {
		return 0, errors.New("cost multiplication overflow")
	}
	product := tokens * micros
	if product > maxInt-(million-1) {
		return 0, errors.New("cost rounding overflow")
	}
	return (product + million - 1) / million, nil
}

func limitExceeded(current, additional, limit int64) bool {
	if limit <= 0 {
		return false
	}
	return additional > limit || current > limit-additional
}

func validateResolvedBudgetBatching(limits domaininference.BudgetLimits, capability domaininference.Capability, descriptor connectors.BatchingDescriptor, batchItems int64) error {
	configuredMode := capability.BatchingMode
	configuredMax := int64(capability.MaxBatchItems)
	if configuredMode == "" {
		configuredMode = domaininference.BatchingModeSingle
		configuredMax = 1
	}
	if configuredMax <= 0 {
		configuredMax = 1
	}
	if descriptor.Mode != configuredMode || configuredMax > descriptor.MaxItems || limits.BatchingMode != configuredMode || limits.MaxBatchItems > configuredMax || batchItems > limits.MaxBatchItems || batchItems > descriptor.MaxItems || (configuredMode == domaininference.BatchingModeSingle && batchItems != 1) {
		return BudgetError{Reason: "connector_batching_unsupported"}
	}
	return nil
}

func (m *Module) reserveInvocationBudget(ctx context.Context, req inferenceruntime.InvokeRequest, resolved inferenceruntime.ResolveResult, connector connectors.Connector) (domaininference.BudgetAttempt, error) {
	if err := validateBudgetIdentity("attempt", string(req.BudgetAttemptID)); err != nil {
		return domaininference.BudgetAttempt{}, err
	}
	if err := validateBudgetIdentity("request", req.RequestID); err != nil {
		return domaininference.BudgetAttempt{}, err
	}
	scope, err := m.GetBudgetScope(ctx, req.BudgetScopeID)
	if err != nil {
		if errors.Is(err, inferencestorage.ErrBudgetScopeNotFound) {
			return domaininference.BudgetAttempt{}, BudgetError{Reason: "scope_not_found"}
		}
		return domaininference.BudgetAttempt{}, err
	}
	batchItems := req.BatchItems
	if batchItems == 0 {
		batchItems = 1
	}
	descriptor := connectors.ConnectorBatchingDescriptor(connector)
	if err := validateResolvedBudgetBatching(scope.Limits, resolved.Capability, descriptor, batchItems); err != nil {
		return domaininference.BudgetAttempt{}, err
	}
	inputTokens, err := invocationInputTokenReservation(req, resolved, scope.Limits)
	if err != nil {
		return domaininference.BudgetAttempt{}, err
	}
	outputTokens, err := invocationOutputTokenReservation(req, resolved, scope.Limits)
	if err != nil {
		return domaininference.BudgetAttempt{}, err
	}
	reservation := domaininference.BudgetReservationRequest{ScopeID: req.BudgetScopeID, AttemptID: req.BudgetAttemptID, RequestID: req.RequestID, Route: domaininference.BudgetRoute{EndpointID: resolved.Endpoint.ID, ModelID: resolved.Model.ID, CapabilityID: resolved.Capability.ID, Operation: resolved.Capability.Operation}, Retry: req.BudgetRetry, RetryIndex: req.BudgetRetryIndex, BatchItems: batchItems, InputTokens: inputTokens, OutputTokens: outputTokens, ProviderMillis: scope.Limits.MaxProviderMillisPerRequest, Now: time.Now().UTC()}
	_, attempt, err := m.ReserveProviderAttempt(ctx, reservation)
	return attempt, err
}

func (m *Module) failReservedInvocationBudget(req inferenceruntime.InvokeRequest, outcome string) error {
	now := time.Now().UTC()
	completionCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := m.CompleteProviderAttempt(completionCtx, domaininference.BudgetCompletionRequest{ScopeID: req.BudgetScopeID, AttemptID: req.BudgetAttemptID, State: domaininference.BudgetAttemptFailed, UsageKnown: true, OutcomeKnown: true, ProviderDurationKnown: true, Outcome: outcome, CompletedAt: now})
	return err
}

func (m *Module) completeInvocationBudget(req inferenceruntime.InvokeRequest, connectorType domaininference.ConnectorType, response connectorResponse, providerErr error, started, completed time.Time) error {
	state := domaininference.BudgetAttemptSucceeded
	outcome := "succeeded"
	if providerErr != nil {
		state = domaininference.BudgetAttemptFailed
		outcome = budgetOutcome(providerErr)
		if errors.Is(providerErr, context.Canceled) || errors.Is(providerErr, context.DeadlineExceeded) {
			state = domaininference.BudgetAttemptCanceled
		}
	}
	usageKnown := budgetUsageKnown(response.Usage, connectorType, req.Resolve.Operation)
	outcomeKnown := providerErr == nil
	if providerErr != nil {
		var connectorErr connectors.ConnectorError
		outcomeKnown = errors.As(providerErr, &connectorErr) && connectorErr.StatusCode > 0
	}
	// Usage counters cannot prove that an ambiguous provider attempt stopped.
	// Retain every conservative token and cost reservation until the provider
	// outcome itself is known, even if a partial response carried valid usage.
	usageKnown = usageKnown && outcomeKnown
	providerMillis := completed.Sub(started).Milliseconds()
	if providerMillis <= 0 {
		providerMillis = 1
	}
	scope, err := m.GetBudgetScope(context.Background(), req.BudgetScopeID)
	if err != nil {
		return err
	}
	cost := int64(0)
	costSaturated := false
	if usageKnown {
		cost, err = estimatedBudgetCost(scope.Limits, response.Usage.InputTokens, response.Usage.OutputTokens)
		if err != nil {
			var budgetErr BudgetError
			if !errors.As(err, &budgetErr) || budgetErr.Reason != "cost_overflow" {
				return err
			}
			cost = int64(^uint64(0) >> 1)
			costSaturated = true
		}
	}
	completion := domaininference.BudgetCompletionRequest{ScopeID: req.BudgetScopeID, AttemptID: req.BudgetAttemptID, State: state, InputTokens: response.Usage.InputTokens, OutputTokens: response.Usage.OutputTokens, ProviderMillis: providerMillis, CostMicros: cost, CostSaturated: costSaturated, UsageKnown: usageKnown, OutcomeKnown: outcomeKnown, ProviderDurationKnown: outcomeKnown, Outcome: outcome, CompletedAt: completed}
	completionCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = m.CompleteProviderAttempt(completionCtx, completion)
	return err
}

func invocationInputTokenReservation(req inferenceruntime.InvokeRequest, resolved inferenceruntime.ResolveResult, limits domaininference.BudgetLimits) (int64, error) {
	required := limits.MaxInputTokensPerRequest > 0 || limits.MaxInputTokens > 0 || limits.MaxEstimatedCostMicros > 0
	if !required {
		return 0, nil
	}
	var tokens int64
	effectiveProviderModel := strings.TrimSpace(resolved.Capability.ProviderModelOverride)
	if effectiveProviderModel == "" {
		effectiveProviderModel = resolved.Model.ProviderModelName
	}
	switch {
	case req.Resolve.Operation == domaininference.OperationEmbeddings && resolved.Endpoint.ConnectorType == domaininference.ConnectorFake:
		// The daemon owns the fake connector and its deterministic tokenizer.
		tokens = connectors.EstimateTokens(req.Input)
	case req.Resolve.Operation == domaininference.OperationEmbeddings && resolved.Endpoint.ConnectorType == domaininference.ConnectorOpenAICompatible && usesCL100KEmbeddingTokenizer(effectiveProviderModel):
		cl100kTokenizerOnce.Do(func() { cl100kTokenizer, cl100kTokenizerErr = tokenizer.Get(tokenizer.Cl100kBase) })
		if cl100kTokenizerErr != nil {
			return 0, BudgetError{Reason: "input_token_reservation_unavailable"}
		}
		count, err := cl100kTokenizer.Count(req.Input)
		if err != nil || count < 0 {
			return 0, BudgetError{Reason: "input_token_reservation_unavailable"}
		}
		tokens = int64(count)
	default:
		// Non-semantic routes do not advertise exact tokenizer enforcement. Keep
		// their existing conservative aggregate reservation behavior.
		if req.Resolve.Operation != domaininference.OperationEmbeddings && limits.MaxInputTokensPerRequest > 0 {
			return limits.MaxInputTokensPerRequest, nil
		}
		return 0, BudgetError{Reason: "input_token_reservation_unavailable"}
	}
	if limits.MaxInputTokensPerRequest > 0 && tokens > limits.MaxInputTokensPerRequest {
		return 0, BudgetError{Reason: "input_tokens_per_request_exceeded"}
	}
	return tokens, nil
}

func usesCL100KEmbeddingTokenizer(model string) bool {
	switch strings.ToLower(strings.TrimSpace(model)) {
	case "text-embedding-3-small", "text-embedding-3-large", "text-embedding-ada-002":
		return true
	default:
		return false
	}
}

func budgetUsageKnown(usage domaininference.Usage, connectorType domaininference.ConnectorType, operation domaininference.Operation) bool {
	source := strings.ToLower(strings.TrimSpace(usage.TokenCountSource))
	if source == "estimated" {
		// The in-process fake connector defines this deterministic estimate as
		// its authoritative usage. Hosted connectors may never refund a hard
		// reservation from an estimate.
		return connectorType == domaininference.ConnectorFake && usage.InputTokens >= 0 && usage.OutputTokens >= 0 && usage.TotalTokens == usage.InputTokens+usage.OutputTokens
	}
	if source != "provider_reported" || usage.InputTokens < 0 || usage.OutputTokens < 0 || usage.TotalTokens <= 0 {
		return false
	}
	if operation == domaininference.OperationEmbeddings {
		return usage.OutputTokens == 0 && usage.TotalTokens == usage.InputTokens
	}
	return usage.TotalTokens == usage.InputTokens+usage.OutputTokens
}

func invocationOutputTokenReservation(req inferenceruntime.InvokeRequest, resolved inferenceruntime.ResolveResult, limits domaininference.BudgetLimits) (int64, error) {
	if req.Resolve.Operation == domaininference.OperationEmbeddings {
		return 0, nil
	}
	parameters := mergeParameters(resolved.Profile.DefaultParameters, req.Resolve.Parameters)
	value := int64(parameters.MaxOutputTokens)
	if limits.MaxOutputTokensPerRequest > 0 && (value <= 0 || limits.MaxOutputTokensPerRequest < value) {
		// A hard budget clamps a looser caller/profile setting while preserving
		// any stricter positive request ceiling.
		value = limits.MaxOutputTokensPerRequest
	}
	if value <= 0 {
		value = int64(resolved.Capability.MaxOutputTokens)
	}
	if value <= 0 {
		value = int64(resolved.Model.MaxOutputTokens)
	}
	if value <= 0 && (limits.MaxOutputTokens > 0 || limits.MaxEstimatedCostMicros > 0) {
		return 0, BudgetError{Reason: "output_token_reservation_unavailable"}
	}
	return value, nil
}

func budgetDenialReason(err error) string {
	var budgetErr BudgetError
	if errors.As(err, &budgetErr) && budgetErr.Reason != "" {
		return budgetErr.Reason
	}
	if errors.Is(err, ErrBudgetAccounting) {
		return "reservation_exceeded"
	}
	return "budget_operation_failed"
}

func budgetOutcome(err error) string {
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	var connectorErr connectors.ConnectorError
	if !errors.As(err, &connectorErr) {
		return "connector_error"
	}
	switch {
	case connectorErr.StatusCode == 429:
		return "provider_rate_limited"
	case connectorErr.StatusCode >= 500:
		return "provider_unavailable"
	case connectorErr.StatusCode > 0:
		return "provider_rejected"
	case connectorErr.Retryable:
		return "provider_transport_error"
	default:
		return "connector_error"
	}
}

func sanitizeBudgetOutcome(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if len(value) > maxBudgetOutcomeLength {
		return "connector_error"
	}
	if _, ok := allowedBudgetOutcomes[value]; ok {
		return value
	}
	return "connector_error"
}
