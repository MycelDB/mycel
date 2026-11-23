package service

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	AdmissionErrorInfoDomain     = "mycel.usage"
	ReasonUsageLimitExceeded     = "usage_limit_exceeded"
	ReasonUsageWriteGateBlocked  = "usage_write_gate_blocked"
	defaultAdmissionListPageSize = 100
	maxAdmissionListPageSize     = 1000
)

var ErrUsageLimitNotFound = errors.New("usage limit not found")

type AdmissionError struct {
	Reason        string
	LimitID       string
	Scope         Scope
	MetricName    string
	MetricVersion uint32
	Current       uint64
	Reserved      uint64
	Attempted     uint64
	Limit         uint64
	State         AccountingState
	Message       string
}

func (e *AdmissionError) Error() string {
	if e == nil {
		return "usage admission rejected"
	}
	if strings.TrimSpace(e.Message) != "" {
		return e.Message
	}
	switch e.Reason {
	case ReasonUsageLimitExceeded:
		return fmt.Sprintf("usage limit exceeded for %s: current=%d reserved=%d attempted=%d limit=%d", e.MetricName, e.Current, e.Reserved, e.Attempted, e.Limit)
	case ReasonUsageWriteGateBlocked:
		return fmt.Sprintf("usage write gate blocked while accounting state is %s", e.State)
	default:
		return "usage admission rejected"
	}
}

func (m *Module) UpsertUsageLimit(ctx context.Context, req LimitMutationRequest) (UsageLimit, error) {
	if err := ctx.Err(); err != nil {
		return UsageLimit{}, err
	}
	now := time.Now().UTC()
	limit := normalizeUsageLimit(req.Limit, now)
	if err := m.validateUsageLimit(limit); err != nil {
		return UsageLimit{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	next := m.cloneStateLocked()
	if next.Limits == nil {
		next.Limits = map[string]UsageLimit{}
	}
	if existing, ok := next.Limits[limit.LimitID]; ok && !existing.CreatedAt.IsZero() {
		limit.CreatedAt = existing.CreatedAt
	}
	if limit.CreatedAt.IsZero() {
		limit.CreatedAt = now
	}
	limit.UpdatedAt = now
	next.Limits[limit.LimitID] = limit
	next.Sequence++
	next.UpdatedAt = now
	if err := m.persistLocked(next); err != nil {
		return UsageLimit{}, err
	}
	m.state = next
	return limit, nil
}

func (m *Module) DeleteUsageLimit(ctx context.Context, req LimitLookupRequest) (UsageLimit, error) {
	if err := ctx.Err(); err != nil {
		return UsageLimit{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	limitID := strings.TrimSpace(req.LimitID)
	if limitID == "" {
		limitID = usageLimitID(req.Scope, req.DomainInclusion, req.MetricName, req.MetricVersion)
	}
	if limitID == "" {
		return UsageLimit{}, fmt.Errorf("usage limit lookup requires limit_id or scope/metric")
	}
	limit, ok := m.state.Limits[limitID]
	if !ok {
		return UsageLimit{}, ErrUsageLimitNotFound
	}
	next := m.cloneStateLocked()
	delete(next.Limits, limitID)
	next.Sequence++
	next.UpdatedAt = time.Now().UTC()
	if err := m.persistLocked(next); err != nil {
		return UsageLimit{}, err
	}
	m.state = next
	return limit, nil
}

func (m *Module) GetUsageLimit(ctx context.Context, req LimitLookupRequest) (UsageLimit, error) {
	if err := ctx.Err(); err != nil {
		return UsageLimit{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	limitID := strings.TrimSpace(req.LimitID)
	if limitID == "" {
		limitID = usageLimitID(req.Scope, req.DomainInclusion, req.MetricName, req.MetricVersion)
	}
	if limitID == "" {
		return UsageLimit{}, fmt.Errorf("usage limit lookup requires limit_id or scope/metric")
	}
	limit, ok := m.state.Limits[limitID]
	if !ok {
		return UsageLimit{}, ErrUsageLimitNotFound
	}
	return limit, nil
}

func (m *Module) ListUsageLimits(ctx context.Context, req LimitListRequest) (LimitListResponse, error) {
	if err := ctx.Err(); err != nil {
		return LimitListResponse{}, err
	}
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = defaultAdmissionListPageSize
	}
	if pageSize > maxAdmissionListPageSize {
		pageSize = maxAdmissionListPageSize
	}
	start := 0
	if token := strings.TrimSpace(req.PageToken); token != "" {
		parsed, err := strconv.Atoi(token)
		if err != nil || parsed < 0 {
			return LimitListResponse{}, fmt.Errorf("invalid usage limit page token")
		}
		start = parsed
	}
	parent := req.Parent.normalized()
	if parent.Type != "" {
		if err := parent.Validate(); err != nil {
			return LimitListResponse{}, err
		}
	}
	metricFilter := strings.TrimSpace(req.MetricName)
	m.mu.RLock()
	limits := make([]UsageLimit, 0, len(m.state.Limits))
	for _, limit := range m.state.Limits {
		if parent.Type != "" && !usageLimitInParent(limit, parent) {
			continue
		}
		if metricFilter != "" && limit.MetricName != metricFilter {
			continue
		}
		limits = append(limits, limit)
	}
	m.mu.RUnlock()
	sort.Slice(limits, func(i, j int) bool { return limits[i].LimitID < limits[j].LimitID })
	if start > len(limits) {
		start = len(limits)
	}
	end := start + pageSize
	if end > len(limits) {
		end = len(limits)
	}
	resp := LimitListResponse{Limits: append([]UsageLimit(nil), limits[start:end]...)}
	if end < len(limits) {
		resp.NextPageToken = strconv.Itoa(end)
	}
	return resp, nil
}

// ReserveDeltaForAdmission reserves capacity for a pending durable write. The
// returned release function must be called after the write either commits and
// applies its ordinary usage delta or fails. Reservations are intentionally
// in-memory: after a crash, committed records replay their deltas without an
// admission check, while uncommitted reservations disappear.
func (m *Module) ReserveDeltaForAdmission(ctx context.Context, delta Delta) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	keys, err := m.admissionCounterDeltas(delta)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return func() {}, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	limited := map[counterKey]UsageLimit{}
	for key := range keys {
		limit, ok := m.matchingEnabledLimitLocked(key)
		if ok {
			limited[key] = limit
		}
	}
	if len(limited) == 0 {
		return func() {}, nil
	}
	if m.state.AccountingState != AccountingStateReady {
		return nil, &AdmissionError{Reason: ReasonUsageWriteGateBlocked, State: m.state.AccountingState, Scope: delta.Scope.normalized(), Message: "usage write gate blocked because quota-safe accounting is not ready"}
	}
	for key, amount := range keys {
		limit, ok := limited[key]
		if !ok {
			continue
		}
		current := m.state.Counters[key]
		reserved := m.reservationTotals[key]
		if exceedsUsageLimit(current, reserved, amount, limit.HardLimit) {
			return nil, &AdmissionError{Reason: ReasonUsageLimitExceeded, LimitID: limit.LimitID, Scope: key.Scope, MetricName: key.MetricName, MetricVersion: key.MetricVersion, Current: current, Reserved: reserved, Attempted: amount, Limit: limit.HardLimit}
		}
	}
	if m.reservationTotals == nil {
		m.reservationTotals = map[counterKey]uint64{}
	}
	for key, amount := range keys {
		m.reservationTotals[key] += amount
	}
	var once bool
	return func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		if once {
			return
		}
		once = true
		for key, amount := range keys {
			if current := m.reservationTotals[key]; current <= amount {
				delete(m.reservationTotals, key)
			} else {
				m.reservationTotals[key] = current - amount
			}
		}
	}, nil
}

func (m *Module) admissionCounterDeltas(delta Delta) (map[counterKey]uint64, error) {
	scope := delta.Scope.normalized()
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	domainClass := delta.DomainClass
	if domainClass == "" {
		domainClass = DomainClassRetainedNonSystem
	}
	if domainClass != DomainClassRetainedNonSystem && domainClass != DomainClassSystem {
		return nil, fmt.Errorf("unsupported usage domain class %q", domainClass)
	}
	out := map[counterKey]uint64{}
	for _, metricDelta := range delta.Metrics {
		if metricDelta.Delta <= 0 {
			continue
		}
		if metricDelta.Version == 0 {
			metricDelta.Version = 1
		}
		def, err := m.registry.MustDefinition(metricDelta.Name, metricDelta.Version)
		if err != nil {
			return nil, err
		}
		if !def.QuotaSafe || !def.HardLimitEligible {
			continue
		}
		for _, key := range aggregateKeys(scope, domainClass, metricDelta) {
			out[key] += uint64(metricDelta.Delta)
		}
	}
	return out, nil
}

func (m *Module) matchingEnabledLimitLocked(key counterKey) (UsageLimit, bool) {
	limitID := usageLimitID(key.Scope, key.DomainInclusion, key.MetricName, key.MetricVersion)
	limit, ok := m.state.Limits[limitID]
	if !ok || !limit.Enabled {
		return UsageLimit{}, false
	}
	return limit, true
}

func exceedsUsageLimit(current, reserved, attempted, hardLimit uint64) bool {
	if current > hardLimit || reserved > hardLimit-current {
		return true
	}
	base := current + reserved
	return attempted > hardLimit-base
}

func normalizeUsageLimit(limit UsageLimit, now time.Time) UsageLimit {
	limit.Scope = limit.Scope.normalized()
	limit.MetricName = strings.TrimSpace(limit.MetricName)
	if limit.MetricVersion == 0 {
		limit.MetricVersion = 1
	}
	limit.DomainInclusion = normalizeDomainInclusion(limit.Scope, limit.DomainInclusion)
	limit.PolicyRef = strings.TrimSpace(limit.PolicyRef)
	limit.Description = strings.TrimSpace(limit.Description)
	if limit.LimitID == "" {
		limit.LimitID = usageLimitID(limit.Scope, limit.DomainInclusion, limit.MetricName, limit.MetricVersion)
	}
	limit.LimitID = strings.TrimSpace(limit.LimitID)
	if limit.CreatedAt.IsZero() {
		limit.CreatedAt = now
	}
	if limit.UpdatedAt.IsZero() {
		limit.UpdatedAt = now
	}
	return limit
}

func (m *Module) validateUsageLimit(limit UsageLimit) error {
	if strings.TrimSpace(limit.LimitID) == "" {
		return fmt.Errorf("usage limit id is required")
	}
	if err := limit.Scope.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(limit.MetricName) == "" {
		return fmt.Errorf("usage limit metric_name is required")
	}
	def, err := m.registry.MustDefinition(limit.MetricName, limit.MetricVersion)
	if err != nil {
		return err
	}
	if !def.QuotaSafe || !def.HardLimitEligible {
		return fmt.Errorf("usage metric %s is not hard-limit eligible", limit.MetricName)
	}
	if want := usageLimitID(limit.Scope, limit.DomainInclusion, limit.MetricName, limit.MetricVersion); want != limit.LimitID {
		return fmt.Errorf("usage limit id %q does not match scope/metric key %q", limit.LimitID, want)
	}
	return nil
}

func usageLimitID(scope Scope, inclusion DomainInclusionMode, metricName string, metricVersion uint32) string {
	scope = scope.normalized()
	metricName = strings.TrimSpace(metricName)
	if metricName == "" || scope.Type == "" {
		return ""
	}
	if metricVersion == 0 {
		metricVersion = 1
	}
	inclusion = normalizeDomainInclusion(scope, inclusion)
	if scope.Type == ScopeTypeDomain {
		return fmt.Sprintf("domain/%s/%s/%s@%d", scope.SpaceID, scope.DomainID, metricName, metricVersion)
	}
	return fmt.Sprintf("space/%s/%s/%s@%d", scope.SpaceID, inclusion, metricName, metricVersion)
}

func usageLimitInParent(limit UsageLimit, parent Scope) bool {
	parent = parent.normalized()
	if parent.Type == ScopeTypeSpace {
		return limit.Scope.SpaceID == parent.SpaceID
	}
	return limit.Scope == parent
}

func cloneLimits(in map[string]UsageLimit) map[string]UsageLimit {
	out := map[string]UsageLimit{}
	for key, value := range in {
		out[key] = value
	}
	return out
}

func persistentLimitFromUsageLimit(limit UsageLimit) persistentLimit {
	return persistentLimit{LimitID: limit.LimitID, Scope: limit.Scope, DomainInclusion: limit.DomainInclusion, MetricName: limit.MetricName, MetricVersion: limit.MetricVersion, HardLimit: limit.HardLimit, Enabled: limit.Enabled, PolicyRef: limit.PolicyRef, Description: limit.Description, CreatedAt: limit.CreatedAt, UpdatedAt: limit.UpdatedAt}
}

func usageLimitFromPersistent(limit persistentLimit) UsageLimit {
	return UsageLimit{LimitID: limit.LimitID, Scope: limit.Scope, DomainInclusion: limit.DomainInclusion, MetricName: limit.MetricName, MetricVersion: limit.MetricVersion, HardLimit: limit.HardLimit, Enabled: limit.Enabled, PolicyRef: limit.PolicyRef, Description: limit.Description, CreatedAt: limit.CreatedAt, UpdatedAt: limit.UpdatedAt}
}
