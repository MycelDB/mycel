package accounting

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/filestore"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
)

const (
	manifestFile = "manifest.json"
	activeLedger = "inference-usage-000001.kusag"
)

type manifest struct {
	Format        string    `json:"format"`
	ActiveSegment string    `json:"active_segment"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type defaultManager struct {
	mu       sync.Mutex
	location string
	manifest manifest
}

func (m *defaultManager) Init(ctx context.Context, location string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(location) == "" {
		return fmt.Errorf("%w: location is required", ErrInvalidInput)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.location = location
	for _, dir := range []string{location, filepath.Join(location, "indexes"), filepath.Join(location, "rollups")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	path := filepath.Join(location, manifestFile)
	if _, err := os.Stat(path); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		now := time.Now().UTC()
		m.manifest = manifest{Format: "mycel-inference-accounting-v1", ActiveSegment: activeLedger, CreatedAt: now, UpdatedAt: now}
		if err := persistJSON(path, m.manifest); err != nil {
			return err
		}
		_, err := os.OpenFile(filepath.Join(location, activeLedger), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		return err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(raw, &m.manifest); err != nil {
		return err
	}
	if m.manifest.ActiveSegment == "" {
		m.manifest.ActiveSegment = activeLedger
	}
	_, err = os.OpenFile(filepath.Join(location, m.manifest.ActiveSegment), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	return err
}

func (m *defaultManager) Append(ctx context.Context, event domainsemantic.InferenceUsageEvent) (domainsemantic.InferenceUsageEvent, error) {
	if err := validateEvent(ctx, event); err != nil {
		return domainsemantic.InferenceUsageEvent{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if event.ID == uuid.Nil {
		event.ID = newID()
	}
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now().UTC()
	}
	if event.TotalTokens == 0 {
		event.TotalTokens = event.InputTokens + event.OutputTokens
	}
	if event.TokenCountSource == "" {
		event.TokenCountSource = "unavailable"
	}
	raw, err := json.Marshal(event)
	if err != nil {
		return domainsemantic.InferenceUsageEvent{}, err
	}
	f, err := os.OpenFile(filepath.Join(m.location, m.manifest.ActiveSegment), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return domainsemantic.InferenceUsageEvent{}, err
	}
	defer f.Close()
	if _, err := f.Write(append(raw, '\n')); err != nil {
		return domainsemantic.InferenceUsageEvent{}, err
	}
	if err := f.Sync(); err != nil {
		return domainsemantic.InferenceUsageEvent{}, err
	}
	m.manifest.UpdatedAt = time.Now().UTC()
	if err := persistJSON(filepath.Join(m.location, manifestFile), m.manifest); err != nil {
		return domainsemantic.InferenceUsageEvent{}, err
	}
	return event, nil
}

func (m *defaultManager) List(ctx context.Context, filter Filter) ([]domainsemantic.InferenceUsageEvent, error) {
	events, _, err := m.scan(ctx)
	if err != nil {
		return nil, err
	}
	out := []domainsemantic.InferenceUsageEvent{}
	for _, event := range events {
		if matches(event, filter) {
			out = append(out, event)
			if filter.Limit > 0 && len(out) >= filter.Limit {
				break
			}
		}
	}
	return out, nil
}

func (m *defaultManager) Summarize(ctx context.Context, filter Filter, groupBy []string) ([]SummaryRow, error) {
	events, err := m.List(ctx, filter)
	if err != nil {
		return nil, err
	}
	rows := map[string]*SummaryRow{}
	for _, event := range events {
		group := groupFor(event, groupBy)
		key, _ := json.Marshal(group)
		row := rows[string(key)]
		if row == nil {
			row = &SummaryRow{Group: group}
			rows[string(key)] = row
		}
		addToSummary(row, event)
	}
	out := make([]SummaryRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, *row)
	}
	sort.Slice(out, func(i, j int) bool { return fmt.Sprint(out[i].Group) < fmt.Sprint(out[j].Group) })
	return out, nil
}

func (m *defaultManager) RebuildIndexes(ctx context.Context) error {
	events, refs, err := m.scan(ctx)
	if err != nil {
		return err
	}
	root := filepath.Join(m.location, "indexes")
	if err := os.RemoveAll(root); err != nil {
		return err
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	buckets := map[string][]IndexEntry{}
	for i, event := range events {
		month := event.CreatedAt.Format("2006-01")
		entry := IndexEntry{EventID: event.ID, Segment: refs[i].Segment, Line: refs[i].Line, CreatedAt: event.CreatedAt}
		seenPrincipals := map[string]struct{}{}
		for _, principal := range []string{event.ActorPrincipalID.String(), event.EffectivePrincipalID.String(), event.OnBehalfOfPrincipalID.String()} {
			if principal == "" || principal == uuid.Nil.String() {
				continue
			}
			if _, ok := seenPrincipals[principal]; ok {
				continue
			}
			seenPrincipals[principal] = struct{}{}
			addIndex(buckets, "by_principal", principal, month, entry)
		}
		addIndex(buckets, "by_space", event.SpaceID.String(), month, entry)
		addIndex(buckets, "by_domain", event.DomainID.String(), month, entry)
		addIndex(buckets, "by_node", event.TargetNodeID.String(), month, entry)
		addIndex(buckets, "by_operation", event.Operation, month, entry)
		addIndex(buckets, "by_model_endpoint", event.ModelEndpointID.String(), month, entry)
		addIndex(buckets, "by_model", event.ModelID.String(), month, entry)
		addIndex(buckets, "by_credential_grant", event.CredentialGrantID.String(), month, entry)
	}
	for key, entries := range buckets {
		if err := persistJSON(filepath.Join(root, key), map[string]any{"entries": entries}); err != nil {
			return err
		}
	}
	return nil
}

func (m *defaultManager) RebuildRollups(ctx context.Context) error {
	events, err := m.List(ctx, Filter{})
	if err != nil {
		return err
	}
	root := filepath.Join(m.location, "rollups")
	if err := os.RemoveAll(root); err != nil {
		return err
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	for _, spec := range map[string][]string{
		"principal-monthly.json": {"month", "user"},
		"space-monthly.json":     {"month", "space"},
		"domain-monthly.json":    {"month", "domain"},
		"endpoint-monthly.json":  {"month", "model-endpoint"},
	} {
		rows := summarizeEvents(events, spec)
		if err := persistJSON(filepath.Join(root, specName(spec)), rows); err != nil {
			return err
		}
	}
	return nil
}

func specName(groupBy []string) string {
	switch strings.Join(groupBy, ",") {
	case "month,user":
		return "principal-monthly.json"
	case "month,space":
		return "space-monthly.json"
	case "month,domain":
		return "domain-monthly.json"
	case "month,model-endpoint":
		return "endpoint-monthly.json"
	default:
		return "rollup.json"
	}
}

type ledgerRef struct {
	Segment string
	Line    int
}

func (m *defaultManager) scan(ctx context.Context) ([]domainsemantic.InferenceUsageEvent, []ledgerRef, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	segments, err := filepath.Glob(filepath.Join(m.location, "inference-usage-*.kusag"))
	if err != nil {
		return nil, nil, err
	}
	sort.Strings(segments)
	events := []domainsemantic.InferenceUsageEvent{}
	refs := []ledgerRef{}
	for _, segmentPath := range segments {
		f, err := os.Open(segmentPath)
		if err != nil {
			return nil, nil, err
		}
		scanner := bufio.NewScanner(f)
		line := 0
		for scanner.Scan() {
			line++
			text := strings.TrimSpace(scanner.Text())
			if text == "" {
				continue
			}
			var event domainsemantic.InferenceUsageEvent
			if err := json.Unmarshal([]byte(text), &event); err != nil {
				_ = f.Close()
				return nil, nil, fmt.Errorf("decode %s line %d: %w", segmentPath, line, err)
			}
			events = append(events, event)
			refs = append(refs, ledgerRef{Segment: filepath.Base(segmentPath), Line: line})
		}
		if err := scanner.Err(); err != nil {
			_ = f.Close()
			return nil, nil, err
		}
		_ = f.Close()
	}
	return events, refs, nil
}

func validateEvent(ctx context.Context, event domainsemantic.InferenceUsageEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(event.Status) == "" {
		return fmt.Errorf("%w: status is required", ErrInvalidInput)
	}
	if strings.TrimSpace(event.Operation) == "" {
		return fmt.Errorf("%w: operation is required", ErrInvalidInput)
	}
	if event.InputTokens < 0 || event.OutputTokens < 0 || event.TotalTokens < 0 {
		return fmt.Errorf("%w: token counts must not be negative", ErrInvalidInput)
	}
	return nil
}

func matches(e domainsemantic.InferenceUsageEvent, f Filter) bool {
	if f.From != nil && e.CreatedAt.Before(*f.From) {
		return false
	}
	if f.To != nil && !e.CreatedAt.Before(*f.To) {
		return false
	}
	if f.PrincipalID != uuid.Nil && e.ActorPrincipalID != f.PrincipalID && e.EffectivePrincipalID != f.PrincipalID && e.OnBehalfOfPrincipalID != f.PrincipalID {
		return false
	}
	if f.SpaceID != uuid.Nil && e.SpaceID != f.SpaceID {
		return false
	}
	if f.DomainID != uuid.Nil && e.DomainID != f.DomainID {
		return false
	}
	if f.NodeID != uuid.Nil && e.TargetNodeID != f.NodeID {
		return false
	}
	if f.SemanticIndexID != uuid.Nil && e.SemanticIndexID != f.SemanticIndexID {
		return false
	}
	if f.Operation != "" && e.Operation != f.Operation {
		return false
	}
	if f.ModelEndpointID != uuid.Nil && e.ModelEndpointID != f.ModelEndpointID {
		return false
	}
	if f.ModelID != uuid.Nil && e.ModelID != f.ModelID {
		return false
	}
	if f.CredentialGrantID != uuid.Nil && e.CredentialGrantID != f.CredentialGrantID {
		return false
	}
	if f.Status != "" && e.Status != f.Status {
		return false
	}
	return true
}

func summarizeEvents(events []domainsemantic.InferenceUsageEvent, groupBy []string) []SummaryRow {
	rows := map[string]*SummaryRow{}
	for _, event := range events {
		group := groupFor(event, groupBy)
		key, _ := json.Marshal(group)
		row := rows[string(key)]
		if row == nil {
			row = &SummaryRow{Group: group}
			rows[string(key)] = row
		}
		addToSummary(row, event)
	}
	out := make([]SummaryRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, *row)
	}
	sort.Slice(out, func(i, j int) bool { return fmt.Sprint(out[i].Group) < fmt.Sprint(out[j].Group) })
	return out
}

func addToSummary(row *SummaryRow, event domainsemantic.InferenceUsageEvent) {
	row.CallCount++
	if event.Status == "success" {
		row.SuccessCount++
	}
	if event.Status == "failed" {
		row.FailedCount++
	}
	row.InputTokens += event.InputTokens
	row.OutputTokens += event.OutputTokens
	row.TotalTokens += event.TotalTokens
	switch event.TokenCountSource {
	case "provider_reported":
		row.ProviderReportedTokens += event.TotalTokens
	case "estimated":
		row.EstimatedTokens += event.TotalTokens
	case "unavailable", "":
		row.UnavailableTokenCount++
	}
}

func groupFor(event domainsemantic.InferenceUsageEvent, groupBy []string) map[string]string {
	group := map[string]string{}
	for _, key := range groupBy {
		switch strings.TrimSpace(key) {
		case "month":
			group["month"] = event.CreatedAt.Format("2006-01")
		case "user", "principal":
			group["user"] = firstNonNilUUID(event.OnBehalfOfPrincipalID, event.EffectivePrincipalID, event.ActorPrincipalID)
		case "space":
			group["space"] = event.SpaceID.String()
		case "domain":
			group["domain"] = event.DomainID.String()
		case "node":
			group["node"] = event.TargetNodeID.String()
		case "operation":
			group["operation"] = event.Operation
		case "model-endpoint":
			group["model_endpoint"] = event.ModelEndpointID.String()
		case "model":
			group["model"] = event.ModelID.String()
		case "credential-grant":
			group["credential_grant"] = event.CredentialGrantID.String()
		case "status":
			group["status"] = event.Status
		}
	}
	return group
}

func firstNonNilUUID(values ...uuid.UUID) string {
	for _, value := range values {
		if value != uuid.Nil {
			return value.String()
		}
	}
	return ""
}

func addIndex(buckets map[string][]IndexEntry, category, value, month string, entry IndexEntry) {
	if value == "" || value == uuid.Nil.String() {
		return
	}
	buckets[filepath.Join(category, value, month+".kidx")] = append(buckets[filepath.Join(category, value, month+".kidx")], entry)
}

func persistJSON(path string, v any) error {
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	return filestore.WriteFileAtomic(path, raw, 0o600)
}

func newID() uuid.UUID {
	id, err := uuid.NewV7()
	if err == nil {
		return id
	}
	return uuid.New()
}

// WriteCSV writes events to CSV for CLI export.
func WriteCSV(w io.Writer, events []domainsemantic.InferenceUsageEvent) error {
	cw := csv.NewWriter(w)
	if err := cw.Write([]string{"id", "created_at", "status", "operation", "actor_principal_id", "space_id", "domain_id", "target_node_id", "model_endpoint_id", "model_id", "credential_grant_id", "input_tokens", "output_tokens", "total_tokens", "token_count_source"}); err != nil {
		return err
	}
	for _, e := range events {
		if err := cw.Write([]string{e.ID.String(), e.CreatedAt.Format(time.RFC3339Nano), e.Status, e.Operation, e.ActorPrincipalID.String(), e.SpaceID.String(), e.DomainID.String(), e.TargetNodeID.String(), e.ModelEndpointID.String(), e.ModelID.String(), e.CredentialGrantID.String(), fmt.Sprint(e.InputTokens), fmt.Sprint(e.OutputTokens), fmt.Sprint(e.TotalTokens), e.TokenCountSource}); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}
