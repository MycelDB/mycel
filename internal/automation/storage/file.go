package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	graph "github.com/myceldb/mycel/internal/graph/model"
)

type FileStore struct{ root string }

func NewFileStore(root string) *FileStore { return &FileStore{root: root} }

func (s *FileStore) PutDefinition(ctx context.Context, def automation.Definition) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path := s.definitionPath(def.DomainID, def.ID)
	return writeJSONAtomic(path, def)
}

func (s *FileStore) GetDefinition(ctx context.Context, domainID graph.DomainID, id string) (automation.Definition, error) {
	if err := ctx.Err(); err != nil {
		return automation.Definition{}, err
	}
	var out automation.Definition
	if err := readJSON(s.definitionPath(domainID, id), &out); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return out, ErrNotFound
		}
		return out, err
	}
	return out, nil
}

func (s *FileStore) DeleteDefinition(ctx context.Context, domainID graph.DomainID, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	err := os.Remove(s.definitionPath(domainID, id))
	if errors.Is(err, os.ErrNotExist) {
		return ErrNotFound
	}
	return err
}

func (s *FileStore) ListDefinitionDomains(ctx context.Context) ([]graph.DomainID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dir := filepath.Join(s.root, "definitions")
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := []graph.DomainID{}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		id, err := uuid.Parse(entry.Name())
		if err == nil {
			out = append(out, graph.DomainID(id))
		}
	}
	return out, nil
}

func (s *FileStore) ListDefinitions(ctx context.Context, domainID graph.DomainID) ([]automation.Definition, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dir := filepath.Join(s.root, "definitions", domainID.String())
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := []automation.Definition{}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		var def automation.Definition
		if err := readJSON(filepath.Join(dir, entry.Name()), &def); err != nil {
			return nil, err
		}
		out = append(out, def)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *FileStore) PutInvocation(ctx context.Context, inv automation.Invocation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	day := inv.CreatedAt.Format("2006-01-02")
	if day == "0001-01-01" {
		day = "undated"
	}
	return writeJSONAtomic(filepath.Join(s.root, "invocations", inv.DomainID.String(), day, safeName(inv.ID)+".json"), inv)
}

func (s *FileStore) GetInvocation(ctx context.Context, domainID graph.DomainID, id string) (automation.Invocation, error) {
	if err := ctx.Err(); err != nil {
		return automation.Invocation{}, err
	}
	items, err := s.ListInvocations(ctx, domainID, InvocationFilter{Limit: 0})
	if err != nil {
		return automation.Invocation{}, err
	}
	for _, item := range items {
		if item.ID == id {
			return item, nil
		}
	}
	return automation.Invocation{}, ErrNotFound
}

func (s *FileStore) ListInvocations(ctx context.Context, domainID graph.DomainID, filter InvocationFilter) ([]automation.Invocation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root := filepath.Join(s.root, "invocations", domainID.String())
	out := []automation.Invocation{}
	if err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}
		var inv automation.Invocation
		if err := readJSON(path, &inv); err != nil {
			return err
		}
		if filter.AutomationID != "" && inv.AutomationID != filter.AutomationID {
			return nil
		}
		if filter.Status != "" && inv.Status != filter.Status {
			return nil
		}
		out = append(out, inv)
		return nil
	}); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	if filter.Limit > 0 && len(out) > filter.Limit {
		out = out[:filter.Limit]
	}
	return out, nil
}

func (s *FileStore) PutRun(ctx context.Context, run automation.Run) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	day := run.StartedAt.Format("2006-01-02")
	if day == "0001-01-01" {
		day = "undated"
	}
	return writeJSONAtomic(filepath.Join(s.root, "runs", run.DomainID.String(), day, safeName(run.ID)+".json"), run)
}

func (s *FileStore) GetRun(ctx context.Context, domainID graph.DomainID, runID string) (automation.Run, error) {
	if err := ctx.Err(); err != nil {
		return automation.Run{}, err
	}
	root := filepath.Join(s.root, "runs", domainID.String())
	var out automation.Run
	var invocationRun automation.Run
	foundExact := false
	foundInvocation := false
	if err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}
		var run automation.Run
		if err := readJSON(path, &run); err != nil {
			return err
		}
		if run.ID == runID {
			out, foundExact = run, true
			return nil
		}
		if run.InvocationID == runID && newerAutomationRun(run, invocationRun) {
			invocationRun, foundInvocation = run, true
		}
		return nil
	}); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return out, ErrNotFound
		}
		return out, err
	}
	if foundExact {
		return out, nil
	}
	if foundInvocation {
		return invocationRun, nil
	}
	return out, ErrNotFound
}

func newerAutomationRun(candidate, current automation.Run) bool {
	if current.ID == "" {
		return true
	}
	if candidate.AttemptNumber != current.AttemptNumber {
		return candidate.AttemptNumber > current.AttemptNumber
	}
	if !candidate.StartedAt.Equal(current.StartedAt) {
		return candidate.StartedAt.After(current.StartedAt)
	}
	return candidate.ID > current.ID
}

func (s *FileStore) PutProposal(ctx context.Context, proposal automation.Proposal) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	day := proposal.CreatedAt.Format("2006-01-02")
	if day == "0001-01-01" {
		day = "undated"
	}
	return writeJSONAtomic(filepath.Join(s.root, "proposals", proposal.DomainID.String(), day, safeName(proposal.ID)+".json"), proposal)
}

func (s *FileStore) GetProposal(ctx context.Context, domainID graph.DomainID, id string) (automation.Proposal, error) {
	items, err := s.ListProposals(ctx, domainID, "", 0)
	if err != nil {
		return automation.Proposal{}, err
	}
	for _, item := range items {
		if item.ID == id {
			return item, nil
		}
	}
	return automation.Proposal{}, ErrNotFound
}

func (s *FileStore) ListProposals(ctx context.Context, domainID graph.DomainID, status string, limit int) ([]automation.Proposal, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root := filepath.Join(s.root, "proposals", domainID.String())
	out := []automation.Proposal{}
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}
		var proposal automation.Proposal
		if err := readJSON(path, &proposal); err != nil {
			return err
		}
		if status == "" || proposal.Status == status {
			out = append(out, proposal)
		}
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *FileStore) PutPolicy(ctx context.Context, policy automation.Policy) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return writeJSONAtomic(filepath.Join(s.root, "policies", policy.DomainID.String()+".json"), policy)
}

func (s *FileStore) GetPolicy(ctx context.Context, domainID graph.DomainID) (automation.Policy, error) {
	if err := ctx.Err(); err != nil {
		return automation.Policy{}, err
	}
	var out automation.Policy
	if err := readJSON(filepath.Join(s.root, "policies", domainID.String()+".json"), &out); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return out, ErrNotFound
		}
		return out, err
	}
	return out, nil
}

func (s *FileStore) PutScheduleCheckpoint(ctx context.Context, checkpoint ScheduleCheckpoint) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return writeJSONAtomic(filepath.Join(s.root, "schedule-checkpoints", checkpoint.DomainID.String(), safeName(checkpoint.AutomationID)+".json"), checkpoint)
}

func (s *FileStore) GetScheduleCheckpoint(ctx context.Context, domainID graph.DomainID, automationID string) (ScheduleCheckpoint, error) {
	if err := ctx.Err(); err != nil {
		return ScheduleCheckpoint{}, err
	}
	var out ScheduleCheckpoint
	if err := readJSON(filepath.Join(s.root, "schedule-checkpoints", domainID.String(), safeName(automationID)+".json"), &out); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return out, ErrNotFound
		}
		return out, err
	}
	return out, nil
}

func (s *FileStore) PutWorkflowInstance(ctx context.Context, instance automation.WorkflowInstance) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	day := instance.CreatedAt.Format("2006-01-02")
	if day == "0001-01-01" {
		day = "undated"
	}
	return writeJSONAtomic(filepath.Join(s.root, "workflow-instances", instance.DomainID.String(), day, safeName(instance.ID)+".json"), instance)
}

func (s *FileStore) GetWorkflowInstance(ctx context.Context, domainID graph.DomainID, id string) (automation.WorkflowInstance, error) {
	items, err := s.ListWorkflowInstances(ctx, domainID, "", 0)
	if err != nil {
		return automation.WorkflowInstance{}, err
	}
	for _, item := range items {
		if item.ID == id {
			return item, nil
		}
	}
	return automation.WorkflowInstance{}, ErrNotFound
}

func (s *FileStore) ListWorkflowInstances(ctx context.Context, domainID graph.DomainID, status string, limit int) ([]automation.WorkflowInstance, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root := filepath.Join(s.root, "workflow-instances", domainID.String())
	out := []automation.WorkflowInstance{}
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}
		var inst automation.WorkflowInstance
		if err := readJSON(path, &inst); err != nil {
			return err
		}
		if status == "" || inst.Status == status {
			out = append(out, inst)
		}
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *FileStore) PutWorkflowStepRun(ctx context.Context, run automation.WorkflowStepRun) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	day := run.StartedAt.Format("2006-01-02")
	if day == "0001-01-01" {
		day = "undated"
	}
	return writeJSONAtomic(filepath.Join(s.root, "workflow-steps", run.DomainID.String(), day, safeName(run.ID)+".json"), run)
}

func (s *FileStore) ListWorkflowStepRuns(ctx context.Context, domainID graph.DomainID, instanceID string) ([]automation.WorkflowStepRun, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root := filepath.Join(s.root, "workflow-steps", domainID.String())
	out := []automation.WorkflowStepRun{}
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}
		var run automation.WorkflowStepRun
		if err := readJSON(path, &run); err != nil {
			return err
		}
		if instanceID == "" || run.InstanceID == instanceID {
			out = append(out, run)
		}
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.Before(out[j].StartedAt) })
	return out, nil
}

func (s *FileStore) PutSuccessfulInputIndex(ctx context.Context, record SuccessfulInputIndex) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return writeJSONAtomic(s.successfulInputIndexPath(record.DomainID, record.AutomationID, record.Version, record.ChangedElementID, record.InputHash), record)
}

func (s *FileStore) GetSuccessfulInputIndex(ctx context.Context, domainID graph.DomainID, automationID string, version int, changedElementID string, inputHash string) (SuccessfulInputIndex, error) {
	if err := ctx.Err(); err != nil {
		return SuccessfulInputIndex{}, err
	}
	var out SuccessfulInputIndex
	if err := readJSON(s.successfulInputIndexPath(domainID, automationID, version, changedElementID, inputHash), &out); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return out, ErrNotFound
		}
		return out, err
	}
	return out, nil
}

func (s *FileStore) definitionPath(domainID graph.DomainID, id string) string {
	return filepath.Join(s.root, "definitions", domainID.String(), safeName(id)+".json")
}

func (s *FileStore) successfulInputIndexPath(domainID graph.DomainID, automationID string, version int, changedElementID string, inputHash string) string {
	return filepath.Join(s.root, "indexes", "successful-input", domainID.String(), safeName(automationID), fmt.Sprintf("v%d", version), safeName(changedElementID), safeName(inputHash)+".json")
}

func safeName(value string) string {
	value = strings.TrimSpace(value)
	value = strings.ReplaceAll(value, string(filepath.Separator), "_")
	return value
}

func readJSON(path string, out any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, out)
}

func writeJSONAtomic(path string, value any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename automation file: %w", err)
	}
	return nil
}
