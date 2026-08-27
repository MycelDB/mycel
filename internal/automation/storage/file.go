package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	graph "github.com/myceldb/mycel/internal/graph/model"
)

type FileStore struct{ root string }

func NewFileStore(root string) *FileStore { return &FileStore{root: root} }

func (s *FileStore) PutProcedure(ctx context.Context, procedure automation.Procedure) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return writeJSONAtomic(s.procedurePath(procedure.DomainID, procedure.ID), procedure)
}

func (s *FileStore) GetProcedure(ctx context.Context, domainID graph.DomainID, id string) (automation.Procedure, error) {
	if err := ctx.Err(); err != nil {
		return automation.Procedure{}, err
	}
	var out automation.Procedure
	if err := readJSON(s.procedurePath(domainID, id), &out); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return out, ErrNotFound
		}
		return out, err
	}
	return out, nil
}

func (s *FileStore) DeleteProcedure(ctx context.Context, domainID graph.DomainID, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	err := os.Remove(s.procedurePath(domainID, id))
	if errors.Is(err, os.ErrNotExist) {
		return ErrNotFound
	}
	return err
}

func (s *FileStore) ListProcedureDomains(ctx context.Context) ([]graph.DomainID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.listDomains("procedures")
}

func (s *FileStore) ListProcedures(ctx context.Context, domainID graph.DomainID) ([]automation.Procedure, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return listJSONFiles[automation.Procedure](filepath.Join(s.root, "procedures", domainID.String()))
}

func (s *FileStore) PutBinding(ctx context.Context, binding automation.Binding) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return writeJSONAtomic(s.bindingPath(binding.DomainID, binding.ID), binding)
}

func (s *FileStore) GetBinding(ctx context.Context, domainID graph.DomainID, id string) (automation.Binding, error) {
	if err := ctx.Err(); err != nil {
		return automation.Binding{}, err
	}
	var out automation.Binding
	if err := readJSON(s.bindingPath(domainID, id), &out); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return out, ErrNotFound
		}
		return out, err
	}
	return out, nil
}

func (s *FileStore) DeleteBinding(ctx context.Context, domainID graph.DomainID, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	err := os.Remove(s.bindingPath(domainID, id))
	if errors.Is(err, os.ErrNotExist) {
		return ErrNotFound
	}
	return err
}

func (s *FileStore) ListBindingDomains(ctx context.Context) ([]graph.DomainID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.listDomains("bindings")
}

func (s *FileStore) ListBindings(ctx context.Context, domainID graph.DomainID) ([]automation.Binding, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return listJSONFiles[automation.Binding](filepath.Join(s.root, "bindings", domainID.String()))
}

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
	return s.listDomains("definitions")
}

func (s *FileStore) ListDefinitions(ctx context.Context, domainID graph.DomainID) ([]automation.Definition, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return listJSONFiles[automation.Definition](filepath.Join(s.root, "definitions", domainID.String()))
}

func (s *FileStore) PutInvocation(ctx context.Context, inv automation.Invocation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	day := inv.CreatedAt.Format("2006-01-02")
	if day == "0001-01-01" {
		day = "undated"
	}
	path := filepath.Join(s.root, "invocations", inv.DomainID.String(), day, safeName(inv.ID)+".json")
	if err := writeJSONAtomic(path, inv); err != nil {
		return err
	}
	return s.deleteDuplicateInvocationFiles(ctx, inv.DomainID, inv.ID, path)
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

func (s *FileStore) DeleteInvocation(ctx context.Context, domainID graph.DomainID, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	root := filepath.Join(s.root, "invocations", domainID.String())
	removed := false
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
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
		if inv.ID == id {
			removed = true
			return os.Remove(path)
		}
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if !removed {
		return ErrNotFound
	}
	return nil
}

func (s *FileStore) deleteDuplicateInvocationFiles(ctx context.Context, domainID graph.DomainID, id string, keepPath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	root := filepath.Join(s.root, "invocations", domainID.String())
	return filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Ext(path) != ".json" || path == keepPath {
			return nil
		}
		var inv automation.Invocation
		if err := readJSON(path, &inv); err != nil {
			return err
		}
		if inv.ID == id {
			return os.Remove(path)
		}
		return nil
	})
}

func (s *FileStore) ListInvocationDomains(ctx context.Context) ([]graph.DomainID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.listDomains("invocations")
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

func (s *FileStore) DeleteRun(ctx context.Context, domainID graph.DomainID, runID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	root := filepath.Join(s.root, "runs", domainID.String())
	removed := false
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
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
			removed = true
			return os.Remove(path)
		}
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if !removed {
		return ErrNotFound
	}
	return nil
}

func (s *FileStore) ListRunDomains(ctx context.Context) ([]graph.DomainID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.listDomains("runs")
}

func (s *FileStore) ListRuns(ctx context.Context, domainID graph.DomainID) ([]automation.Run, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root := filepath.Join(s.root, "runs", domainID.String())
	out := []automation.Run{}
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
		out = append(out, run)
		return nil
	}); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.After(out[j].StartedAt) })
	return out, nil
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
	if err := readJSON(s.scheduleCheckpointPath(domainID, automationID), &out); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return out, ErrNotFound
		}
		return out, err
	}
	return out, nil
}

func (s *FileStore) DeleteScheduleCheckpoint(ctx context.Context, domainID graph.DomainID, automationID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	err := os.Remove(s.scheduleCheckpointPath(domainID, automationID))
	if errors.Is(err, os.ErrNotExist) {
		return ErrNotFound
	}
	return err
}

func (s *FileStore) ListScheduleCheckpointDomains(ctx context.Context) ([]graph.DomainID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.listDomains("schedule-checkpoints")
}

func (s *FileStore) ListScheduleCheckpoints(ctx context.Context, domainID graph.DomainID) ([]ScheduleCheckpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return listJSONFiles[ScheduleCheckpoint](filepath.Join(s.root, "schedule-checkpoints", domainID.String()))
}

func (s *FileStore) PutGraphReplayCursor(ctx context.Context, cursor GraphReplayCursor) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return writeJSONAtomic(s.graphReplayCursorPath(cursor.SpaceID, cursor.DomainID), cursor)
}

func (s *FileStore) GetGraphReplayCursor(ctx context.Context, spaceID string, domainID graph.DomainID) (GraphReplayCursor, error) {
	if err := ctx.Err(); err != nil {
		return GraphReplayCursor{}, err
	}
	var out GraphReplayCursor
	if err := readJSON(s.graphReplayCursorPath(spaceID, domainID), &out); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return out, ErrNotFound
		}
		return out, err
	}
	return out, nil
}

func (s *FileStore) DeleteGraphReplayCursor(ctx context.Context, spaceID string, domainID graph.DomainID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	err := os.Remove(s.graphReplayCursorPath(spaceID, domainID))
	if errors.Is(err, os.ErrNotExist) {
		return ErrNotFound
	}
	return err
}

func (s *FileStore) ListGraphReplayCursors(ctx context.Context) ([]GraphReplayCursor, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root := filepath.Join(s.root, "graph-replay-cursors")
	out := []GraphReplayCursor{}
	if err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}
		var cursor GraphReplayCursor
		if err := readJSON(path, &cursor); err != nil {
			return err
		}
		out = append(out, cursor)
		return nil
	}); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SpaceID != out[j].SpaceID {
			return out[i].SpaceID < out[j].SpaceID
		}
		return out[i].DomainID.String() < out[j].DomainID.String()
	})
	return out, nil
}

func (s *FileStore) PutWorkflowInstance(ctx context.Context, instance automation.WorkflowInstance) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return writeJSONAtomic(s.workflowInstancePath(instance.DomainID, instance.CreatedAt, instance.ID), instance)
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

func (s *FileStore) DeleteWorkflowInstance(ctx context.Context, domainID graph.DomainID, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	items, err := s.ListWorkflowInstances(ctx, domainID, "", 0)
	if err != nil {
		return err
	}
	for _, item := range items {
		if item.ID == id {
			err := os.Remove(s.workflowInstancePath(domainID, item.CreatedAt, id))
			if errors.Is(err, os.ErrNotExist) {
				return ErrNotFound
			}
			return err
		}
	}
	return ErrNotFound
}

func (s *FileStore) ListWorkflowInstanceDomains(ctx context.Context) ([]graph.DomainID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.listDomains("workflow-instances")
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
	return writeJSONAtomic(s.workflowStepRunPath(run.DomainID, run.StartedAt, run.ID), run)
}

func (s *FileStore) DeleteWorkflowStepRun(ctx context.Context, domainID graph.DomainID, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	runs, err := s.ListWorkflowStepRuns(ctx, domainID, "")
	if err != nil {
		return err
	}
	for _, run := range runs {
		if run.ID == id {
			err := os.Remove(s.workflowStepRunPath(domainID, run.StartedAt, id))
			if errors.Is(err, os.ErrNotExist) {
				return ErrNotFound
			}
			return err
		}
	}
	return ErrNotFound
}

func (s *FileStore) ListWorkflowStepRunDomains(ctx context.Context) ([]graph.DomainID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.listDomains("workflow-steps")
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
	elementID := firstNonEmpty(record.TargetElementID, record.ChangedElementID)
	return writeJSONAtomic(s.successfulInputIndexPath(record.DomainID, record.AutomationID, record.Version, elementID, record.InputHash), record)
}

func (s *FileStore) GetSuccessfulInputIndex(ctx context.Context, domainID graph.DomainID, automationID string, version int, elementID string, inputHash string) (SuccessfulInputIndex, error) {
	if err := ctx.Err(); err != nil {
		return SuccessfulInputIndex{}, err
	}
	var out SuccessfulInputIndex
	if err := readJSON(s.successfulInputIndexPath(domainID, automationID, version, elementID, inputHash), &out); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return out, ErrNotFound
		}
		return out, err
	}
	return out, nil
}

func (s *FileStore) procedurePath(domainID graph.DomainID, id string) string {
	return filepath.Join(s.root, "procedures", domainID.String(), safeName(id)+".json")
}

func (s *FileStore) bindingPath(domainID graph.DomainID, id string) string {
	return filepath.Join(s.root, "bindings", domainID.String(), safeName(id)+".json")
}

func (s *FileStore) definitionPath(domainID graph.DomainID, id string) string {
	return filepath.Join(s.root, "definitions", domainID.String(), safeName(id)+".json")
}

func (s *FileStore) scheduleCheckpointPath(domainID graph.DomainID, automationID string) string {
	return filepath.Join(s.root, "schedule-checkpoints", domainID.String(), safeName(automationID)+".json")
}

func (s *FileStore) graphReplayCursorPath(spaceID string, domainID graph.DomainID) string {
	return filepath.Join(s.root, "graph-replay-cursors", safeName(spaceID), domainID.String()+".json")
}

func (s *FileStore) workflowInstancePath(domainID graph.DomainID, createdAt time.Time, id string) string {
	day := createdAt.Format("2006-01-02")
	if day == "0001-01-01" {
		day = "undated"
	}
	return filepath.Join(s.root, "workflow-instances", domainID.String(), day, safeName(id)+".json")
}

func (s *FileStore) workflowStepRunPath(domainID graph.DomainID, startedAt time.Time, id string) string {
	day := startedAt.Format("2006-01-02")
	if day == "0001-01-01" {
		day = "undated"
	}
	return filepath.Join(s.root, "workflow-steps", domainID.String(), day, safeName(id)+".json")
}

func (s *FileStore) listDomains(kind string) ([]graph.DomainID, error) {
	dir := filepath.Join(s.root, kind)
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

func listJSONFiles[T any](dir string) ([]T, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := []T{}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		var item T
		if err := readJSON(filepath.Join(dir, entry.Name()), &item); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool { return jsonSortKey(out[i]) < jsonSortKey(out[j]) })
	return out, nil
}

func jsonSortKey(value any) string {
	v := reflect.ValueOf(value)
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	if v.IsValid() && v.Kind() == reflect.Struct {
		for _, name := range []string{"ID", "AutomationID"} {
			field := v.FieldByName(name)
			if field.IsValid() && field.Kind() == reflect.String {
				return field.String()
			}
		}
	}
	return fmt.Sprint(value)
}

func (s *FileStore) DeleteSuccessfulInputIndex(ctx context.Context, record SuccessfulInputIndex) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	elementID := firstNonEmpty(record.TargetElementID, record.ChangedElementID)
	err := os.Remove(s.successfulInputIndexPath(record.DomainID, record.AutomationID, record.Version, elementID, record.InputHash))
	if errors.Is(err, os.ErrNotExist) {
		return ErrNotFound
	}
	return err
}

func (s *FileStore) ListSuccessfulInputIndexDomains(ctx context.Context) ([]graph.DomainID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.listDomains(filepath.Join("indexes", "successful-input"))
}

func (s *FileStore) ListSuccessfulInputIndexes(ctx context.Context, domainID graph.DomainID) ([]SuccessfulInputIndex, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root := filepath.Join(s.root, "indexes", "successful-input", domainID.String())
	out := []SuccessfulInputIndex{}
	if err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}
		var record SuccessfulInputIndex
		if err := readJSON(path, &record); err != nil {
			return err
		}
		out = append(out, record)
		return nil
	}); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return jsonSortKey(out[i]) < jsonSortKey(out[j]) })
	return out, nil
}

func (s *FileStore) successfulInputIndexPath(domainID graph.DomainID, automationID string, version int, elementID string, inputHash string) string {
	return filepath.Join(s.root, "indexes", "successful-input", domainID.String(), safeName(automationID), fmt.Sprintf("v%d", version), safeName(elementID), safeName(inputHash)+".json")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
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
