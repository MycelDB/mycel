package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/automation/actions"
	automation "github.com/myceldb/mycel/internal/automation/model"
	autooutput "github.com/myceldb/mycel/internal/automation/output"
	"github.com/myceldb/mycel/internal/automation/provider"
	"github.com/myceldb/mycel/internal/automation/render"
	sessionservice "github.com/myceldb/mycel/internal/session/service"
)

const automationActor = "automation"

func (m *AutomationManager) executeInvocation(ctx context.Context, def automation.Definition, inv automation.Invocation) (automation.Run, error) {
	now := m.now()
	run := automation.Run{ID: newRunID(), DomainID: inv.DomainID, InvocationID: inv.ID, AttemptNumber: 1, Status: "running", Provider: def.Model.Provider, Model: def.Model.Model, StartedAt: now}
	if m.sessions == nil || m.graphs == nil {
		result, err := render.Render(def.Input, render.Context{})
		if err != nil {
			return run, err
		}
		run.RenderedInputHash = result.Hash
		output, err := m.generateText(ctx, def, result.Text, &run)
		if err != nil {
			return run, err
		}
		run.Status = "succeeded"
		run.CompletedAt = now
		outSum := sha256.Sum256([]byte(output))
		run.OutputHash = hex.EncodeToString(outSum[:])
		return run, nil
	}
	sess, err := m.sessions.OpenSession(ctx, sessionservice.OpenSessionInput{UserID: automationActor, SpaceID: inv.SpaceID, DomainID: inv.DomainID.String()})
	if err != nil {
		return run, err
	}
	tx, err := m.sessions.BeginTransaction(ctx, sessionservice.BeginTransactionInput{UserID: automationActor, SessionID: sess.ID, Mode: sessionservice.TransactionModeReadWrite})
	if err != nil {
		return run, err
	}
	node, err := m.graphs.GetNode(ctx, tx, inv.ChangedElementID)
	if err != nil {
		_, _ = m.sessions.RollbackTransaction(ctx, automationActor, tx.ID)
		return run, err
	}
	condition, err := m.evaluateCondition(ctx, tx, def, node, inv.OldNode)
	if err != nil {
		_, _ = m.sessions.RollbackTransaction(ctx, automationActor, tx.ID)
		return run, err
	} else if !condition.Matched {
		_, _ = m.sessions.RollbackTransaction(ctx, automationActor, tx.ID)
		run.Status = "skipped"
		run.Error = conditionFalseReason
		run.CompletedAt = m.now()
		return run, nil
	}
	rendered, err := render.Render(def.Input, render.Context{Changed: node, Old: inv.OldNode, Aliases: condition.Aliases})
	if err != nil {
		_, _ = m.sessions.RollbackTransaction(ctx, automationActor, tx.ID)
		return run, err
	}
	run.RenderedInputHash = rendered.Hash
	if def.Safety.Idempotency.SkipIfOutputUnchanged {
		duplicate, err := m.hasSuccessfulInputHash(ctx, inv, rendered.Hash)
		if err != nil {
			_, _ = m.sessions.RollbackTransaction(ctx, automationActor, tx.ID)
			return run, err
		}
		if duplicate {
			_, _ = m.sessions.RollbackTransaction(ctx, automationActor, tx.ID)
			run.Status = "skipped"
			run.Error = skipReasonDuplicateInput
			run.CompletedAt = m.now()
			return run, nil
		}
	}
	output, err := m.generateText(ctx, def, rendered.Text, &run)
	if err != nil {
		_, _ = m.sessions.RollbackTransaction(ctx, automationActor, tx.ID)
		return run, err
	}
	parsed, err := autooutput.Parse(def.Output.Mode, def.Output.Schema, output)
	if err != nil {
		_, _ = m.sessions.RollbackTransaction(ctx, automationActor, tx.ID)
		return run, err
	}
	run.ActionFingerprint = actionFingerprint(def, parsed)
	summary, err := (actions.Engine{Graphs: m.graphs}).Apply(ctx, tx, actions.Context{Definition: def, RunID: run.ID, Changed: node, Result: parsed})
	if err != nil {
		_, _ = m.sessions.RollbackTransaction(ctx, automationActor, tx.ID)
		return run, err
	}
	if !summary.Changed {
		_, _ = m.sessions.RollbackTransaction(ctx, automationActor, tx.ID)
		run.Status = "skipped"
		run.CompletedAt = m.now()
		return run, nil
	}
	graphCommit, err := m.graphs.CommitTransactionGraph(ctx, tx)
	if err != nil {
		return run, err
	}
	commit, err := m.sessions.CommitTransactionAtRevision(ctx, automationActor, tx.ID, graphCommit.OperationCount, graphCommit.CommittedRevision)
	if err != nil {
		return run, err
	}
	run.MutationID = commit.ID
	run.Status = "succeeded"
	run.CompletedAt = m.now()
	outSum := sha256.Sum256([]byte(output))
	run.OutputHash = hex.EncodeToString(outSum[:])
	return run, nil
}

func newRunID() string { return uuid.NewString() }

func (m *AutomationManager) generateText(ctx context.Context, def automation.Definition, rendered string, run *automation.Run) (string, error) {
	p := m.provider
	if p == nil {
		run.Usage = automation.TokenUsage{Status: provider.UsageStatusUnavailable}
		return "", provider.ErrUnavailable
	}
	resp, err := p.GenerateText(ctx, provider.Request{Provider: def.Model.Provider, Model: def.Model.Model, Prompt: def.Prompt, Input: rendered, Temperature: def.Model.Temperature, MaxOutputTokens: def.Model.MaxOutputTokens})
	if err != nil {
		return "", err
	}
	run.ProviderRequestID = resp.ProviderRequestID
	usageStatus := resp.Usage.Status
	if usageStatus == "" {
		usageStatus = provider.UsageStatusUnavailable
	}
	totalTokens := resp.Usage.TotalTokens
	if totalTokens == 0 && (resp.Usage.InputTokens != 0 || resp.Usage.OutputTokens != 0) {
		totalTokens = resp.Usage.InputTokens + resp.Usage.OutputTokens
	}
	run.Usage = automation.TokenUsage{InputTokens: resp.Usage.InputTokens, OutputTokens: resp.Usage.OutputTokens, TotalTokens: totalTokens, CachedInputTokens: resp.Usage.CachedInputTokens, ReasoningTokens: resp.Usage.ReasoningTokens, Status: usageStatus, Metadata: resp.Usage.Metadata}
	run.Cost = m.accounting.Estimate(def.Model.Provider, def.Model.Model, run.Usage)
	if m.maxTokensPerRun > 0 && run.Usage.TotalTokens > m.maxTokensPerRun {
		return "", fmt.Errorf("automation token ceiling exceeded: %d > %d", run.Usage.TotalTokens, m.maxTokensPerRun)
	}
	if m.maxCostPerRun > 0 && run.Cost.TotalCost > m.maxCostPerRun {
		return "", fmt.Errorf("automation cost ceiling exceeded: %.6f > %.6f", run.Cost.TotalCost, m.maxCostPerRun)
	}
	return resp.Text, nil
}
