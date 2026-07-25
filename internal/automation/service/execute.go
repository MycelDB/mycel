package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/google/uuid"
	"strings"

	automation "github.com/myceldb/mycel/internal/automation/model"
	graph "github.com/myceldb/mycel/internal/graph/model"
	graphservice "github.com/myceldb/mycel/internal/graph/service"
	sessionservice "github.com/myceldb/mycel/internal/session/service"
)

const automationActor = "automation"

func (m *AutomationManager) executeInvocation(ctx context.Context, def automation.Definition, inv automation.Invocation) (automation.Run, error) {
	now := m.now()
	run := automation.Run{ID: newRunID(), DomainID: inv.DomainID, InvocationID: inv.ID, AttemptNumber: 1, Status: "running", Provider: def.Model.Provider, Model: def.Model.Model, StartedAt: now}
	if m.sessions == nil || m.graphs == nil {
		run.Status = "succeeded"
		run.CompletedAt = now
		sum := sha256.Sum256([]byte(strings.Join(def.Input.Fields, "\n")))
		run.RenderedInputHash = hex.EncodeToString(sum[:])
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
	rendered := renderNodeFields(node, def.Input.Fields)
	sum := sha256.Sum256([]byte(rendered))
	run.RenderedInputHash = hex.EncodeToString(sum[:])
	output := synthesizeTextOutput(def, rendered)
	updated, changed, err := applyTextOutput(node, def, output, run.ID)
	if err != nil {
		_, _ = m.sessions.RollbackTransaction(ctx, automationActor, tx.ID)
		return run, err
	}
	if !changed {
		_, _ = m.sessions.RollbackTransaction(ctx, automationActor, tx.ID)
		run.Status = "skipped"
		run.CompletedAt = m.now()
		return run, nil
	}
	_, err = m.graphs.UpdateNode(ctx, tx, graphservice.UpdateNodeInput{NodeID: updated.ID.String(), Labels: updated.Labels, Properties: updated.Properties, Payload: updated.Payload, Meta: updated.Meta, Content: &updated.Content, Props: updated.Props})
	if err != nil {
		_, _ = m.sessions.RollbackTransaction(ctx, automationActor, tx.ID)
		return run, err
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

func renderNodeFields(node graph.Node, fields []string) string {
	var b strings.Builder
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		b.WriteString("# " + field + "\n")
		b.WriteString(fmt.Sprint(readNodePath(node, field)))
		b.WriteString("\n\n")
	}
	return b.String()
}

func synthesizeTextOutput(def automation.Definition, rendered string) string {
	// Phase 5 wires the full action path; provider-backed generation can replace this deterministic placeholder.
	return strings.TrimSpace(def.Prompt + "\n\n" + rendered)
}

func applyTextOutput(node graph.Node, def automation.Definition, output string, runID string) (graph.Node, bool, error) {
	if len(def.Output.Actions) != 1 || def.Output.Actions[0].UpdateNode == nil {
		return node, false, fmt.Errorf("missing update_node action")
	}
	for path := range def.Output.Actions[0].UpdateNode.Set {
		current := fmt.Sprint(readNodePath(node, path))
		if current == output {
			return node, false, nil
		}
		writeNodePath(&node, path, output)
		if node.Meta == nil {
			node.Meta = map[string]any{}
		}
		node.Meta["automation"] = map[string]any{"run_id": runID, "automation_id": def.ID, "generated": true, "depth": 1}
		return node, true, nil
	}
	return node, false, fmt.Errorf("missing output field path")
}

func readNodePath(node graph.Node, path string) any {
	switch {
	case path == "content":
		return node.Content
	case strings.HasPrefix(path, "props."):
		return node.Props[strings.TrimPrefix(path, "props.")]
	case strings.HasPrefix(path, "properties."):
		return node.Properties[strings.TrimPrefix(path, "properties.")]
	case strings.HasPrefix(path, "payload."):
		return node.Payload[strings.TrimPrefix(path, "payload.")]
	default:
		return ""
	}
}

func writeNodePath(node *graph.Node, path string, value any) {
	switch {
	case path == "content":
		node.Content = fmt.Sprint(value)
	case strings.HasPrefix(path, "props."):
		if node.Props == nil {
			node.Props = map[string]any{}
		}
		node.Props[strings.TrimPrefix(path, "props.")] = value
	case strings.HasPrefix(path, "properties."):
		if node.Properties == nil {
			node.Properties = map[string]any{}
		}
		node.Properties[strings.TrimPrefix(path, "properties.")] = value
	case strings.HasPrefix(path, "payload."):
		if node.Payload == nil {
			node.Payload = map[string]any{}
		}
		node.Payload[strings.TrimPrefix(path, "payload.")] = value
	}
}
