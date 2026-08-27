package service

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
	graph "github.com/myceldb/mycel/internal/graph/model"
)

type runnableAutomation struct {
	Procedure  automation.Procedure
	Binding    automation.Binding
	Definition automation.Definition
	Legacy     bool
}

func (m *AutomationManager) listRunnableAutomations(ctx context.Context, domainID graph.DomainID, status string) ([]runnableAutomation, error) {
	byID := map[string]runnableAutomation{}
	bindings, err := m.store.ListBindings(ctx, domainID)
	if err != nil {
		return nil, mapStoreError(err)
	}
	for _, binding := range bindings {
		binding.DomainID = domainID
		binding = binding.Normalize()
		if status != "" && binding.Status != status {
			continue
		}
		procedure, err := m.store.GetProcedure(ctx, domainID, binding.ProcedureID)
		if err != nil {
			continue
		}
		procedure.DomainID = domainID
		procedure = procedure.Normalize()
		if status == automation.StatusEnabled && procedure.Status != automation.StatusEnabled {
			continue
		}
		def := automation.ComposeDefinition(procedure, binding)
		byID[binding.ID] = runnableAutomation{Procedure: procedure, Binding: binding, Definition: def}
	}
	if m.raftEnabled() {
		out := make([]runnableAutomation, 0, len(byID))
		for _, item := range byID {
			out = append(out, item)
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Binding.ID < out[j].Binding.ID })
		return out, nil
	}
	defs, err := m.store.ListDefinitions(ctx, domainID)
	if err != nil {
		return nil, mapStoreError(err)
	}
	for _, def := range defs {
		def.DomainID = domainID
		def = def.Normalize()
		if status != "" && def.Status != status {
			continue
		}
		if _, explicit := byID[def.ID]; explicit {
			continue
		}
		procedure, binding := automation.ExpandDefinition(def)
		byID[binding.ID] = runnableAutomation{Procedure: procedure, Binding: binding, Definition: def, Legacy: true}
	}
	out := make([]runnableAutomation, 0, len(byID))
	for _, item := range byID {
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Binding.ID < out[j].Binding.ID })
	return out, nil
}

func (m *AutomationManager) resolveInvocationAutomation(ctx context.Context, domainID graph.DomainID, inv automation.Invocation) (runnableAutomation, error) {
	bindingID := strings.TrimSpace(firstNonEmptyString(inv.BindingID, inv.AutomationID))
	if bindingID != "" {
		binding, err := m.store.GetBinding(ctx, domainID, bindingID)
		if err == nil {
			procedure, err := m.store.GetProcedure(ctx, domainID, binding.ProcedureID)
			if err != nil {
				return runnableAutomation{}, mapStoreError(err)
			}
			binding.DomainID = domainID
			procedure.DomainID = domainID
			definition := automation.ComposeDefinition(procedure, binding)
			return runnableAutomation{Procedure: procedure.Normalize(), Binding: binding.Normalize(), Definition: definition}, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return runnableAutomation{}, mapStoreError(err)
		}
	}
	if m.raftEnabled() {
		return runnableAutomation{}, ErrAutomationNotFound
	}
	def, err := m.GetAutomation(ctx, domainID, inv.AutomationID)
	if err != nil {
		return runnableAutomation{}, err
	}
	procedure, binding := automation.ExpandDefinition(def)
	return runnableAutomation{Procedure: procedure, Binding: binding, Definition: def, Legacy: true}, nil
}

func invocationRuntime(binding automation.Binding, eventOriginPrincipalID string) (actor string, owner string, onBehalf string) {
	binding = binding.Normalize()
	actor = firstNonEmptyString(binding.Runtime.ActorPrincipalID, automationActor)
	owner = strings.TrimSpace(binding.Runtime.OwnerPrincipalID)
	onBehalf = strings.TrimSpace(binding.Runtime.OnBehalfOfPrincipalID)
	if binding.Runtime.EventOriginOverride == automation.RuntimeEventOriginAllow && strings.TrimSpace(eventOriginPrincipalID) != "" {
		onBehalf = strings.TrimSpace(eventOriginPrincipalID)
	}
	onBehalf = firstNonEmptyString(onBehalf, owner, actor)
	return actor, owner, onBehalf
}

func invocationForRunnable(now func() time.Time, domainID graph.DomainID, item runnableAutomation, base automation.Invocation, eventOriginPrincipalID string) automation.Invocation {
	actor, owner, onBehalf := invocationRuntime(item.Binding, eventOriginPrincipalID)
	base.DomainID = domainID
	base.AutomationID = item.Binding.ID
	base.AutomationVersion = item.Binding.Version
	base.BindingID = item.Binding.ID
	base.BindingVersion = item.Binding.Version
	base.ProcedureID = item.Procedure.ID
	base.ProcedureVersion = item.Procedure.Version
	base.ActorPrincipalID = actor
	base.OwnerPrincipalID = owner
	base.OnBehalfOfPrincipalID = onBehalf
	base.AutomationOwnerPrincipalID = owner
	base.EventOriginPrincipalID = strings.TrimSpace(eventOriginPrincipalID)
	if strings.TrimSpace(base.SpaceID) == "" {
		base.SpaceID = strings.TrimSpace(item.Binding.Scope.SpaceID)
	}
	base.Status = "pending"
	if base.CreatedAt.IsZero() {
		base.CreatedAt = now()
	}
	if base.UpdatedAt.IsZero() {
		base.UpdatedAt = base.CreatedAt
	}
	return base
}
