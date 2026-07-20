package space

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/graph/model"
	storetemplate "github.com/myceldb/mycel/internal/graph/template/storage"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"github.com/myceldb/mycel/internal/wal"
)

func templateFromImport(spaceID domainspace.SpaceID, in storetemplate.TemplateImport) graph.Template {
	allowed := make([]graph.TemplateProperty, 0, len(in.Properties.Allowed))
	for _, p := range in.Properties.Allowed {
		allowed = append(allowed, graph.TemplateProperty{Name: strings.TrimSpace(p.Name), Type: p.Type, Required: p.Required, Default: p.Default, Description: strings.TrimSpace(p.Description)})
	}
	refs := make([]graph.TemplateRef, 0, len(in.Children.AllowedTemplates))
	for _, r := range in.Children.AllowedTemplates {
		refs = append(refs, graph.TemplateRef{Key: strings.TrimSpace(r.Key), Version: strings.TrimSpace(r.Version)})
	}
	var order *graph.ChildOrderPolicy
	if in.Children.Order != nil {
		order = &graph.ChildOrderPolicy{Mode: in.Children.Order.Mode, Property: strings.TrimSpace(in.Children.Order.Property), Direction: in.Children.Order.Direction}
	}
	return graph.Template{ID: uuid.New(), SpaceID: spaceID, Key: strings.TrimSpace(in.Key), Version: strings.TrimSpace(in.Version), DisplayName: strings.TrimSpace(in.DisplayName), Description: strings.TrimSpace(in.Description), System: in.System, State: graph.TemplateStateActive, Properties: graph.PropertyPolicy{AllowExtra: in.Properties.AllowExtra, Allowed: allowed, Forbidden: append([]string(nil), in.Properties.Forbidden...)}, Children: graph.ChildPolicy{Allowed: in.Children.Allowed, AllowedTemplates: refs, Order: order}}
}

func (m *Module) commitTemplatePut(ctx context.Context, template graph.Template) (graph.Template, error) {
	record := putTemplateRecord{Template: template}
	if m.raftGroups != nil {
		cmd, err := m.buildPutTemplateRaftCommand(record, m.partitionCount, newInternalCommandID("space-template-put"))
		if err != nil {
			return graph.Template{}, err
		}
		if err := m.proposeSpaceMetadataCommand(ctx, cmd); err != nil {
			return graph.Template{}, err
		}
		return template, nil
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return graph.Template{}, err
	}
	lsn, err := m.wal.Append(ctx, wal.PendingRecord{Type: recordTypePutTemplate, SchemaVersion: 1, Encoding: wal.PayloadEncodingJSON, Payload: payload})
	if err != nil {
		return graph.Template{}, err
	}
	if err := m.wal.Sync(ctx, lsn); err != nil {
		return graph.Template{}, err
	}
	applied, err := m.templates.ApplyPut(ctx, template)
	if err != nil {
		return graph.Template{}, err
	}
	if err := m.markWALApplied(ctx, lsn); err != nil {
		return graph.Template{}, err
	}
	return applied, nil
}

func (m *Module) commitTemplateDelete(ctx context.Context, template graph.Template) error {
	record := deleteTemplateRecord{TemplateID: template.ID, SpaceID: template.SpaceID}
	if m.raftGroups != nil {
		cmd, err := m.buildDeleteTemplateRaftCommand(record, m.partitionCount, newInternalCommandID("space-template-delete"))
		if err != nil {
			return err
		}
		return m.proposeSpaceMetadataCommand(ctx, cmd)
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return err
	}
	lsn, err := m.wal.Append(ctx, wal.PendingRecord{Type: recordTypeDeleteTemplate, SchemaVersion: 1, Encoding: wal.PayloadEncodingJSON, Payload: payload})
	if err != nil {
		return err
	}
	if err := m.wal.Sync(ctx, lsn); err != nil {
		return err
	}
	if err := m.templates.ApplyDelete(ctx, template.ID); err != nil {
		return err
	}
	return m.markWALApplied(ctx, lsn)
}

func (m *Module) markWALApplied(ctx context.Context, lsn wal.LSN) error {
	if m.walProgress != nil {
		if err := m.walProgress.SetAppliedLSN(ctx, lsn); err != nil {
			return err
		}
	}
	if m.walWaiter != nil {
		m.walWaiter.SetApplied(lsn)
	}
	return nil
}
