package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/space/access"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"github.com/myceldb/mycel/internal/wal"
)

const (
	recordTypeCreateSpaceWithDefaultDomain wal.RecordType = "space.create_with_default_domain.v1"
	recordTypeCreateDomain                 wal.RecordType = "space.domain.create.v1"
	recordTypeUpdateDomain                 wal.RecordType = "space.domain.update.v1"
	recordTypeDeleteDomain                 wal.RecordType = "space.domain.delete.v1"
	recordTypeGrantSpaceUser               wal.RecordType = "space.acl.grant.v1"
	recordTypeDeleteSpace                  wal.RecordType = "space.delete.v1"
	recordTypePutTemplate                  wal.RecordType = "space.template.put.v1"
	recordTypeDeleteTemplate               wal.RecordType = "space.template.delete.v1"
)

type createSpaceWithDefaultDomainRecord struct {
	Space         domainspace.Space      `json:"space"`
	DefaultDomain graph.Domain           `json:"default_domain"`
	OwnerGrant    access.SpaceAccessRule `json:"owner_grant"`
}

type createDomainRecord struct {
	Domain graph.Domain `json:"domain"`
}

type updateDomainRecord struct {
	Domain graph.Domain `json:"domain"`
}

type deleteDomainRecord struct {
	DomainID graph.DomainID      `json:"domain_id"`
	SpaceID  domainspace.SpaceID `json:"space_id"`
}

type grantSpaceUserRecord struct {
	Rule access.SpaceAccessRule `json:"rule"`
}

type deleteSpaceRecord struct {
	SpaceID domainspace.SpaceID `json:"space_id"`
}

type putTemplateRecord struct {
	Template graph.Template `json:"template"`
}

type deleteTemplateRecord struct {
	TemplateID graph.TemplateID    `json:"template_id"`
	SpaceID    domainspace.SpaceID `json:"space_id"`
}

func (m *Module) buildCreateSpaceRecord(input CreateSpaceInput) createSpaceWithDefaultDomainRecord {
	now := time.Now().UTC()
	spaceID := uuid.New()
	key := strings.TrimSpace(input.DefaultDomainKey)
	if key == "" {
		key = graph.DefaultDomainKey
	}
	name := strings.TrimSpace(input.DefaultDomainName)
	if name == "" {
		if key == graph.DefaultDomainKey {
			name = graph.DefaultDomainName
		} else {
			name = key
		}
	}
	sp := domainspace.Space{SpaceID: spaceID, OwnerID: input.OwnerUserID, Name: input.Name, Status: "active", CreatedAt: now, UpdatedAt: now}
	domain := graph.Domain{ID: uuid.New(), SpaceID: spaceID, Key: key, Name: name, DiscoveryMode: graph.DomainDiscoveryModeNormal, SearchMode: graph.DomainSearchModeNormal, SemanticMode: graph.DomainSemanticModeNormal, Default: true, CreatedAt: now, UpdatedAt: now}
	grant := access.SpaceAccessRule{ID: uuid.New(), SpaceID: spaceID, UserID: input.OwnerUserID, Permissions: []access.SpacePermission{access.SpacePermissionAdmin}}
	return createSpaceWithDefaultDomainRecord{Space: sp, DefaultDomain: domain, OwnerGrant: grant}
}

func (m *Module) applyCreateSpaceWithDefaultDomain(ctx context.Context, rec wal.Record) error {
	var payload createSpaceWithDefaultDomainRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	_, _, err := m.applyCreateSpaceRecord(ctx, payload)
	return err
}

func (m *Module) buildCreateDomainRecord(spaceID domainspace.SpaceID, input CreateDomainInput) createDomainRecord {
	now := time.Now().UTC()
	key := strings.TrimSpace(input.Key)
	if key == "" {
		key = input.Name
	}
	name := strings.TrimSpace(input.Name)
	if name == "" {
		name = key
	}
	return createDomainRecord{Domain: graph.Domain{ID: uuid.New(), SpaceID: spaceID, Key: key, Name: name, Description: strings.TrimSpace(input.Description), DiscoveryMode: graph.NormalizeDomainDiscoveryMode(input.DiscoveryMode), SearchMode: graph.NormalizeDomainSearchMode(input.SearchMode), SemanticMode: graph.NormalizeDomainSemanticMode(input.SemanticMode), ReadOnly: input.ReadOnly, CreatedAt: now, UpdatedAt: now}}
}

func (m *Module) applyCreateDomain(ctx context.Context, rec wal.Record) error {
	var payload createDomainRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	_, err := m.domains.ApplyCreate(ctx, payload.Domain)
	return err
}

func (m *Module) applyUpdateDomain(ctx context.Context, rec wal.Record) error {
	var payload updateDomainRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	_, err := m.domains.ApplyUpdate(ctx, payload.Domain)
	return err
}

func (m *Module) applyDeleteDomain(ctx context.Context, rec wal.Record) error {
	var payload deleteDomainRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	return m.domains.ApplyDelete(ctx, payload.DomainID)
}

func (m *Module) applyGrantSpaceUser(ctx context.Context, rec wal.Record) error {
	var payload grantSpaceUserRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	_, err := m.access.ApplyGrant(ctx, payload.Rule)
	return err
}

func (m *Module) applyDeleteSpace(ctx context.Context, rec wal.Record) error {
	var payload deleteSpaceRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	if err := m.domains.DeleteForSpace(ctx, payload.SpaceID); err != nil {
		return err
	}
	if err := m.access.DeleteForSpace(ctx, payload.SpaceID); err != nil {
		return err
	}
	if err := m.templates.DeleteForSpace(ctx, payload.SpaceID); err != nil {
		return err
	}
	return m.spaces.ApplyDelete(ctx, payload.SpaceID)
}

func (m *Module) applyPutTemplate(ctx context.Context, rec wal.Record) error {
	var payload putTemplateRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	_, err := m.templates.ApplyPut(ctx, payload.Template)
	return err
}

func (m *Module) applyDeleteTemplate(ctx context.Context, rec wal.Record) error {
	var payload deleteTemplateRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	return m.templates.ApplyDelete(ctx, payload.TemplateID)
}

func (m *Module) applyCreateSpaceRecord(ctx context.Context, payload createSpaceWithDefaultDomainRecord) (domainspace.Space, graph.Domain, error) {
	sp, err := m.spaces.ApplyCreate(ctx, payload.Space)
	if err != nil {
		return domainspace.Space{}, graph.Domain{}, err
	}
	domain, err := m.domains.ApplyCreate(ctx, payload.DefaultDomain)
	if err != nil {
		return domainspace.Space{}, graph.Domain{}, err
	}
	if payload.OwnerGrant.SpaceID != sp.SpaceID || payload.OwnerGrant.UserID != sp.OwnerID {
		return domainspace.Space{}, graph.Domain{}, fmt.Errorf("%w: owner grant does not match space", ErrInvalidInput)
	}
	if _, err := m.access.ApplyGrant(ctx, payload.OwnerGrant); err != nil {
		return domainspace.Space{}, graph.Domain{}, err
	}
	return sp, domain, nil
}
