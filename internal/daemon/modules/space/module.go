package space

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/daemon/quiesce"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	"github.com/myceldb/mycel/internal/graph/model"
	storetemplate "github.com/myceldb/mycel/internal/graph/template/storage"
	"github.com/myceldb/mycel/internal/identity/model"
	"github.com/myceldb/mycel/internal/space/access"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"github.com/myceldb/mycel/internal/space/storage/acl"
	storedomains "github.com/myceldb/mycel/internal/space/storage/domains"
	storespaces "github.com/myceldb/mycel/internal/space/storage/spaces"
)

var ErrSpaceNotFound = errors.New("space not found")
var ErrUnauthorized = errors.New("space unauthorized")
var ErrInvalidInput = errors.New("invalid space input")

type Module struct {
	spaces    storespaces.Manager
	domains   storedomains.Manager
	templates storetemplate.Manager
	access    acl.Manager
	dataDir   string
	gate      *quiesce.Gate
}

func NewModule() *Module { return &Module{gate: quiesce.NewGate(ModuleName)} }

func (m *Module) Name() string { return ModuleName }

func (m *Module) Init(ctx context.Context, rt *daemonruntime.Runtime) daemonruntime.InitResult {
	metaDir := filepath.Join(rt.Config.DataDir, "meta")
	created, err := ensureDir(metaDir, 0o700)
	if err != nil {
		return daemonruntime.Abort(ModuleName, "filesystem", "failed to create meta directory", err)
	}
	rt.Logger.Info("space metadata directory ready", "path", metaDir, "created", created)

	spaces := storespaces.NewManager()
	if err := spaces.Init(ctx, metaDir); err != nil {
		return daemonruntime.Abort(ModuleName, "store", "failed to open space store", err)
	}
	domains := storedomains.NewManager()
	if err := domains.Init(ctx, metaDir); err != nil {
		return daemonruntime.Abort(ModuleName, "store", "failed to open domain store", err)
	}
	templates := storetemplate.NewManager()
	if err := templates.Init(ctx, filepath.Join(rt.Config.DataDir, "templates")); err != nil {
		return daemonruntime.Abort(ModuleName, "store", "failed to open template store", err)
	}
	accessMgr := acl.NewManager()
	if err := accessMgr.Init(ctx, metaDir); err != nil {
		return daemonruntime.Abort(ModuleName, "store", "failed to open access store", err)
	}
	m.spaces = spaces
	m.domains = domains
	m.templates = templates
	m.access = accessMgr
	m.dataDir = rt.Config.DataDir
	if m.gate == nil {
		m.gate = quiesce.NewGate(ModuleName)
	}
	if rt.Quiesce != nil {
		if err := rt.Quiesce.Register(m.gate); err != nil {
			return daemonruntime.Abort(ModuleName, "quiesce", "register space quiesce participant", err)
		}
	}
	return daemonruntime.OK(ModuleName)
}

func (m *Module) ListVisibleSpaces(ctx context.Context, userID string, includeArchived bool) ([]domainspace.Space, error) {
	uid, err := parseUserID(userID)
	if err != nil {
		return nil, err
	}
	spaces, err := m.spaces.List(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]domainspace.Space, 0, len(spaces))
	for _, sp := range spaces {
		if !includeArchived && isArchived(sp) {
			continue
		}
		canRead, err := m.canRead(ctx, uid, sp)
		if err != nil {
			return nil, err
		}
		if canRead {
			out = append(out, sp)
		}
	}
	return out, nil
}

func (m *Module) GetVisibleSpace(ctx context.Context, userID string, spaceID string) (domainspace.Space, error) {
	uid, err := parseUserID(userID)
	if err != nil {
		return domainspace.Space{}, err
	}
	sp, err := m.GetSpace(ctx, spaceID)
	if err != nil {
		return domainspace.Space{}, err
	}
	canRead, err := m.canRead(ctx, uid, sp)
	if err != nil {
		return domainspace.Space{}, err
	}
	if !canRead {
		return domainspace.Space{}, ErrSpaceNotFound
	}
	return sp, nil
}

func (m *Module) ListSpaces(ctx context.Context, includeArchived bool) ([]domainspace.Space, error) {
	spaces, err := m.spaces.List(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]domainspace.Space, 0, len(spaces))
	for _, sp := range spaces {
		if !includeArchived && isArchived(sp) {
			continue
		}
		out = append(out, sp)
	}
	return out, nil
}

func (m *Module) GetSpace(ctx context.Context, spaceID string) (domainspace.Space, error) {
	id, err := parseSpaceID(spaceID)
	if err != nil {
		return domainspace.Space{}, err
	}
	sp, err := m.spaces.GetByID(ctx, id)
	if err != nil {
		if errors.Is(err, storespaces.ErrSpaceNotFound) {
			return domainspace.Space{}, ErrSpaceNotFound
		}
		return domainspace.Space{}, err
	}
	return sp, nil
}

func (m *Module) CreateSpace(ctx context.Context, input CreateSpaceInput) (domainspace.Space, graph.Domain, error) {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return domainspace.Space{}, graph.Domain{}, err
	}
	defer release()
	if strings.TrimSpace(input.Name) == "" {
		return domainspace.Space{}, graph.Domain{}, fmt.Errorf("%w: name is required", ErrInvalidInput)
	}
	if input.OwnerUserID == uuid.Nil {
		return domainspace.Space{}, graph.Domain{}, fmt.Errorf("%w: owner_user_id is required", ErrInvalidInput)
	}
	sp, err := m.spaces.Create(ctx, storespaces.CreateInput{OwnerID: input.OwnerUserID, Name: input.Name})
	if err != nil {
		return domainspace.Space{}, graph.Domain{}, err
	}
	var domain graph.Domain
	if strings.TrimSpace(input.DefaultDomainKey) != "" {
		name := input.DefaultDomainName
		if strings.TrimSpace(name) == "" {
			name = input.DefaultDomainKey
		}
		domain, err = m.domains.Create(ctx, storedomains.CreateInput{SpaceID: sp.SpaceID, Key: input.DefaultDomainKey, Name: name, Default: true})
	} else {
		domain, err = m.domains.EnsureDefault(ctx, sp.SpaceID)
	}
	if err != nil {
		return domainspace.Space{}, graph.Domain{}, err
	}
	if _, err := m.access.Grant(ctx, acl.GrantInput{SpaceID: sp.SpaceID, UserID: input.OwnerUserID, Permissions: []access.SpacePermission{access.SpacePermissionAdmin}}); err != nil {
		return domainspace.Space{}, graph.Domain{}, err
	}
	return sp, domain, nil
}

func (m *Module) DeleteSpace(ctx context.Context, spaceID string) error {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return err
	}
	defer release()
	id, err := parseSpaceID(spaceID)
	if err != nil {
		return err
	}
	if err := m.domains.DeleteForSpace(ctx, id); err != nil {
		return err
	}
	if err := m.access.DeleteForSpace(ctx, id); err != nil {
		return err
	}
	if err := m.templates.DeleteForSpace(ctx, id); err != nil {
		return err
	}
	if err := m.spaces.DeleteByID(ctx, id); err != nil {
		if errors.Is(err, storespaces.ErrSpaceNotFound) {
			return ErrSpaceNotFound
		}
		return err
	}
	if m.dataDir != "" {
		_ = os.RemoveAll(filepath.Join(m.dataDir, "graphs", id.String()))
	}
	return nil
}

func (m *Module) GrantSpaceUser(ctx context.Context, spaceID string, userID string, role string) (SpaceGrant, error) {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return SpaceGrant{}, err
	}
	defer release()
	sp, err := m.GetSpace(ctx, spaceID)
	if err != nil {
		return SpaceGrant{}, err
	}
	uid, err := parseUserID(userID)
	if err != nil {
		return SpaceGrant{}, err
	}
	permissions, normalizedRole, capabilities, err := permissionsForSpaceRole(role)
	if err != nil {
		return SpaceGrant{}, err
	}
	if existing, ok, err := m.existingSpaceGrant(ctx, sp.SpaceID, uid); err != nil {
		return SpaceGrant{}, err
	} else if ok {
		existingRole, existingCaps := strongestRoleForPermissions(existing.Permissions)
		if spaceRoleRank(existingRole) >= spaceRoleRank(normalizedRole) {
			return SpaceGrant{ID: existing.ID.String(), SpaceID: existing.SpaceID.String(), UserID: existing.UserID.String(), Role: existingRole, Capabilities: existingCaps}, nil
		}
	}
	rule, err := m.access.Grant(ctx, acl.GrantInput{SpaceID: sp.SpaceID, UserID: uid, Permissions: permissions})
	if err != nil {
		return SpaceGrant{}, err
	}
	return SpaceGrant{ID: rule.ID.String(), SpaceID: rule.SpaceID.String(), UserID: rule.UserID.String(), Role: normalizedRole, Capabilities: capabilities}, nil
}

func (m *Module) existingSpaceGrant(ctx context.Context, spaceID uuid.UUID, userID uuid.UUID) (access.SpaceAccessRule, bool, error) {
	rules, err := m.access.RulesForSpace(ctx, spaceID)
	if err != nil {
		return access.SpaceAccessRule{}, false, err
	}
	for _, rule := range rules {
		if rule.UserID == userID {
			return rule, true, nil
		}
	}
	return access.SpaceAccessRule{}, false, nil
}

func permissionsForSpaceRole(role string) ([]access.SpacePermission, string, []string, error) {
	switch strings.ToLower(strings.TrimSpace(role)) {
	case "admin", "owner":
		return []access.SpacePermission{access.SpacePermissionAdmin}, "admin", ownerCapabilities(), nil
	case "writer", "write":
		return []access.SpacePermission{access.SpacePermissionWrite}, "writer", writerCapabilities(), nil
	case "reader", "read":
		return []access.SpacePermission{access.SpacePermissionRead}, "reader", readerCapabilities(), nil
	default:
		return nil, "", nil, fmt.Errorf("%w: space role must be admin, writer, or reader", ErrInvalidInput)
	}
}

func strongestRoleForPermissions(permissions []access.SpacePermission) (string, []string) {
	role := ""
	for _, perm := range permissions {
		switch perm {
		case access.SpacePermissionAdmin:
			return "admin", ownerCapabilities()
		case access.SpacePermissionWrite:
			role = "writer"
		case access.SpacePermissionRead:
			if role == "" {
				role = "reader"
			}
		}
	}
	switch role {
	case "writer":
		return role, writerCapabilities()
	case "reader":
		return role, readerCapabilities()
	default:
		return "", nil
	}
}

func spaceRoleRank(role string) int {
	switch role {
	case "admin":
		return 3
	case "writer":
		return 2
	case "reader":
		return 1
	default:
		return 0
	}
}

func (m *Module) EffectiveAccess(ctx context.Context, userID string, sp domainspace.Space) (EffectiveAccess, error) {
	uid, err := parseUserID(userID)
	if err != nil {
		return EffectiveAccess{}, err
	}
	if uid == sp.OwnerID {
		return EffectiveAccess{Roles: []string{"owner"}, Capabilities: ownerCapabilities()}, nil
	}
	rules, err := m.access.RulesForSpace(ctx, sp.SpaceID)
	if err != nil {
		return EffectiveAccess{}, err
	}
	roles := []string{}
	caps := map[string]bool{}
	for _, rule := range rules {
		if rule.UserID != uid {
			continue
		}
		for _, perm := range rule.Permissions {
			switch perm {
			case access.SpacePermissionAdmin:
				roles = append(roles, "admin")
				for _, cap := range ownerCapabilities() {
					caps[cap] = true
				}
			case access.SpacePermissionWrite:
				roles = append(roles, "writer")
				for _, cap := range writerCapabilities() {
					caps[cap] = true
				}
			case access.SpacePermissionRead:
				roles = append(roles, "reader")
				for _, cap := range readerCapabilities() {
					caps[cap] = true
				}
			}
		}
	}
	out := make([]string, 0, len(caps))
	for cap := range caps {
		out = append(out, cap)
	}
	return EffectiveAccess{Roles: roles, Capabilities: out}, nil
}

func (m *Module) DomainEffectiveAccess(ctx context.Context, userID string, spaceID string) (EffectiveAccess, error) {
	sp, err := m.GetSpace(ctx, spaceID)
	if err != nil {
		return EffectiveAccess{}, err
	}
	return m.EffectiveAccess(ctx, userID, sp)
}

func (m *Module) ListDomains(ctx context.Context, spaceID string, includeSystem bool) ([]graph.Domain, error) {
	sp, err := m.GetSpace(ctx, spaceID)
	if err != nil {
		return nil, err
	}
	domains, err := m.domains.ListBySpace(ctx, sp.SpaceID)
	if err != nil {
		return nil, err
	}
	return filterDiscoverableDomains(domains), nil
}

func (m *Module) GetDomainByRef(ctx context.Context, spaceID string, domainRef string) (graph.Domain, error) {
	sp, err := m.GetSpace(ctx, spaceID)
	if err != nil {
		return graph.Domain{}, err
	}
	if strings.TrimSpace(domainRef) == "" {
		return m.resolveDomain(ctx, sp.SpaceID, "", "")
	}
	if id, err := uuid.Parse(strings.TrimSpace(domainRef)); err == nil && id != uuid.Nil {
		return m.resolveDomain(ctx, sp.SpaceID, id.String(), "")
	}
	return m.resolveDomain(ctx, sp.SpaceID, "", domainRef)
}

func (m *Module) ListVisibleDomains(ctx context.Context, userID string, spaceID string, includeSystem bool) ([]graph.Domain, error) {
	uid, err := parseUserID(userID)
	if err != nil {
		return nil, err
	}
	sp, err := m.GetSpace(ctx, spaceID)
	if err != nil {
		return nil, err
	}
	canRead, err := m.canRead(ctx, uid, sp)
	if err != nil {
		return nil, err
	}
	if !canRead {
		return nil, ErrSpaceNotFound
	}
	domains, err := m.domains.ListBySpace(ctx, sp.SpaceID)
	if err != nil {
		return nil, err
	}
	return filterDiscoverableDomains(domains), nil
}

func (m *Module) GetVisibleDomain(ctx context.Context, userID string, spaceID string, domainID string, key string) (graph.Domain, error) {
	uid, err := parseUserID(userID)
	if err != nil {
		return graph.Domain{}, err
	}
	sp, err := m.GetSpace(ctx, spaceID)
	if err != nil {
		return graph.Domain{}, err
	}
	canRead, err := m.canRead(ctx, uid, sp)
	if err != nil {
		return graph.Domain{}, err
	}
	if !canRead {
		return graph.Domain{}, ErrSpaceNotFound
	}
	domain, err := m.resolveDomain(ctx, sp.SpaceID, domainID, key)
	if err != nil {
		return graph.Domain{}, err
	}
	if graph.NormalizeDomainDiscoveryMode(domain.DiscoveryMode) == graph.DomainDiscoveryModeHidden {
		canAdmin, err := m.canAdmin(ctx, uid, sp)
		if err != nil {
			return graph.Domain{}, err
		}
		if !canAdmin {
			return graph.Domain{}, ErrSpaceNotFound
		}
	}
	return domain, nil
}

func (m *Module) CreateDomain(ctx context.Context, userID string, input CreateDomainInput) (graph.Domain, error) {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return graph.Domain{}, err
	}
	defer release()
	uid, err := parseUserID(userID)
	if err != nil {
		return graph.Domain{}, err
	}
	sp, err := m.GetSpace(ctx, input.SpaceID)
	if err != nil {
		return graph.Domain{}, err
	}
	canAdmin, err := m.canAdmin(ctx, uid, sp)
	if err != nil {
		return graph.Domain{}, err
	}
	if !canAdmin {
		return graph.Domain{}, ErrUnauthorized
	}
	key := strings.TrimSpace(input.Key)
	if key == "" {
		key = input.Name
	}
	return m.domains.Create(ctx, storedomains.CreateInput{SpaceID: sp.SpaceID, Key: key, Name: input.Name, Description: input.Description, DiscoveryMode: input.DiscoveryMode, SearchMode: input.SearchMode, SemanticMode: input.SemanticMode, ReadOnly: input.ReadOnly})
}

func (m *Module) UpdateDomain(ctx context.Context, userID string, input UpdateDomainInput) (graph.Domain, error) {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return graph.Domain{}, err
	}
	defer release()
	uid, err := parseUserID(userID)
	if err != nil {
		return graph.Domain{}, err
	}
	sp, err := m.GetSpace(ctx, input.SpaceID)
	if err != nil {
		return graph.Domain{}, err
	}
	canAdmin, err := m.canAdmin(ctx, uid, sp)
	if err != nil {
		return graph.Domain{}, err
	}
	if !canAdmin {
		return graph.Domain{}, ErrUnauthorized
	}
	domain, err := m.resolveDomain(ctx, sp.SpaceID, input.DomainID, "")
	if err != nil {
		return graph.Domain{}, err
	}
	if domain.Default && input.Name != nil && strings.TrimSpace(*input.Name) != domain.Name {
		return graph.Domain{}, fmt.Errorf("%w: default domain name cannot be changed", ErrInvalidInput)
	}
	return m.domains.Update(ctx, storedomains.UpdateInput{DomainID: domain.ID, Name: input.Name, Description: input.Description, DiscoveryMode: input.DiscoveryMode, SearchMode: input.SearchMode, SemanticMode: input.SemanticMode, ReadOnly: input.ReadOnly})
}

func filterDiscoverableDomains(domains []graph.Domain) []graph.Domain {
	out := domains[:0]
	for _, domain := range domains {
		if graph.DomainDiscoverable(domain) {
			out = append(out, domain)
		}
	}
	return out
}

func (m *Module) DeleteDomain(ctx context.Context, userID string, spaceID string, domainID string) error {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return err
	}
	defer release()
	uid, err := parseUserID(userID)
	if err != nil {
		return err
	}
	sp, err := m.GetSpace(ctx, spaceID)
	if err != nil {
		return err
	}
	canAdmin, err := m.canAdmin(ctx, uid, sp)
	if err != nil {
		return err
	}
	if !canAdmin {
		return ErrUnauthorized
	}
	domain, err := m.resolveDomain(ctx, sp.SpaceID, domainID, "")
	if err != nil {
		return err
	}
	if domain.Default {
		return fmt.Errorf("%w: default domain cannot be deleted", ErrInvalidInput)
	}
	if err := m.domains.DeleteByID(ctx, domain.ID); err != nil {
		if errors.Is(err, storedomains.ErrDomainNotFound) {
			return ErrSpaceNotFound
		}
		return err
	}
	if m.dataDir != "" {
		_ = os.RemoveAll(filepath.Join(m.dataDir, "graphs", sp.SpaceID.String(), "domains", domain.ID.String()))
	}
	return nil
}

func (m *Module) resolveDomain(ctx context.Context, spaceID domainspace.SpaceID, domainID string, key string) (graph.Domain, error) {
	if strings.TrimSpace(domainID) != "" {
		id, err := uuid.Parse(strings.TrimSpace(domainID))
		if err != nil || id == uuid.Nil {
			return graph.Domain{}, fmt.Errorf("%w: domain_id is required", ErrInvalidInput)
		}
		domain, err := m.domains.GetByID(ctx, id)
		if err != nil {
			if errors.Is(err, storedomains.ErrDomainNotFound) {
				return graph.Domain{}, ErrSpaceNotFound
			}
			return graph.Domain{}, err
		}
		if domain.SpaceID != spaceID {
			return graph.Domain{}, ErrSpaceNotFound
		}
		return domain, nil
	}
	if strings.TrimSpace(key) == "" {
		domain, err := m.domains.GetDefault(ctx, spaceID)
		if err != nil {
			if errors.Is(err, storedomains.ErrDomainNotFound) {
				return graph.Domain{}, ErrSpaceNotFound
			}
			return graph.Domain{}, err
		}
		return domain, nil
	}
	domain, err := m.domains.FindBySpaceAndKey(ctx, spaceID, key)
	if err != nil {
		if errors.Is(err, storedomains.ErrDomainNotFound) {
			return graph.Domain{}, ErrSpaceNotFound
		}
		return graph.Domain{}, err
	}
	return domain, nil
}

func (m *Module) ListVisibleTemplates(ctx context.Context, userID string, spaceID string, includeSystem bool, includeArchived bool) ([]graph.Template, error) {
	uid, err := parseUserID(userID)
	if err != nil {
		return nil, err
	}
	sp, err := m.GetSpace(ctx, spaceID)
	if err != nil {
		return nil, err
	}
	canRead, err := m.canRead(ctx, uid, sp)
	if err != nil {
		return nil, err
	}
	if !canRead {
		return nil, ErrSpaceNotFound
	}
	templates, err := m.templates.ListBySpace(ctx, sp.SpaceID)
	if err != nil {
		return nil, mapTemplateStoreError(err)
	}
	out := make([]graph.Template, 0, len(templates))
	for _, template := range templates {
		if template.System && !includeSystem {
			continue
		}
		if template.State == graph.TemplateStateArchived && !includeArchived {
			continue
		}
		out = append(out, template)
	}
	return out, nil
}

func (m *Module) GetVisibleTemplate(ctx context.Context, userID string, spaceID string, templateID string) (graph.Template, error) {
	uid, sp, err := m.requireSpaceRead(ctx, userID, spaceID)
	if err != nil {
		return graph.Template{}, err
	}
	_ = uid
	id, err := parseTemplateID(templateID)
	if err != nil {
		return graph.Template{}, err
	}
	template, err := m.templates.GetByID(ctx, id)
	if err != nil {
		return graph.Template{}, mapTemplateStoreError(err)
	}
	if template.SpaceID != sp.SpaceID {
		return graph.Template{}, ErrSpaceNotFound
	}
	return template, nil
}

func (m *Module) FindVisibleTemplate(ctx context.Context, userID string, spaceID string, key string, version string) (graph.Template, error) {
	_, sp, err := m.requireSpaceRead(ctx, userID, spaceID)
	if err != nil {
		return graph.Template{}, err
	}
	template, err := m.templates.Find(ctx, sp.SpaceID, key, version)
	if err != nil {
		return graph.Template{}, mapTemplateStoreError(err)
	}
	return template, nil
}

func (m *Module) CreateTemplate(ctx context.Context, userID string, spaceID string, template storetemplate.TemplateImport) (graph.Template, error) {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return graph.Template{}, err
	}
	defer release()
	_, sp, err := m.requireSpaceAdmin(ctx, userID, spaceID)
	if err != nil {
		return graph.Template{}, err
	}
	created, err := m.templates.Import(ctx, sp.SpaceID, storetemplate.ImportDocument{SchemaVersion: 1, Templates: []storetemplate.TemplateImport{template}})
	if err != nil {
		return graph.Template{}, mapTemplateStoreError(err)
	}
	if len(created) != 1 {
		return graph.Template{}, fmt.Errorf("%w: template create returned no template", ErrInvalidInput)
	}
	return created[0], nil
}

func (m *Module) UpdateTemplate(ctx context.Context, userID string, spaceID string, templateID string, displayName *string, description *string) (graph.Template, error) {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return graph.Template{}, err
	}
	defer release()
	_, sp, err := m.requireSpaceAdmin(ctx, userID, spaceID)
	if err != nil {
		return graph.Template{}, err
	}
	id, err := parseTemplateID(templateID)
	if err != nil {
		return graph.Template{}, err
	}
	existing, err := m.templates.GetByID(ctx, id)
	if err != nil {
		return graph.Template{}, mapTemplateStoreError(err)
	}
	if existing.SpaceID != sp.SpaceID {
		return graph.Template{}, ErrSpaceNotFound
	}
	return m.templates.Update(ctx, storetemplate.UpdateInput{TemplateID: id, DisplayName: displayName, Description: description})
}

func (m *Module) ArchiveTemplate(ctx context.Context, userID string, spaceID string, templateID string) (graph.Template, error) {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return graph.Template{}, err
	}
	defer release()
	_, sp, err := m.requireSpaceAdmin(ctx, userID, spaceID)
	if err != nil {
		return graph.Template{}, err
	}
	id, err := parseTemplateID(templateID)
	if err != nil {
		return graph.Template{}, err
	}
	existing, err := m.templates.GetByID(ctx, id)
	if err != nil {
		return graph.Template{}, mapTemplateStoreError(err)
	}
	if existing.SpaceID != sp.SpaceID {
		return graph.Template{}, ErrSpaceNotFound
	}
	return m.templates.Archive(ctx, id)
}

func (m *Module) DeleteTemplate(ctx context.Context, userID string, spaceID string, templateID string) error {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return err
	}
	defer release()
	_, sp, err := m.requireSpaceAdmin(ctx, userID, spaceID)
	if err != nil {
		return err
	}
	id, err := parseTemplateID(templateID)
	if err != nil {
		return err
	}
	existing, err := m.templates.GetByID(ctx, id)
	if err != nil {
		return mapTemplateStoreError(err)
	}
	if existing.SpaceID != sp.SpaceID {
		return ErrSpaceNotFound
	}
	if err := m.templates.DeleteByID(ctx, id); err != nil {
		return mapTemplateStoreError(err)
	}
	return nil
}

func (m *Module) ImportTemplates(ctx context.Context, userID string, spaceID string, templates []storetemplate.TemplateImport) ([]graph.Template, error) {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	_, sp, err := m.requireSpaceAdmin(ctx, userID, spaceID)
	if err != nil {
		return nil, err
	}
	created, err := m.templates.Import(ctx, sp.SpaceID, storetemplate.ImportDocument{SchemaVersion: 1, Templates: templates})
	if err != nil {
		return nil, mapTemplateStoreError(err)
	}
	return created, nil
}

func (m *Module) enterWrite(ctx context.Context) (func(), error) {
	if m.gate == nil {
		return func() {}, nil
	}
	release, err := m.gate.Enter(ctx)
	if err != nil {
		return nil, quiesce.GRPCError(err)
	}
	return release, nil
}

func (m *Module) requireSpaceRead(ctx context.Context, userID string, spaceID string) (identity.UserID, domainspace.Space, error) {
	uid, err := parseUserID(userID)
	if err != nil {
		return uuid.Nil, domainspace.Space{}, err
	}
	sp, err := m.GetSpace(ctx, spaceID)
	if err != nil {
		return uuid.Nil, domainspace.Space{}, err
	}
	canRead, err := m.canRead(ctx, uid, sp)
	if err != nil {
		return uuid.Nil, domainspace.Space{}, err
	}
	if !canRead {
		return uuid.Nil, domainspace.Space{}, ErrSpaceNotFound
	}
	return uid, sp, nil
}

func (m *Module) requireSpaceAdmin(ctx context.Context, userID string, spaceID string) (identity.UserID, domainspace.Space, error) {
	uid, err := parseUserID(userID)
	if err != nil {
		return uuid.Nil, domainspace.Space{}, err
	}
	sp, err := m.GetSpace(ctx, spaceID)
	if err != nil {
		return uuid.Nil, domainspace.Space{}, err
	}
	canAdmin, err := m.canAdmin(ctx, uid, sp)
	if err != nil {
		return uuid.Nil, domainspace.Space{}, err
	}
	if !canAdmin {
		return uuid.Nil, domainspace.Space{}, ErrUnauthorized
	}
	return uid, sp, nil
}

func mapTemplateStoreError(err error) error {
	if errors.Is(err, storetemplate.ErrTemplateNotFound) {
		return ErrSpaceNotFound
	}
	if errors.Is(err, storetemplate.ErrInvalidInput) || errors.Is(err, storetemplate.ErrDuplicateTemplateVersion) {
		return fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	return err
}

func (m *Module) canRead(ctx context.Context, userID identity.UserID, sp domainspace.Space) (bool, error) {
	if sp.OwnerID == userID {
		return true, nil
	}
	return m.access.Can(ctx, userID, sp.SpaceID, access.SpacePermissionRead)
}

func (m *Module) canAdmin(ctx context.Context, userID identity.UserID, sp domainspace.Space) (bool, error) {
	if sp.OwnerID == userID {
		return true, nil
	}
	return m.access.Can(ctx, userID, sp.SpaceID, access.SpacePermissionAdmin)
}

func parseUserID(userID string) (identity.UserID, error) {
	id, err := uuid.Parse(strings.TrimSpace(userID))
	if err != nil || id == uuid.Nil {
		return uuid.Nil, fmt.Errorf("%w: user_id is required", ErrInvalidInput)
	}
	return id, nil
}

func parseTemplateID(templateID string) (graph.TemplateID, error) {
	id, err := uuid.Parse(strings.TrimSpace(templateID))
	if err != nil || id == uuid.Nil {
		return uuid.Nil, fmt.Errorf("%w: template_id is required", ErrInvalidInput)
	}
	return id, nil
}

func parseSpaceID(spaceID string) (domainspace.SpaceID, error) {
	id, err := uuid.Parse(strings.TrimSpace(spaceID))
	if err != nil || id == uuid.Nil {
		return uuid.Nil, fmt.Errorf("%w: space_id is required", ErrInvalidInput)
	}
	return id, nil
}

func isArchived(sp domainspace.Space) bool { return strings.EqualFold(sp.Status, "archived") }

func ownerCapabilities() []string {
	return []string{"CAPABILITY_SPACE_READ", "CAPABILITY_SPACE_UPDATE", "CAPABILITY_SPACE_MANAGE_ACCESS", "CAPABILITY_SPACE_ARCHIVE", "CAPABILITY_SPACE_DELETE", "CAPABILITY_DOMAIN_READ", "CAPABILITY_DOMAIN_CREATE", "CAPABILITY_DOMAIN_UPDATE", "CAPABILITY_DOMAIN_DELETE", "CAPABILITY_GRAPH_READ", "CAPABILITY_GRAPH_WRITE", "CAPABILITY_GRAPH_DELETE", "CAPABILITY_TEMPLATE_READ", "CAPABILITY_TEMPLATE_MANAGE", "CAPABILITY_BLOB_READ", "CAPABILITY_BLOB_WRITE", "CAPABILITY_BLOB_DELETE", "CAPABILITY_METADATA_READ", "CAPABILITY_METADATA_WRITE", "CAPABILITY_QUERY_RUN", "CAPABILITY_SEMANTIC_SEARCH"}
}
func writerCapabilities() []string {
	return []string{"CAPABILITY_SPACE_READ", "CAPABILITY_DOMAIN_READ", "CAPABILITY_GRAPH_READ", "CAPABILITY_GRAPH_WRITE", "CAPABILITY_TEMPLATE_READ", "CAPABILITY_BLOB_READ", "CAPABILITY_BLOB_WRITE", "CAPABILITY_METADATA_READ", "CAPABILITY_METADATA_WRITE", "CAPABILITY_QUERY_RUN", "CAPABILITY_SEMANTIC_SEARCH"}
}
func readerCapabilities() []string {
	return []string{"CAPABILITY_SPACE_READ", "CAPABILITY_DOMAIN_READ", "CAPABILITY_GRAPH_READ", "CAPABILITY_TEMPLATE_READ", "CAPABILITY_BLOB_READ", "CAPABILITY_METADATA_READ", "CAPABILITY_QUERY_RUN", "CAPABILITY_SEMANTIC_SEARCH"}
}

func ensureDir(path string, perm os.FileMode) (bool, error) {
	if info, err := os.Stat(path); err == nil {
		if !info.IsDir() {
			return false, fmt.Errorf("%s exists and is not a directory", path)
		}
		return false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if err := os.MkdirAll(path, perm); err != nil {
		return false, err
	}
	return true, nil
}
