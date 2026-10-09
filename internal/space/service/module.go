package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/myceldb/mycel/internal/fsperm"

	"github.com/google/uuid"

	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/clustering/routing"
	graph "github.com/myceldb/mycel/internal/graph/model"
	identity "github.com/myceldb/mycel/internal/identity/model"
	principalservice "github.com/myceldb/mycel/internal/identity/service/principal"
	runtime "github.com/myceldb/mycel/internal/runtime"
	"github.com/myceldb/mycel/internal/runtime/quiesce"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	storedomains "github.com/myceldb/mycel/internal/space/storage/domains"
	storespaces "github.com/myceldb/mycel/internal/space/storage/spaces"
	"github.com/myceldb/mycel/internal/wal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ErrSpaceNotFound = errors.New("space not found")
var ErrUnauthorized = errors.New("space unauthorized")
var ErrInvalidInput = errors.New("invalid space input")

func hostRaftPartitionCount(host runtime.Host) uint32 {
	value := reflect.Indirect(reflect.ValueOf(host))
	if !value.IsValid() || value.Kind() != reflect.Struct {
		return 0
	}
	configField := value.FieldByName("Config")
	if !configField.IsValid() {
		return 0
	}
	clusterField := configField.FieldByName("Cluster")
	if !clusterField.IsValid() {
		return 0
	}
	partitionField := clusterField.FieldByName("RaftPartitionCount")
	if !partitionField.IsValid() {
		return 0
	}
	switch partitionField.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return uint32(partitionField.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return uint32(partitionField.Uint())
	default:
		return 0
	}
}

type Module struct {
	spaces               storespaces.Manager
	domains              storedomains.Manager
	authorizer           PrincipalAccessAuthorizer
	dataDir              string
	gate                 *quiesce.Gate
	wal                  *wal.Manager
	walProgress          wal.AppliedLSNStore
	walWaiter            *wal.ApplyWaiter
	writeAllowed         func() error
	partitionExec        routing.PartitionExecutor
	partitionCount       uint32
	raftGroups           *consensus.MultiGroup
	raftLocalNode        consensus.NodeID
	raftMu               sync.Mutex
	raftCreateByID       map[string]CreateSpaceResult
	raftHashByID         map[string][]byte
	raftNodeAddrs        []string
	raftBackendAuthToken string
}

func NewModule() *Module { return &Module{gate: quiesce.NewGate(ModuleName)} }

func (m *Module) WithPrincipalAuthorizer(authorizer PrincipalAccessAuthorizer) *Module {
	m.authorizer = authorizer
	return m
}

func (m *Module) Name() string { return ModuleName }

func (m *Module) Init(ctx context.Context, host runtime.Host) runtime.InitResult {
	metaDir := filepath.Join(host.DataDir(), "meta")
	created, err := fsperm.EnsureDir(metaDir, fsperm.PrivateDir)
	if err != nil {
		return runtime.Abort(ModuleName, "filesystem", "failed to create meta directory", err)
	}
	if logger := host.Log(); logger != nil {
		logger.Info("space metadata directory ready", "path", metaDir, "created", created)
	}

	spaces := storespaces.NewManager()
	if err := spaces.Init(ctx, metaDir); err != nil {
		return runtime.Abort(ModuleName, "store", "failed to open space store", err)
	}
	domains := storedomains.NewManager()
	if err := domains.Init(ctx, metaDir); err != nil {
		return runtime.Abort(ModuleName, "store", "failed to open domain store", err)
	}
	m.spaces = spaces
	m.domains = domains
	m.dataDir = host.DataDir()
	if provider, ok := host.(runtime.WALProvider); ok {
		m.wal = provider.WALManager()
		m.walProgress = provider.WALProgressStore()
		m.walWaiter = provider.WALWaiterStore()
	}
	m.writeAllowed = func() error { return nil }
	m.partitionCount = hostRaftPartitionCount(host)
	m.partitionExec = routing.NewLocalExecutor(m.partitionCount)
	if provider, ok := host.(runtime.WALProvider); ok {
		if registry := provider.WALRegistryStore(); registry != nil {
			if err := registry.Register(recordTypeCreateSpaceWithDefaultDomain, wal.ApplierFunc(m.applyCreateSpaceWithDefaultDomain)); err != nil {
				return runtime.Abort(ModuleName, "wal", "register space create WAL applier", err)
			}
			if err := registry.Register(recordTypeCreateDomain, wal.ApplierFunc(m.applyCreateDomain)); err != nil {
				return runtime.Abort(ModuleName, "wal", "register domain create WAL applier", err)
			}
			if err := registry.Register(recordTypeUpdateDomain, wal.ApplierFunc(m.applyUpdateDomain)); err != nil {
				return runtime.Abort(ModuleName, "wal", "register domain update WAL applier", err)
			}
			if err := registry.Register(recordTypeDeleteDomain, wal.ApplierFunc(m.applyDeleteDomain)); err != nil {
				return runtime.Abort(ModuleName, "wal", "register domain delete WAL applier", err)
			}
			if err := registry.Register(recordTypeDeleteSpace, wal.ApplierFunc(m.applyDeleteSpace)); err != nil {
				return runtime.Abort(ModuleName, "wal", "register space delete WAL applier", err)
			}
		}
	}
	if m.gate == nil {
		m.gate = quiesce.NewGate(ModuleName)
	}
	if _, ok := host.(runtime.QuiesceRegistrar); ok {
		if err := host.(runtime.QuiesceRegistrar).RegisterQuiesceParticipant(m.gate); err != nil {
			return runtime.Abort(ModuleName, "quiesce", "register space quiesce participant", err)
		}
	}
	return runtime.OK(ModuleName)
}

func (m *Module) ListVisibleSpaces(ctx context.Context, principalID string, includeArchived bool) ([]domainspace.Space, error) {
	uid, err := parsePrincipalID(principalID)
	if err != nil {
		return nil, err
	}
	spaces, err := m.ListSpaces(ctx, includeArchived)
	if err != nil {
		return nil, err
	}
	out := make([]domainspace.Space, 0, len(spaces))
	for _, sp := range spaces {
		if err := m.authorize(ctx, uid, sp, "space.read"); err == nil {
			out = append(out, sp)
		} else if !authorizationDenied(err) {
			return nil, err
		}
	}
	return out, nil
}

func (m *Module) GetVisibleSpace(ctx context.Context, principalID string, spaceID string) (domainspace.Space, error) {
	uid, err := parsePrincipalID(principalID)
	if err != nil {
		return domainspace.Space{}, err
	}
	sp, err := m.GetSpace(ctx, spaceID)
	if err != nil {
		return domainspace.Space{}, err
	}
	if err := m.authorize(ctx, uid, sp, "space.read"); err != nil {
		return domainspace.Space{}, ErrSpaceNotFound
	}
	return sp, nil
}

func (m *Module) ListSpaces(ctx context.Context, includeArchived bool) ([]domainspace.Space, error) {
	if m.raftGroups != nil {
		return m.listSpacesViaRaftLeaders(ctx, includeArchived)
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
		out = append(out, sp)
	}
	return out, nil
}

func (m *Module) GetSpace(ctx context.Context, spaceID string) (domainspace.Space, error) {
	id, err := parseSpaceID(spaceID)
	if err != nil {
		return domainspace.Space{}, err
	}
	return routing.ForSpaceValue[domainspace.Space](m.partitionExec, ctx, id.String(), func(ctx context.Context) (domainspace.Space, error) {
		sp, err := m.spaces.GetByID(ctx, id)
		if err != nil {
			if errors.Is(err, storespaces.ErrSpaceNotFound) {
				return domainspace.Space{}, ErrSpaceNotFound
			}
			return domainspace.Space{}, err
		}
		return sp, nil
	})
}

func (m *Module) CreateSpace(ctx context.Context, input CreateSpaceInput) (domainspace.Space, graph.Domain, error) {
	result, err := m.CreateSpaceWithResult(ctx, input)
	if err != nil {
		return domainspace.Space{}, graph.Domain{}, err
	}
	return result.Space, result.Domain, nil
}

func (m *Module) CreateSpaceWithResult(ctx context.Context, input CreateSpaceInput) (CreateSpaceResult, error) {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return CreateSpaceResult{}, err
	}
	defer release()
	if strings.TrimSpace(input.Name) == "" {
		return CreateSpaceResult{}, fmt.Errorf("%w: name is required", ErrInvalidInput)
	}
	if strings.TrimSpace(string(input.OwnerPrincipalID)) == "" {
		return CreateSpaceResult{}, fmt.Errorf("%w: owner_principal_id is required", ErrInvalidInput)
	}
	if m.raftGroups != nil {
		return m.createSpaceViaRaft(ctx, input)
	}
	if m.wal == nil {
		sp, err := m.spaces.Create(ctx, storespaces.CreateInput{OwnerID: input.OwnerPrincipalID, Name: input.Name})
		if err != nil {
			return CreateSpaceResult{}, err
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
			return CreateSpaceResult{}, err
		}
		return CreateSpaceResult{Space: sp, Domain: domain}, nil
	}
	record := m.buildCreateSpaceRecord(input)
	return routing.ForSpaceValue[CreateSpaceResult](m.partitionExec, ctx, record.Space.SpaceID.String(), func(ctx context.Context) (CreateSpaceResult, error) {
		payload, err := json.Marshal(record)
		if err != nil {
			return CreateSpaceResult{}, err
		}
		lsn, err := m.wal.Append(ctx, wal.PendingRecord{Type: recordTypeCreateSpaceWithDefaultDomain, SchemaVersion: 1, Timestamp: record.Space.CreatedAt, Encoding: wal.PayloadEncodingJSON, Payload: payload})
		if err != nil {
			return CreateSpaceResult{}, err
		}
		if err := m.wal.Sync(ctx, lsn); err != nil {
			return CreateSpaceResult{}, err
		}
		sp, domain, err := m.applyCreateSpaceRecord(ctx, record)
		if err != nil {
			return CreateSpaceResult{}, err
		}
		if m.walProgress != nil {
			if err := m.walProgress.SetAppliedLSN(ctx, lsn); err != nil {
				return CreateSpaceResult{}, err
			}
		}
		if m.walWaiter != nil {
			m.walWaiter.SetApplied(lsn)
		}
		return CreateSpaceResult{Space: sp, Domain: domain, CommitLSN: lsn}, nil
	})
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
	if m.wal == nil && m.raftGroups == nil {
		if err := m.domains.DeleteForSpace(ctx, id); err != nil {
			return err
		}
		if err := m.spaces.DeleteByID(ctx, id); err != nil {
			if errors.Is(err, storespaces.ErrSpaceNotFound) {
				return ErrSpaceNotFound
			}
			return err
		}
	} else {
		if exists, err := m.spaces.ExistsByID(ctx, id); err != nil {
			return err
		} else if !exists {
			return ErrSpaceNotFound
		}
		record := deleteSpaceRecord{SpaceID: id}
		if m.raftGroups != nil {
			cmd, err := m.buildDeleteSpaceRaftCommand(record, m.partitionCount, newInternalCommandID("space-delete"))
			if err != nil {
				return err
			}
			if err := m.proposeSpaceMetadataCommand(ctx, cmd); err != nil {
				return err
			}
		} else {
			payload, err := json.Marshal(record)
			if err != nil {
				return err
			}
			lsn, err := m.wal.Append(ctx, wal.PendingRecord{Type: recordTypeDeleteSpace, SchemaVersion: 1, Encoding: wal.PayloadEncodingJSON, Payload: payload})
			if err != nil {
				return err
			}
			if err := m.wal.Sync(ctx, lsn); err != nil {
				return err
			}
			if err := m.applyDeleteSpace(ctx, wal.Record{Payload: payload}); err != nil {
				return err
			}
			if m.walProgress != nil {
				if err := m.walProgress.SetAppliedLSN(ctx, lsn); err != nil {
					return err
				}
			}
			if m.walWaiter != nil {
				m.walWaiter.SetApplied(lsn)
			}
		}
	}
	if m.dataDir != "" {
		_ = os.RemoveAll(filepath.Join(m.dataDir, "graphs", id.String()))
	}
	return nil
}

func (m *Module) EffectiveAccess(ctx context.Context, principalID string, sp domainspace.Space) (EffectiveAccess, error) {
	uid, err := parsePrincipalID(principalID)
	if err != nil {
		return EffectiveAccess{}, err
	}
	if uid == sp.OwnerID {
		return EffectiveAccess{Roles: []string{"owner"}, Capabilities: ownerCapabilities()}, nil
	}
	if m.authorizer == nil {
		return EffectiveAccess{}, nil
	}
	access, err := m.authorizer.EffectiveAccess(ctx, string(uid), principalservice.AccessScope{Type: "space", SpaceID: sp.SpaceID.String()})
	if err != nil {
		return EffectiveAccess{}, err
	}
	return EffectiveAccess{Roles: access.Roles, Capabilities: access.Capabilities}, nil
}

func (m *Module) DomainEffectiveAccess(ctx context.Context, principalID string, spaceID string) (EffectiveAccess, error) {
	sp, err := m.GetSpace(ctx, spaceID)
	if err != nil {
		return EffectiveAccess{}, err
	}
	return m.EffectiveAccess(ctx, principalID, sp)
}

func (m *Module) ListDomains(ctx context.Context, spaceID string, includeSystem bool) ([]graph.Domain, error) {
	id, err := parseSpaceID(spaceID)
	if err != nil {
		return nil, err
	}
	if m.raftGroups != nil {
		sp, err := m.GetLocalRaftSpace(ctx, id.String())
		if err != nil {
			return nil, err
		}
		domains, err := m.domains.ListBySpace(ctx, sp.SpaceID)
		if err != nil {
			return nil, err
		}
		return filterDiscoverableDomains(domains), nil
	}
	return routing.ForSpaceValue[[]graph.Domain](m.partitionExec, ctx, id.String(), func(ctx context.Context) ([]graph.Domain, error) {
		sp, err := m.GetSpace(ctx, id.String())
		if err != nil {
			return nil, err
		}
		domains, err := m.domains.ListBySpace(ctx, sp.SpaceID)
		if err != nil {
			return nil, err
		}
		return filterDiscoverableDomains(domains), nil
	})
}

func (m *Module) GetDomain(ctx context.Context, domainID string) (graph.Domain, error) {
	id, err := uuid.Parse(strings.TrimSpace(domainID))
	if err != nil || id == uuid.Nil {
		return graph.Domain{}, fmt.Errorf("%w: domain_id is required", ErrInvalidInput)
	}
	return m.domains.GetByID(ctx, graph.DomainID(id))
}

func (m *Module) GetDomainByRef(ctx context.Context, spaceID string, domainRef string) (graph.Domain, error) {
	id, err := parseSpaceID(spaceID)
	if err != nil {
		return graph.Domain{}, err
	}
	if m.raftGroups != nil {
		sp, err := m.GetLocalRaftSpace(ctx, id.String())
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
	return routing.ForSpaceValue[graph.Domain](m.partitionExec, ctx, id.String(), func(ctx context.Context) (graph.Domain, error) {
		sp, err := m.GetSpace(ctx, id.String())
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
	})
}

func (m *Module) ListVisibleDomains(ctx context.Context, principalID string, spaceID string, includeSystem bool) ([]graph.Domain, error) {
	uid, err := parsePrincipalID(principalID)
	if err != nil {
		return nil, err
	}
	sp, err := m.GetSpace(ctx, spaceID)
	if err != nil {
		return nil, err
	}
	if err := m.authorize(ctx, uid, sp, "domain.read"); err != nil {
		return nil, ErrSpaceNotFound
	}
	domains, err := m.domains.ListBySpace(ctx, sp.SpaceID)
	if err != nil {
		return nil, err
	}
	return filterDiscoverableDomains(domains), nil
}

func (m *Module) GetVisibleDomain(ctx context.Context, principalID string, spaceID string, domainID string, key string) (graph.Domain, error) {
	uid, err := parsePrincipalID(principalID)
	if err != nil {
		return graph.Domain{}, err
	}
	sp, err := m.GetSpace(ctx, spaceID)
	if err != nil {
		return graph.Domain{}, err
	}
	if err := m.authorize(ctx, uid, sp, "domain.read"); err != nil {
		return graph.Domain{}, ErrSpaceNotFound
	}
	domain, err := m.resolveDomain(ctx, sp.SpaceID, domainID, key)
	if err != nil {
		return graph.Domain{}, err
	}
	if graph.NormalizeDomainDiscoveryMode(domain.DiscoveryMode) == graph.DomainDiscoveryModeHidden {
		if err := m.authorize(ctx, uid, sp, "space.manage_access"); err != nil {
			return graph.Domain{}, ErrSpaceNotFound
		}
	}
	return domain, nil
}

func (m *Module) CreateDomain(ctx context.Context, principalID string, input CreateDomainInput) (graph.Domain, error) {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return graph.Domain{}, err
	}
	defer release()
	uid, err := parsePrincipalID(principalID)
	if err != nil {
		return graph.Domain{}, err
	}
	sp, err := m.GetSpace(ctx, input.SpaceID)
	if err != nil {
		return graph.Domain{}, err
	}
	if err := m.authorize(ctx, uid, sp, "domain.create"); err != nil {
		return graph.Domain{}, ErrUnauthorized
	}
	key := strings.TrimSpace(input.Key)
	if key == "" {
		key = input.Name
	}
	if strings.TrimSpace(key) == "" {
		return graph.Domain{}, fmt.Errorf("%w: key is required", ErrInvalidInput)
	}
	if existing, err := m.domains.FindBySpaceAndKey(ctx, sp.SpaceID, key); err == nil {
		return existing, nil
	} else if err != nil && !errors.Is(err, storedomains.ErrDomainNotFound) {
		return graph.Domain{}, err
	}
	if m.wal == nil && m.raftGroups == nil {
		return m.domains.Create(ctx, storedomains.CreateInput{SpaceID: sp.SpaceID, Key: key, Name: input.Name, Description: input.Description, DiscoveryMode: input.DiscoveryMode, SearchMode: input.SearchMode, SemanticMode: input.SemanticMode, ReadOnly: input.ReadOnly})
	}
	record := m.buildCreateDomainRecord(sp.SpaceID, input)
	if m.raftGroups != nil {
		cmd, err := m.buildCreateDomainRaftCommand(record, m.partitionCount, newInternalCommandID("space-domain-create"))
		if err != nil {
			return graph.Domain{}, err
		}
		if err := m.proposeSpaceMetadataCommand(ctx, cmd); err != nil {
			return graph.Domain{}, err
		}
		return record.Domain, nil
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return graph.Domain{}, err
	}
	lsn, err := m.wal.Append(ctx, wal.PendingRecord{Type: recordTypeCreateDomain, SchemaVersion: 1, Timestamp: record.Domain.CreatedAt, Encoding: wal.PayloadEncodingJSON, Payload: payload})
	if err != nil {
		return graph.Domain{}, err
	}
	if err := m.wal.Sync(ctx, lsn); err != nil {
		return graph.Domain{}, err
	}
	domain, err := m.domains.ApplyCreate(ctx, record.Domain)
	if err != nil {
		return graph.Domain{}, err
	}
	if m.walProgress != nil {
		if err := m.walProgress.SetAppliedLSN(ctx, lsn); err != nil {
			return graph.Domain{}, err
		}
	}
	if m.walWaiter != nil {
		m.walWaiter.SetApplied(lsn)
	}
	return domain, nil
}

func (m *Module) UpdateDomain(ctx context.Context, principalID string, input UpdateDomainInput) (graph.Domain, error) {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return graph.Domain{}, err
	}
	defer release()
	uid, err := parsePrincipalID(principalID)
	if err != nil {
		return graph.Domain{}, err
	}
	sp, err := m.GetSpace(ctx, input.SpaceID)
	if err != nil {
		return graph.Domain{}, err
	}
	if err := m.authorize(ctx, uid, sp, "domain.update"); err != nil {
		return graph.Domain{}, ErrUnauthorized
	}
	domain, err := m.resolveDomain(ctx, sp.SpaceID, input.DomainID, "")
	if err != nil {
		return graph.Domain{}, err
	}
	if domain.Default && input.Name != nil && strings.TrimSpace(*input.Name) != domain.Name {
		return graph.Domain{}, fmt.Errorf("%w: default domain name cannot be changed", ErrInvalidInput)
	}
	if m.wal == nil && m.raftGroups == nil {
		return m.domains.Update(ctx, storedomains.UpdateInput{DomainID: domain.ID, Name: input.Name, Description: input.Description, DiscoveryMode: input.DiscoveryMode, SearchMode: input.SearchMode, SemanticMode: input.SemanticMode, ReadOnly: input.ReadOnly})
	}
	updated := domain
	if input.Name != nil {
		name := strings.TrimSpace(*input.Name)
		if name == "" {
			return graph.Domain{}, fmt.Errorf("%w: name is required", ErrInvalidInput)
		}
		updated.Name = name
	}
	if input.Description != nil {
		updated.Description = strings.TrimSpace(*input.Description)
	}
	if input.DiscoveryMode != nil {
		mode := graph.NormalizeDomainDiscoveryMode(*input.DiscoveryMode)
		if !graph.ValidDomainDiscoveryMode(mode) {
			return graph.Domain{}, fmt.Errorf("%w: discovery_mode must be normal, explicit_only, or hidden", ErrInvalidInput)
		}
		updated.DiscoveryMode = mode
	}
	if input.SearchMode != nil {
		mode := graph.NormalizeDomainSearchMode(*input.SearchMode)
		if !graph.ValidDomainSearchMode(mode) {
			return graph.Domain{}, fmt.Errorf("%w: search_mode must be normal, explicit_only, or disabled", ErrInvalidInput)
		}
		updated.SearchMode = mode
	}
	if input.SemanticMode != nil {
		mode := graph.NormalizeDomainSemanticMode(*input.SemanticMode)
		if !graph.ValidDomainSemanticMode(mode) {
			return graph.Domain{}, fmt.Errorf("%w: semantic_mode must be normal, explicit_only, or disabled", ErrInvalidInput)
		}
		updated.SemanticMode = mode
	}
	if input.ReadOnly != nil {
		updated.ReadOnly = *input.ReadOnly
	}
	updated.UpdatedAt = time.Now().UTC()
	record := updateDomainRecord{Domain: updated}
	if m.raftGroups != nil {
		cmd, err := m.buildUpdateDomainRaftCommand(record, m.partitionCount, newInternalCommandID("space-domain-update"))
		if err != nil {
			return graph.Domain{}, err
		}
		if err := m.proposeSpaceMetadataCommand(ctx, cmd); err != nil {
			return graph.Domain{}, err
		}
		return updated, nil
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return graph.Domain{}, err
	}
	lsn, err := m.wal.Append(ctx, wal.PendingRecord{Type: recordTypeUpdateDomain, SchemaVersion: 1, Timestamp: updated.UpdatedAt, Encoding: wal.PayloadEncodingJSON, Payload: payload})
	if err != nil {
		return graph.Domain{}, err
	}
	if err := m.wal.Sync(ctx, lsn); err != nil {
		return graph.Domain{}, err
	}
	applied, err := m.domains.ApplyUpdate(ctx, updated)
	if err != nil {
		return graph.Domain{}, err
	}
	if m.walProgress != nil {
		if err := m.walProgress.SetAppliedLSN(ctx, lsn); err != nil {
			return graph.Domain{}, err
		}
	}
	if m.walWaiter != nil {
		m.walWaiter.SetApplied(lsn)
	}
	return applied, nil
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

func (m *Module) DeleteDomain(ctx context.Context, principalID string, spaceID string, domainID string) error {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return err
	}
	defer release()
	uid, err := parsePrincipalID(principalID)
	if err != nil {
		return err
	}
	sp, err := m.GetSpace(ctx, spaceID)
	if err != nil {
		return err
	}
	if err := m.authorize(ctx, uid, sp, "domain.delete"); err != nil {
		return ErrUnauthorized
	}
	domain, err := m.resolveDomain(ctx, sp.SpaceID, domainID, "")
	if err != nil {
		return err
	}
	if domain.Default {
		return fmt.Errorf("%w: default domain cannot be deleted", ErrInvalidInput)
	}
	if m.wal == nil && m.raftGroups == nil {
		if err := m.domains.DeleteByID(ctx, domain.ID); err != nil {
			if errors.Is(err, storedomains.ErrDomainNotFound) {
				return ErrSpaceNotFound
			}
			return err
		}
	} else {
		record := deleteDomainRecord{DomainID: domain.ID, SpaceID: sp.SpaceID}
		if m.raftGroups != nil {
			cmd, err := m.buildDeleteDomainRaftCommand(record, m.partitionCount, newInternalCommandID("space-domain-delete"))
			if err != nil {
				return err
			}
			if err := m.proposeSpaceMetadataCommand(ctx, cmd); err != nil {
				return err
			}
		} else {
			payload, err := json.Marshal(record)
			if err != nil {
				return err
			}
			lsn, err := m.wal.Append(ctx, wal.PendingRecord{Type: recordTypeDeleteDomain, SchemaVersion: 1, Encoding: wal.PayloadEncodingJSON, Payload: payload})
			if err != nil {
				return err
			}
			if err := m.wal.Sync(ctx, lsn); err != nil {
				return err
			}
			if err := m.domains.ApplyDelete(ctx, domain.ID); err != nil {
				return err
			}
			if m.walProgress != nil {
				if err := m.walProgress.SetAppliedLSN(ctx, lsn); err != nil {
					return err
				}
			}
			if m.walWaiter != nil {
				m.walWaiter.SetApplied(lsn)
			}
		}
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

func (m *Module) enterWrite(ctx context.Context) (func(), error) {
	if err := m.requireLocalWriteAllowed(); err != nil {
		return nil, err
	}
	if m.gate == nil {
		return func() {}, nil
	}
	release, err := m.gate.Enter(ctx)
	if err != nil {
		return nil, quiesce.GRPCError(err)
	}
	return release, nil
}

func (m *Module) requireLocalWriteAllowed() error {
	if m.writeAllowed == nil {
		return nil
	}
	return m.writeAllowed()
}

func (m *Module) requireSpaceRead(ctx context.Context, principalID string, spaceID string) (identity.PrincipalID, domainspace.Space, error) {
	uid, err := parsePrincipalID(principalID)
	if err != nil {
		return "", domainspace.Space{}, err
	}
	sp, err := m.GetSpace(ctx, spaceID)
	if err != nil {
		return "", domainspace.Space{}, err
	}
	if err := m.authorize(ctx, uid, sp, "space.read"); err != nil {
		return "", domainspace.Space{}, ErrSpaceNotFound
	}
	return uid, sp, nil
}

func (m *Module) requireSpaceAdmin(ctx context.Context, principalID string, spaceID string) (identity.PrincipalID, domainspace.Space, error) {
	uid, err := parsePrincipalID(principalID)
	if err != nil {
		return "", domainspace.Space{}, err
	}
	sp, err := m.GetSpace(ctx, spaceID)
	if err != nil {
		return "", domainspace.Space{}, err
	}
	if err := m.authorize(ctx, uid, sp, "space.manage_access"); err != nil {
		return "", domainspace.Space{}, ErrUnauthorized
	}
	return uid, sp, nil
}

func (m *Module) authorize(ctx context.Context, principalID identity.PrincipalID, sp domainspace.Space, capability string) error {
	if sp.OwnerID == principalID {
		return nil
	}
	if m.authorizer == nil {
		return ErrUnauthorized
	}
	return m.authorizer.Authorize(ctx, string(principalID), capability, principalservice.AccessScope{Type: "space", SpaceID: sp.SpaceID.String()})
}

func authorizationDenied(err error) bool {
	return errors.Is(err, ErrUnauthorized) || status.Code(err) == codes.PermissionDenied
}

func parsePrincipalID(principalID string) (identity.PrincipalID, error) {
	id := strings.TrimSpace(principalID)
	if id == "" {
		return "", fmt.Errorf("%w: principal_id is required", ErrInvalidInput)
	}
	return identity.PrincipalID(id), nil
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
	return []string{"CAPABILITY_SPACE_READ", "CAPABILITY_SPACE_UPDATE", "CAPABILITY_SPACE_MANAGE_ACCESS", "CAPABILITY_SPACE_ARCHIVE", "CAPABILITY_SPACE_DELETE", "CAPABILITY_DOMAIN_READ", "CAPABILITY_DOMAIN_CREATE", "CAPABILITY_DOMAIN_UPDATE", "CAPABILITY_DOMAIN_DELETE", "CAPABILITY_GRAPH_READ", "CAPABILITY_GRAPH_WRITE", "CAPABILITY_GRAPH_DELETE", "CAPABILITY_BLOB_READ", "CAPABILITY_BLOB_WRITE", "CAPABILITY_BLOB_DELETE", "CAPABILITY_METADATA_READ", "CAPABILITY_METADATA_WRITE", "CAPABILITY_QUERY_RUN", "CAPABILITY_SEMANTIC_SEARCH"}
}
func writerCapabilities() []string {
	return []string{"CAPABILITY_SPACE_READ", "CAPABILITY_DOMAIN_READ", "CAPABILITY_GRAPH_READ", "CAPABILITY_GRAPH_WRITE", "CAPABILITY_BLOB_READ", "CAPABILITY_BLOB_WRITE", "CAPABILITY_METADATA_READ", "CAPABILITY_METADATA_WRITE", "CAPABILITY_QUERY_RUN", "CAPABILITY_SEMANTIC_SEARCH"}
}
func readerCapabilities() []string {
	return []string{"CAPABILITY_SPACE_READ", "CAPABILITY_DOMAIN_READ", "CAPABILITY_GRAPH_READ", "CAPABILITY_BLOB_READ", "CAPABILITY_METADATA_READ", "CAPABILITY_QUERY_RUN", "CAPABILITY_SEMANTIC_SEARCH"}
}
