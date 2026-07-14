package space

import (
	"context"
	"encoding/json"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/daemon/config"
	"github.com/myceldb/mycel/internal/daemon/quiesce"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	"github.com/myceldb/mycel/internal/graph/model"
	storetemplate "github.com/myceldb/mycel/internal/graph/template/storage"
	"github.com/myceldb/mycel/internal/wal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestModuleWALCreateSpaceAppendsAndApplies(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	walManager, err := wal.Open(ctx, wal.Options{Dir: filepath.Join(dataDir, "wal"), SegmentBytes: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer walManager.Close()
	progress := wal.NewFileProgressStore(filepath.Join(dataDir, "meta", "wal", "progress.json"))
	waiter := wal.NewApplyWaiter()
	rt := &daemonruntime.Runtime{Config: config.Config{DataDir: dataDir}, Logger: slog.Default(), WAL: walManager, WALRegistry: wal.NewRegistry(), WALProgress: progress, WALWaiter: waiter}
	m := NewModule()
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	owner := uuid.New()
	result, err := m.CreateSpaceWithResult(ctx, CreateSpaceInput{Name: "wal-space", OwnerUserID: owner})
	if err != nil {
		t.Fatalf("CreateSpaceWithResult() error = %v", err)
	}
	sp, domain := result.Space, result.Domain
	if sp.SpaceID == uuid.Nil || domain.ID == uuid.Nil {
		t.Fatalf("expected resolved ids: %#v %#v", sp, domain)
	}
	if result.CommitLSN != 1 {
		t.Fatalf("CommitLSN = %v, want 1", result.CommitLSN)
	}
	if got := walManager.LastCommittedLSN(); got != 1 {
		t.Fatalf("LastCommittedLSN() = %v, want 1", got)
	}
	if applied, err := progress.AppliedLSN(ctx); err != nil || applied != result.CommitLSN {
		t.Fatalf("AppliedLSN() = %v, %v; want %v", applied, err, result.CommitLSN)
	}
	if err := waiter.WaitUntilApplied(ctx, result.CommitLSN); err != nil {
		t.Fatalf("WaitUntilApplied() error = %v", err)
	}
	spaces, err := m.ListSpaces(ctx, true)
	if err != nil || len(spaces) != 1 || spaces[0].SpaceID != sp.SpaceID {
		t.Fatalf("spaces=%#v err=%v", spaces, err)
	}
}

func TestModuleWALCreateDomainAppendsAndApplies(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	walManager, err := wal.Open(ctx, wal.Options{Dir: filepath.Join(dataDir, "wal"), SegmentBytes: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer walManager.Close()
	progress := wal.NewFileProgressStore(filepath.Join(dataDir, "meta", "wal", "progress.json"))
	rt := &daemonruntime.Runtime{Config: config.Config{DataDir: dataDir}, Logger: slog.Default(), WAL: walManager, WALRegistry: wal.NewRegistry(), WALProgress: progress, WALWaiter: wal.NewApplyWaiter()}
	m := NewModule()
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	owner := uuid.New()
	sp, _, err := m.CreateSpace(ctx, CreateSpaceInput{Name: "wal-space", OwnerUserID: owner})
	if err != nil {
		t.Fatalf("CreateSpace() error = %v", err)
	}
	domain, err := m.CreateDomain(ctx, owner.String(), CreateDomainInput{SpaceID: sp.SpaceID.String(), Key: "docs", Name: "Docs"})
	if err != nil {
		t.Fatalf("CreateDomain() error = %v", err)
	}
	if domain.ID == uuid.Nil || domain.Key != "docs" {
		t.Fatalf("domain=%#v", domain)
	}
	if got := walManager.LastCommittedLSN(); got != 2 {
		t.Fatalf("LastCommittedLSN() = %v, want 2", got)
	}
	if applied, err := progress.AppliedLSN(ctx); err != nil || applied != 2 {
		t.Fatalf("AppliedLSN() = %v, %v; want 2", applied, err)
	}
}

func TestModuleWALUpdateDomainAppendsAndApplies(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	walManager, err := wal.Open(ctx, wal.Options{Dir: filepath.Join(dataDir, "wal"), SegmentBytes: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer walManager.Close()
	progress := wal.NewFileProgressStore(filepath.Join(dataDir, "meta", "wal", "progress.json"))
	rt := &daemonruntime.Runtime{Config: config.Config{DataDir: dataDir}, Logger: slog.Default(), WAL: walManager, WALRegistry: wal.NewRegistry(), WALProgress: progress, WALWaiter: wal.NewApplyWaiter()}
	m := NewModule()
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	owner := uuid.New()
	sp, _, err := m.CreateSpace(ctx, CreateSpaceInput{Name: "wal-space", OwnerUserID: owner})
	if err != nil {
		t.Fatalf("CreateSpace() error = %v", err)
	}
	domain, err := m.CreateDomain(ctx, owner.String(), CreateDomainInput{SpaceID: sp.SpaceID.String(), Key: "docs", Name: "Docs"})
	if err != nil {
		t.Fatalf("CreateDomain() error = %v", err)
	}
	desc := "updated docs"
	updated, err := m.UpdateDomain(ctx, owner.String(), UpdateDomainInput{SpaceID: sp.SpaceID.String(), DomainID: domain.ID.String(), Description: &desc})
	if err != nil {
		t.Fatalf("UpdateDomain() error = %v", err)
	}
	if updated.Description != desc {
		t.Fatalf("updated domain=%#v", updated)
	}
	if got := walManager.LastCommittedLSN(); got != 3 {
		t.Fatalf("LastCommittedLSN() = %v, want 3", got)
	}
	if applied, err := progress.AppliedLSN(ctx); err != nil || applied != 3 {
		t.Fatalf("AppliedLSN() = %v, %v; want 3", applied, err)
	}
}

func TestModuleWALDeleteDomainAppendsAndApplies(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	walManager, err := wal.Open(ctx, wal.Options{Dir: filepath.Join(dataDir, "wal"), SegmentBytes: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer walManager.Close()
	progress := wal.NewFileProgressStore(filepath.Join(dataDir, "meta", "wal", "progress.json"))
	rt := &daemonruntime.Runtime{Config: config.Config{DataDir: dataDir}, Logger: slog.Default(), WAL: walManager, WALRegistry: wal.NewRegistry(), WALProgress: progress, WALWaiter: wal.NewApplyWaiter()}
	m := NewModule()
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	owner := uuid.New()
	sp, _, err := m.CreateSpace(ctx, CreateSpaceInput{Name: "wal-space", OwnerUserID: owner})
	if err != nil {
		t.Fatalf("CreateSpace() error = %v", err)
	}
	domain, err := m.CreateDomain(ctx, owner.String(), CreateDomainInput{SpaceID: sp.SpaceID.String(), Key: "docs", Name: "Docs"})
	if err != nil {
		t.Fatalf("CreateDomain() error = %v", err)
	}
	if err := m.DeleteDomain(ctx, owner.String(), sp.SpaceID.String(), domain.ID.String()); err != nil {
		t.Fatalf("DeleteDomain() error = %v", err)
	}
	if got := walManager.LastCommittedLSN(); got != 3 {
		t.Fatalf("LastCommittedLSN() = %v, want 3", got)
	}
	if _, err := m.GetDomainByRef(ctx, sp.SpaceID.String(), domain.ID.String()); err == nil {
		t.Fatal("expected deleted domain lookup to fail")
	}
	if applied, err := progress.AppliedLSN(ctx); err != nil || applied != 3 {
		t.Fatalf("AppliedLSN() = %v, %v; want 3", applied, err)
	}
}

func TestModuleWALGrantSpaceUserAppendsAndApplies(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	walManager, err := wal.Open(ctx, wal.Options{Dir: filepath.Join(dataDir, "wal"), SegmentBytes: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer walManager.Close()
	progress := wal.NewFileProgressStore(filepath.Join(dataDir, "meta", "wal", "progress.json"))
	rt := &daemonruntime.Runtime{Config: config.Config{DataDir: dataDir}, Logger: slog.Default(), WAL: walManager, WALRegistry: wal.NewRegistry(), WALProgress: progress, WALWaiter: wal.NewApplyWaiter()}
	m := NewModule()
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	owner := uuid.New()
	sp, _, err := m.CreateSpace(ctx, CreateSpaceInput{Name: "wal-space", OwnerUserID: owner})
	if err != nil {
		t.Fatalf("CreateSpace() error = %v", err)
	}
	user := uuid.New()
	grant, err := m.GrantSpaceUser(ctx, sp.SpaceID.String(), user.String(), "reader")
	if err != nil {
		t.Fatalf("GrantSpaceUser() error = %v", err)
	}
	if grant.ID == "" || grant.Role != "reader" {
		t.Fatalf("grant=%#v", grant)
	}
	if got := walManager.LastCommittedLSN(); got != 2 {
		t.Fatalf("LastCommittedLSN() = %v, want 2", got)
	}
	if applied, err := progress.AppliedLSN(ctx); err != nil || applied != 2 {
		t.Fatalf("AppliedLSN() = %v, %v; want 2", applied, err)
	}
}

func TestModuleWALDeleteSpaceAppendsAndApplies(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	walManager, err := wal.Open(ctx, wal.Options{Dir: filepath.Join(dataDir, "wal"), SegmentBytes: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer walManager.Close()
	progress := wal.NewFileProgressStore(filepath.Join(dataDir, "meta", "wal", "progress.json"))
	rt := &daemonruntime.Runtime{Config: config.Config{DataDir: dataDir}, Logger: slog.Default(), WAL: walManager, WALRegistry: wal.NewRegistry(), WALProgress: progress, WALWaiter: wal.NewApplyWaiter()}
	m := NewModule()
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	owner := uuid.New()
	sp, _, err := m.CreateSpace(ctx, CreateSpaceInput{Name: "wal-space", OwnerUserID: owner})
	if err != nil {
		t.Fatalf("CreateSpace() error = %v", err)
	}
	if err := m.DeleteSpace(ctx, sp.SpaceID.String()); err != nil {
		t.Fatalf("DeleteSpace() error = %v", err)
	}
	if got := walManager.LastCommittedLSN(); got != 2 {
		t.Fatalf("LastCommittedLSN() = %v, want 2", got)
	}
	if _, err := m.GetSpace(ctx, sp.SpaceID.String()); err == nil {
		t.Fatal("expected deleted space lookup to fail")
	}
	if applied, err := progress.AppliedLSN(ctx); err != nil || applied != 2 {
		t.Fatalf("AppliedLSN() = %v, %v; want 2", applied, err)
	}
}

func TestModuleWALTemplateMutationsAppendAndApply(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	walManager, err := wal.Open(ctx, wal.Options{Dir: filepath.Join(dataDir, "wal"), SegmentBytes: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer walManager.Close()
	progress := wal.NewFileProgressStore(filepath.Join(dataDir, "meta", "wal", "progress.json"))
	rt := &daemonruntime.Runtime{Config: config.Config{DataDir: dataDir}, Logger: slog.Default(), WAL: walManager, WALRegistry: wal.NewRegistry(), WALProgress: progress, WALWaiter: wal.NewApplyWaiter()}
	m := NewModule()
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	owner := uuid.New()
	sp, _, err := m.CreateSpace(ctx, CreateSpaceInput{Name: "wal-space", OwnerUserID: owner})
	if err != nil {
		t.Fatalf("CreateSpace() error = %v", err)
	}
	created, err := m.CreateTemplate(ctx, owner.String(), sp.SpaceID.String(), storetemplate.TemplateImport{Key: "note", Version: "1.0.0", DisplayName: "Note"})
	if err != nil {
		t.Fatalf("CreateTemplate() error = %v", err)
	}
	name := "Updated Note"
	updated, err := m.UpdateTemplate(ctx, owner.String(), sp.SpaceID.String(), created.ID.String(), &name, nil)
	if err != nil {
		t.Fatalf("UpdateTemplate() error = %v", err)
	}
	if updated.DisplayName != name {
		t.Fatalf("updated=%#v", updated)
	}
	archived, err := m.ArchiveTemplate(ctx, owner.String(), sp.SpaceID.String(), created.ID.String())
	if err != nil {
		t.Fatalf("ArchiveTemplate() error = %v", err)
	}
	if archived.State != graph.TemplateStateArchived {
		t.Fatalf("archived=%#v", archived)
	}
	if err := m.DeleteTemplate(ctx, owner.String(), sp.SpaceID.String(), created.ID.String()); err != nil {
		t.Fatalf("DeleteTemplate() error = %v", err)
	}
	if got := walManager.LastCommittedLSN(); got != 5 {
		t.Fatalf("LastCommittedLSN() = %v, want 5", got)
	}
	if applied, err := progress.AppliedLSN(ctx); err != nil || applied != 5 {
		t.Fatalf("AppliedLSN() = %v, %v; want 5", applied, err)
	}
}

func TestModuleWALImportTemplatesAppendsAndApplies(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	walManager, err := wal.Open(ctx, wal.Options{Dir: filepath.Join(dataDir, "wal"), SegmentBytes: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer walManager.Close()
	rt := &daemonruntime.Runtime{Config: config.Config{DataDir: dataDir}, Logger: slog.Default(), WAL: walManager, WALRegistry: wal.NewRegistry(), WALProgress: wal.NewFileProgressStore(filepath.Join(dataDir, "meta", "wal", "progress.json")), WALWaiter: wal.NewApplyWaiter()}
	m := NewModule()
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	owner := uuid.New()
	sp, _, err := m.CreateSpace(ctx, CreateSpaceInput{Name: "wal-space", OwnerUserID: owner})
	if err != nil {
		t.Fatal(err)
	}
	created, err := m.ImportTemplates(ctx, owner.String(), sp.SpaceID.String(), []storetemplate.TemplateImport{{Key: "a", Version: "1.0.0"}, {Key: "b", Version: "1.0.0"}})
	if err != nil {
		t.Fatalf("ImportTemplates() error = %v", err)
	}
	if len(created) != 2 {
		t.Fatalf("created=%#v", created)
	}
	if got := walManager.LastCommittedLSN(); got != 3 {
		t.Fatalf("LastCommittedLSN() = %v, want 3", got)
	}
}

func TestModuleWALRecoveryAppliesCommittedCreateSpace(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	walDir := filepath.Join(dataDir, "wal")
	seed := NewModule()
	record := seed.buildCreateSpaceRecord(CreateSpaceInput{Name: "recovered", OwnerUserID: uuid.New()})
	payload, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := wal.Open(ctx, wal.Options{Dir: walDir, SegmentBytes: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	lsn, err := writer.Append(ctx, wal.PendingRecord{Type: recordTypeCreateSpaceWithDefaultDomain, SchemaVersion: 1, Timestamp: record.Space.CreatedAt, Encoding: wal.PayloadEncodingJSON, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Sync(ctx, lsn); err != nil {
		t.Fatal(err)
	}
	_ = writer.Close()

	walManager, err := wal.Open(ctx, wal.Options{Dir: walDir, SegmentBytes: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer walManager.Close()
	registry := wal.NewRegistry()
	rt := &daemonruntime.Runtime{Config: config.Config{DataDir: dataDir}, Logger: slog.Default(), WAL: walManager, WALRegistry: registry}
	m := NewModule()
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	recovery := wal.NewRecovery(walManager, registry, wal.NewFileProgressStore(filepath.Join(dataDir, "meta", "wal", "progress.json")))
	applied, err := recovery.Recover(ctx)
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if applied != 1 {
		t.Fatalf("applied=%v want 1", applied)
	}
	sp, err := m.GetSpace(ctx, record.Space.SpaceID.String())
	if err != nil {
		t.Fatalf("GetSpace(recovered) error = %v", err)
	}
	if sp.Name != "recovered" {
		t.Fatalf("space=%#v", sp)
	}
	if _, _, err := m.applyCreateSpaceRecord(ctx, record); err != nil {
		t.Fatalf("reapply should be idempotent: %v", err)
	}
}

func TestModuleQuiesceRejectsCreateSpace(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, Logger: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	lease, err := m.gate.Quiesce(ctx, quiesce.Request{Reason: "test backup", Source: "test"})
	if err != nil {
		t.Fatalf("Quiesce() error = %v", err)
	}
	defer lease.Release(ctx)
	_, _, err = m.CreateSpace(ctx, CreateSpaceInput{Name: "blocked", OwnerUserID: uuid.New()})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("CreateSpace() code = %v, want %v (err=%v)", status.Code(err), codes.Unavailable, err)
	}
}
