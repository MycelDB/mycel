package template

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/graph"
	domainspace "martinbeauvais.com/mbgit/knotbase/knotdb/domain/space"
)

func TestDefaultManager_ImportValidDocument(t *testing.T) {
	m := NewManager()
	if err := m.Init(context.Background(), filepath.Join(t.TempDir(), "store")); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	spaceID := domainspace.SpaceID(uuid.New())

	templates, err := m.Import(context.Background(), spaceID, validDocument())
	if err != nil {
		t.Fatalf("import failed: %v", err)
	}
	if len(templates) != 2 {
		t.Fatalf("expected 2 templates, got %d", len(templates))
	}
	note := templates[0]
	if note.ID == uuid.Nil || note.SpaceID != spaceID || note.Key != "note" || note.Version != "1.0.0" {
		t.Fatalf("unexpected template: %#v", note)
	}
	if len(note.Properties.Allowed) != 1 || note.Properties.Allowed[0].Name != "title" {
		t.Fatalf("unexpected properties: %#v", note.Properties)
	}

	got, err := m.GetByID(context.Background(), note.ID)
	if err != nil {
		t.Fatalf("get by id failed: %v", err)
	}
	if got.ID != note.ID {
		t.Fatalf("unexpected get result: %#v", got)
	}
	found, err := m.Find(context.Background(), spaceID, "note", "1.0.0")
	if err != nil {
		t.Fatalf("find failed: %v", err)
	}
	if found.ID != note.ID {
		t.Fatalf("unexpected find result: %#v", found)
	}
}

func TestDefaultManager_ImportRejectsDuplicateExistingVersion(t *testing.T) {
	m := NewManager()
	if err := m.Init(context.Background(), filepath.Join(t.TempDir(), "store")); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	if _, err := m.Import(context.Background(), spaceID, validDocument()); err != nil {
		t.Fatalf("first import failed: %v", err)
	}
	_, err := m.Import(context.Background(), spaceID, validDocument())
	if !errors.Is(err, ErrDuplicateTemplateVersion) {
		t.Fatalf("expected ErrDuplicateTemplateVersion, got: %v", err)
	}
}

func TestDefaultManager_ImportRejectsInvalidSemver(t *testing.T) {
	m := NewManager()
	if err := m.Init(context.Background(), filepath.Join(t.TempDir(), "store")); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	doc := validDocument()
	doc.Templates[0].Version = "v1"
	_, err := m.Import(context.Background(), domainspace.SpaceID(uuid.New()), doc)
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput, got: %v", err)
	}
}

func TestDefaultManager_ImportRejectsAllowedForbiddenOverlap(t *testing.T) {
	m := NewManager()
	if err := m.Init(context.Background(), filepath.Join(t.TempDir(), "store")); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	doc := validDocument()
	doc.Templates[0].Properties.Forbidden = []string{"title"}
	_, err := m.Import(context.Background(), domainspace.SpaceID(uuid.New()), doc)
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput, got: %v", err)
	}
}

func TestDefaultManager_ImportRejectsChildRefWithoutVersion(t *testing.T) {
	m := NewManager()
	if err := m.Init(context.Background(), filepath.Join(t.TempDir(), "store")); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	doc := validDocument()
	doc.Templates[0].Children.AllowedTemplates[0].Version = ""
	_, err := m.Import(context.Background(), domainspace.SpaceID(uuid.New()), doc)
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput, got: %v", err)
	}
}

func TestDefaultManager_ImportRejectsDisallowedChildrenWithRefs(t *testing.T) {
	m := NewManager()
	if err := m.Init(context.Background(), filepath.Join(t.TempDir(), "store")); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	doc := validDocument()
	doc.Templates[0].Children.Allowed = false
	_, err := m.Import(context.Background(), domainspace.SpaceID(uuid.New()), doc)
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput, got: %v", err)
	}
}

func validDocument() ImportDocument {
	return ImportDocument{
		SchemaVersion: 1,
		Templates: []TemplateImport{
			{
				Key:         "note",
				Version:     "1.0.0",
				DisplayName: "Note",
				Properties: PropertyPolicyImport{
					AllowExtra: false,
					Allowed: []TemplatePropertyImport{
						{Name: "title", Type: graph.PropertyTypeString, Required: true},
					},
					Forbidden: []string{"secret"},
				},
				Children: ChildPolicyImport{
					Allowed: true,
					AllowedTemplates: []TemplateRefImport{
						{Key: "task", Version: "1.0.0"},
					},
				},
			},
			{
				Key:         "task",
				Version:     "1.0.0",
				DisplayName: "Task",
				Properties: PropertyPolicyImport{
					AllowExtra: true,
					Allowed: []TemplatePropertyImport{
						{Name: "done", Type: graph.PropertyTypeBool, Default: false},
					},
				},
				Children: ChildPolicyImport{Allowed: false},
			},
		},
	}
}
