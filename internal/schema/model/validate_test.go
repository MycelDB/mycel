package model

import (
	"testing"

	"github.com/google/uuid"
)

func TestValidateRejectsDuplicateTypeNames(t *testing.T) {
	s := DomainSchema{DomainID: uuid.New(), Mode: SchemaModeStrict, NodeTypes: []NodeType{{Name: "Person"}, {Name: "Person"}}}
	if err := Validate(s); err == nil {
		t.Fatalf("expected duplicate node type error")
	}
}

func TestValidateRejectsInvalidFieldType(t *testing.T) {
	s := DomainSchema{DomainID: uuid.New(), Mode: SchemaModeStrict, NodeTypes: []NodeType{{Name: "Person", Properties: []FieldSpec{{Name: "age", Type: FieldType("integer")}}}}}
	if err := Validate(s); err == nil {
		t.Fatalf("expected invalid field type error")
	}
}

func TestValidateRejectsUnknownEndpointType(t *testing.T) {
	s := DomainSchema{DomainID: uuid.New(), Mode: SchemaModeStrict, NodeTypes: []NodeType{{Name: "Person", Labels: []string{"Person"}}}, EdgeTypes: []EdgeType{{Name: "Knows", Labels: []string{"KNOWS"}, From: EndpointSpec{NodeTypes: []string{"Person"}}, To: EndpointSpec{NodeTypes: []string{"Missing"}}}}}
	if err := Validate(s); err == nil {
		t.Fatalf("expected unknown endpoint type error")
	}
}
