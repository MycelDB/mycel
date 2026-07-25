package client

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	"github.com/myceldb/mycel/internal/schema/service"
	"github.com/myceldb/mycel/internal/schema/storage"
)

func TestSchemaServicePutGetValidate(t *testing.T) {
	ctx := daemonauth.ContextWithPrincipal(context.Background(), daemonauth.Principal{Kind: daemonauth.PrincipalKindUser, UserID: uuid.NewString(), Username: "alice"})
	svc := NewSchemaService(service.NewManager(storage.NewMemoryStore()))
	domainID := uuid.NewString()
	schemaJSON := `{"DomainID":"` + domainID + `","Name":"test","Version":"v1","Mode":"strict","NodeTypes":[{"Name":"Person","Labels":["Person"],"Properties":[{"Name":"firstName","Type":"string","Required":true}]}]}`
	valid, err := svc.ValidateSchema(ctx, &clientv1.ValidateSchemaRequest{SchemaJson: schemaJSON})
	if err != nil || !valid.GetValid() {
		t.Fatalf("ValidateSchema() = %+v, %v", valid, err)
	}
	put, err := svc.PutDomainSchema(ctx, &clientv1.PutDomainSchemaRequest{DomainId: domainID, SchemaJson: schemaJSON})
	if err != nil || !strings.Contains(put.GetSchemaJson(), "Person") {
		t.Fatalf("PutDomainSchema() = %+v, %v", put, err)
	}
	get, err := svc.GetDomainSchema(ctx, &clientv1.GetDomainSchemaRequest{DomainId: domainID})
	if err != nil || !strings.Contains(get.GetSchemaJson(), "Person") {
		t.Fatalf("GetDomainSchema() = %+v, %v", get, err)
	}
}
