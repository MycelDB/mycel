package service

import (
	"reflect"
	"testing"

	domaininference "github.com/myceldb/mycel/internal/inference/model"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
)

func TestSemanticPolicyProjectionPreservesProcedureAndRouteSelectors(t *testing.T) {
	policy := domainsemantic.InferencePolicy{
		Scope:        domainsemantic.ProcessingScope{ProcedureID: "proc", ProcedureVersion: 3, BindingID: "binding", BindingVersion: 4, InvocationID: "inv", RunID: "run"},
		EndpointRefs: []string{"endpoint"}, ModelRefs: []string{"model"}, CapabilityRefs: []string{"capability"},
		Effect: domainsemantic.PolicyEffectAllow,
	}
	projected := semanticPolicyToInference("space", policy)
	if projected.Scope.ProcedureID != "proc" || projected.Scope.ProcedureVersion != 3 || projected.Scope.BindingID != "binding" || projected.Scope.BindingVersion != 4 || projected.Scope.InvocationID != "inv" || projected.Scope.RunID != "run" {
		t.Fatalf("projected scope=%+v", projected.Scope)
	}
	if !reflect.DeepEqual(projected.EndpointRefs, []string{"endpoint"}) || !reflect.DeepEqual(projected.ModelRefs, []string{"model"}) || !reflect.DeepEqual(projected.CapabilityRefs, []string{"capability"}) {
		t.Fatalf("projected route selectors=%+v", projected)
	}
}

func TestSemanticGrantProjectionPreservesExactProcedureScope(t *testing.T) {
	grant := domainsemantic.CredentialGrant{Scope: domainsemantic.ProcessingScope{ProcedureID: "proc", ProcedureVersion: 2, BindingID: "binding", BindingVersion: 5}}
	projected := semanticGrantToInference("space", grant)
	want := domaininference.Scope{ProcedureID: "proc", ProcedureVersion: 2, BindingID: "binding", BindingVersion: 5}
	if projected.Scope.ProcedureID != want.ProcedureID || projected.Scope.ProcedureVersion != want.ProcedureVersion || projected.Scope.BindingID != want.BindingID || projected.Scope.BindingVersion != want.BindingVersion {
		t.Fatalf("projected scope=%+v", projected.Scope)
	}
}
