package model

import (
	"testing"
	"time"

	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
)

func TestValidateProcedureInferenceAttemptTransition(t *testing.T) {
	now := time.Now().UTC()
	requested := ProcedureInferenceAttempt{
		ID: "attempt-a", RecordVersion: 1, SpaceID: uuid.NewString(), DomainID: graph.DomainID(uuid.New()),
		ProcedureID: "proc", ProcedureVersion: 2, BindingID: "binding", BindingVersion: 3,
		InvocationID: "inv", RunID: "run", RunAttemptNumber: 1, ProviderAttemptNumber: 1,
		State: InferenceAttemptStateRequested, RequestedAt: now,
	}
	if err := ValidateProcedureInferenceAttemptTransition(nil, requested); err != nil {
		t.Fatalf("requested transition: %v", err)
	}

	bypassed := requested
	bypassed.RecordVersion = 2
	bypassed.State = InferenceAttemptStateStarted
	bypassed.StartedAt = now.Add(time.Second)
	if err := ValidateProcedureInferenceAttemptTransition(&requested, bypassed); err == nil {
		t.Fatal("expected requested-to-started authorization bypass rejection")
	}

	authorized := requested
	authorized.RecordVersion = 2
	authorized.State = InferenceAttemptStateAuthorized
	authorized.AuthorizedAt = now.Add(time.Second)
	if err := ValidateProcedureInferenceAttemptTransition(&requested, authorized); err != nil {
		t.Fatalf("authorized transition: %v", err)
	}

	started := authorized
	started.RecordVersion = 3
	started.State = InferenceAttemptStateStarted
	started.StartedAt = now.Add(2 * time.Second)
	if err := ValidateProcedureInferenceAttemptTransition(&authorized, started); err != nil {
		t.Fatalf("started transition: %v", err)
	}

	succeeded := started
	succeeded.RecordVersion = 4
	succeeded.State = InferenceAttemptStateSucceeded
	succeeded.CompletedAt = now.Add(3 * time.Second)
	if err := ValidateProcedureInferenceAttemptTransition(&started, succeeded); err != nil {
		t.Fatalf("succeeded transition: %v", err)
	}
	if !IsTerminalProcedureInferenceAttemptState(succeeded.State) {
		t.Fatal("succeeded state should be terminal")
	}

	mutated := started
	mutated.RecordVersion = 4
	mutated.State = InferenceAttemptStateSucceeded
	mutated.BindingVersion++
	if err := ValidateProcedureInferenceAttemptTransition(&started, mutated); err == nil {
		t.Fatal("expected immutable binding identity rejection")
	}

	restarted := succeeded
	restarted.RecordVersion = 5
	restarted.State = InferenceAttemptStateStarted
	if err := ValidateProcedureInferenceAttemptTransition(&succeeded, restarted); err == nil {
		t.Fatal("expected terminal transition rejection")
	}
}
