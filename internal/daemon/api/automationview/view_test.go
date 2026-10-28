package automationview

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	graph "github.com/myceldb/mycel/internal/graph/model"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestSnapshotPaginationAndChangeResumeAreBoundAndRaceFree(t *testing.T) {
	codec, err := NewCursorCodec([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	spaceID := uuid.NewString()
	domainID := graph.DomainID(uuid.New())
	a := testAttempt("a", spaceID, domainID, now)
	b := testAttempt("b", spaceID, domainID, now.Add(time.Second))
	ledger := automation.ProcedureInferenceLedger{SpaceID: spaceID, HighWaterSequence: 3, EarliestRetainedSequence: 1, EarliestRetainedStateSequence: 1, Attempts: []automation.ProcedureInferenceAttempt{withState(a, 2, automation.InferenceAttemptStateAuthorized), b}, Changes: []automation.ProcedureInferenceAttemptChange{
		testChange(1, a),
		testChange(2, withState(a, 2, automation.InferenceAttemptStateAuthorized)),
		testChange(3, b),
	}}
	filter := &commonv1.ProcedureInferenceAttemptFilter{DomainId: domainID.String()}
	first, err := codec.ListCurrent(ledger, filter, 1, "", "principal")
	if err != nil || len(first.Attempts) != 1 || first.Attempts[0].ID != "b" || first.NextPageToken == "" || first.SnapshotCursor == "" || first.SnapshotHighWaterSequence != 3 {
		t.Fatalf("first page=%+v err=%v", first, err)
	}

	started := withState(a, 3, automation.InferenceAttemptStateStarted)
	ledger.HighWaterSequence = 4
	ledger.Attempts[0] = started
	ledger.Changes = append(ledger.Changes, testChange(4, started))
	second, err := codec.ListCurrent(ledger, filter, 1, first.NextPageToken, "principal")
	if err != nil || len(second.Attempts) != 1 || second.Attempts[0].ID != "a" || second.Attempts[0].State != automation.InferenceAttemptStateAuthorized || second.SnapshotHighWaterSequence != 3 {
		t.Fatalf("snapshot page changed under pagination: %+v err=%v", second, err)
	}
	changes, err := codec.ListChanges(ledger, filter, 10, first.SnapshotCursor, "principal")
	if err != nil || len(changes.Changes) != 1 || changes.Changes[0].Sequence != 4 {
		t.Fatalf("snapshot resume=%+v err=%v", changes, err)
	}
	if _, err := codec.ListCurrent(ledger, &commonv1.ProcedureInferenceAttemptFilter{ProcedureId: "other"}, 1, first.NextPageToken, "principal"); err != ErrInvalidCursor {
		t.Fatalf("filter-rebound cursor error=%v", err)
	}
	if _, err := codec.ListCurrent(ledger, filter, 1, first.NextPageToken, "other-principal"); err != ErrInvalidCursor {
		t.Fatalf("principal-rebound cursor error=%v", err)
	}
	ledger.EarliestRetainedStateSequence = 4
	if _, err := codec.ListCurrent(ledger, filter, 1, first.NextPageToken, "principal"); err != ErrInvalidCursor {
		t.Fatalf("stale state snapshot cursor error=%v", err)
	}
}

func TestCurrentStateUsesMaterializedAttemptsWhenChangeHistoryIsPruned(t *testing.T) {
	codec, _ := NewCursorCodec([]byte("0123456789abcdef0123456789abcdef"))
	spaceID := uuid.NewString()
	domainID := graph.DomainID(uuid.New())
	a := testAttempt("a", spaceID, domainID, time.Now().UTC())
	b := testAttempt("b", spaceID, domainID, time.Now().UTC().Add(time.Second))
	ledger := automation.ProcedureInferenceLedger{SpaceID: spaceID, HighWaterSequence: 9, EarliestRetainedSequence: 8, EarliestRetainedStateSequence: 8, Attempts: []automation.ProcedureInferenceAttempt{a, b}, Changes: []automation.ProcedureInferenceAttemptChange{testChange(8, a), testChange(9, b)}}
	page, err := codec.ListCurrent(ledger, nil, 10, "", "principal")
	if err != nil || len(page.Attempts) != 2 {
		t.Fatalf("materialized current page=%+v err=%v", page, err)
	}
}

func TestFilteredEmptyChangePageAdvancesAndGapIsExplicit(t *testing.T) {
	codec, _ := NewCursorCodec([]byte("0123456789abcdef0123456789abcdef"))
	spaceID := uuid.NewString()
	attempt := testAttempt("a", spaceID, graph.DomainID(uuid.New()), time.Now().UTC())
	ledger := automation.ProcedureInferenceLedger{SpaceID: spaceID, HighWaterSequence: 2, EarliestRetainedSequence: 1, Changes: []automation.ProcedureInferenceAttemptChange{testChange(1, attempt), testChange(2, withState(attempt, 2, automation.InferenceAttemptStateFailed))}}
	page, err := codec.ListChanges(ledger, &commonv1.ProcedureInferenceAttemptFilter{ProcedureId: "no-match"}, 10, "", "principal")
	if err != nil || len(page.Changes) != 0 || page.NextCursor == "" {
		t.Fatalf("empty filtered page=%+v err=%v", page, err)
	}
	cursor, err := codec.decode(page.NextCursor)
	if err != nil || cursor.Position != 2 {
		t.Fatalf("examined cursor=%+v err=%v", cursor, err)
	}
	oldCursor, err := codec.encode(cursorPayload{Version: cursorVersion, Kind: "changes", SpaceID: spaceID, BindingHash: cursor.BindingHash, Position: 1})
	if err != nil {
		t.Fatal(err)
	}
	ledger.EarliestRetainedSequence = 3
	gap, err := codec.ListChanges(ledger, &commonv1.ProcedureInferenceAttemptFilter{ProcedureId: "no-match"}, 10, oldCursor, "principal")
	if err != nil || !gap.HistoryGap || len(gap.Changes) != 0 {
		t.Fatalf("gap=%+v err=%v", gap, err)
	}
}

func TestTypedMappingsExcludeSensitiveRunAndAttemptData(t *testing.T) {
	attempt := testAttempt("a", uuid.NewString(), graph.DomainID(uuid.New()), time.Now().UTC())
	attempt.PolicyDecisionID = uuid.NewString()
	attempt.CredentialGrantID = uuid.NewString()
	attemptProto, err := protojson.Marshal(AttemptToProto(attempt))
	if err != nil {
		t.Fatal(err)
	}
	run := automation.Run{ID: "run", DomainID: attempt.DomainID, InvocationID: "inv", ProcedureID: "proc", ProcedureVersion: 1, BindingID: "binding", BindingVersion: 1, Status: "failed", CredentialID: "credential-secret", ProviderRequestID: "provider-secret", Error: "raw-secret-error", Context: map[string]automation.RunContextSummary{"secret": {Error: "secret"}}, InferenceAttemptIDs: []string{attempt.ID}}
	runProto, err := protojson.Marshal(RunToProto(run))
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal([]json.RawMessage{attemptProto, runProto})
	for _, prohibited := range []string{"credential-secret", "provider-secret", "raw-secret-error", "Context", "prompt", "output"} {
		if strings.Contains(strings.ToLower(string(encoded)), strings.ToLower(prohibited)) {
			t.Fatalf("typed mapping contains %q: %s", prohibited, encoded)
		}
	}
}

func testAttempt(id, spaceID string, domainID graph.DomainID, requestedAt time.Time) automation.ProcedureInferenceAttempt {
	return automation.ProcedureInferenceAttempt{ID: id, RecordVersion: 1, SpaceID: spaceID, DomainID: domainID, ProcedureID: "proc", ProcedureVersion: 1, BindingID: "binding", BindingVersion: 1, InvocationID: "inv-" + id, RunID: "run-" + id, RunAttemptNumber: 1, ProviderAttemptNumber: 1, State: automation.InferenceAttemptStateRequested, RequestedAt: requestedAt}
}

func withState(in automation.ProcedureInferenceAttempt, version uint64, state string) automation.ProcedureInferenceAttempt {
	in.RecordVersion = version
	in.State = state
	if state == automation.InferenceAttemptStateAuthorized {
		in.AuthorizedAt = in.RequestedAt.Add(time.Millisecond)
	}
	if state == automation.InferenceAttemptStateStarted {
		in.StartedAt = in.RequestedAt.Add(2 * time.Millisecond)
	}
	if automation.IsTerminalProcedureInferenceAttemptState(state) {
		in.CompletedAt = in.RequestedAt.Add(3 * time.Millisecond)
	}
	return in
}

func testChange(sequence uint64, attempt automation.ProcedureInferenceAttempt) automation.ProcedureInferenceAttemptChange {
	return automation.ProcedureInferenceAttemptChange{EventID: uuid.NewString(), SchemaVersion: automation.InferenceAttemptChangeSchemaV1, SpaceID: attempt.SpaceID, Sequence: sequence, ChangeKind: attempt.State, AttemptID: attempt.ID, RecordVersion: attempt.RecordVersion, Attempt: attempt, OccurredAt: attempt.RequestedAt, CommittedAt: attempt.RequestedAt}
}
