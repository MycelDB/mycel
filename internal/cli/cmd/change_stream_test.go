package cmd

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/myceldb/mycel/internal/cli/app"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
)

func TestChangeStreamWatchReceivesCommitEvent(t *testing.T) {
	_, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	createTestUser(t, addr, adminPassword, "change-user", "change-pass")
	out, err := runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "space", "add", "Change Stream Space", "--owner-username", "change-user")
	if err != nil {
		t.Fatalf("space add failed: %v\n%s", err, out)
	}
	var createdSpace adminv1.CreateSpaceResponse
	if err := json.Unmarshal([]byte(out), &createdSpace); err != nil {
		t.Fatalf("decode space add: %v\n%s", err, out)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, authCtx, _, err := loginDaemonPrincipal(ctx, &app.App{DaemonAddr: addr, UserRef: "change-user", Password: "change-pass"})
	if err != nil {
		t.Fatalf("login user: %v", err)
	}
	defer conn.Close()
	spaceID := createdSpace.GetSpace().GetSpaceId()
	domainID := createdSpace.GetDefaultDomainId()
	cliOut, err := runCLI(t, "--daemon-addr", addr, "-u", "change-user", "-p", "change-pass", "--output", "json", "change-stream", "watch", "--space-id", spaceID, "--domain", domainID, "--include-current", "--max-events", "1")
	if err != nil {
		t.Fatalf("change-stream watch CLI failed: %v\n%s", err, cliOut)
	}
	if !strings.Contains(cliOut, "checkpoint") {
		t.Fatalf("expected checkpoint JSON from change-stream CLI, got %s", cliOut)
	}
	stream, err := clientv1.NewGraphChangeServiceClient(conn).WatchGraphChanges(authCtx, &clientv1.WatchGraphChangesRequest{SpaceId: spaceID, DomainId: domainID, IncludeCurrent: true, Projection: &clientv1.GraphChangeProjection{IncludeOrigin: true, IncludeAffectedNodeIds: true, IncludeAffectedEdgeIds: true, IncludeChangedFields: true, IncludeNewNodeSnapshot: true, IncludeNewEdgeSnapshot: true}})
	if err != nil {
		t.Fatalf("watch domain changes: %v", err)
	}
	first, err := stream.Recv()
	if err != nil {
		t.Fatalf("receive checkpoint: %v", err)
	}
	if first.GetCheckpoint() == nil || first.GetCheckpoint().GetCurrentRevision() != 0 {
		t.Fatalf("expected initial checkpoint revision 0, got %#v", first)
	}
	session, err := clientv1.NewSessionServiceClient(conn).OpenSession(authCtx, &clientv1.OpenSessionRequest{SpaceId: spaceID, DomainId: domainID})
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	tx, err := clientv1.NewTransactionServiceClient(conn).BeginTransaction(authCtx, &clientv1.BeginTransactionRequest{SessionId: session.GetSession().GetSessionId(), Mode: clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE})
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := clientv1.NewGraphServiceClient(conn).CreateNode(authCtx, &clientv1.CreateNodeRequest{TransactionId: tx.GetTransaction().GetTransactionId(), Node: &clientv1.NodeCreate{Payload: protoStruct(map[string]any{"text": "stream me"})}}); err != nil {
		t.Fatalf("create node: %v", err)
	}
	commit, err := clientv1.NewTransactionServiceClient(conn).CommitTransaction(authCtx, &clientv1.CommitTransactionRequest{TransactionId: tx.GetTransaction().GetTransactionId()})
	if err != nil {
		t.Fatalf("commit tx: %v", err)
	}
	msg, err := stream.Recv()
	if err != nil {
		t.Fatalf("receive commit event: %v", err)
	}
	event := msg.GetEvent()
	if event == nil || event.GetRevision() != commit.GetCommit().GetCommittedRevision() || event.GetOrigin().GetOperationId() != commit.GetCommit().GetOperationId() {
		t.Fatalf("unexpected event %#v commit %#v", event, commit.GetCommit())
	}
	if len(event.GetChanges()) != 1 {
		t.Fatalf("unexpected changes: %#v", event.GetChanges())
	}
	if event.GetChanges()[0].GetType() != clientv1.GraphChangeType_GRAPH_CHANGE_TYPE_NODE_CREATED || nodePayloadText(event.GetChanges()[0].GetNewNode()) != "stream me" {
		t.Fatalf("expected node-created payload, got %#v", event.GetChanges()[0])
	}
	resumeAfter := int64(0)
	replay, err := clientv1.NewGraphChangeServiceClient(conn).WatchGraphChanges(authCtx, &clientv1.WatchGraphChangesRequest{SpaceId: spaceID, DomainId: domainID, AfterRevision: &resumeAfter, Filter: &clientv1.GraphChangeFilter{EventTypes: []clientv1.GraphChangeType{clientv1.GraphChangeType_GRAPH_CHANGE_TYPE_NODE_CREATED}}, Projection: &clientv1.GraphChangeProjection{IncludeNewNodeSnapshot: true}})
	if err != nil {
		t.Fatalf("watch replay: %v", err)
	}
	replayed, err := replay.Recv()
	if err != nil {
		t.Fatalf("receive replay: %v", err)
	}
	if got := replayed.GetEvent(); got == nil || got.GetRevision() != event.GetRevision() || len(got.GetChanges()) != 1 || nodePayloadText(got.GetChanges()[0].GetNewNode()) != "stream me" {
		t.Fatalf("unexpected replay event: %#v", replayed.GetEvent())
	}
}
