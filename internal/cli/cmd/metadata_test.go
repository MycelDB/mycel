package cmd

import (
	"encoding/json"
	"testing"

	adminv1 "github.com/myceldb/mycel-api/gen/go/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel-api/gen/go/mycel/client/v1"
)

func TestMetadataCatalogCommandsUseDaemonGRPC(t *testing.T) {
	_, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	createTestUser(t, addr, adminPassword, "metadata-user", "metadata-pass")
	out, err := runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "space", "add", "Metadata Space", "--owner-username", "metadata-user")
	if err != nil {
		t.Fatalf("space add failed: %v\n%s", err, out)
	}
	var createdSpace adminv1.CreateSpaceResponse
	if err := json.Unmarshal([]byte(out), &createdSpace); err != nil {
		t.Fatalf("decode space add: %v\n%s", err, out)
	}
	spaceID := createdSpace.GetSpace().GetSpaceId()
	domainID := createdSpace.GetDefaultDomainId()
	base := []string{"--daemon-addr", addr, "-u", "metadata-user", "-p", "metadata-pass", "--output", "json"}
	out, err = runCLI(t, append(base, "session", "open", "--space-id", spaceID, "--domain-id", domainID)...)
	if err != nil {
		t.Fatalf("session open failed: %v\n%s", err, out)
	}
	var session clientv1.GraphSession
	if err := json.Unmarshal([]byte(out), &session); err != nil {
		t.Fatalf("decode session: %v\n%s", err, out)
	}
	out, err = runCLI(t, append(base, "transaction", "begin", session.GetSessionId(), "--mode", "read-write")...)
	if err != nil {
		t.Fatalf("transaction begin failed: %v\n%s", err, out)
	}
	var tx clientv1.GraphTransaction
	if err := json.Unmarshal([]byte(out), &tx); err != nil {
		t.Fatalf("decode tx: %v\n%s", err, out)
	}
	_, err = runCLI(t, append(base, "graph", "node", "create", "--transaction-id", tx.GetTransactionId(), "--content", "A", "--props-json", `{"tags":["Project","Urgent"],"properties":{"Priority":"high","Status":"active"}}`)...)
	if err != nil {
		t.Fatalf("create node A failed: %v", err)
	}
	_, err = runCLI(t, append(base, "graph", "node", "create", "--transaction-id", tx.GetTransactionId(), "--content", "B", "--props-json", `{"tags":["project"],"properties":{"priority":"low"}}`)...)
	if err != nil {
		t.Fatalf("create node B failed: %v", err)
	}
	out, err = runCLI(t, append(base, "metadata", "tags", "--transaction-id", tx.GetTransactionId())...)
	if err != nil {
		t.Fatalf("metadata tags failed: %v\n%s", err, out)
	}
	var tags clientv1.ListTagsResponse
	if err := json.Unmarshal([]byte(out), &tags); err != nil {
		t.Fatalf("decode tags: %v\n%s", err, out)
	}
	if countTag(tags.GetTags(), "project") != 2 || countTag(tags.GetTags(), "urgent") != 1 {
		t.Fatalf("unexpected tags: %#v", tags.GetTags())
	}
	out, err = runCLI(t, append(base, "metadata", "properties", "--transaction-id", tx.GetTransactionId())...)
	if err != nil {
		t.Fatalf("metadata properties failed: %v\n%s", err, out)
	}
	var props clientv1.ListPropertyNamesResponse
	if err := json.Unmarshal([]byte(out), &props); err != nil {
		t.Fatalf("decode properties: %v\n%s", err, out)
	}
	if countProperty(props.GetProperties(), "priority") != 2 || countProperty(props.GetProperties(), "status") != 1 {
		t.Fatalf("unexpected properties: %#v", props.GetProperties())
	}
}

func countTag(tags []*clientv1.TagSummary, name string) int64 {
	for _, tag := range tags {
		if tag.GetName() == name {
			return tag.GetNodeCount()
		}
	}
	return 0
}

func countProperty(properties []*clientv1.PropertySummary, name string) int64 {
	for _, property := range properties {
		if property.GetName() == name {
			return property.GetNodeCount()
		}
	}
	return 0
}
