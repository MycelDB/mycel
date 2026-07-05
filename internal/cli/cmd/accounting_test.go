package cmd

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/domain/graph"
	"github.com/myceldb/mycel/domain/identity"
	domainsemantic "github.com/myceldb/mycel/domain/semantic"
	mycelengine "github.com/myceldb/mycel/engine"
	storeaccounting "github.com/myceldb/mycel/store/accounting"
)

func TestAccountingUsageCLI(t *testing.T) {
	ctx := context.Background()
	dataDir := filepath.Join(t.TempDir(), "mycel")
	eng, err := mycelengine.NewEngine(mycelengine.EngineConfig{DataDir: dataDir, Mode: mycelengine.EngineModeStandalone, CreateIfMissing: true, AdminUsername: "admin", AdminPassword: "pass"}, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("init engine failed: %v", err)
	}
	auth, err := eng.Authenticate(ctx, mycelengine.AuthInput{UserRef: identity.UserRef("admin"), Password: "pass"})
	if err != nil {
		t.Fatalf("auth failed: %v", err)
	}
	bob, err := eng.CreateUser(ctx, mycelengine.CreateUserInput{AccessToken: auth.AccessToken, User: identity.UserInput{Username: identity.UserRef("bob"), Status: identity.UserStatusActive}, Password: "pass"})
	if err != nil {
		t.Fatalf("create bob failed: %v", err)
	}
	space, err := eng.CreateSpace(ctx, mycelengine.CreateSpaceInput{AccessToken: auth.AccessToken, Name: "Accounting Space"})
	if err != nil {
		t.Fatalf("create accounting space failed: %v", err)
	}
	domain, err := eng.CreateDomain(ctx, mycelengine.CreateDomainInput{AccessToken: auth.AccessToken, SpaceID: space.SpaceID, Key: "usage-domain", Name: "Usage Domain"})
	if err != nil {
		t.Fatalf("create usage domain failed: %v", err)
	}
	_ = eng.Close()

	mgr := storeaccounting.NewManager()
	if err := mgr.Init(ctx, filepath.Join(dataDir, "meta", "accounting")); err != nil {
		t.Fatalf("accounting init failed: %v", err)
	}
	spaceID := space.SpaceID
	domainID := domain.ID
	nodeID := graph.NodeID(uuid.New())
	endpointID := domainsemantic.ModelEndpointID(uuid.New())
	modelID := domainsemantic.InferenceModelID(uuid.New())
	grantID := domainsemantic.CredentialGrantID(uuid.New())
	if _, err := mgr.Append(ctx, domainsemantic.InferenceUsageEvent{CreatedAt: time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC), Status: "success", Operation: "chat", ActorPrincipalID: bob.ID, SpaceID: spaceID, DomainID: domainID, TargetNodeID: nodeID, ModelEndpointID: endpointID, ModelID: modelID, CredentialGrantID: grantID, InputTokens: 11, OutputTokens: 7, TokenCountSource: "provider_reported"}); err != nil {
		t.Fatalf("append failed: %v", err)
	}
	if _, err := mgr.Append(ctx, domainsemantic.InferenceUsageEvent{CreatedAt: time.Date(2026, 6, 11, 0, 0, 0, 0, time.UTC), Status: "failed", Operation: "content_embedding", ActorPrincipalID: bob.ID, SpaceID: spaceID, DomainID: domainID, ModelEndpointID: endpointID, ModelID: modelID, InputTokens: 3, TokenCountSource: "estimated"}); err != nil {
		t.Fatalf("append failed event failed: %v", err)
	}

	exportPath := filepath.Join(t.TempDir(), "usage.csv")
	runMycelCommand(t, "-d", dataDir, "-u", "admin", "-p", "pass", "accounting", "usage", "summarize", "--from", "2026-06-01", "--to", "2026-07-01", "--user", "bob", "--space", "Accounting Space", "--domain", "usage-domain", "--group-by", "operation")
	runMycelCommand(t, "-d", dataDir, "-u", "admin", "-p", "pass", "accounting", "usage", "events", "--space", spaceID.String(), "--limit", "1")
	runMycelCommand(t, "-d", dataDir, "-u", "admin", "-p", "pass", "accounting", "usage", "export", "--format", "csv", "--output", exportPath)
	if raw, err := os.ReadFile(exportPath); err != nil || !strings.Contains(string(raw), "chat") {
		t.Fatalf("expected csv export with chat, err=%v raw=%s", err, string(raw))
	}
	runMycelCommand(t, "-d", dataDir, "-u", "admin", "-p", "pass", "accounting", "usage", "rebuild-indexes")
	if _, err := os.Stat(filepath.Join(dataDir, "meta", "accounting", "indexes", "by_space", spaceID.String(), "2026-06.kidx")); err != nil {
		t.Fatalf("expected rebuilt by_space index: %v", err)
	}
	runMycelCommand(t, "-d", dataDir, "-u", "admin", "-p", "pass", "accounting", "usage", "rebuild-rollups")
	if _, err := os.Stat(filepath.Join(dataDir, "meta", "accounting", "rollups", "space-monthly.json")); err != nil {
		t.Fatalf("expected rebuilt space rollup: %v", err)
	}
	expectMycelCommandError(t, "-d", dataDir, "-u", "bob", "-p", "pass", "accounting", "usage", "events")
}
