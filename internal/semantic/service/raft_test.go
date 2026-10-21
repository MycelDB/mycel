package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	clusterbackend "github.com/myceldb/mycel/internal/clustering/backend"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	clustermodel "github.com/myceldb/mycel/internal/clustering/model"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	graphmodel "github.com/myceldb/mycel/internal/graph/model"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	inferenceservice "github.com/myceldb/mycel/internal/inference/service"
	config "github.com/myceldb/mycel/internal/runtime/runtimetest"
	daemonruntime "github.com/myceldb/mycel/internal/runtime/runtimetest"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"github.com/myceldb/mycel/internal/wal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSemanticRaftStateMachineAppliesGlobalAndMaintenanceMutations(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	m.raftPartitionCount = 64
	sm := RaftStateMachine{Module: m, PartitionCount: 64}
	vectorStore := semanticVectorStore("global-raft")
	globalRec := semanticMutationRecord{Kind: "vector_store.upsert", Payload: raw(vectorStore)}
	globalPayload, err := json.Marshal(globalRec)
	if err != nil {
		t.Fatal(err)
	}
	globalCmd, err := m.buildSemanticGlobalRaftCommand(globalRec, globalPayload, "semantic-global-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, globalCmd); err != nil {
		t.Fatalf("apply global command: %v", err)
	}
	stores, err := m.globalBase.ListVectorStores(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasVectorStoreKey(stores, vectorStore.Key) {
		t.Fatalf("vector stores=%#v want key %q", stores, vectorStore.Key)
	}

	spaceID := domainspace.SpaceID(uuid.New())
	dirtyEvent := semanticDirtyEvent(spaceID)
	maintRec := maintenanceMutationRecord{Kind: "dirty_event.append", SpaceID: spaceID, Payload: raw(dirtyEvent)}
	maintPayload, err := json.Marshal(maintRec)
	if err != nil {
		t.Fatal(err)
	}
	maintCmd, err := m.buildSemanticMaintenanceRaftCommand(maintRec, maintPayload, "semantic-maintenance-"+spaceID.String()+"-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 2, RaftTerm: 1}, maintCmd); err != nil {
		t.Fatalf("apply maintenance command: %v", err)
	}
	mgr, err := m.baseMaintenanceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	events, err := mgr.ListGraphDirtyEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].ID != dirtyEvent.ID {
		t.Fatalf("dirty events=%#v want %s", events, dirtyEvent.ID)
	}
}

func TestDeterministicCredentialRejectionRemainsRejectedAcrossRaftRestartReplay(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	newModule := func() *Module {
		module := NewModule()
		if result := module.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: dataDir}, LoggerValue: slog.Default()}); !result.OK {
			t.Fatalf("init failed: %v", result.Error)
		}
		module.raftPartitionCount = 64
		return module
	}
	source := newModule()
	endpoint, err := source.globalBase.UpsertModelEndpoint(ctx, domainsemantic.ModelEndpoint{Key: "api", Name: "api", ConnectorType: domainsemantic.ConnectorOpenAICompatible, EndpointURL: "https://example.invalid/v1", NetworkClass: domainsemantic.NetworkClassExternalHTTPS, PrivacyClass: domainsemantic.PrivacyClassThirdParty, AuthModes: []domainsemantic.AuthMode{domainsemantic.AuthModeAPIKey}, Operations: []domainsemantic.Operation{domainsemantic.OperationEmbeddings}, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(95, 0).UTC()
	credentialAID := domainsemantic.InferenceCredentialID(uuid.New())
	secretAID := domainsemantic.SecretID(uuid.New())
	secretA := domainsemantic.Secret{ID: secretAID, OwnerType: domainsemantic.CredentialOwnerUser, OwnerID: "owner-a", Kind: domainsemantic.SecretKindInlineEncrypted, Ciphertext: &domainsemantic.EncryptedSecretPayload{Algorithm: "test", CipherB64: "a"}, SecretSuffix: "a", CreatedAt: now, UpdatedAt: now}
	credentialA := domainsemantic.InferenceCredential{ID: credentialAID, Key: "duplicate-key", Name: "a", ModelEndpointID: endpoint.ID, OwnerType: domainsemantic.CredentialOwnerUser, OwnerID: "owner-a", AuthType: domainsemantic.AuthModeAPIKey, SecretRef: secretAID, SecretSuffix: "a", Status: domainsemantic.CredentialStatusActive, CreatedAt: now, UpdatedAt: now}
	credentialBID := domainsemantic.InferenceCredentialID(uuid.New())
	secretBID := domainsemantic.SecretID(uuid.New())
	secretB := domainsemantic.Secret{ID: secretBID, OwnerType: domainsemantic.CredentialOwnerUser, OwnerID: "owner-b", Kind: domainsemantic.SecretKindInlineEncrypted, Ciphertext: &domainsemantic.EncryptedSecretPayload{Algorithm: "test", CipherB64: "b"}, SecretSuffix: "b", CreatedAt: now, UpdatedAt: now}
	credentialB := domainsemantic.InferenceCredential{ID: credentialBID, Key: credentialA.Key, Name: "b", ModelEndpointID: endpoint.ID, OwnerType: domainsemantic.CredentialOwnerUser, OwnerID: "owner-b", AuthType: domainsemantic.AuthModeAPIKey, SecretRef: secretBID, SecretSuffix: "b", Status: domainsemantic.CredentialStatusActive, CreatedAt: now, UpdatedAt: now}

	buildCommand := func(kind string, mutation any, commandID string) consensus.RaftCommand {
		t.Helper()
		record := semanticMutationRecord{Kind: kind, Payload: raw(mutation)}
		payload, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		command, err := source.buildSemanticGlobalRaftCommand(record, payload, commandID)
		if err != nil {
			t.Fatal(err)
		}
		return command
	}
	createACommand := buildCommand("credential.create", credentialCreateMutation{Credential: credentialA, Secret: &secretA}, "semantic-global-credential-create-a")
	createBCommand := buildCommand("credential.create", credentialCreateMutation{Credential: credentialB, Secret: &secretB}, "semantic-global-credential-create-b-rejected")
	deleteACommand := buildCommand("credential.delete_lifecycle", credentialDeleteMutation{CredentialID: credentialAID, ExpectedSecretRef: secretAID, DeleteSecret: true}, "semantic-global-credential-delete-a")

	sm := RaftStateMachine{Module: source, System: true, PartitionCount: 64}
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, createACommand); err != nil {
		t.Fatalf("create credential A: %v", err)
	}

	// Make completion-marker persistence fail after the deterministic conflict.
	// The domain rejection must become fatal until its marker can be persisted.
	markerPath := source.raftAppliedCommandsLogPath()
	markerBackup := markerPath + ".backup"
	if err := os.Rename(markerPath, markerBackup); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(markerPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 2, RaftTerm: 1}, createBCommand); !consensus.IsFatalApplyError(err) {
		t.Fatalf("rejected command marker failure=%v, want fatal apply error", err)
	}
	if source.raftCommandApplied(createBCommand.CommandID) {
		t.Fatal("rejected command recorded despite marker persistence failure")
	}
	if err := os.Remove(markerPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(markerBackup, markerPath); err != nil {
		t.Fatal(err)
	}
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 2, RaftTerm: 2, Replay: true}, createBCommand); !errors.Is(err, storesemantic.ErrConflict) || consensus.IsFatalApplyError(err) {
		t.Fatalf("replayed deterministic rejection=%v, want nonfatal conflict", err)
	}
	if !source.raftCommandApplied(createBCommand.CommandID) {
		t.Fatal("deterministic rejection command ID was not recorded")
	}
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 3, RaftTerm: 2}, deleteACommand); err != nil {
		t.Fatalf("delete credential A: %v", err)
	}

	restarted := newModule()
	replay := RaftStateMachine{Module: restarted, System: true, PartitionCount: 64}
	for index, command := range []consensus.RaftCommand{createACommand, createBCommand, deleteACommand} {
		if err := replay.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: uint64(index + 1), RaftTerm: 2, Replay: true}, command); err != nil {
			t.Fatalf("restart replay command %d: %v", index+1, err)
		}
	}
	credentials, err := restarted.globalBase.ListCredentials(ctx)
	if err != nil || len(credentials) != 0 {
		t.Fatalf("restart replay resurrected rejected credential: credentials=%+v err=%v", credentials, err)
	}
	secrets, err := restarted.globalBase.ListSecrets(ctx)
	if err != nil || len(secrets) != 0 {
		t.Fatalf("restart replay resurrected rejected secret: secrets=%+v err=%v", secrets, err)
	}
}

func TestTornRejectedCredentialMarkerRepairsAcrossTwoRestarts(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	newModule := func() *Module {
		module := NewModule()
		if result := module.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: dataDir}, LoggerValue: slog.Default()}); !result.OK {
			t.Fatalf("init failed: %v", result.Error)
		}
		module.raftPartitionCount = 64
		return module
	}
	source := newModule()
	endpoint, err := source.globalBase.UpsertModelEndpoint(ctx, domainsemantic.ModelEndpoint{Key: "api-torn", Name: "api-torn", ConnectorType: domainsemantic.ConnectorOpenAICompatible, EndpointURL: "https://example.invalid/v1", NetworkClass: domainsemantic.NetworkClassExternalHTTPS, PrivacyClass: domainsemantic.PrivacyClassThirdParty, AuthModes: []domainsemantic.AuthMode{domainsemantic.AuthModeAPIKey}, Operations: []domainsemantic.Operation{domainsemantic.OperationEmbeddings}, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(96, 0).UTC()
	credentialAID := domainsemantic.InferenceCredentialID(uuid.New())
	secretAID := domainsemantic.SecretID(uuid.New())
	secretA := domainsemantic.Secret{ID: secretAID, OwnerType: domainsemantic.CredentialOwnerUser, OwnerID: "owner-a", Kind: domainsemantic.SecretKindInlineEncrypted, Ciphertext: &domainsemantic.EncryptedSecretPayload{Algorithm: "test", CipherB64: "a"}, SecretSuffix: "a", CreatedAt: now, UpdatedAt: now}
	credentialA := domainsemantic.InferenceCredential{ID: credentialAID, Key: "torn-duplicate-key", Name: "a", ModelEndpointID: endpoint.ID, OwnerType: domainsemantic.CredentialOwnerUser, OwnerID: "owner-a", AuthType: domainsemantic.AuthModeAPIKey, SecretRef: secretAID, SecretSuffix: "a", Status: domainsemantic.CredentialStatusActive, CreatedAt: now, UpdatedAt: now}
	secretBID := domainsemantic.SecretID(uuid.New())
	secretB := domainsemantic.Secret{ID: secretBID, OwnerType: domainsemantic.CredentialOwnerUser, OwnerID: "owner-b", Kind: domainsemantic.SecretKindInlineEncrypted, Ciphertext: &domainsemantic.EncryptedSecretPayload{Algorithm: "test", CipherB64: "b"}, SecretSuffix: "b", CreatedAt: now, UpdatedAt: now}
	credentialB := domainsemantic.InferenceCredential{ID: domainsemantic.InferenceCredentialID(uuid.New()), Key: credentialA.Key, Name: "b", ModelEndpointID: endpoint.ID, OwnerType: domainsemantic.CredentialOwnerUser, OwnerID: "owner-b", AuthType: domainsemantic.AuthModeAPIKey, SecretRef: secretBID, SecretSuffix: "b", Status: domainsemantic.CredentialStatusActive, CreatedAt: now, UpdatedAt: now}
	buildCommand := func(kind string, mutation any, commandID string) consensus.RaftCommand {
		t.Helper()
		record := semanticMutationRecord{Kind: kind, Payload: raw(mutation)}
		payload, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		command, err := source.buildSemanticGlobalRaftCommand(record, payload, commandID)
		if err != nil {
			t.Fatal(err)
		}
		return command
	}
	createACommand := buildCommand("credential.create", credentialCreateMutation{Credential: credentialA, Secret: &secretA}, "semantic-global-torn-create-a")
	createBCommand := buildCommand("credential.create", credentialCreateMutation{Credential: credentialB, Secret: &secretB}, "semantic-global-torn-create-b-rejected")
	deleteACommand := buildCommand("credential.delete_lifecycle", credentialDeleteMutation{CredentialID: credentialAID, ExpectedSecretRef: secretAID, DeleteSecret: true}, "semantic-global-torn-delete-a")
	if err := (RaftStateMachine{Module: source, System: true, PartitionCount: 64}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, createACommand); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash after only a prefix of B's rejection marker reached disk.
	journal, err := os.OpenFile(source.raftAppliedCommandsLogPath(), os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := journal.WriteString(`{"command_id":"` + createBCommand.CommandID); err != nil {
		t.Fatal(err)
	}
	if err := journal.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}

	firstRestart := newModule()
	firstReplay := RaftStateMachine{Module: firstRestart, System: true, PartitionCount: 64}
	if err := firstReplay.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 2, Replay: true}, createACommand); err != nil {
		t.Fatalf("first restart create A replay: %v", err)
	}
	if err := firstReplay.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 2, RaftTerm: 2, Replay: true}, createBCommand); !errors.Is(err, storesemantic.ErrConflict) || consensus.IsFatalApplyError(err) {
		t.Fatalf("first restart rejected B replay=%v, want nonfatal conflict", err)
	}
	if err := firstReplay.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 3, RaftTerm: 2}, deleteACommand); err != nil {
		t.Fatalf("first restart delete A: %v", err)
	}

	secondRestart := newModule()
	secondReplay := RaftStateMachine{Module: secondRestart, System: true, PartitionCount: 64}
	for index, command := range []consensus.RaftCommand{createACommand, createBCommand, deleteACommand} {
		if err := secondReplay.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: uint64(index + 1), RaftTerm: 3, Replay: true}, command); err != nil {
			t.Fatalf("second restart replay command %d: %v", index+1, err)
		}
	}
	credentials, err := secondRestart.globalBase.ListCredentials(ctx)
	if err != nil || len(credentials) != 0 {
		t.Fatalf("torn marker replay resurrected credential B: credentials=%+v err=%v", credentials, err)
	}
	secrets, err := secondRestart.globalBase.ListSecrets(ctx)
	if err != nil || len(secrets) != 0 {
		t.Fatalf("torn marker replay resurrected secret B: secrets=%+v err=%v", secrets, err)
	}
}

func TestCredentialIntentRecoveryErrorIsFatalRaftApply(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	module := NewModule()
	if result := module.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: dataDir}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	intentPath := filepath.Join(dataDir, "meta", "inference", "lifecycle_intent.json")
	if err := os.WriteFile(intentPath, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	store := semanticVectorStore("recovery-error-must-be-fatal")
	record := semanticMutationRecord{Kind: "vector_store.upsert", Payload: raw(store)}
	payload, _ := json.Marshal(record)
	command, err := module.buildSemanticGlobalRaftCommand(record, payload, "semantic-global-recovery-error")
	if err != nil {
		t.Fatal(err)
	}
	err = (RaftStateMachine{Module: module, System: true, PartitionCount: 64}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, command)
	if !consensus.IsFatalApplyError(err) || !errors.Is(err, storesemantic.ErrInvalidInput) {
		t.Fatalf("recovery apply error=%v, want fatal invalid persisted intent", err)
	}
	if module.raftCommandApplied(command.CommandID) {
		t.Fatal("command was recorded despite failing during pre-apply recovery")
	}
}

type credentialProjectionOrderRecorder struct {
	InferenceProjection
	mu     sync.Mutex
	events []string
}

func (r *credentialProjectionOrderRecorder) DeleteDerivedCredential(ctx context.Context, id domaininference.CredentialID) error {
	r.mu.Lock()
	r.events = append(r.events, "credential:"+id.String())
	r.mu.Unlock()
	return r.InferenceProjection.DeleteDerivedCredential(ctx, id)
}

func (r *credentialProjectionOrderRecorder) DeleteDerivedSecret(ctx context.Context, id domaininference.SecretID) error {
	credentials, err := r.InferenceProjection.ListDerivedCredentials(ctx)
	if err != nil {
		return err
	}
	for _, credential := range credentials {
		if credential.SecretID == id {
			return fmt.Errorf("derived secret %s deleted before credential %s", id, credential.ID)
		}
	}
	r.mu.Lock()
	r.events = append(r.events, "secret:"+id.String())
	r.mu.Unlock()
	return r.InferenceProjection.DeleteDerivedSecret(ctx, id)
}

func (r *credentialProjectionOrderRecorder) snapshotEvents() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

func TestCredentialLifecycleRaftReplayAndSnapshotPreserveStatusAndSecretIdentity(t *testing.T) {
	ctx := context.Background()
	newModule := func(dataDir string) (*Module, *inferenceservice.Module, *credentialProjectionOrderRecorder) {
		host := &daemonruntime.Runtime{Config: config.Config{DataDir: dataDir}, LoggerValue: slog.Default()}
		inference := inferenceservice.NewModule()
		if result := inference.Init(ctx, host); !result.OK {
			t.Fatalf("init inference module: %v", result.Error)
		}
		recorder := &credentialProjectionOrderRecorder{InferenceProjection: inference}
		module := NewModule(Config{InferenceProjection: recorder})
		if result := module.Init(ctx, host); !result.OK {
			t.Fatalf("init failed: %v", result.Error)
		}
		module.raftPartitionCount = 64
		return module, inference, recorder
	}
	sourceDataDir := t.TempDir()
	source, inference, projectionRecorder := newModule(sourceDataDir)
	endpoint, err := source.globalBase.UpsertModelEndpoint(ctx, domainsemantic.ModelEndpoint{Key: "api", Name: "api", ConnectorType: domainsemantic.ConnectorOpenAICompatible, EndpointURL: "https://example.invalid/v1", NetworkClass: domainsemantic.NetworkClassExternalHTTPS, PrivacyClass: domainsemantic.PrivacyClassThirdParty, AuthModes: []domainsemantic.AuthMode{domainsemantic.AuthModeAPIKey}, Operations: []domainsemantic.Operation{domainsemantic.OperationEmbeddings}, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0).UTC()
	secretID := domainsemantic.SecretID(uuid.New())
	credentialID := domainsemantic.InferenceCredentialID(uuid.New())
	secret := domainsemantic.Secret{ID: secretID, OwnerType: domainsemantic.CredentialOwnerUser, OwnerID: "owner", Kind: domainsemantic.SecretKindInlineEncrypted, Ciphertext: &domainsemantic.EncryptedSecretPayload{Algorithm: "test", CipherB64: "old"}, SecretSuffix: "old", CreatedAt: now, UpdatedAt: now}
	credential := domainsemantic.InferenceCredential{ID: credentialID, Key: "api", Name: "api", ModelEndpointID: endpoint.ID, OwnerType: domainsemantic.CredentialOwnerUser, OwnerID: "owner", AuthType: domainsemantic.AuthModeAPIKey, SecretRef: secretID, SecretSuffix: "old", Status: domainsemantic.CredentialStatusActive, CreatedAt: now, UpdatedAt: now}
	if _, _, err := source.globalBase.(storesemantic.CredentialLifecycleManager).CreateCredentialLifecycle(ctx, credential, &secret); err != nil {
		t.Fatal(err)
	}
	if err := source.rebuildInferenceGlobalProjection(ctx); err != nil {
		t.Fatal(err)
	}

	statusMutation := credentialStatusMutation{CredentialID: credentialID, Status: domainsemantic.CredentialStatusRevoked, UpdatedAt: now.Add(time.Second)}
	statusRecord := semanticMutationRecord{Kind: "credential.set_status", Payload: raw(statusMutation)}
	statusPayload, _ := json.Marshal(statusRecord)
	statusCommand, err := source.buildSemanticGlobalRaftCommand(statusRecord, statusPayload, "credential-status-1")
	if err != nil {
		t.Fatal(err)
	}
	rotatedSecret := secret
	rotatedSecret.Ciphertext = &domainsemantic.EncryptedSecretPayload{Algorithm: "test", CipherB64: "new"}
	rotatedSecret.SecretSuffix = "new"
	rotatedSecret.UpdatedAt = now.Add(2 * time.Second)
	rotateMutation := credentialRotateMutation{CredentialID: credentialID, ExpectedSecretRef: secretID, Secret: rotatedSecret}
	rotateRecord := semanticMutationRecord{Kind: "credential.rotate", Payload: raw(rotateMutation)}
	rotatePayload, _ := json.Marshal(rotateRecord)
	rotateCommand, err := source.buildSemanticGlobalRaftCommand(rotateRecord, rotatePayload, "credential-rotate-1")
	if err != nil {
		t.Fatal(err)
	}
	sm := RaftStateMachine{Module: source, System: true, PartitionCount: 64}
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, statusCommand); err != nil {
		t.Fatal(err)
	}
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 2, RaftTerm: 1}, rotateCommand); err != nil {
		t.Fatal(err)
	}
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 3, RaftTerm: 2}, rotateCommand); err != nil {
		t.Fatalf("duplicate replay: %v", err)
	}
	assertCredentialLifecycle := func(module *Module, suffix, ciphertext string) {
		credentials, err := module.globalBase.ListCredentials(ctx)
		if err != nil || len(credentials) != 1 {
			t.Fatalf("credentials=%+v err=%v", credentials, err)
		}
		if credentials[0].Status != domainsemantic.CredentialStatusRevoked || credentials[0].SecretRef != secretID || credentials[0].SecretSuffix != suffix {
			t.Fatalf("credential lifecycle state=%+v", credentials[0])
		}
		secrets, err := module.globalBase.ListSecrets(ctx)
		if err != nil || len(secrets) != 1 || secrets[0].ID != secretID || secrets[0].Ciphertext.CipherB64 != ciphertext {
			t.Fatalf("secrets=%+v err=%v", secrets, err)
		}
	}
	assertCredentialLifecycle(source, "new", "new")

	persistenceInjector, ok := source.globalBase.(storesemantic.CredentialLifecyclePersistenceFaultInjector)
	if !ok {
		t.Fatal("semantic global manager does not expose persistence failure injection")
	}
	failIntent := true
	persistenceInjector.SetCredentialLifecyclePersistenceFaultInjector(func(_ string, path string) error {
		if failIntent && filepath.Base(path) == "lifecycle_intent.json" {
			failIntent = false
			return errors.New("injected raft lifecycle intent failure")
		}
		return nil
	})
	replayedSecret := rotatedSecret
	replayedSecret.Ciphertext = &domainsemantic.EncryptedSecretPayload{Algorithm: "test", CipherB64: "replayed"}
	replayedSecret.SecretSuffix = "replayed"
	replayMutation := credentialRotateMutation{CredentialID: credentialID, ExpectedSecretRef: secretID, Secret: replayedSecret}
	replayRecord := semanticMutationRecord{Kind: "credential.rotate", Payload: raw(replayMutation)}
	replayPayload, _ := json.Marshal(replayRecord)
	replayCommand, err := source.buildSemanticGlobalRaftCommand(replayRecord, replayPayload, "credential-rotate-intent-failure")
	if err != nil {
		t.Fatal(err)
	}
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 4, RaftTerm: 2}, replayCommand); !consensus.IsFatalApplyError(err) {
		t.Fatalf("intent-write apply error=%v, want fatal replay error", err)
	}
	persistenceInjector.SetCredentialLifecyclePersistenceFaultInjector(nil)
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 4, RaftTerm: 3, Replay: true}, replayCommand); err != nil {
		t.Fatalf("replayed intent-write command: %v", err)
	}
	credentialsAfterReplay, err := source.globalBase.ListCredentials(ctx)
	if err != nil || len(credentialsAfterReplay) != 1 || credentialsAfterReplay[0].SecretSuffix != "replayed" || credentialsAfterReplay[0].Status != domainsemantic.CredentialStatusRevoked {
		t.Fatalf("credentials after replay=%+v err=%v", credentialsAfterReplay, err)
	}

	deleteMutation := credentialDeleteMutation{CredentialID: credentialID, ExpectedSecretRef: secretID, DeleteSecret: true}
	deleteRecord := semanticMutationRecord{Kind: "credential.delete_lifecycle", Payload: raw(deleteMutation)}
	deletePayload, _ := json.Marshal(deleteRecord)
	deleteCommand, err := source.buildSemanticGlobalRaftCommand(deleteRecord, deletePayload, "credential-delete-1")
	if err != nil {
		t.Fatal(err)
	}
	credentialsDir := filepath.Join(sourceDataDir, "meta", "credentials")
	credentialsBackup := credentialsDir + ".backup"
	if err := os.Rename(credentialsDir, credentialsBackup); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(credentialsDir, []byte("block credential persistence"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 5, RaftTerm: 3}, deleteCommand); !consensus.IsFatalApplyError(err) {
		t.Fatalf("delete apply error=%v, want fatal persistence failure", err)
	}
	assertCredentialLifecycle(source, "replayed", "replayed")
	if _, err := sm.Snapshot(); err == nil {
		t.Fatal("snapshot error=nil while credential lifecycle intent cannot recover")
	}
	if err := os.Remove(credentialsDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(credentialsBackup, credentialsDir); err != nil {
		t.Fatal(err)
	}
	vectorStore := semanticVectorStore("credential-recovery-probe")
	probeRecord := semanticMutationRecord{Kind: "vector_store.upsert", Payload: raw(vectorStore)}
	probePayload, _ := json.Marshal(probeRecord)
	probeCommand, err := source.buildSemanticGlobalRaftCommand(probeRecord, probePayload, "credential-recovery-probe-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 6, RaftTerm: 4}, probeCommand); err != nil {
		t.Fatalf("later command did not recover pending lifecycle intent: %v", err)
	}
	credentials, err := source.globalBase.ListCredentials(ctx)
	if err != nil || len(credentials) != 0 {
		t.Fatalf("recovered delete credentials=%+v err=%v", credentials, err)
	}
	secrets, err := source.globalBase.ListSecrets(ctx)
	if err != nil || len(secrets) != 0 {
		t.Fatalf("recovered delete secrets=%+v err=%v", secrets, err)
	}
	derivedCredentials, err := inference.GlobalManager().ListCredentials(ctx)
	if err != nil || len(derivedCredentials) != 0 {
		t.Fatalf("recovered derived credentials=%+v err=%v", derivedCredentials, err)
	}
	derivedSecrets, err := inference.GlobalManager().ListSecrets(ctx)
	if err != nil || len(derivedSecrets) != 0 {
		t.Fatalf("recovered derived secrets=%+v err=%v", derivedSecrets, err)
	}
	projectionEvents := projectionRecorder.snapshotEvents()
	credentialDelete := "credential:" + credentialID.String()
	secretDelete := "secret:" + secretID.String()
	credentialDeleteIndex, secretDeleteIndex := -1, -1
	for index, event := range projectionEvents {
		if event == credentialDelete {
			credentialDeleteIndex = index
		}
		if event == secretDelete {
			secretDeleteIndex = index
		}
	}
	if credentialDeleteIndex < 0 || secretDeleteIndex < 0 || credentialDeleteIndex >= secretDeleteIndex {
		t.Fatalf("derived deletion order=%v, want credential before secret", projectionEvents)
	}

	snapshot, err := sm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored, _, _ := newModule(t.TempDir())
	if err := (RaftStateMachine{Module: restored, System: true, PartitionCount: 64}).RestoreSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	restoredCredentials, err := restored.globalBase.ListCredentials(ctx)
	if err != nil || len(restoredCredentials) != 0 {
		t.Fatalf("restored credentials=%+v err=%v", restoredCredentials, err)
	}
	restoredSecrets, err := restored.globalBase.ListSecrets(ctx)
	if err != nil || len(restoredSecrets) != 0 {
		t.Fatalf("restored secrets=%+v err=%v", restoredSecrets, err)
	}
}

func TestCredentialLifecycleWALFailureRecoversBeforeLaterProgress(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	wm, err := wal.Open(ctx, wal.Options{Dir: t.TempDir(), SegmentBytes: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer wm.Close()
	host := &daemonruntime.Runtime{Config: config.Config{DataDir: dataDir}, LoggerValue: slog.Default(), WAL: wm, WALRegistry: wal.NewRegistry(), WALWaiter: wal.NewApplyWaiter()}
	inference := inferenceservice.NewModule()
	if result := inference.Init(ctx, host); !result.OK {
		t.Fatalf("init inference module: %v", result.Error)
	}
	module := NewModule(Config{InferenceProjection: inference})
	if result := module.Init(ctx, host); !result.OK {
		t.Fatalf("init semantic module: %v", result.Error)
	}
	endpoint, err := module.GlobalManager().UpsertModelEndpoint(ctx, domainsemantic.ModelEndpoint{Key: "api", Name: "api", ConnectorType: domainsemantic.ConnectorOpenAICompatible, EndpointURL: "https://example.invalid/v1", NetworkClass: domainsemantic.NetworkClassExternalHTTPS, PrivacyClass: domainsemantic.PrivacyClassThirdParty, AuthModes: []domainsemantic.AuthMode{domainsemantic.AuthModeAPIKey}, Operations: []domainsemantic.Operation{domainsemantic.OperationEmbeddings}, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	created, err := module.CreateInferenceCredential(ctx, CredentialCreateInput{Credential: domainsemantic.InferenceCredential{Key: "api", Name: "api", ModelEndpointID: endpoint.ID, OwnerType: domainsemantic.CredentialOwnerUser, OwnerID: "owner", AuthType: domainsemantic.AuthModeAPIKey, Status: domainsemantic.CredentialStatusActive}, Secret: &domainsemantic.Secret{OwnerType: domainsemantic.CredentialOwnerUser, OwnerID: "owner", Kind: domainsemantic.SecretKindInlineEncrypted, Ciphertext: &domainsemantic.EncryptedSecretPayload{Algorithm: "test", CipherB64: "old"}, SecretSuffix: "old"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := module.SetInferenceCredentialStatus(ctx, CredentialStatusInput{CredentialID: created.Credential.ID, Status: domainsemantic.CredentialStatusRevoked}); err != nil {
		t.Fatal(err)
	}
	persistenceInjector, ok := module.globalBase.(storesemantic.CredentialLifecyclePersistenceFaultInjector)
	if !ok {
		t.Fatal("semantic global manager does not expose persistence failure injection")
	}
	failIntent := true
	persistenceInjector.SetCredentialLifecyclePersistenceFaultInjector(func(_ string, path string) error {
		if failIntent && filepath.Base(path) == "lifecycle_intent.json" {
			failIntent = false
			return errors.New("injected lifecycle intent persistence failure")
		}
		return nil
	})
	rotatedSecret := *created.Secret
	rotatedSecret.Ciphertext = &domainsemantic.EncryptedSecretPayload{Algorithm: "test", CipherB64: "new"}
	rotatedSecret.SecretSuffix = "new"
	_, rotateErr := module.RotateInferenceCredential(ctx, CredentialRotateInput{CredentialID: created.Credential.ID, ExpectedSecretRef: created.Credential.SecretRef, Secret: rotatedSecret})
	if rotateErr == nil {
		t.Fatal("RotateInferenceCredential() error=nil, want injected WAL apply failure")
	}
	if module.semanticWALFailure == nil || module.semanticWALFailure.Record.Type != recordTypeSemanticGlobal {
		t.Fatalf("failed lifecycle record was not retained: %+v", module.semanticWALFailure)
	}
	persistenceInjector.SetCredentialLifecyclePersistenceFaultInjector(nil)
	if _, err := module.GlobalManager().UpsertVectorStore(ctx, semanticVectorStore("wal-recovery-probe")); err != nil {
		t.Fatalf("later WAL mutation did not recover lifecycle intent: %v", err)
	}
	credentials, err := module.globalBase.ListCredentials(ctx)
	if err != nil || len(credentials) != 1 {
		t.Fatalf("credentials=%+v err=%v", credentials, err)
	}
	if credentials[0].Status != domainsemantic.CredentialStatusRevoked || credentials[0].SecretRef != created.Credential.SecretRef || credentials[0].SecretSuffix != "new" {
		t.Fatalf("recovered credential=%+v", credentials[0])
	}
	secrets, err := module.globalBase.ListSecrets(ctx)
	if err != nil || len(secrets) != 1 || secrets[0].Ciphertext.CipherB64 != "new" {
		t.Fatalf("recovered secrets=%+v err=%v", secrets, err)
	}
	derivedCredentials, err := inference.GlobalManager().ListCredentials(ctx)
	if err != nil || len(derivedCredentials) != 1 || string(derivedCredentials[0].Status) != string(domainsemantic.CredentialStatusRevoked) || derivedCredentials[0].SecretSuffix != "new" {
		t.Fatalf("recovered derived credentials=%+v err=%v", derivedCredentials, err)
	}

	failRotationAtIntent := func(suffix string) {
		t.Helper()
		failIntent := true
		persistenceInjector.SetCredentialLifecyclePersistenceFaultInjector(func(_ string, path string) error {
			if failIntent && filepath.Base(path) == "lifecycle_intent.json" {
				failIntent = false
				return errors.New("injected lifecycle intent persistence failure")
			}
			return nil
		})
		candidate := rotatedSecret
		candidate.Ciphertext = &domainsemantic.EncryptedSecretPayload{Algorithm: "test", CipherB64: suffix}
		candidate.SecretSuffix = suffix
		if _, err := module.RotateInferenceCredential(ctx, CredentialRotateInput{CredentialID: created.Credential.ID, ExpectedSecretRef: created.Credential.SecretRef, Secret: candidate}); err == nil {
			t.Fatalf("rotation %q error=nil, want injected intent failure", suffix)
		}
		persistenceInjector.SetCredentialLifecyclePersistenceFaultInjector(nil)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	failRotationAtIntent("space")
	spaceManager, err := module.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := spaceManager.UpsertSemanticIndex(ctx, semanticIndex(spaceID)); err != nil {
		t.Fatalf("space WAL mutation did not retry failed global record: %v", err)
	}
	failRotationAtIntent("maintenance")
	maintenanceManager, err := module.MaintenanceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	if err := maintenanceManager.SaveCheckpoint(ctx, storesemantic.MaintenanceCheckpoint{Consumer: "wal-recovery", SpaceID: spaceID, LastGraphRevision: 1}); err != nil {
		t.Fatalf("maintenance WAL mutation did not retry failed global record: %v", err)
	}
	failRotationAtIntent("timed")
	timedAppender, ok := maintenanceManager.(storesemantic.TimedGraphDirtyEventAppender)
	if !ok {
		t.Fatal("maintenance manager does not expose timed append")
	}
	if _, _, err := timedAppender.AppendGraphDirtyEventWithTiming(ctx, semanticDirtyEvent(spaceID)); err != nil {
		t.Fatalf("timed maintenance WAL mutation did not retry failed global record: %v", err)
	}
	credentials, err = module.globalBase.ListCredentials(ctx)
	if err != nil || len(credentials) != 1 || credentials[0].SecretSuffix != "timed" || credentials[0].Status != domainsemantic.CredentialStatusRevoked {
		t.Fatalf("final recovered credential=%+v err=%v", credentials, err)
	}
}

func TestSemanticGlobalUsesSystemRaftWhenEnabled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	wm, err := wal.Open(ctx, wal.Options{Dir: t.TempDir(), SegmentBytes: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	m := NewModule()
	host := &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default(), WAL: wm, WALRegistry: wal.NewRegistry(), WALWaiter: wal.NewApplyWaiter()}
	if result := m.Init(ctx, host); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	router := consensus.NewLocalMessageRouter()
	transport := consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { return router, true })}
	groups, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: 1, PeerNodeIDs: []consensus.NodeID{1}, PartitionCount: 4, Transport: transport, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return RaftStateMachine{Module: m, PartitionCount: 4} }, Partition: func(uint32) consensus.StateMachine { return &consensus.MemoryStateMachine{} }}, ElectionTick: 5, HeartbeatTick: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer groups.Stop()
	for _, g := range groups.Groups() {
		router.Register(g)
	}
	m.EnableExperimentalRaft(groups, 4)
	systemGroup, _ := groups.Group(consensus.SystemGroupID)
	if err := consensus.TickUntil(ctx, 10*time.Millisecond, groups.Tick, func() bool { return systemGroup.Leader() == 1 }); err != nil {
		t.Fatal(err)
	}
	vectorStore := semanticVectorStore("global-system-raft")
	vectorStore.ID = uuid.Nil
	storedVectorStore, err := m.GlobalManager().UpsertVectorStore(ctx, vectorStore)
	if err != nil {
		t.Fatalf("UpsertVectorStore() error = %v", err)
	}
	if storedVectorStore.ID == uuid.Nil || storedVectorStore.CreatedAt.IsZero() || storedVectorStore.UpdatedAt.IsZero() {
		t.Fatalf("UpsertVectorStore() returned non-canonical value: %+v", storedVectorStore)
	}
	stores, err := m.globalBase.ListVectorStores(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasVectorStoreKey(stores, vectorStore.Key) {
		t.Fatalf("vector stores=%#v want key %q", stores, vectorStore.Key)
	}
	secret, err := m.GlobalManager().UpsertSecret(ctx, domainsemantic.Secret{OwnerType: domainsemantic.CredentialOwnerSystem, OwnerID: "system", Kind: domainsemantic.SecretKindInlineEncrypted, Ciphertext: &domainsemantic.EncryptedSecretPayload{Algorithm: "MYCEL-ENVELOPE-V1", CipherB64: "cipher"}, SecretSuffix: "test"})
	if err != nil {
		t.Fatalf("UpsertSecret() error = %v", err)
	}
	if secret.ID == uuid.Nil || secret.CreatedAt.IsZero() || secret.UpdatedAt.IsZero() {
		t.Fatalf("UpsertSecret() returned non-canonical value: %+v", secret)
	}
	credential, err := m.GlobalManager().UpsertCredential(ctx, domainsemantic.InferenceCredential{Key: "test-credential", ModelEndpointID: uuid.New(), OwnerType: domainsemantic.CredentialOwnerSystem, OwnerID: "system", AuthType: domainsemantic.AuthModeAPIKey, SecretRef: secret.ID})
	if err != nil {
		t.Fatalf("UpsertCredential() error = %v", err)
	}
	if credential.ID == uuid.Nil || credential.SecretRef != secret.ID || credential.Status != domainsemantic.CredentialStatusActive || credential.CreatedAt.IsZero() || credential.UpdatedAt.IsZero() {
		t.Fatalf("UpsertCredential() returned non-canonical value: %+v", credential)
	}
}

func TestSemanticGlobalRaftFailsClosedWithoutSystemLeader(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	transport := consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { return nil, false })}
	groups, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: 1, PeerNodeIDs: []consensus.NodeID{1}, PartitionCount: 4, Transport: transport, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return RaftStateMachine{Module: m, PartitionCount: 4} }, Partition: func(uint32) consensus.StateMachine { return &consensus.MemoryStateMachine{} }}, ElectionTick: 50, HeartbeatTick: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer groups.Stop()
	m.EnableExperimentalRaft(groups, 4)
	rec := semanticMutationRecord{Kind: "vector_store.upsert", Payload: raw(semanticVectorStore("no-leader"))}
	if err := m.commitSemanticMutation(ctx, recordTypeSemanticGlobal, rec); status.Code(err) != codes.Unavailable {
		t.Fatalf("commitSemanticMutation() error = %v, want Unavailable", err)
	}
}

func TestSemanticRaftStateMachineAppliesMaintenanceMutation(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	event := semanticDirtyEvent(spaceID)
	rec := maintenanceMutationRecord{Kind: "dirty_event.append", SpaceID: spaceID, Payload: raw(event)}
	payload, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	m.EnableExperimentalRaft(nil, 64)
	cmd, err := m.buildSemanticMaintenanceRaftCommand(rec, payload, "semantic-maintenance-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := (RaftStateMachine{Module: m, PartitionCount: 64}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, cmd); err != nil {
		t.Fatalf("ApplyCommand() error = %v", err)
	}
	mgr, err := m.MaintenanceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	events, err := mgr.ListGraphDirtyEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].ID != event.ID {
		t.Fatalf("events=%#v want %s", events, event.ID)
	}
}

func TestSemanticRaftStateMachineMissingWorkCompleteIsIdempotent(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	workID := uuid.New()
	rec := maintenanceMutationRecord{Kind: "work.complete", SpaceID: spaceID, Payload: raw(struct {
		ID     uuid.UUID                `json:"id"`
		Result storesemantic.WorkResult `json:"result"`
	}{ID: workID})}
	payload, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	m.EnableExperimentalRaft(nil, 64)
	cmd, err := m.buildSemanticMaintenanceRaftCommand(rec, payload, "semantic-maintenance-missing-work-complete")
	if err != nil {
		t.Fatal(err)
	}
	if err := (RaftStateMachine{Module: m, PartitionCount: 64}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, cmd); err != nil {
		t.Fatalf("ApplyCommand() error = %v, want nil", err)
	}
}

func TestSemanticRaftStateMachineMissingWorkFailIsIdempotent(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	workID := uuid.New()
	rec := maintenanceMutationRecord{Kind: "work.fail", SpaceID: spaceID, Payload: raw(struct {
		ID      uuid.UUID                 `json:"id"`
		Failure storesemantic.WorkFailure `json:"failure"`
	}{ID: workID, Failure: storesemantic.WorkFailure{Category: "stale", Message: "missing"}})}
	payload, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	m.EnableExperimentalRaft(nil, 64)
	cmd, err := m.buildSemanticMaintenanceRaftCommand(rec, payload, "semantic-maintenance-missing-work-fail")
	if err != nil {
		t.Fatal(err)
	}
	if err := (RaftStateMachine{Module: m, PartitionCount: 64}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, cmd); err != nil {
		t.Fatalf("ApplyCommand() error = %v, want nil", err)
	}
}

func TestSemanticMaintenanceManagerUsesRaftWhenEnabled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	router := consensus.NewLocalMessageRouter()
	transport := consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { return router, true })}
	groups, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: 1, PeerNodeIDs: []consensus.NodeID{1}, PartitionCount: 4, Transport: transport, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return consensus.NewSystemStateMachine() }, Partition: func(uint32) consensus.StateMachine { return RaftStateMachine{Module: m, PartitionCount: 4} }}, ElectionTick: 5, HeartbeatTick: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer groups.Stop()
	for _, g := range groups.Groups() {
		router.Register(g)
	}
	m.EnableExperimentalRaft(groups, 4)
	spaceID := domainspace.SpaceID(uuid.New())
	probe, _ := m.buildSemanticMaintenanceRaftCommand(maintenanceMutationRecord{Kind: "dirty_event.append", SpaceID: spaceID}, []byte(`{}`), "maintenance-probe")
	group, _ := groups.Group(consensus.PartitionGroupID(probe.PartitionID))
	if err := consensus.TickUntil(ctx, 10*time.Millisecond, groups.Tick, func() bool { return group.Leader() == 1 }); err != nil {
		t.Fatal(err)
	}
	mgr, err := m.MaintenanceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	event := semanticDirtyEvent(spaceID)
	if _, err := mgr.AppendGraphDirtyEvent(ctx, event); err != nil {
		t.Fatalf("AppendGraphDirtyEvent() error = %v", err)
	}
	mgr, err = m.MaintenanceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	events, err := mgr.ListGraphDirtyEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].ID != event.ID {
		t.Fatalf("events=%#v want %s", events, event.ID)
	}
}

func TestExecuteLocalRaftSemanticReadSpaceAndMaintenance(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	idx := semanticIndex(spaceID)
	mgr, err := m.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.UpsertSemanticIndex(ctx, idx); err != nil {
		t.Fatal(err)
	}
	event := semanticDirtyEvent(spaceID)
	mm, err := m.MaintenanceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mm.AppendGraphDirtyEvent(ctx, event); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(raftSemanticReadRequest{Op: "list_indexes", SpaceID: spaceID})
	resPayload, err := m.ExecuteLocalRaftSemanticRead(ctx, spaceID.String(), payload)
	if err != nil {
		t.Fatalf("ExecuteLocalRaftSemanticRead(list_indexes) error = %v", err)
	}
	var indexes raftSemanticIndexesResponse
	if err := json.Unmarshal(resPayload, &indexes); err != nil {
		t.Fatal(err)
	}
	if len(indexes.Indexes) != 1 || indexes.Indexes[0].ID != idx.ID {
		t.Fatalf("indexes=%#v want %s", indexes.Indexes, idx.ID)
	}
	payload, _ = json.Marshal(raftSemanticReadRequest{Op: "list_dirty_events", SpaceID: spaceID})
	resPayload, err = m.ExecuteLocalRaftSemanticRead(ctx, spaceID.String(), payload)
	if err != nil {
		t.Fatalf("ExecuteLocalRaftSemanticRead(list_dirty_events) error = %v", err)
	}
	var events raftSemanticDirtyEventsResponse
	if err := json.Unmarshal(resPayload, &events); err != nil {
		t.Fatal(err)
	}
	if len(events.Events) != 1 || events.Events[0].ID != event.ID {
		t.Fatalf("events=%#v want %s", events.Events, event.ID)
	}
}

func TestSemanticRaftDedupeSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	spaceID := domainspace.SpaceID(uuid.New())
	idx := semanticIndex(spaceID)
	rec := semanticMutationRecord{Kind: "semantic_index.upsert", SpaceID: spaceID, Payload: raw(idx)}
	payload, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	m1 := NewModule()
	if result := m1.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: dataDir}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init first module failed: %v", result.Error)
	}
	m1.EnableExperimentalRaft(nil, 64)
	cmd, err := m1.buildSemanticSpaceRaftCommand(rec, payload, "semantic-durable-command")
	if err != nil {
		t.Fatal(err)
	}
	if err := (RaftStateMachine{Module: m1, PartitionCount: 64}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, cmd); err != nil {
		t.Fatalf("first ApplyCommand() error = %v", err)
	}
	m2 := NewModule()
	if result := m2.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: dataDir}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init second module failed: %v", result.Error)
	}
	m2.EnableExperimentalRaft(nil, 64)
	if err := (RaftStateMachine{Module: m2, PartitionCount: 64}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 2, RaftTerm: 1}, cmd); err != nil {
		t.Fatalf("duplicate ApplyCommand() after restart error = %v", err)
	}
	mgr, err := m2.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	indexes, err := mgr.ListSemanticIndexes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(indexes) != 1 || indexes[0].ID != idx.ID {
		t.Fatalf("indexes=%#v want %s", indexes, idx.ID)
	}
}

func TestSemanticRaftRejectsSpaceMismatch(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	rec := semanticMutationRecord{Kind: "semantic_index.upsert", SpaceID: spaceID, Payload: raw(semanticIndex(spaceID))}
	payload, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	m.EnableExperimentalRaft(nil, 64)
	cmd, err := m.buildSemanticSpaceRaftCommand(rec, payload, "semantic-mismatch")
	if err != nil {
		t.Fatal(err)
	}
	rec.SpaceID = domainspace.SpaceID(uuid.New())
	cmd.Payload, err = json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	if err := (RaftStateMachine{Module: m, PartitionCount: 64}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, cmd); err == nil {
		t.Fatal("ApplyCommand() error = nil, want space mismatch error")
	}
}

func TestSemanticRaftSpaceMutationReplicatesAcrossThreeNodes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	peers := []consensus.NodeID{1, 2, 3}
	partitionCount := uint32(4)
	modules := map[consensus.NodeID]*Module{}
	routers := map[consensus.NodeID]*consensus.LocalMessageRouter{1: consensus.NewLocalMessageRouter(), 2: consensus.NewLocalMessageRouter(), 3: consensus.NewLocalMessageRouter()}
	transport := consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { r, ok := routers[nodeID]; return r, ok })}
	groupsByNode := map[consensus.NodeID]*consensus.MultiGroup{}
	for _, nodeID := range peers {
		m := NewModule()
		if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
			t.Fatalf("init node %d failed: %v", nodeID, result.Error)
		}
		modules[nodeID] = m
		mg, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: nodeID, PeerNodeIDs: peers, PartitionCount: partitionCount, Transport: transport, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return consensus.NewSystemStateMachine() }, Partition: func(uint32) consensus.StateMachine {
			return RaftStateMachine{Module: m, PartitionCount: partitionCount}
		}}, ElectionTick: 5, HeartbeatTick: 1})
		if err != nil {
			t.Fatalf("StartMultiGroup(%d) error = %v", nodeID, err)
		}
		groupsByNode[nodeID] = mg
		m.EnableExperimentalRaft(mg, partitionCount)
		for _, g := range mg.Groups() {
			for _, router := range routers {
				router.Register(g)
			}
		}
	}
	defer func() {
		for _, mg := range groupsByNode {
			mg.Stop()
		}
	}()
	stopTick := make(chan struct{})
	defer close(stopTick)
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopTick:
				return
			case <-ticker.C:
				for _, mg := range groupsByNode {
					mg.Tick()
				}
			}
		}
	}()
	spaceID := domainspace.SpaceID(uuid.New())
	probe, err := modules[1].buildSemanticSpaceRaftCommand(semanticMutationRecord{Kind: "semantic_index.upsert", SpaceID: spaceID}, []byte(`{}`), "probe")
	if err != nil {
		t.Fatal(err)
	}
	if err := consensus.TickUntil(ctx, 20*time.Millisecond, func() {
		for _, mg := range groupsByNode {
			mg.Tick()
		}
	}, func() bool {
		leaders := map[consensus.NodeID]int{}
		for _, mg := range groupsByNode {
			if g, ok := mg.Group(consensus.PartitionGroupID(probe.PartitionID)); ok && g.Leader() != 0 {
				leaders[g.Leader()]++
			}
		}
		for _, count := range leaders {
			if count >= 2 {
				return true
			}
		}
		return false
	}); err != nil {
		t.Fatalf("leader not elected: %v", err)
	}
	mgr, err := modules[1].SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	idx := semanticIndex(spaceID)
	if _, err := mgr.UpsertSemanticIndex(ctx, idx); err != nil {
		t.Fatalf("UpsertSemanticIndex() error = %v", err)
	}
	for nodeID, m := range modules {
		if err := consensus.WaitUntil(ctx, 20*time.Millisecond, func() bool {
			mgr, err := m.SpaceManager(ctx, spaceID)
			if err != nil {
				return false
			}
			indexes, err := mgr.ListSemanticIndexes(ctx)
			return err == nil && len(indexes) == 1 && indexes[0].ID == idx.ID
		}); err != nil {
			t.Fatalf("node %d did not apply semantic index: %v", nodeID, err)
		}
	}
}

func TestSemanticRaftVectorRecordsForwardToPartitionLeader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	peers := []consensus.NodeID{1, 2, 3}
	partitionCount := uint32(4)
	modules := map[consensus.NodeID]*Module{}
	routers := map[consensus.NodeID]*consensus.LocalMessageRouter{1: consensus.NewLocalMessageRouter(), 2: consensus.NewLocalMessageRouter(), 3: consensus.NewLocalMessageRouter()}
	transport := consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { r, ok := routers[nodeID]; return r, ok })}
	groupsByNode := map[consensus.NodeID]*consensus.MultiGroup{}
	for _, nodeID := range peers {
		m := NewModule()
		if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
			t.Fatalf("init node %d failed: %v", nodeID, result.Error)
		}
		modules[nodeID] = m
		mg, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: nodeID, PeerNodeIDs: peers, PartitionCount: partitionCount, Transport: transport, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return consensus.NewSystemStateMachine() }, Partition: func(uint32) consensus.StateMachine {
			return RaftStateMachine{Module: m, PartitionCount: partitionCount}
		}}, ElectionTick: 5, HeartbeatTick: 1})
		if err != nil {
			t.Fatalf("StartMultiGroup(%d) error = %v", nodeID, err)
		}
		groupsByNode[nodeID] = mg
		m.EnableExperimentalRaft(mg, partitionCount)
		for _, g := range mg.Groups() {
			for _, router := range routers {
				router.Register(g)
			}
		}
	}
	defer func() {
		for _, mg := range groupsByNode {
			mg.Stop()
		}
	}()
	servers := map[consensus.NodeID]*grpc.Server{}
	addrs := make([]string, len(peers))
	for _, nodeID := range peers {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		srv := grpc.NewServer()
		backend := clusterbackend.NewService(clustermodel.NodeIdentity{}, clustermodel.NodeStateClustered, nil)
		backend.SemanticReader = modules[nodeID]
		clusterpb.RegisterClusterBackendServiceServer(srv, backend)
		servers[nodeID] = srv
		addrs[int(nodeID)-1] = lis.Addr().String()
		go func() { _ = srv.Serve(lis) }()
	}
	defer func() {
		for _, srv := range servers {
			srv.Stop()
		}
	}()
	for _, nodeID := range peers {
		modules[nodeID].EnableExperimentalRaftNetworking(nodeID, addrs, "")
	}
	stopTick := make(chan struct{})
	defer close(stopTick)
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopTick:
				return
			case <-ticker.C:
				for _, mg := range groupsByNode {
					mg.Tick()
				}
			}
		}
	}()
	spaceID := domainspace.SpaceID(uuid.New())
	probe, err := modules[1].buildSemanticSpaceRaftCommand(semanticMutationRecord{Kind: "semantic_index.upsert", SpaceID: spaceID}, []byte(`{}`), "probe")
	if err != nil {
		t.Fatal(err)
	}
	leaderForPartition := func() consensus.NodeID {
		leaders := map[consensus.NodeID]int{}
		for _, mg := range groupsByNode {
			if g, ok := mg.Group(consensus.PartitionGroupID(probe.PartitionID)); ok && g.Leader() != 0 {
				leaders[g.Leader()]++
			}
		}
		for leader, count := range leaders {
			if count >= 2 {
				return leader
			}
		}
		return 0
	}
	if err := consensus.TickUntil(ctx, 20*time.Millisecond, func() {
		for _, mg := range groupsByNode {
			mg.Tick()
		}
	}, func() bool { return leaderForPartition() != 0 }); err != nil {
		t.Fatalf("leader not elected: %v", err)
	}
	leaderID := leaderForPartition()
	followerID := consensus.NodeID(1)
	if followerID == leaderID {
		followerID = 2
	}
	idx := semanticIndex(spaceID)
	mgr, err := modules[leaderID].SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.UpsertSemanticIndex(ctx, idx); err != nil {
		t.Fatalf("UpsertSemanticIndex() error = %v", err)
	}
	rec, err := modules[leaderID].localVectorBackend().Upsert(ctx, domainsemantic.AdvancedEmbeddingRecord{SpaceID: spaceID, DomainID: idx.DomainID, SemanticIndexID: idx.ID, NodeID: graphmodel.NodeID(uuid.New()), SourceHash: "sha256:test", SourceMode: "self", ModelEndpointID: idx.ModelEndpointID, ModelID: idx.ModelID, VectorStoreID: idx.VectorStoreID, VectorSpaceKey: "test/3", Dimensions: 3, Vector: []float64{1, 0, 0}, CreatedAt: time.Now().UTC()})
	if err != nil {
		t.Fatalf("leader vector upsert: %v", err)
	}
	local, err := modules[followerID].localVectorBackend().ListRecords(ctx, spaceID, idx.ID)
	if err != nil {
		t.Fatalf("follower local vector list: %v", err)
	}
	if len(local) != 0 {
		t.Fatalf("follower unexpectedly had local vector records: %#v", local)
	}
	forwarded, err := modules[followerID].ListVectorRecords(ctx, spaceID, idx.ID)
	if err != nil {
		t.Fatalf("follower forwarded vector list: %v", err)
	}
	if len(forwarded) != 1 || forwarded[0].ID != rec.ID {
		t.Fatalf("forwarded records=%#v want %s from leader %d via follower %d", forwarded, rec.ID, leaderID, followerID)
	}
}

func TestSemanticRaftSpaceMutationSurvivesLeaderFailover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	peers := []consensus.NodeID{1, 2, 3}
	partitionCount := uint32(4)
	modules := map[consensus.NodeID]*Module{}
	routers := map[consensus.NodeID]*consensus.LocalMessageRouter{1: consensus.NewLocalMessageRouter(), 2: consensus.NewLocalMessageRouter(), 3: consensus.NewLocalMessageRouter()}
	transport := consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { r, ok := routers[nodeID]; return r, ok })}
	groupsByNode := map[consensus.NodeID]*consensus.MultiGroup{}
	for _, nodeID := range peers {
		m := NewModule()
		if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
			t.Fatalf("init node %d failed: %v", nodeID, result.Error)
		}
		modules[nodeID] = m
		mg, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: nodeID, PeerNodeIDs: peers, PartitionCount: partitionCount, Transport: transport, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return consensus.NewSystemStateMachine() }, Partition: func(uint32) consensus.StateMachine {
			return RaftStateMachine{Module: m, PartitionCount: partitionCount}
		}}, ElectionTick: 5, HeartbeatTick: 1})
		if err != nil {
			t.Fatalf("StartMultiGroup(%d) error = %v", nodeID, err)
		}
		groupsByNode[nodeID] = mg
		m.EnableExperimentalRaft(mg, partitionCount)
		for _, g := range mg.Groups() {
			for _, router := range routers {
				router.Register(g)
			}
		}
	}
	defer func() {
		for _, mg := range groupsByNode {
			mg.Stop()
		}
	}()
	active := map[consensus.NodeID]bool{1: true, 2: true, 3: true}
	var activeMu sync.RWMutex
	isActive := func(id consensus.NodeID) bool {
		activeMu.RLock()
		defer activeMu.RUnlock()
		return active[id]
	}
	tickActive := func() {
		for id, mg := range groupsByNode {
			if isActive(id) {
				mg.Tick()
			}
		}
	}
	stopTick := make(chan struct{})
	defer close(stopTick)
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopTick:
				return
			case <-ticker.C:
				tickActive()
			}
		}
	}()
	spaceID := domainspace.SpaceID(uuid.New())
	probe, err := modules[1].buildSemanticSpaceRaftCommand(semanticMutationRecord{Kind: "semantic_index.upsert", SpaceID: spaceID}, []byte(`{}`), "failover-probe")
	if err != nil {
		t.Fatal(err)
	}
	leaderForPartition := func() consensus.NodeID {
		counts := map[consensus.NodeID]int{}
		for id, mg := range groupsByNode {
			if !isActive(id) {
				continue
			}
			if g, ok := mg.Group(consensus.PartitionGroupID(probe.PartitionID)); ok && g.Leader() != 0 {
				counts[g.Leader()]++
			}
		}
		for leader, count := range counts {
			if isActive(leader) && count >= 2 {
				return leader
			}
		}
		return 0
	}
	if err := consensus.TickUntil(ctx, 20*time.Millisecond, tickActive, func() bool { return leaderForPartition() != 0 }); err != nil {
		t.Fatalf("initial leader not elected: %v", err)
	}
	oldLeader := leaderForPartition()
	activeMu.Lock()
	active[oldLeader] = false
	activeMu.Unlock()
	groupsByNode[oldLeader].Stop()
	for _, router := range routers {
		router.UnregisterNode(oldLeader)
	}
	if err := consensus.TickUntil(ctx, 20*time.Millisecond, tickActive, func() bool { l := leaderForPartition(); return l != 0 && l != oldLeader }); err != nil {
		t.Fatalf("new leader not elected after stopping %d: %v", oldLeader, err)
	}
	writer := consensus.NodeID(1)
	if writer == oldLeader {
		writer = 2
	}
	mgr, err := modules[writer].SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	idx := semanticIndex(spaceID)
	if _, err := mgr.UpsertSemanticIndex(ctx, idx); err != nil {
		t.Fatalf("UpsertSemanticIndex() after failover error = %v", err)
	}
	for nodeID, m := range modules {
		if !isActive(nodeID) {
			continue
		}
		if err := consensus.WaitUntil(ctx, 20*time.Millisecond, func() bool {
			mgr, err := m.SpaceManager(ctx, spaceID)
			if err != nil {
				return false
			}
			indexes, err := mgr.ListSemanticIndexes(ctx)
			return err == nil && len(indexes) == 1 && indexes[0].ID == idx.ID
		}); err != nil {
			t.Fatalf("node %d did not apply post-failover semantic index: %v", nodeID, err)
		}
	}
}

func TestSemanticRaftStateMachineAppliesSpaceMutation(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	idx := semanticIndex(spaceID)
	rec := semanticMutationRecord{Kind: "semantic_index.upsert", SpaceID: spaceID, Payload: raw(idx)}
	payload, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	m.EnableExperimentalRaft(nil, 64)
	cmd, err := m.buildSemanticSpaceRaftCommand(rec, payload, "semantic-1")
	if err != nil {
		t.Fatalf("buildSemanticSpaceRaftCommand() error = %v", err)
	}
	if err := (RaftStateMachine{Module: m, PartitionCount: 64}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, cmd); err != nil {
		t.Fatalf("ApplyCommand() error = %v", err)
	}
	mgr, err := m.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	indexes, err := mgr.ListSemanticIndexes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(indexes) != 1 || indexes[0].ID != idx.ID {
		t.Fatalf("indexes=%#v want %s", indexes, idx.ID)
	}
}

func TestSemanticSpaceManagerUsesRaftWhenEnabled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	router := consensus.NewLocalMessageRouter()
	transport := consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { return router, true })}
	groups, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: 1, PeerNodeIDs: []consensus.NodeID{1}, PartitionCount: 4, Transport: transport, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return consensus.NewSystemStateMachine() }, Partition: func(uint32) consensus.StateMachine { return RaftStateMachine{Module: m, PartitionCount: 4} }}, ElectionTick: 5, HeartbeatTick: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer groups.Stop()
	for _, g := range groups.Groups() {
		router.Register(g)
	}
	m.EnableExperimentalRaft(groups, 4)
	spaceID := domainspace.SpaceID(uuid.New())
	probe, _ := m.buildSemanticSpaceRaftCommand(semanticMutationRecord{Kind: "semantic_index.upsert", SpaceID: spaceID}, []byte(`{}`), "probe")
	group, _ := groups.Group(consensus.PartitionGroupID(probe.PartitionID))
	if err := consensus.TickUntil(ctx, 10*time.Millisecond, groups.Tick, func() bool { return group.Leader() == 1 }); err != nil {
		t.Fatal(err)
	}
	mgr, err := m.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	idx := semanticIndex(spaceID)
	idx.ID = uuid.Nil
	storedIndex, err := mgr.UpsertSemanticIndex(ctx, idx)
	if err != nil {
		t.Fatalf("UpsertSemanticIndex() error = %v", err)
	}
	if storedIndex.ID == uuid.Nil || storedIndex.CreatedAt.IsZero() || storedIndex.UpdatedAt.IsZero() {
		t.Fatalf("UpsertSemanticIndex() returned non-canonical value: %+v", storedIndex)
	}
	mgr, err = m.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	indexes, err := mgr.ListSemanticIndexes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(indexes) != 1 || indexes[0].ID != storedIndex.ID {
		t.Fatalf("indexes=%#v want %s", indexes, storedIndex.ID)
	}
}

func semanticIndex(spaceID domainspace.SpaceID) domainsemantic.SemanticIndex {
	now := time.Now().UTC()
	return domainsemantic.SemanticIndex{ID: domainsemantic.SemanticIndexID(uuid.New()), SpaceID: spaceID, DomainID: graphmodel.DomainID(uuid.New()), Key: "idx", Name: "Index", Purpose: domainsemantic.SemanticIndexPurposeSearch, SourcePolicy: domainsemantic.SemanticSourcePolicy{Extraction: domainsemantic.SourceExtractionSelf}, ModelEndpointID: uuid.New(), ModelID: uuid.New(), VectorStoreID: uuid.New(), Enabled: true, CreatedAt: now, UpdatedAt: now}
}

func semanticDirtyEvent(spaceID domainspace.SpaceID) domainsemantic.GraphDirtyEvent {
	return domainsemantic.GraphDirtyEvent{ID: domainsemantic.GraphDirtyEventID(uuid.New()), TxnID: uuid.New(), GraphRevision: 1, SpaceID: spaceID, DomainIDs: []graphmodel.DomainID{graphmodel.DomainID(uuid.New())}, CommittedAt: time.Now().UTC()}
}

func semanticVectorStore(key string) domainsemantic.VectorStoreBackend {
	now := time.Now().UTC()
	return domainsemantic.VectorStoreBackend{ID: domainsemantic.VectorStoreID(uuid.New()), Key: key, Name: key, Type: domainsemantic.VectorStoreMycelFile, Config: map[string]any{}, PrivacyClass: domainsemantic.PrivacyClassLocalOnly, Enabled: true, CreatedAt: now, UpdatedAt: now}
}

func hasVectorStoreKey(stores []domainsemantic.VectorStoreBackend, key string) bool {
	for _, store := range stores {
		if store.Key == key {
			return true
		}
	}
	return false
}
