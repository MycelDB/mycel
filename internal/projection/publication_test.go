package projection

import (
	"context"
	"errors"
	"testing"
)

type fakeArtifactPublication struct {
	validated   bool
	active      bool
	aborted     bool
	validateErr error
	activateErr error
	rollbackErr error
}

func (f *fakeArtifactPublication) Validate(context.Context, ArtifactManifest) error {
	if f.validateErr != nil {
		return f.validateErr
	}
	f.validated = true
	return nil
}

func (f *fakeArtifactPublication) Activate(context.Context) (func(context.Context) error, error) {
	if f.activateErr != nil {
		return nil, f.activateErr
	}
	f.active = true
	return func(context.Context) error {
		f.active = false
		return f.rollbackErr
	}, nil
}

func (f *fakeArtifactPublication) Abort(context.Context) error {
	f.aborted = true
	return nil
}

type fakeCheckpointCAS struct {
	checkpoint Checkpoint
	expected   CheckpointExpectation
	err        error
	called     bool
}

func (f *fakeCheckpointCAS) PublishCheckpoint(_ context.Context, checkpoint Checkpoint, expected CheckpointExpectation) error {
	f.called = true
	if f.err != nil {
		return f.err
	}
	f.checkpoint = checkpoint
	f.expected = expected
	return nil
}

func testManifest() ArtifactManifest {
	return ArtifactManifest{ProjectionID: "prj_rules", Kind: KindSemantic, Scope: Scope{SpaceID: "space-a", DomainID: "domain-a"}, ConfigurationHash: "config-v1", ConfigurationEpoch: 1, MaterializerVersion: "semantic-v1", Source: Frontier{Revision: 9, Cursor: "cursor-9"}, FencingEpoch: 3, Generation: "artifact-9", Format: "semantic-test-v1", Checksum: "checksum-9", Bytes: 128}
}

func TestArtifactManifestMustMatchDefinition(t *testing.T) {
	manifest := testManifest()
	definition := Definition{ID: manifest.ProjectionID, Kind: manifest.Kind, Scope: manifest.Scope, ConfigurationHash: manifest.ConfigurationHash, ConfigurationEpoch: manifest.ConfigurationEpoch, MaterializerVersion: manifest.MaterializerVersion, Key: "rules"}
	if err := manifest.ValidateAgainstDefinition(definition); err != nil {
		t.Fatalf("ValidateAgainstDefinition() error = %v", err)
	}
	manifest.ConfigurationEpoch++
	if err := manifest.ValidateAgainstDefinition(definition); err == nil {
		t.Fatal("mismatched manifest unexpectedly validated")
	}
}

func TestPublishArtifactKeepsActivationOnSuccessfulCAS(t *testing.T) {
	artifact := &fakeArtifactPublication{}
	checkpoints := &fakeCheckpointCAS{}
	manifest := testManifest()
	expected := CheckpointExpectation{ProjectionID: manifest.ProjectionID}
	if err := PublishArtifact(context.Background(), PublicationCandidate{Manifest: manifest, State: StateReady, Artifact: artifact}, expected, checkpoints); err != nil {
		t.Fatalf("PublishArtifact() error = %v", err)
	}
	if !artifact.validated || !artifact.active || artifact.aborted || !checkpoints.called {
		t.Fatalf("publication state = validated:%t active:%t aborted:%t called:%t", artifact.validated, artifact.active, artifact.aborted, checkpoints.called)
	}
	if checkpoints.checkpoint.ArtifactGeneration != manifest.Generation || checkpoints.checkpoint.Published != manifest.Source {
		t.Fatalf("published checkpoint = %+v", checkpoints.checkpoint)
	}
}

func TestPublishArtifactRollsBackWhenCASLosesRace(t *testing.T) {
	artifact := &fakeArtifactPublication{}
	checkpoints := &fakeCheckpointCAS{err: ErrCheckpointConflict}
	manifest := testManifest()
	err := PublishArtifact(context.Background(), PublicationCandidate{Manifest: manifest, State: StateReady, Artifact: artifact}, CheckpointExpectation{ProjectionID: manifest.ProjectionID}, checkpoints)
	if err == nil || !errors.Is(err, ErrCheckpointConflict) {
		t.Fatalf("PublishArtifact() error = %v, want checkpoint conflict", err)
	}
	if artifact.active || !artifact.aborted {
		t.Fatalf("failed publication did not roll back/abort: active=%t aborted=%t", artifact.active, artifact.aborted)
	}
}

func TestPublishArtifactDoesNotPublishAfterActivationFailure(t *testing.T) {
	artifact := &fakeArtifactPublication{activateErr: errors.New("activation failed")}
	checkpoints := &fakeCheckpointCAS{}
	manifest := testManifest()
	if err := PublishArtifact(context.Background(), PublicationCandidate{Manifest: manifest, State: StateReady, Artifact: artifact}, CheckpointExpectation{ProjectionID: manifest.ProjectionID}, checkpoints); err == nil {
		t.Fatal("activation failure unexpectedly succeeded")
	}
	if checkpoints.called || !artifact.aborted {
		t.Fatalf("activation failure state = called:%t aborted:%t", checkpoints.called, artifact.aborted)
	}
}
