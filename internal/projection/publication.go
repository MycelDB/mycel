package projection

import (
	"context"
	"errors"
	"fmt"
)

const (
	MaxArtifactFormatBytes   = 128
	MaxArtifactChecksumBytes = 256
)

// ArtifactManifest is the bounded, non-sensitive proof attached to one local
// artifact generation. It contains no filesystem paths, graph payloads, or
// provider data.
type ArtifactManifest struct {
	ProjectionID        ProjectionID `json:"projection_id"`
	Kind                Kind         `json:"kind"`
	Scope               Scope        `json:"scope"`
	ConfigurationHash   string       `json:"configuration_hash,omitempty"`
	ConfigurationEpoch  uint64       `json:"configuration_epoch"`
	MaterializerVersion string       `json:"materializer_version"`
	Source              Frontier     `json:"source"`
	FencingEpoch        uint64       `json:"fencing_epoch"`
	Generation          string       `json:"generation"`
	Format              string       `json:"format"`
	Checksum            string       `json:"checksum"`
	Bytes               uint64       `json:"bytes"`
	Tombstones          uint64       `json:"tombstones,omitempty"`
}

func (m ArtifactManifest) Validate() error {
	if err := validateProjectionID(m.ProjectionID); err != nil {
		return err
	}
	if err := validateKind(m.Kind); err != nil {
		return err
	}
	if err := m.Scope.validate(); err != nil {
		return err
	}
	if err := validateBounded("configuration_hash", m.ConfigurationHash, MaxHashBytes, true); err != nil {
		return err
	}
	if err := validateBounded("materializer_version", m.MaterializerVersion, MaxMaterializerBytes, false); err != nil {
		return err
	}
	if err := m.Source.validate(); err != nil {
		return err
	}
	if err := validateBounded("generation", m.Generation, MaxArtifactGenerationBytes, false); err != nil {
		return err
	}
	if err := validateBounded("format", m.Format, MaxArtifactFormatBytes, false); err != nil {
		return err
	}
	return validateBounded("checksum", m.Checksum, MaxArtifactChecksumBytes, false)
}

func (m ArtifactManifest) ValidateAgainstDefinition(definition Definition) error {
	if err := definition.Validate(); err != nil {
		return err
	}
	if err := m.Validate(); err != nil {
		return err
	}
	if m.ProjectionID != definition.ID || m.Kind != definition.Kind || m.Scope != definition.Scope || m.ConfigurationHash != definition.ConfigurationHash || m.ConfigurationEpoch != definition.ConfigurationEpoch || m.MaterializerVersion != definition.MaterializerVersion {
		return fmt.Errorf("artifact manifest does not match projection definition")
	}
	return nil
}

func (m ArtifactManifest) Checkpoint(state State) (Checkpoint, error) {
	if err := m.Validate(); err != nil {
		return Checkpoint{}, err
	}
	if !validState(state) {
		return Checkpoint{}, fmt.Errorf("invalid publication state %q", state)
	}
	return Checkpoint{ProjectionID: m.ProjectionID, Published: m.Source, ConfigurationEpoch: m.ConfigurationEpoch, ConfigurationHash: m.ConfigurationHash, MaterializerVersion: m.MaterializerVersion, FencingEpoch: m.FencingEpoch, ArtifactGeneration: m.Generation, State: state}, nil
}

// ArtifactPublication represents a private generation that can be validated,
// activated atomically within its own store, and rolled back if the
// authoritative checkpoint CAS loses a race.
type ArtifactPublication interface {
	Validate(context.Context, ArtifactManifest) error
	Activate(context.Context) (func(context.Context) error, error)
	Abort(context.Context) error
}

type CheckpointCAS interface {
	PublishCheckpoint(context.Context, Checkpoint, CheckpointExpectation) error
}

type CheckpointCASFunc func(context.Context, Checkpoint, CheckpointExpectation) error

func (f CheckpointCASFunc) PublishCheckpoint(ctx context.Context, checkpoint Checkpoint, expected CheckpointExpectation) error {
	if f == nil {
		return errors.New("checkpoint publisher is not configured")
	}
	return f(ctx, checkpoint, expected)
}

type PublicationCandidate struct {
	Manifest ArtifactManifest
	State    State
	Artifact ArtifactPublication
}

// PublishArtifact validates and activates a private generation, then commits
// its checkpoint through the authoritative CAS. A losing or failed CAS leaves
// the previous checkpoint and active generation intact when the artifact store
// honors the rollback contract.
func PublishArtifact(ctx context.Context, candidate PublicationCandidate, expected CheckpointExpectation, checkpoints CheckpointCAS) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if checkpoints == nil {
		return errors.New("checkpoint publisher is required")
	}
	if candidate.Artifact == nil {
		return errors.New("artifact publication is required")
	}
	checkpoint, err := candidate.Manifest.Checkpoint(candidate.State)
	if err != nil {
		return err
	}
	if expected.ProjectionID != checkpoint.ProjectionID {
		return fmt.Errorf("checkpoint expectation projection_id does not match publication")
	}
	if err := expected.Validate(); err != nil {
		return err
	}
	if err := candidate.Artifact.Validate(ctx, candidate.Manifest); err != nil {
		return fmt.Errorf("validate private artifact generation: %w", err)
	}
	rollback, err := candidate.Artifact.Activate(ctx)
	if err != nil {
		_ = candidate.Artifact.Abort(ctx)
		return fmt.Errorf("activate private artifact generation: %w", err)
	}
	if err := checkpoints.PublishCheckpoint(ctx, checkpoint, expected); err != nil {
		rollbackErr := error(nil)
		if rollback != nil {
			rollbackErr = rollback(ctx)
		}
		_ = candidate.Artifact.Abort(ctx)
		if rollbackErr != nil {
			return fmt.Errorf("publish checkpoint: %w; rollback active artifact: %v", err, rollbackErr)
		}
		return fmt.Errorf("publish checkpoint: %w", err)
	}
	return nil
}
