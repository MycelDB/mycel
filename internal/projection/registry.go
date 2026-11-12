package projection

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
)

var ErrCheckpointConflict = errors.New("projection checkpoint compare-and-swap conflict")

// Registry is the authoritative in-memory view applied by a source-partition
// state machine. It contains control metadata only; physical projection
// artifacts remain owned by projection-specific materializers.
type Registry struct {
	mu              sync.RWMutex
	definitions     map[ProjectionID]Definition
	desired         map[ProjectionID]DesiredFrontier
	checkpoints     map[ProjectionID]Checkpoint
	replicas        map[ProjectionID]map[string]ReplicaStatus
	appliedCommands map[string]string
}

func NewRegistry() *Registry {
	return &Registry{
		definitions:     map[ProjectionID]Definition{},
		desired:         map[ProjectionID]DesiredFrontier{},
		checkpoints:     map[ProjectionID]Checkpoint{},
		replicas:        map[ProjectionID]map[string]ReplicaStatus{},
		appliedCommands: map[string]string{},
	}
}

func (r *Registry) Apply(ctx context.Context, commandID string, commandHash []byte, record Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := record.Validate(); err != nil {
		return err
	}
	if commandID == "" {
		return fmt.Errorf("projection command_id is required")
	}
	if len(commandHash) == 0 {
		return fmt.Errorf("projection command hash is required")
	}
	hash := hex.EncodeToString(commandHash)

	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureMapsLocked()
	if previous, ok := r.appliedCommands[commandID]; ok {
		if previous != hash {
			return fmt.Errorf("projection command %q was previously applied with a different hash", commandID)
		}
		return nil
	}
	if err := r.applyRecordLocked(record); err != nil {
		return err
	}
	r.appliedCommands[commandID] = hash
	return nil
}

func (r *Registry) ApplyRecord(ctx context.Context, record Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := record.Validate(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureMapsLocked()
	return r.applyRecordLocked(record)
}

func (r *Registry) applyRecordLocked(record Record) error {
	switch record.Type {
	case RecordTypeDefinitionUpsert:
		definition := *record.Definition
		if existing, ok := r.definitions[definition.ID]; ok {
			if existing == definition {
				return nil
			}
			if definition.ConfigurationEpoch <= existing.ConfigurationEpoch {
				return fmt.Errorf("projection definition %q changed without advancing configuration epoch", definition.ID)
			}
			// A new configuration generation invalidates the previous local
			// artifact and checkpoint. Existing replicas remain visible but are
			// explicitly fenced until they publish the new generation.
			delete(r.desired, definition.ID)
			delete(r.checkpoints, definition.ID)
			for nodeID, status := range r.replicas[definition.ID] {
				status.State = StateRebuildRequired
				status.Reasons = []Reason{ReasonConfigurationMismatch, ReasonRebuildRequired}
				status.ConfigurationEpoch = definition.ConfigurationEpoch
				status.MaterializerVersion = definition.MaterializerVersion
				r.replicas[definition.ID][nodeID] = status
			}
		}
		r.definitions[definition.ID] = definition
		return nil

	case RecordTypeFrontierAdvance:
		frontier := *record.Frontier
		definition, ok := r.definitions[frontier.ProjectionID]
		if !ok {
			return fmt.Errorf("projection definition %q is missing", frontier.ProjectionID)
		}
		if frontier.ConfigurationEpoch != definition.ConfigurationEpoch {
			return fmt.Errorf("projection %q frontier configuration epoch %d does not match definition epoch %d", frontier.ProjectionID, frontier.ConfigurationEpoch, definition.ConfigurationEpoch)
		}
		if existing, ok := r.desired[frontier.ProjectionID]; ok {
			if frontier.Required.Revision < existing.Required.Revision {
				return fmt.Errorf("projection desired frontier %q regressed", frontier.ProjectionID)
			}
			if frontier.Required.Revision == existing.Required.Revision && frontier.Required.Cursor != existing.Required.Cursor {
				return fmt.Errorf("projection desired frontier %q has a conflicting cursor at the same source revision", frontier.ProjectionID)
			}
		}
		r.desired[frontier.ProjectionID] = frontier
		return nil

	case RecordTypeCheckpointPublish:
		checkpoint := *record.Checkpoint
		if expected := record.Expected; expected != nil {
			current, exists := r.checkpoints[checkpoint.ProjectionID]
			if expected.Exists != exists || (exists && !checkpointExpectationMatches(*expected, current)) {
				return fmt.Errorf("%w: projection %q expected checkpoint does not match current state", ErrCheckpointConflict, checkpoint.ProjectionID)
			}
		}
		definition, ok := r.definitions[checkpoint.ProjectionID]
		if !ok {
			return fmt.Errorf("projection definition %q is missing", checkpoint.ProjectionID)
		}
		if err := validateCheckpointAgainstDefinition(checkpoint, definition); err != nil {
			return err
		}
		if existing, ok := r.checkpoints[checkpoint.ProjectionID]; ok {
			if checkpoint.ConfigurationEpoch < existing.ConfigurationEpoch || checkpoint.FencingEpoch < existing.FencingEpoch || checkpoint.Published.Revision < existing.Published.Revision {
				return fmt.Errorf("projection checkpoint %q regressed in configuration, fencing, or source frontier", checkpoint.ProjectionID)
			}
			if checkpoint.FencingEpoch == existing.FencingEpoch && checkpoint.Published.Revision == existing.Published.Revision && checkpoint.Published.Cursor != existing.Published.Cursor {
				return fmt.Errorf("projection checkpoint %q has a conflicting cursor at the same source revision", checkpoint.ProjectionID)
			}
		}
		r.checkpoints[checkpoint.ProjectionID] = checkpoint
		return nil

	case RecordTypeReplicaStatus:
		status := *record.Replica
		if _, ok := r.definitions[status.ProjectionID]; !ok {
			return fmt.Errorf("projection definition %q is missing", status.ProjectionID)
		}
		if r.replicas[status.ProjectionID] == nil {
			r.replicas[status.ProjectionID] = map[string]ReplicaStatus{}
		}
		if existing, ok := r.replicas[status.ProjectionID][status.NodeID]; ok {
			if status.ConfigurationEpoch < existing.ConfigurationEpoch || status.FencingEpoch < existing.FencingEpoch || (status.FencingEpoch == existing.FencingEpoch && status.Published.Revision < existing.Published.Revision) {
				return fmt.Errorf("projection replica %q on node %q regressed in configuration, fencing, or source frontier", status.ProjectionID, status.NodeID)
			}
			if status.FencingEpoch == existing.FencingEpoch && status.Published.Revision == existing.Published.Revision && status.Published.Cursor != existing.Published.Cursor {
				return fmt.Errorf("projection replica %q on node %q has a conflicting cursor at the same source revision", status.ProjectionID, status.NodeID)
			}
		}
		r.replicas[status.ProjectionID][status.NodeID] = cloneReplicaStatus(status)
		return nil

	default:
		return fmt.Errorf("unsupported projection record type %q", record.Type)
	}
}

func checkpointExpectationMatches(expected CheckpointExpectation, current Checkpoint) bool {
	return expected.ProjectionID == current.ProjectionID && expected.Published == current.Published && expected.ConfigurationEpoch == current.ConfigurationEpoch && expected.FencingEpoch == current.FencingEpoch && expected.ArtifactGeneration == current.ArtifactGeneration
}

func (r *Registry) PublishCheckpoint(ctx context.Context, commandID string, commandHash []byte, checkpoint Checkpoint, expected CheckpointExpectation) error {
	return r.Apply(ctx, commandID, commandHash, NewCheckpointCASRecord(checkpoint, expected))
}

func validateCheckpointAgainstDefinition(checkpoint Checkpoint, definition Definition) error {
	if checkpoint.ConfigurationEpoch != definition.ConfigurationEpoch {
		return fmt.Errorf("projection %q configuration epoch %d does not match definition epoch %d", checkpoint.ProjectionID, checkpoint.ConfigurationEpoch, definition.ConfigurationEpoch)
	}
	if checkpoint.ConfigurationHash != definition.ConfigurationHash {
		return fmt.Errorf("projection %q configuration hash does not match definition", checkpoint.ProjectionID)
	}
	if checkpoint.MaterializerVersion != definition.MaterializerVersion {
		return fmt.Errorf("projection %q materializer version does not match definition", checkpoint.ProjectionID)
	}
	return nil
}

func (r *Registry) Definition(id ProjectionID) (Definition, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	definition, ok := r.definitions[id]
	return definition, ok
}

func (r *Registry) DesiredFrontier(id ProjectionID) (DesiredFrontier, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	frontier, ok := r.desired[id]
	return frontier, ok
}

func (r *Registry) Checkpoint(id ProjectionID) (Checkpoint, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	checkpoint, ok := r.checkpoints[id]
	return checkpoint, ok
}

func (r *Registry) ReplicaStatus(id ProjectionID, nodeID string) (ReplicaStatus, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	status, ok := r.replicas[id][nodeID]
	if !ok {
		return ReplicaStatus{}, false
	}
	return cloneReplicaStatus(status), true
}

func (r *Registry) Readiness(id ProjectionID, nodeID string, requirement ReadinessRequirement) (Readiness, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	definition, definitionOK := r.definitions[id]
	status, statusOK := r.replicas[id][nodeID]
	return readinessFromState(id, nodeID, definition, definitionOK, status, statusOK, requirement), nil
}

func readinessFromState(id ProjectionID, nodeID string, definition Definition, definitionOK bool, status ReplicaStatus, statusOK bool, requirement ReadinessRequirement) Readiness {
	out := Readiness{ProjectionID: id, NodeID: nodeID, State: StateAbsent, Required: requirement.Required, ConfigurationEpoch: requirement.ConfigurationEpoch, MaterializerVersion: requirement.MaterializerVersion, FencingEpoch: requirement.FencingEpoch}
	if !definitionOK {
		out.State = StateRebuildRequired
		out.Reasons = []Reason{ReasonDefinitionMissing, ReasonRebuildRequired}
		return out
	}
	if !statusOK {
		out.State = StateRebuildRequired
		out.Reasons = []Reason{ReasonMissingManifest, ReasonRebuildRequired}
		return out
	}
	out.State = status.State
	out.Observed = status.Published
	out.ConfigurationEpoch = status.ConfigurationEpoch
	out.MaterializerVersion = status.MaterializerVersion
	out.FencingEpoch = status.FencingEpoch
	out.Reasons = append([]Reason(nil), status.Reasons...)
	if status.ConfigurationEpoch != definition.ConfigurationEpoch || status.ConfigurationEpoch != requirement.ConfigurationEpoch {
		out.Reasons = appendReason(out.Reasons, ReasonConfigurationMismatch)
	}
	if status.MaterializerVersion != definition.MaterializerVersion || status.MaterializerVersion != requirement.MaterializerVersion {
		out.Reasons = appendReason(out.Reasons, ReasonMaterializerMismatch)
	}
	if status.FencingEpoch != requirement.FencingEpoch {
		out.Reasons = appendReason(out.Reasons, ReasonFencingMismatch)
	}
	cursorRequired := requirement.Required.Cursor != ""
	if status.Published.Revision < requirement.Required.Revision || (cursorRequired && status.Published.Cursor != requirement.Required.Cursor) {
		out.Reasons = appendReason(out.Reasons, ReasonSourceLag)
	}
	if len(out.Reasons) > 0 || status.State != StateReady {
		if status.State == StateReady {
			out.State = StateStale
		}
		return out
	}
	out.State = StateReady
	return out
}

func appendReason(reasons []Reason, reason Reason) []Reason {
	for _, existing := range reasons {
		if existing == reason {
			return reasons
		}
	}
	return append(reasons, reason)
}

// BootstrapUnknown registers a definition whose existing physical artifact
// could not be proven compatible. It intentionally starts in rebuild_required.
func (r *Registry) BootstrapUnknown(ctx context.Context, definition Definition, nodeID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := definition.Validate(); err != nil {
		return err
	}
	status := ReplicaStatus{ProjectionID: definition.ID, NodeID: nodeID, State: StateRebuildRequired, Reasons: []Reason{ReasonMissingManifest, ReasonRebuildRequired}, Required: Frontier{}, Published: Frontier{}, ConfigurationEpoch: definition.ConfigurationEpoch, MaterializerVersion: definition.MaterializerVersion}
	if err := status.Validate(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureMapsLocked()
	if existing, ok := r.definitions[definition.ID]; ok && existing != definition {
		return fmt.Errorf("projection definition %q already exists with different content", definition.ID)
	}
	r.definitions[definition.ID] = definition
	if r.replicas[definition.ID] == nil {
		r.replicas[definition.ID] = map[string]ReplicaStatus{}
	}
	if existing, ok := r.replicas[definition.ID][nodeID]; !ok || existing.State == StateAbsent {
		r.replicas[definition.ID][nodeID] = status
	}
	return nil
}

func (r *Registry) Snapshot() Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return SnapshotFromRegistry(r.definitions, r.desired, r.checkpoints, r.replicas, r.appliedCommands)
}

func (r *Registry) Restore(snapshot Snapshot) error {
	if err := snapshot.Validate(); err != nil {
		return err
	}
	definitions := map[ProjectionID]Definition{}
	desired := map[ProjectionID]DesiredFrontier{}
	checkpoints := map[ProjectionID]Checkpoint{}
	replicas := map[ProjectionID]map[string]ReplicaStatus{}
	applied := map[string]string{}
	for _, definition := range snapshot.Definitions {
		definitions[definition.ID] = definition
	}
	for _, frontier := range snapshot.DesiredFrontiers {
		definition, ok := definitions[frontier.ProjectionID]
		if !ok {
			return fmt.Errorf("projection snapshot desired frontier %q has no definition", frontier.ProjectionID)
		}
		if frontier.ConfigurationEpoch != definition.ConfigurationEpoch {
			return fmt.Errorf("projection snapshot desired frontier %q configuration epoch does not match definition", frontier.ProjectionID)
		}
		desired[frontier.ProjectionID] = frontier
	}
	for _, checkpoint := range snapshot.Checkpoints {
		definition, ok := definitions[checkpoint.ProjectionID]
		if !ok {
			return fmt.Errorf("projection snapshot checkpoint %q has no definition", checkpoint.ProjectionID)
		}
		if err := validateCheckpointAgainstDefinition(checkpoint, definition); err != nil {
			return err
		}
		checkpoints[checkpoint.ProjectionID] = checkpoint
	}
	for _, status := range snapshot.Replicas {
		if _, ok := definitions[status.ProjectionID]; !ok {
			return fmt.Errorf("projection snapshot replica %q has no definition", status.ProjectionID)
		}
		if replicas[status.ProjectionID] == nil {
			replicas[status.ProjectionID] = map[string]ReplicaStatus{}
		}
		replicas[status.ProjectionID][status.NodeID] = cloneReplicaStatus(status)
	}
	for commandID, hash := range snapshot.AppliedCommands {
		if commandID == "" || hash == "" {
			return fmt.Errorf("projection snapshot contains an invalid applied command")
		}
		applied[commandID] = hash
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.definitions = definitions
	r.desired = desired
	r.checkpoints = checkpoints
	r.replicas = replicas
	r.appliedCommands = applied
	return nil
}

func (r *Registry) ensureMapsLocked() {
	if r.definitions == nil {
		r.definitions = map[ProjectionID]Definition{}
	}
	if r.desired == nil {
		r.desired = map[ProjectionID]DesiredFrontier{}
	}
	if r.checkpoints == nil {
		r.checkpoints = map[ProjectionID]Checkpoint{}
	}
	if r.replicas == nil {
		r.replicas = map[ProjectionID]map[string]ReplicaStatus{}
	}
	if r.appliedCommands == nil {
		r.appliedCommands = map[string]string{}
	}
}

func cloneReplicaStatus(status ReplicaStatus) ReplicaStatus {
	status.Reasons = append([]Reason(nil), status.Reasons...)
	return status
}

func (s Snapshot) SortedReplicaKeys() []string {
	keys := make([]string, 0, len(s.Replicas))
	for _, replica := range s.Replicas {
		keys = append(keys, replica.ProjectionID.String()+"/"+replica.NodeID)
	}
	sort.Strings(keys)
	return keys
}
