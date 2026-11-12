package projection

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"
)

const (
	RecordVersion              = 1
	SnapshotVersion            = 1
	MaxProjectionIDBytes       = 128
	MaxScopeIDBytes            = 128
	MaxKeyBytes                = 256
	MaxCursorBytes             = 4096
	MaxHashBytes               = 256
	MaxMaterializerBytes       = 128
	MaxArtifactGenerationBytes = 256
	MaxNodeIDBytes             = 128
	MaxReadinessReasons        = 8
	MaxReadinessBytes          = 8192
)

type Kind string

const (
	KindLexical    Kind = "lexical"
	KindSemantic   Kind = "semantic"
	KindAutomation Kind = "automation"
	KindMetadata   Kind = "metadata"
)

type State string

const (
	StateAbsent          State = "absent"
	StateBuilding        State = "building"
	StateReady           State = "ready"
	StateCatchingUp      State = "catching_up"
	StateStale           State = "stale"
	StateDegraded        State = "degraded"
	StateRebuildRequired State = "rebuild_required"
)

type Reason string

const (
	ReasonDefinitionMissing     Reason = "definition_missing"
	ReasonMissingManifest       Reason = "missing_manifest"
	ReasonSourceLag             Reason = "source_lag"
	ReasonConfigurationMismatch Reason = "configuration_mismatch"
	ReasonMaterializerMismatch  Reason = "materializer_version_mismatch"
	ReasonFencingMismatch       Reason = "fencing_epoch_mismatch"
	ReasonArtifactUnavailable   Reason = "artifact_unavailable"
	ReasonSourceGap             Reason = "source_gap"
	ReasonCorruptArtifact       Reason = "corrupt_artifact"
	ReasonRebuildRequired       Reason = "rebuild_required"
	ReasonInvalidRecord         Reason = "invalid_record"
)

type ProjectionID string

type Scope struct {
	SpaceID  string `json:"space_id"`
	DomainID string `json:"domain_id"`
}

type Definition struct {
	ID                  ProjectionID `json:"id"`
	Kind                Kind         `json:"kind"`
	Scope               Scope        `json:"scope"`
	Key                 string       `json:"key"`
	ConfigurationHash   string       `json:"configuration_hash,omitempty"`
	ConfigurationEpoch  uint64       `json:"configuration_epoch"`
	MaterializerVersion string       `json:"materializer_version"`
}

type Frontier struct {
	Revision uint64 `json:"revision"`
	Cursor   string `json:"cursor,omitempty"`
}

type DesiredFrontier struct {
	ProjectionID       ProjectionID `json:"projection_id"`
	Required           Frontier     `json:"required"`
	ConfigurationEpoch uint64       `json:"configuration_epoch"`
}

type CheckpointExpectation struct {
	ProjectionID       ProjectionID `json:"projection_id"`
	Exists             bool         `json:"exists"`
	Published          Frontier     `json:"published,omitempty"`
	ConfigurationEpoch uint64       `json:"configuration_epoch,omitempty"`
	FencingEpoch       uint64       `json:"fencing_epoch,omitempty"`
	ArtifactGeneration string       `json:"artifact_generation,omitempty"`
}

type Checkpoint struct {
	ProjectionID        ProjectionID `json:"projection_id"`
	Published           Frontier     `json:"published"`
	ConfigurationEpoch  uint64       `json:"configuration_epoch"`
	ConfigurationHash   string       `json:"configuration_hash,omitempty"`
	MaterializerVersion string       `json:"materializer_version"`
	FencingEpoch        uint64       `json:"fencing_epoch"`
	ArtifactGeneration  string       `json:"artifact_generation"`
	State               State        `json:"state"`
}

type ReplicaStatus struct {
	ProjectionID        ProjectionID `json:"projection_id"`
	NodeID              string       `json:"node_id"`
	State               State        `json:"state"`
	Reasons             []Reason     `json:"reasons,omitempty"`
	Required            Frontier     `json:"required"`
	Published           Frontier     `json:"published"`
	ConfigurationEpoch  uint64       `json:"configuration_epoch"`
	MaterializerVersion string       `json:"materializer_version"`
	FencingEpoch        uint64       `json:"fencing_epoch"`
	ArtifactGeneration  string       `json:"artifact_generation,omitempty"`
}

type ReadinessRequirement struct {
	Required            Frontier
	ConfigurationEpoch  uint64
	MaterializerVersion string
	FencingEpoch        uint64
}

type Readiness struct {
	ProjectionID        ProjectionID `json:"projection_id"`
	NodeID              string       `json:"node_id,omitempty"`
	State               State        `json:"state"`
	Reasons             []Reason     `json:"reasons,omitempty"`
	Required            Frontier     `json:"required"`
	Observed            Frontier     `json:"observed"`
	ConfigurationEpoch  uint64       `json:"configuration_epoch"`
	MaterializerVersion string       `json:"materializer_version"`
	FencingEpoch        uint64       `json:"fencing_epoch"`
}

type RecordType string

const (
	RecordTypeDefinitionUpsert  RecordType = "projection.definition.upsert.v1"
	RecordTypeFrontierAdvance   RecordType = "projection.frontier.advance.v1"
	RecordTypeCheckpointPublish RecordType = "projection.checkpoint.publish.v1"
	RecordTypeReplicaStatus     RecordType = "projection.replica.status.v1"
)

type Record struct {
	Version    int                    `json:"version"`
	Type       RecordType             `json:"type"`
	Definition *Definition            `json:"definition,omitempty"`
	Frontier   *DesiredFrontier       `json:"frontier,omitempty"`
	Checkpoint *Checkpoint            `json:"checkpoint,omitempty"`
	Expected   *CheckpointExpectation `json:"expected,omitempty"`
	Replica    *ReplicaStatus         `json:"replica,omitempty"`
}

type Snapshot struct {
	Version          int               `json:"version"`
	Definitions      []Definition      `json:"definitions,omitempty"`
	DesiredFrontiers []DesiredFrontier `json:"desired_frontiers,omitempty"`
	Checkpoints      []Checkpoint      `json:"checkpoints,omitempty"`
	Replicas         []ReplicaStatus   `json:"replicas,omitempty"`
	AppliedCommands  map[string]string `json:"applied_commands,omitempty"`
}

func IDFor(kind Kind, scope Scope, key string) (ProjectionID, error) {
	if err := validateKind(kind); err != nil {
		return "", err
	}
	if err := scope.validate(); err != nil {
		return "", err
	}
	if err := validateBounded("key", key, MaxKeyBytes, false); err != nil {
		return "", err
	}
	payload, err := json.Marshal(struct {
		Kind  Kind   `json:"kind"`
		Scope Scope  `json:"scope"`
		Key   string `json:"key"`
	}{kind, scope, key})
	if err != nil {
		return "", fmt.Errorf("marshal projection identity basis: %w", err)
	}
	digest := sha256.Sum256(payload)
	return ProjectionID("prj_" + hex.EncodeToString(digest[:])), nil
}

func (d Definition) Validate() error {
	if err := validateProjectionID(d.ID); err != nil {
		return err
	}
	if err := validateKind(d.Kind); err != nil {
		return err
	}
	if err := d.Scope.validate(); err != nil {
		return err
	}
	if err := validateBounded("key", d.Key, MaxKeyBytes, false); err != nil {
		return err
	}
	if err := validateBounded("configuration_hash", d.ConfigurationHash, MaxHashBytes, true); err != nil {
		return err
	}
	if err := validateBounded("materializer_version", d.MaterializerVersion, MaxMaterializerBytes, false); err != nil {
		return err
	}
	return nil
}

func (f DesiredFrontier) Validate() error {
	if err := validateProjectionID(f.ProjectionID); err != nil {
		return err
	}
	if err := f.Required.validate(); err != nil {
		return err
	}
	return nil
}

func (e CheckpointExpectation) Validate() error {
	if err := validateProjectionID(e.ProjectionID); err != nil {
		return err
	}
	if !e.Exists {
		if e.Published != (Frontier{}) || e.ConfigurationEpoch != 0 || e.FencingEpoch != 0 || e.ArtifactGeneration != "" {
			return fmt.Errorf("non-existent checkpoint expectation must not contain checkpoint state")
		}
		return nil
	}
	if err := e.Published.validate(); err != nil {
		return err
	}
	if err := validateBounded("artifact_generation", e.ArtifactGeneration, MaxArtifactGenerationBytes, false); err != nil {
		return err
	}
	return nil
}

func (c Checkpoint) Validate() error {
	if err := validateProjectionID(c.ProjectionID); err != nil {
		return err
	}
	if err := c.Published.validate(); err != nil {
		return err
	}
	if err := validateBounded("configuration_hash", c.ConfigurationHash, MaxHashBytes, true); err != nil {
		return err
	}
	if err := validateBounded("materializer_version", c.MaterializerVersion, MaxMaterializerBytes, false); err != nil {
		return err
	}
	if err := validateBounded("artifact_generation", c.ArtifactGeneration, MaxArtifactGenerationBytes, false); err != nil {
		return err
	}
	if !validState(c.State) {
		return fmt.Errorf("invalid projection state %q", c.State)
	}
	return nil
}

func (s ReplicaStatus) Validate() error {
	if err := validateProjectionID(s.ProjectionID); err != nil {
		return err
	}
	if err := validateBounded("node_id", s.NodeID, MaxNodeIDBytes, false); err != nil {
		return err
	}
	if !validState(s.State) {
		return fmt.Errorf("invalid projection state %q", s.State)
	}
	if err := s.Required.validate(); err != nil {
		return err
	}
	if err := s.Published.validate(); err != nil {
		return err
	}
	if err := validateBounded("materializer_version", s.MaterializerVersion, MaxMaterializerBytes, false); err != nil {
		return err
	}
	if err := validateBounded("artifact_generation", s.ArtifactGeneration, MaxArtifactGenerationBytes, true); err != nil {
		return err
	}
	if len(s.Reasons) > MaxReadinessReasons {
		return fmt.Errorf("projection readiness reasons exceed %d entries", MaxReadinessReasons)
	}
	seen := map[Reason]struct{}{}
	for _, reason := range s.Reasons {
		if !validReason(reason) {
			return fmt.Errorf("invalid projection readiness reason %q", reason)
		}
		if _, ok := seen[reason]; ok {
			return fmt.Errorf("duplicate projection readiness reason %q", reason)
		}
		seen[reason] = struct{}{}
	}
	return nil
}

func (r Readiness) Validate() error {
	if err := validateProjectionID(r.ProjectionID); err != nil {
		return err
	}
	if err := validateBounded("node_id", r.NodeID, MaxNodeIDBytes, true); err != nil {
		return err
	}
	if !validState(r.State) {
		return fmt.Errorf("invalid projection readiness state %q", r.State)
	}
	if err := r.Required.validate(); err != nil {
		return err
	}
	if err := r.Observed.validate(); err != nil {
		return err
	}
	if err := validateBounded("materializer_version", r.MaterializerVersion, MaxMaterializerBytes, true); err != nil {
		return err
	}
	if len(r.Reasons) > MaxReadinessReasons {
		return fmt.Errorf("projection readiness reasons exceed %d entries", MaxReadinessReasons)
	}
	seen := map[Reason]struct{}{}
	for _, reason := range r.Reasons {
		if !validReason(reason) {
			return fmt.Errorf("invalid projection readiness reason %q", reason)
		}
		if _, ok := seen[reason]; ok {
			return fmt.Errorf("duplicate projection readiness reason %q", reason)
		}
		seen[reason] = struct{}{}
	}
	return nil
}

func (r Readiness) Marshal() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	data, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	if len(data) > MaxReadinessBytes {
		return nil, fmt.Errorf("projection readiness exceeds %d bytes", MaxReadinessBytes)
	}
	return data, nil
}

func (f Frontier) validate() error {
	return validateBounded("cursor", f.Cursor, MaxCursorBytes, true)
}

func (s Scope) validate() error {
	if err := validateBounded("space_id", s.SpaceID, MaxScopeIDBytes, false); err != nil {
		return err
	}
	return validateBounded("domain_id", s.DomainID, MaxScopeIDBytes, false)
}

func (r Record) Validate() error {
	if r.Version != RecordVersion {
		return fmt.Errorf("unsupported projection record version %d", r.Version)
	}
	switch r.Type {
	case RecordTypeDefinitionUpsert:
		if r.Definition == nil || r.Frontier != nil || r.Checkpoint != nil || r.Expected != nil || r.Replica != nil {
			return fmt.Errorf("definition upsert record must contain only a definition")
		}
		return r.Definition.Validate()
	case RecordTypeFrontierAdvance:
		if r.Frontier == nil || r.Definition != nil || r.Checkpoint != nil || r.Expected != nil || r.Replica != nil {
			return fmt.Errorf("frontier advance record must contain only a desired frontier")
		}
		return r.Frontier.Validate()
	case RecordTypeCheckpointPublish:
		if r.Checkpoint == nil || r.Definition != nil || r.Frontier != nil || r.Replica != nil {
			return fmt.Errorf("checkpoint publish record must contain a checkpoint")
		}
		if r.Expected != nil {
			if r.Expected.ProjectionID != r.Checkpoint.ProjectionID {
				return fmt.Errorf("checkpoint expectation projection_id does not match checkpoint")
			}
			if err := r.Expected.Validate(); err != nil {
				return err
			}
		}
		return r.Checkpoint.Validate()
	case RecordTypeReplicaStatus:
		if r.Replica == nil || r.Definition != nil || r.Frontier != nil || r.Checkpoint != nil || r.Expected != nil {
			return fmt.Errorf("replica status record must contain only replica status")
		}
		return r.Replica.Validate()
	default:
		return fmt.Errorf("unsupported projection record type %q", r.Type)
	}
}

func (r Record) Marshal() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(r)
}

func DecodeRecord(data []byte) (Record, error) {
	var r Record
	if err := json.Unmarshal(data, &r); err != nil {
		return Record{}, fmt.Errorf("decode projection record: %w", err)
	}
	if err := r.Validate(); err != nil {
		return Record{}, err
	}
	return r, nil
}

func SnapshotFromRegistry(definitions map[ProjectionID]Definition, desired map[ProjectionID]DesiredFrontier, checkpoints map[ProjectionID]Checkpoint, replicas map[ProjectionID]map[string]ReplicaStatus, applied map[string]string) Snapshot {
	s := Snapshot{Version: SnapshotVersion, AppliedCommands: map[string]string{}}
	for _, definition := range definitions {
		s.Definitions = append(s.Definitions, definition)
	}
	for _, frontier := range desired {
		s.DesiredFrontiers = append(s.DesiredFrontiers, frontier)
	}
	for _, checkpoint := range checkpoints {
		s.Checkpoints = append(s.Checkpoints, checkpoint)
	}
	for _, statuses := range replicas {
		for _, status := range statuses {
			s.Replicas = append(s.Replicas, cloneReplicaStatus(status))
		}
	}
	for commandID, hash := range applied {
		s.AppliedCommands[commandID] = hash
	}
	sort.Slice(s.Definitions, func(i, j int) bool { return s.Definitions[i].ID < s.Definitions[j].ID })
	sort.Slice(s.DesiredFrontiers, func(i, j int) bool { return s.DesiredFrontiers[i].ProjectionID < s.DesiredFrontiers[j].ProjectionID })
	sort.Slice(s.Checkpoints, func(i, j int) bool { return s.Checkpoints[i].ProjectionID < s.Checkpoints[j].ProjectionID })
	sort.Slice(s.Replicas, func(i, j int) bool {
		if s.Replicas[i].ProjectionID != s.Replicas[j].ProjectionID {
			return s.Replicas[i].ProjectionID < s.Replicas[j].ProjectionID
		}
		return s.Replicas[i].NodeID < s.Replicas[j].NodeID
	})
	return s
}

func (s Snapshot) Validate() error {
	if s.Version != SnapshotVersion {
		return fmt.Errorf("unsupported projection snapshot version %d", s.Version)
	}
	definitions := map[ProjectionID]struct{}{}
	for _, definition := range s.Definitions {
		if err := definition.Validate(); err != nil {
			return err
		}
		if _, ok := definitions[definition.ID]; ok {
			return fmt.Errorf("duplicate projection definition %q", definition.ID)
		}
		definitions[definition.ID] = struct{}{}
	}
	desired := map[ProjectionID]struct{}{}
	for _, frontier := range s.DesiredFrontiers {
		if err := frontier.Validate(); err != nil {
			return err
		}
		if _, ok := definitions[frontier.ProjectionID]; !ok {
			return fmt.Errorf("projection desired frontier %q has no definition", frontier.ProjectionID)
		}
		if _, ok := desired[frontier.ProjectionID]; ok {
			return fmt.Errorf("duplicate projection desired frontier %q", frontier.ProjectionID)
		}
		desired[frontier.ProjectionID] = struct{}{}
	}
	checkpoints := map[ProjectionID]struct{}{}
	for _, checkpoint := range s.Checkpoints {
		if err := checkpoint.Validate(); err != nil {
			return err
		}
		if _, ok := definitions[checkpoint.ProjectionID]; !ok {
			return fmt.Errorf("projection checkpoint %q has no definition", checkpoint.ProjectionID)
		}
		if _, ok := checkpoints[checkpoint.ProjectionID]; ok {
			return fmt.Errorf("duplicate projection checkpoint %q", checkpoint.ProjectionID)
		}
		checkpoints[checkpoint.ProjectionID] = struct{}{}
	}
	replicas := map[string]struct{}{}
	for _, status := range s.Replicas {
		if err := status.Validate(); err != nil {
			return err
		}
		if _, ok := definitions[status.ProjectionID]; !ok {
			return fmt.Errorf("projection replica %q has no definition", status.ProjectionID)
		}
		key := status.ProjectionID.String() + "\x00" + status.NodeID
		if _, ok := replicas[key]; ok {
			return fmt.Errorf("duplicate projection replica status %q", key)
		}
		replicas[key] = struct{}{}
	}
	for commandID, hash := range s.AppliedCommands {
		if strings.TrimSpace(commandID) == "" || strings.TrimSpace(hash) == "" {
			return fmt.Errorf("projection snapshot contains an invalid applied command")
		}
	}
	return nil
}

func (id ProjectionID) String() string { return string(id) }

func validState(state State) bool {
	switch state {
	case StateAbsent, StateBuilding, StateReady, StateCatchingUp, StateStale, StateDegraded, StateRebuildRequired:
		return true
	default:
		return false
	}
}

func validReason(reason Reason) bool {
	switch reason {
	case ReasonDefinitionMissing, ReasonMissingManifest, ReasonSourceLag, ReasonConfigurationMismatch, ReasonMaterializerMismatch, ReasonFencingMismatch, ReasonArtifactUnavailable, ReasonSourceGap, ReasonCorruptArtifact, ReasonRebuildRequired, ReasonInvalidRecord:
		return true
	default:
		return false
	}
}

func validateKind(kind Kind) error {
	switch kind {
	case KindLexical, KindSemantic, KindAutomation, KindMetadata:
		return nil
	default:
		return fmt.Errorf("unsupported projection kind %q", kind)
	}
}

func validateProjectionID(id ProjectionID) error {
	return validateBounded("projection_id", id.String(), MaxProjectionIDBytes, false)
}

func validateBounded(name, value string, max int, optional bool) error {
	if optional && strings.TrimSpace(value) == "" {
		return nil
	}
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required", name)
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("%s is not valid UTF-8", name)
	}
	if len(value) > max {
		return fmt.Errorf("%s exceeds %d bytes", name, max)
	}
	return nil
}
