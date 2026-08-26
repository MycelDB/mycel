package model

import (
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
)

const (
	MaxMessageLength = 2048
	MaxTypeLength    = 128
	MaxMetadataDepth = 8
	MaxMetadataKeys  = 128
)

func NormalizeForAppend(event Event, now time.Time) (Event, error) {
	event.Severity = strings.TrimSpace(event.Severity)
	event.Category = strings.TrimSpace(event.Category)
	event.Type = strings.TrimSpace(event.Type)
	event.Message = strings.TrimSpace(event.Message)
	event.CorrelationID = strings.TrimSpace(event.CorrelationID)
	event.IdempotencyKey = strings.TrimSpace(event.IdempotencyKey)
	event.Source = normalizeSource(event.Source)
	event.Actor.PrincipalID = strings.TrimSpace(event.Actor.PrincipalID)
	event.Actor.Username = strings.TrimSpace(event.Actor.Username)
	event.Resource.Kind = strings.TrimSpace(event.Resource.Kind)
	event.Resource.ID = strings.TrimSpace(event.Resource.ID)
	event.Resource.Name = strings.TrimSpace(event.Resource.Name)
	if event.IngestedAt.IsZero() {
		event.IngestedAt = now.UTC()
	}
	if event.OccurredAt.IsZero() {
		event.OccurredAt = event.IngestedAt
	}
	if event.Metadata != nil {
		if err := validateMetadata(event.Metadata); err != nil {
			return Event{}, err
		}
	}
	if err := Validate(event); err != nil {
		return Event{}, err
	}
	return event, nil
}

func Validate(event Event) error {
	if !validSeverity(event.Severity) {
		return fmt.Errorf("%w: severity must be info, warning, or error", ErrInvalidEvent)
	}
	if !validCategory(event.Category) {
		return fmt.Errorf("%w: unsupported category %q", ErrInvalidEvent, event.Category)
	}
	if event.Type == "" || len(event.Type) > MaxTypeLength || strings.ContainsAny(event.Type, " \t\n\r") {
		return fmt.Errorf("%w: type is required and must be a compact dotted identifier", ErrInvalidEvent)
	}
	if event.Message == "" || len(event.Message) > MaxMessageLength {
		return fmt.Errorf("%w: message is required and must be at most %d bytes", ErrInvalidEvent, MaxMessageLength)
	}
	if event.Source.NodeID == "" && event.Source.NodeName == "" && event.Source.PodName == "" && event.Source.Component == "" && event.Source.Service == "" {
		return fmt.Errorf("%w: source is required", ErrInvalidEvent)
	}
	if event.OccurredAt.IsZero() || event.IngestedAt.IsZero() {
		return fmt.Errorf("%w: occurred_at and ingested_at are required", ErrInvalidEvent)
	}
	return nil
}

func validSeverity(value string) bool {
	switch value {
	case SeverityInfo, SeverityWarning, SeverityError:
		return true
	default:
		return false
	}
}

func validCategory(value string) bool {
	switch value {
	case CategoryLifecycle, CategoryIdentity, CategoryAccess, CategorySpace, CategoryDomain, CategoryBackup, CategoryCluster, CategorySemantic, CategoryAutomation, CategoryExternal:
		return true
	default:
		return false
	}
}

func normalizeSource(source Source) Source {
	source.NodeID = strings.TrimSpace(source.NodeID)
	source.NodeName = strings.TrimSpace(source.NodeName)
	source.PodName = strings.TrimSpace(source.PodName)
	source.Component = strings.TrimSpace(source.Component)
	source.Service = strings.TrimSpace(source.Service)
	return source
}

func validateMetadata(metadata *structpb.Struct) error {
	count := 0
	return validateStruct(metadata, 0, &count)
}

func validateStruct(value *structpb.Struct, depth int, count *int) error {
	if depth > MaxMetadataDepth {
		return fmt.Errorf("%w: metadata exceeds max depth", ErrInvalidEvent)
	}
	for key, child := range value.GetFields() {
		*count = *count + 1
		if *count > MaxMetadataKeys {
			return fmt.Errorf("%w: metadata has too many fields", ErrInvalidEvent)
		}
		if secretLikeKey(key) {
			return fmt.Errorf("%w: metadata key %q is not allowed", ErrInvalidEvent, key)
		}
		if err := validateValue(child, depth+1, count); err != nil {
			return err
		}
	}
	return nil
}

func validateValue(value *structpb.Value, depth int, count *int) error {
	switch kind := value.GetKind().(type) {
	case *structpb.Value_StructValue:
		return validateStruct(kind.StructValue, depth, count)
	case *structpb.Value_ListValue:
		if depth > MaxMetadataDepth {
			return fmt.Errorf("%w: metadata exceeds max depth", ErrInvalidEvent)
		}
		for _, child := range kind.ListValue.GetValues() {
			if err := validateValue(child, depth+1, count); err != nil {
				return err
			}
		}
	}
	return nil
}

func secretLikeKey(key string) bool {
	lower := strings.ToLower(key)
	for _, part := range []string{"password", "token", "secret", "api_key", "apikey", "private_key", "credential"} {
		if strings.Contains(lower, part) {
			return true
		}
	}
	return false
}
