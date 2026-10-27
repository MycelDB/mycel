package semantic

import (
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

var (
	ErrInvalidInput = errors.New("invalid semantic store input")
	ErrNotFound     = errors.New("semantic resource not found")
	ErrConflict     = errors.New("semantic resource conflict")
)

type VectorStoreConflictField string

const (
	VectorStoreConflictKey  VectorStoreConflictField = "key"
	VectorStoreConflictName VectorStoreConflictField = "name"
)

// VectorStoreConflictError reports a sanitized catalog uniqueness conflict.
// Legacy is true when the existing catalog is already ambiguous for the
// normalized value, including an explicit UUID update retaining a value shared
// with another entry.
type VectorStoreConflictError struct {
	Field           VectorStoreConflictField
	NormalizedValue string
	ConflictingIDs  []uuid.UUID
	Legacy          bool
}

func (e *VectorStoreConflictError) Error() string {
	kind := "already exists"
	if e.Legacy {
		kind = "is ambiguous in the legacy catalog"
	}
	return fmt.Sprintf("%s: vector store %s %s", ErrConflict, e.Field, kind)
}

func (e *VectorStoreConflictError) Unwrap() error { return ErrConflict }

func NewVectorStoreConflict(field VectorStoreConflictField, normalizedValue string, ids []uuid.UUID, legacy bool) error {
	copied := append([]uuid.UUID(nil), ids...)
	return &VectorStoreConflictError{Field: field, NormalizedValue: strings.TrimSpace(normalizedValue), ConflictingIDs: copied, Legacy: legacy}
}

// SemanticRuleDisplayNameConflictError reports a semantic rule display-name
// collision without exposing the conflicting rule identities in its message.
type SemanticRuleDisplayNameConflictError struct {
	NormalizedValue string
	ConflictingIDs  []uuid.UUID
}

func (e *SemanticRuleDisplayNameConflictError) Error() string {
	return fmt.Sprintf("%s: semantic rule display_name already exists", ErrConflict)
}

func (e *SemanticRuleDisplayNameConflictError) Unwrap() error { return ErrConflict }

func NewSemanticRuleDisplayNameConflict(normalizedValue string, ids []uuid.UUID) error {
	copied := append([]uuid.UUID(nil), ids...)
	return &SemanticRuleDisplayNameConflictError{NormalizedValue: strings.TrimSpace(normalizedValue), ConflictingIDs: copied}
}
