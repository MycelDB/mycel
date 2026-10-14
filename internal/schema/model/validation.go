package model

import "errors"

var ErrNotFound = errors.New("schema not found")

// ValidationSeverity describes how a schema validation issue should be treated.
type ValidationSeverity string

const (
	ValidationSeverityWarning ValidationSeverity = "warning"
	ValidationSeverityError   ValidationSeverity = "error"
)

// ValidationIssue describes one mismatch between graph data and its schema.
type ValidationIssue struct {
	Severity ValidationSeverity
	Path     string
	Message  string
}

// ValidationResult contains the schema mode and all issues found during validation.
type ValidationResult struct {
	Mode   SchemaMode
	Issues []ValidationIssue
}

// Valid reports whether validation found no error-severity issues.
func (r ValidationResult) Valid() bool {
	for _, issue := range r.Issues {
		if issue.Severity == ValidationSeverityError {
			return false
		}
	}
	return true
}
