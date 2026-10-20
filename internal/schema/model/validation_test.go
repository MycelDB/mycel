package model

import "testing"

func TestValidationResultValidIgnoresWarnings(t *testing.T) {
	result := ValidationResult{Issues: []ValidationIssue{{Severity: ValidationSeverityWarning, Path: "labels", Message: "unknown label"}}}
	if !result.Valid() {
		t.Fatal("warning-only validation result should be valid")
	}
}

func TestValidationResultValidRejectsErrors(t *testing.T) {
	result := ValidationResult{Issues: []ValidationIssue{{Severity: ValidationSeverityError, Path: "labels", Message: "unknown label"}}}
	if result.Valid() {
		t.Fatal("error validation result should be invalid")
	}
}
