package logical

import "fmt"

// ErrorCode is a transport-neutral query failure category.
type ErrorCode string

const (
	ErrorInvalidQuery          ErrorCode = "invalid_query"
	ErrorUnsupportedPlan       ErrorCode = "unsupported_plan"
	ErrorResourceExhausted     ErrorCode = "resource_exhausted"
	ErrorStaleCursor           ErrorCode = "stale_cursor"
	ErrorUnavailableAccessPath ErrorCode = "unavailable_access_path"
	ErrorDependencyFailure     ErrorCode = "dependency_failure"
	ErrorCanceled              ErrorCode = "canceled"
	ErrorInternal              ErrorCode = "internal"
)

// QueryError is returned by neutral query layers before an API adapter maps it
// to a transport status.
type QueryError struct {
	Code          ErrorCode
	Operation     string
	Message       string
	RequiredIndex string
	Cause         error
}

func (e *QueryError) Error() string {
	if e == nil {
		return ""
	}
	message := e.Message
	if message == "" && e.Cause != nil {
		message = e.Cause.Error()
	}
	if e.Operation == "" {
		return message
	}
	if message == "" {
		return e.Operation
	}
	return fmt.Sprintf("%s: %s", e.Operation, message)
}

func (e *QueryError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// NewError constructs a categorized neutral query error.
func NewError(code ErrorCode, operation string, message string, cause error) *QueryError {
	return &QueryError{Code: code, Operation: operation, Message: message, Cause: cause}
}

// WithRequiredIndex attaches the schema index identity needed to make an
// unavailable access path plannable.
func (e *QueryError) WithRequiredIndex(index string) *QueryError {
	if e != nil {
		e.RequiredIndex = index
	}
	return e
}
