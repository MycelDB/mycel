package runtime

import "fmt"

// InitResult describes service initialization outcome.
type InitResult struct {
	OK    bool
	Error *InitError
}

// InitError is a structured initialization error.
type InitError struct {
	Module  string
	Type    string
	Message string
	Err     error
	Abort   bool
}

func (e *InitError) Error() string {
	if e == nil {
		return ""
	}
	if e.Err != nil {
		return fmt.Sprintf("%s: %s: %v", e.Module, e.Message, e.Err)
	}
	return fmt.Sprintf("%s: %s", e.Module, e.Message)
}

func (e *InitError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// OK returns a successful initialization result.
func OK(module string) InitResult { return InitResult{OK: true} }

// Abort returns a fatal initialization result.
func Abort(module, issueType, message string, err error) InitResult {
	return InitResult{Error: &InitError{Module: module, Type: issueType, Message: message, Err: err, Abort: true}}
}

// Continue returns a non-fatal initialization result.
func Continue(module, issueType, message string, err error) InitResult {
	return InitResult{Error: &InitError{Module: module, Type: issueType, Message: message, Err: err, Abort: false}}
}
