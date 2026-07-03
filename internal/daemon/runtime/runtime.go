package runtime

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/myceldb/mycel/internal/daemon/config"
)

type Runtime struct {
	Config config.Config
	Logger *slog.Logger
}

type Module interface {
	Name() string
	Init(context.Context, *Runtime) InitResult
}

type InitResult struct {
	OK    bool
	Error *InitError
}

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

func OK(module string) InitResult {
	return InitResult{OK: true}
}

func Abort(module, issueType, message string, err error) InitResult {
	return InitResult{Error: &InitError{Module: module, Type: issueType, Message: message, Err: err, Abort: true}}
}

func Continue(module, issueType, message string, err error) InitResult {
	return InitResult{Error: &InitError{Module: module, Type: issueType, Message: message, Err: err, Abort: false}}
}

func (r *Runtime) InitModules(ctx context.Context, modules []Module) error {
	for _, module := range modules {
		name := module.Name()
		r.Logger.Info("initializing module", "module", name)
		result := module.Init(ctx, r)
		if result.OK {
			r.Logger.Info("module initialized", "module", name)
			continue
		}
		if result.Error == nil {
			err := &InitError{Module: name, Type: "unknown", Message: "module returned non-ok result without error", Abort: true}
			r.Logger.Error("module initialization failed", "module", err.Module, "type", err.Type, "message", err.Message, "abort", err.Abort)
			return err
		}
		issue := result.Error
		attrs := []any{"module", issue.Module, "type", issue.Type, "message", issue.Message, "abort", issue.Abort}
		if issue.Err != nil {
			attrs = append(attrs, "error", issue.Err)
		}
		if issue.Abort {
			r.Logger.Error("module initialization failed", attrs...)
			return issue
		}
		r.Logger.Warn("module initialization issue; continuing", attrs...)
	}
	return nil
}
