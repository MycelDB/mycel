package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/myceldb/mycel/internal/daemon/config"
)

type Runtime struct {
	Config  config.Config
	Logger  *slog.Logger
	Modules map[string]Module
	LogPath string

	close func() error
}

func New(cfg config.Config, logger *slog.Logger, logPath string, close func() error) *Runtime {
	return &Runtime{Config: cfg, Logger: logger, Modules: map[string]Module{}, LogPath: logPath, close: close}
}

func (r *Runtime) Close() error {
	if r == nil {
		return nil
	}
	var firstErr error
	for _, module := range r.Modules {
		if closer, ok := module.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	if r.close != nil {
		if err := r.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (r *Runtime) Module(name string) (Module, bool) {
	if r == nil || r.Modules == nil {
		return nil, false
	}
	module, ok := r.Modules[name]
	return module, ok
}

func ModuleAs[T Module](r *Runtime, name string) (T, bool) {
	var zero T
	module, ok := r.Module(name)
	if !ok {
		return zero, false
	}
	typed, ok := module.(T)
	if !ok {
		return zero, false
	}
	return typed, true
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
	if r.Modules == nil {
		r.Modules = map[string]Module{}
	}
	for _, module := range modules {
		name := strings.TrimSpace(module.Name())
		if name == "" {
			return &InitError{Module: "", Type: "config", Message: "module name must not be empty", Abort: true}
		}
		if _, exists := r.Modules[name]; exists {
			return &InitError{Module: name, Type: "config", Message: "module is already registered", Abort: true}
		}
		r.Modules[name] = module
		r.Logger.Info("initializing module", "module", name)
		result := module.Init(ctx, r)
		if result.OK {
			r.Logger.Info("module initialized", "module", name)
			continue
		}
		if result.Error == nil {
			err := &InitError{Module: name, Type: "unknown", Message: "module returned non-ok result without error", Abort: true}
			r.Logger.Error("module initialization failed", "module", err.Module, "type", err.Type, "message", err.Message, "abort", err.Abort)
			delete(r.Modules, name)
			return err
		}
		issue := result.Error
		attrs := []any{"module", issue.Module, "type", issue.Type, "message", issue.Message, "abort", issue.Abort}
		if issue.Err != nil {
			attrs = append(attrs, "error", issue.Err)
		}
		if issue.Abort {
			r.Logger.Error("module initialization failed", attrs...)
			delete(r.Modules, name)
			return issue
		}
		r.Logger.Warn("module initialization issue; continuing", attrs...)
	}
	return nil
}
