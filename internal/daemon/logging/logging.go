package logging

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/myceldb/mycel/internal/fsperm"
)

const (
	DefaultMaxBytes int64 = 100 * 1024 * 1024
	DefaultMaxFiles       = 7
)

type Config struct {
	Level    string
	Format   string
	Path     string
	Stderr   bool
	MaxBytes int64
	MaxFiles int
}

type Logger struct {
	*slog.Logger
	close func() error
}

func Configure(cfg Config) (*Logger, error) {
	level, err := ParseLevel(cfg.Level)
	if err != nil {
		return nil, err
	}
	maxBytes := cfg.MaxBytes
	if maxBytes == 0 {
		maxBytes = DefaultMaxBytes
	}
	if maxBytes <= 0 {
		return nil, fmt.Errorf("log max bytes must be positive")
	}
	maxFiles := cfg.MaxFiles
	if maxFiles == 0 {
		maxFiles = DefaultMaxFiles
	}
	if maxFiles <= 0 {
		return nil, fmt.Errorf("log max files must be positive")
	}
	file, err := newRollingFile(cfg.Path, maxBytes, maxFiles)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}
	var writer io.Writer = file
	if cfg.Stderr {
		writer = io.MultiWriter(file, os.Stderr)
	}
	opts := &slog.HandlerOptions{Level: level}
	var handler slog.Handler
	switch strings.ToLower(strings.TrimSpace(cfg.Format)) {
	case "json":
		handler = slog.NewJSONHandler(writer, opts)
	case "text", "":
		handler = slog.NewTextHandler(writer, opts)
	default:
		_ = file.Close()
		return nil, fmt.Errorf("unsupported log format %q", cfg.Format)
	}
	logger := slog.New(handler)
	return &Logger{Logger: logger, close: file.Close}, nil
}

func (l *Logger) Close() error {
	if l == nil || l.close == nil {
		return nil
	}
	return l.close()
}

func ParseLevel(value string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info", "":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, fmt.Errorf("unsupported log level %q", value)
	}
}

type rollingFile struct {
	mu       sync.Mutex
	path     string
	maxBytes int64
	maxFiles int
	file     *os.File
	size     int64
	closed   bool
}

func newRollingFile(path string, maxBytes int64, maxFiles int) (*rollingFile, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("log path must not be empty")
	}
	if maxBytes <= 0 {
		return nil, fmt.Errorf("log max bytes must be positive")
	}
	if maxFiles <= 0 {
		return nil, fmt.Errorf("log max files must be positive")
	}

	if info, err := os.Stat(path); err == nil {
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("log path is not a regular file")
		}
		if info.Size() > maxBytes {
			if err := rotateLogFiles(path, maxFiles); err != nil {
				return nil, err
			}
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}

	file, size, err := openLogFile(path)
	if err != nil {
		return nil, err
	}
	return &rollingFile{path: path, maxBytes: maxBytes, maxFiles: maxFiles, file: file, size: size}, nil
}

func (f *rollingFile) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed || f.file == nil {
		return 0, os.ErrClosed
	}
	if len(p) == 0 {
		return 0, nil
	}
	if f.size > 0 && f.size+int64(len(p)) > f.maxBytes {
		if err := f.rotateLocked(); err != nil {
			return 0, err
		}
	}
	n, err := f.file.Write(p)
	f.size += int64(n)
	return n, err
}

func (f *rollingFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	f.closed = true
	if f.file == nil {
		return nil
	}
	file := f.file
	f.file = nil
	return file.Close()
}

func (f *rollingFile) rotateLocked() error {
	file := f.file
	f.file = nil
	if err := file.Close(); err != nil {
		return err
	}
	if err := rotateLogFiles(f.path, f.maxFiles); err != nil {
		file, size, openErr := openLogFile(f.path)
		if openErr == nil {
			f.file = file
			f.size = size
		}
		return err
	}
	file, size, err := openLogFile(f.path)
	if err != nil {
		return err
	}
	f.file = file
	f.size = size
	return nil
}

func openLogFile(path string) (*os.File, int64, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, fsperm.PrivateFile)
	if err != nil {
		return nil, 0, err
	}
	if err := file.Chmod(fsperm.PrivateFile); err != nil {
		_ = file.Close()
		return nil, 0, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, 0, err
	}
	return file, info.Size(), nil
}

func rotateLogFiles(path string, maxFiles int) error {
	for index := maxFiles; index >= 1; index-- {
		destination := rotatedLogPath(path, index)
		if err := os.Remove(destination); err != nil && !os.IsNotExist(err) {
			return err
		}
		source := rotatedLogPath(path, index-1)
		if index == 1 {
			source = path
		}
		if _, err := os.Stat(source); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		if err := os.Chmod(source, fsperm.PrivateFile); err != nil {
			return err
		}
		if err := os.Rename(source, destination); err != nil {
			return err
		}
	}
	return nil
}

func rotatedLogPath(path string, index int) string {
	return path + "." + strconv.Itoa(index)
}
