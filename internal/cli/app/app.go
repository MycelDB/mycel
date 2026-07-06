package app

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/google/uuid"
	sessionapi "github.com/myceldb/mycel/internal/session/api"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

// App holds process/REPL state shared by commands.
type App struct {
	ConfigFile                  string
	UserRef                     string
	Password                    string
	Output                      string
	DaemonAddr                  string
	DaemonTLS                   bool
	DaemonTLSCAFile             string
	DaemonTLSServerName         string
	DaemonTLSInsecureSkipVerify bool
	DaemonTLSClientCertFile     string
	DaemonTLSClientKeyFile      string
	CurrentSpaceID              *domainspace.SpaceID
}

func DefaultOutput(v string) string {
	if v == "" {
		return "text"
	}
	return v
}

func (a *App) ResolveSpaceID(spaceIDText string) (domainspace.SpaceID, error) {
	if strings.TrimSpace(spaceIDText) != "" {
		return ParseUUID[domainspace.SpaceID](spaceIDText)
	}
	if a.CurrentSpaceID != nil {
		return *a.CurrentSpaceID, nil
	}
	return uuid.Nil, fmt.Errorf("--space-id is required; in REPL you can use set_space SPACE_ID")
}

func (a *App) SetCurrentSpace(spaceID domainspace.SpaceID) {
	a.CurrentSpaceID = &spaceID
}

func (a *App) Print(v any, text string) error {
	if a.Output == "json" {
		b, err := json.MarshalIndent(v, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(b))
		return nil
	}
	fmt.Print(text)
	return nil
}

func ReadTemplateDocument(filePath string) (sessionapi.ImportDocument, error) {
	var raw []byte
	var err error
	if filePath == "-" {
		raw, err = io.ReadAll(os.Stdin)
	} else {
		raw, err = os.ReadFile(filePath)
	}
	if err != nil {
		return sessionapi.ImportDocument{}, err
	}
	var doc sessionapi.ImportDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return sessionapi.ImportDocument{}, fmt.Errorf("invalid template JSON: %w", err)
	}
	return doc, nil
}

func ParseProps(raw string) (map[string]any, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, fmt.Errorf("invalid --props-json: %w", err)
	}
	return out, nil
}

func ParseUUID[T ~[16]byte](s string) (T, error) {
	id, err := uuid.Parse(s)
	if err != nil {
		var zero T
		return zero, err
	}
	return T(id), nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
