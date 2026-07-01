package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/domain/identity"
	domainspace "github.com/myceldb/mycel/domain/space"
	mycelengine "github.com/myceldb/mycel/engine"
	mycelconfig "github.com/myceldb/mycel/internal/config"
	domainsession "github.com/myceldb/mycel/session"
)

// App holds process/REPL state shared by commands.
type App struct {
	DataDir                      string
	ConfigFile                   string
	UserRef                      string
	Password                     string
	Output                       string
	UserStoreEncryptionKeyB64    string
	AuthTokenTTL                 string
	AuthRefreshIdleTTL           string
	AuthRefreshAbsoluteTTL       string
	AuthRefreshAuditRetentionTTL string
	AuthRefreshTokenBytes        int
	BlobStaleTmpAge              string
	BlobMaxSizeBytes             int64
	BlobMaxImageBytes            int64
	BlobMaxPDFBytes              int64
	BlobMaxAudioBytes            int64
	BlobMaxVideoBytes            int64
	BlobMaxOtherBytes            int64
	Engine                       mycelengine.Engine
	Token                        mycelengine.AccessToken
	CurrentSpaceID               *domainspace.SpaceID
	Config                       mycelconfig.Config
}

func DefaultOutput(v string) string {
	if v == "" {
		return "text"
	}
	return v
}

func (a *App) EnsureEngine(ctx context.Context) error {
	if a.Engine != nil {
		return nil
	}
	if strings.TrimSpace(a.Config.DataDir) == "" {
		cfg, err := mycelconfig.Load(mycelconfig.Options{ConfigFile: a.ConfigFile})
		if err != nil {
			return err
		}
		a.Config = cfg
	}
	a.DataDir = mycelengine.ResolveDataDir(firstNonEmpty(a.DataDir, a.Config.DataDir))
	if strings.TrimSpace(a.DataDir) == "" {
		return fmt.Errorf("--data-dir/-d is required or %s must be set", mycelengine.EnvDataDir)
	}
	if strings.TrimSpace(a.UserRef) == "" || strings.TrimSpace(a.Password) == "" {
		return fmt.Errorf("--username/-u and --password/-p are required outside a logged-in REPL")
	}
	engineCfg := a.Config.EngineConfig()
	engineCfg.DataDir = a.DataDir
	engineCfg.CreateIfMissing = false
	eng, err := mycelengine.NewEngine(engineCfg, nil, nil, nil, nil)
	if err != nil {
		return err
	}
	a.Engine = eng
	return nil
}

func (a *App) AccessToken(ctx context.Context) (mycelengine.AccessToken, error) {
	if a.Token != "" {
		return a.Token, nil
	}
	if err := a.EnsureEngine(ctx); err != nil {
		return "", err
	}
	res, err := a.Engine.Authenticate(ctx, mycelengine.AuthInput{UserRef: identity.UserRef(a.UserRef), Password: a.Password})
	if err != nil {
		return "", err
	}
	return res.AccessToken, nil
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

func (a *App) SetCurrentSpace(ctx context.Context, spaceID domainspace.SpaceID) error {
	tok, err := a.AccessToken(ctx)
	if err != nil {
		return err
	}
	sess, err := a.Engine.OpenSession(ctx, mycelengine.OpenSessionInput{AccessToken: tok, SpaceID: spaceID})
	if err != nil {
		return err
	}
	_ = sess.Close()
	a.CurrentSpaceID = &spaceID
	return nil
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

func ReadTemplateDocument(filePath string) (domainsession.ImportDocument, error) {
	var raw []byte
	var err error
	if filePath == "-" {
		raw, err = io.ReadAll(os.Stdin)
	} else {
		raw, err = os.ReadFile(filePath)
	}
	if err != nil {
		return domainsession.ImportDocument{}, err
	}
	var doc domainsession.ImportDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return domainsession.ImportDocument{}, fmt.Errorf("invalid template JSON: %w", err)
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
