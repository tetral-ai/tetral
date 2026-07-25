// Package tetralapi owns service-local public API dependency composition.
package tetralapi

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"go/parser"
	"go/token"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/tetral-ai/tetral/internal/agent"
	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/encryption"
	"github.com/tetral-ai/tetral/internal/environment"
	"github.com/tetral-ai/tetral/internal/eventstream"
	"github.com/tetral-ai/tetral/internal/files"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/memory"
	"github.com/tetral-ai/tetral/internal/session"
	"github.com/tetral-ai/tetral/internal/sessionevent"
	"github.com/tetral-ai/tetral/internal/skill"
	"github.com/tetral-ai/tetral/internal/vault"
	"github.com/tetral-ai/tetral/internal/workload"
)

const (
	envSessionEventBodyByteCap                    = "TETRAL_SESSION_EVENT_BODY_BYTE_CAP"
	envSessionEventMaxEventsPerRequest            = "TETRAL_SESSION_EVENT_MAX_EVENTS_PER_REQUEST"
	envTetralAuthInternalPrincipalPublicKey       = "TETRAL_AUTH_INTERNAL_PRINCIPAL_PUBLIC_KEY_B64"
	envDefaultEnvironmentArtifactRef              = "TETRAL_DEFAULT_ENVIRONMENT_ARTIFACT_REF"
	defaultSessionEventBodyByteCap          int64 = 1 << 20
)

// RouterConfig carries public API composition dependencies.
type RouterConfig struct {
	RuntimeClient     *dbconnect.Client
	RawDatabase       *sql.DB
	VaultKey          string
	DataDir           string
	Logger            *slog.Logger
	Env               Env
	BlobStore         blob.BlobStore
	PrincipalVerifier *auth.InternalPrincipalVerifier
	HTTPMetrics       *workload.HTTPMetrics
}

// ProductionConfig carries startup inputs for the public API composition root.
type ProductionConfig struct {
	VaultKey                    string
	DataDir                     string
	Logger                      *slog.Logger
	Env                         Env
	Open                        StartupOpenFunc
	EnvironmentNetworkPreflight func(context.Context, *dbconnect.Client) error
	HTTPMetrics                 *workload.HTTPMetrics
}

// Application is a fully bootstrapped public API application.
type Application struct {
	Handler http.Handler
	Client  *dbconnect.Client
}

// Close releases application-owned resources.
func (a *Application) Close() error {
	if a == nil || a.Client == nil {
		return nil
	}
	return a.Client.Close()
}

// StartupDatabase is the opened DB state used during production bootstrap.
type StartupDatabase struct {
	OpenResult dbconnect.OpenResult
	Client     StartupReadinessClient
}

// StartupReadinessClient validates database readiness before serving.
type StartupReadinessClient interface {
	MigrateSchema(context.Context) error
	VerifyRuntimeRole(context.Context) error
}

// StartupOpenFunc opens the startup database state.
type StartupOpenFunc func(context.Context) (StartupDatabase, error)

// OpenStartupDatabaseFromEnv opens the configured PostgreSQL database.
func OpenStartupDatabaseFromEnv(ctx context.Context) (StartupDatabase, error) {
	openResult, err := dbconnect.OpenPlainDSNFromEnv(ctx)
	if err != nil {
		return StartupDatabase{}, err
	}
	return StartupDatabase{OpenResult: openResult, Client: openResult.Client}, nil
}

// BuildProductionApplication initializes DB readiness and builds the router.
func BuildProductionApplication(ctx context.Context, cfg ProductionConfig) (*Application, error) {
	if err := validatePublicAPIConfig(cfg.VaultKey); err != nil {
		return nil, err
	}
	open := cfg.Open
	if open == nil {
		open = OpenStartupDatabaseFromEnv
	}
	database, err := PrepareStartupDatabase(ctx, open)
	if err != nil {
		return nil, err
	}
	preflight := cfg.EnvironmentNetworkPreflight
	if preflight == nil {
		preflight = environment.VerifyNetworkingConfigRows
	}
	if err := preflight(ctx, database.OpenResult.Client); err != nil {
		if database.OpenResult.Client != nil {
			_ = database.OpenResult.Client.Close()
		}
		return nil, err
	}
	router, err := BuildRouter(ctx, RouterConfig{
		RuntimeClient: database.OpenResult.Client,
		RawDatabase:   database.OpenResult.RawDatabaseForExcludedStores,
		VaultKey:      cfg.VaultKey,
		DataDir:       cfg.DataDir,
		Logger:        cfg.Logger,
		Env:           cfg.Env,
		HTTPMetrics:   cfg.HTTPMetrics,
	})
	if err != nil {
		_ = database.OpenResult.Client.Close()
		return nil, err
	}
	return &Application{Handler: router, Client: database.OpenResult.Client}, nil
}

// PrepareStartupDatabase migrates schema and verifies runtime role before serving.
func PrepareStartupDatabase(ctx context.Context, open StartupOpenFunc) (StartupDatabase, error) {
	database, err := open(ctx)
	if err != nil {
		return StartupDatabase{}, err
	}
	if database.Client == nil {
		if database.OpenResult.Client != nil {
			_ = database.OpenResult.Client.Close()
		}
		return StartupDatabase{}, fmt.Errorf("runtime database client is required")
	}
	if err := database.Client.MigrateSchema(ctx); err != nil {
		if database.OpenResult.Client != nil {
			_ = database.OpenResult.Client.Close()
		}
		return StartupDatabase{}, fmt.Errorf("schema migration: %w", err)
	}
	if err := database.Client.VerifyRuntimeRole(ctx); err != nil {
		if database.OpenResult.Client != nil {
			_ = database.OpenResult.Client.Close()
		}
		return StartupDatabase{}, err
	}
	return database, nil
}

type osEnv struct{}

func (osEnv) Getenv(key string) string { return os.Getenv(key) }

// BuildRouter constructs the production public API router.
func BuildRouter(ctx context.Context, cfg RouterConfig) (http.Handler, error) {
	if err := validatePublicAPIConfig(cfg.VaultKey); err != nil {
		return nil, err
	}
	if cfg.RuntimeClient == nil {
		return nil, fmt.Errorf("runtime client is required")
	}
	if cfg.RawDatabase == nil {
		return nil, fmt.Errorf("raw database is required")
	}
	if cfg.DataDir == "" {
		cfg.DataDir = "/var/tetral"
	}
	if err := EnsureDataDir(cfg.DataDir); err != nil {
		return nil, err
	}
	if cfg.Env == nil {
		cfg.Env = osEnv{}
	}

	principalVerifier := cfg.PrincipalVerifier
	if principalVerifier == nil {
		var err error
		principalVerifier, err = loadInternalPrincipalVerifierFromEnv(cfg.Env)
		if err != nil {
			return nil, fmt.Errorf("internal principal verifier configuration: %w", err)
		}
	}

	runtimeControlConfig, err := loadRuntimeControlConfigFromEnv(cfg.Env)
	if err != nil {
		return nil, fmt.Errorf("runtime control configuration: %w", err)
	}
	defaultEnvironmentArtifactRef, err := loadDefaultEnvironmentArtifactRefFromEnv(cfg.Env)
	if err != nil {
		return nil, err
	}
	encryptor, err := encryption.NewAES256GCMEncryptor(cfg.VaultKey)
	if err != nil {
		return nil, err
	}
	eventPageTokenSecret, err := hex.DecodeString(cfg.VaultKey)
	if err != nil {
		return nil, fmt.Errorf("event page token secret: %w", err)
	}
	blobStore, err := buildOptionalBlobStore(ctx, cfg.BlobStore)
	if err != nil {
		return nil, err
	}
	var fileStore *files.PostgreSQLFileStore
	if blobStore != nil {
		fileStore = files.NewPostgreSQLStore(cfg.RuntimeClient, blobStore)
	}

	agentStore := agent.NewPostgreSQLAgentStore(cfg.RuntimeClient)
	environmentStore := environment.NewPostgreSQLEnvironmentStore(
		cfg.RuntimeClient,
		environment.WithDefaultArtifactRef(defaultEnvironmentArtifactRef),
	)
	vaultStore := vault.NewPostgreSQLVaultStore(cfg.RuntimeClient)
	credentialStore := vault.NewPostgreSQLCredentialStore(cfg.RuntimeClient, encryptor)
	memoryStore := memory.NewPostgreSQLStore(cfg.RuntimeClient)
	sessionStore := session.NewPostgreSQLSessionStore(
		cfg.RuntimeClient,
		session.WithPageTokenSecret(eventPageTokenSecret),
	)
	environmentService := environment.NewService(environmentStore)
	vaultService := vault.NewService(vaultStore, credentialStore)
	memoryService := memory.NewService(memoryStore)
	fileIdentityService := buildFileIdentityService(cfg.RuntimeClient)

	sessionEventStoreOptions := make([]sessionevent.PostgreSQLStoreOption, 0, 1)
	if fileStore != nil {
		sessionEventStoreOptions = append(sessionEventStoreOptions, sessionevent.WithFileAttachmentValidator(fileStore))
	}
	sessionEventService := sessionevent.NewService(
		sessionevent.NewPostgreSQLStore(cfg.RuntimeClient, sessionEventStoreOptions...),
		sessionevent.WithLimits(runtimeControlConfig.SessionEventLimits),
	)
	sessionEventHandler := httpapi.NewSessionEventHandler(
		sessionEventService,
		httpapi.WithSessionEventBodyByteCap(runtimeControlConfig.SessionEventBodyByteCap),
		httpapi.WithSessionEventLimits(runtimeControlConfig.SessionEventLimits),
	)
	sessionEventListHandler := eventstream.NewListHandler(eventstream.NewPostgreSQLReader(
		cfg.RuntimeClient,
		eventstream.WithPageTokenSecret(eventPageTokenSecret),
	))

	skillRefResolver := buildSkillReferenceResolver(cfg.RuntimeClient, eventPageTokenSecret)
	agentService := agent.NewService(
		agentStore,
		skillRefResolver,
		agent.WithPageTokenSecret(eventPageTokenSecret),
	)
	sessionService := session.NewService(
		agentService,
		environmentService,
		fileIdentityService,
		memoryService,
		vaultService,
		sessionStore,
		encryptor,
	)

	routerOpts := []httpapi.RouterOption{
		httpapi.WithInternalPrincipalVerifier(principalVerifier),
		httpapi.WithAgentHandler(httpapi.NewAgentHandler(agentService)),
		httpapi.WithEnvironmentHandler(httpapi.NewEnvironmentHandler(environmentService)),
		httpapi.WithVaultHandler(httpapi.NewVaultHandler(vaultService)),
		httpapi.WithMemoryHandler(httpapi.NewMemoryHandler(memoryService)),
		httpapi.WithSessionEventHandler(sessionEventHandler),
		httpapi.WithSessionEventListHandler(sessionEventListHandler),
	}
	if cfg.HTTPMetrics != nil {
		routerOpts = append(routerOpts, httpapi.WithRequestMetrics(cfg.HTTPMetrics))
	}
	if cfg.Logger != nil {
		routerOpts = append(routerOpts, httpapi.WithLogger(cfg.Logger))
	}
	if skillHandler, err := buildSkillHandler(ctx, cfg.RuntimeClient, cfg.DataDir, blobStore, eventPageTokenSecret); err != nil {
		return nil, err
	} else if skillHandler != nil {
		routerOpts = append(routerOpts, httpapi.WithSkillHandler(skillHandler))
	}
	fileHandler, _, err := buildFileHandler(cfg.DataDir, fileStore)
	if err != nil {
		return nil, fmt.Errorf("files: %w", err)
	}
	if fileHandler != nil {
		routerOpts = append(routerOpts, httpapi.WithFileHandler(fileHandler))
	}
	return httpapi.NewRouter(httpapi.NewSessionHandler(sessionService), "", routerOpts...), nil
}

// asStartupConfigError re-expresses a config-VALIDATION error from a foreign
// package (blob.ConfigError) or auth's WeakBootstrapKeyError
// as a *workload.ConfigError carrying that error's safe static message, so the
// startup-failure log classifies it config_error and emits the operator-facing
// text instead of a class-only line.
//
// This is the SINGLE conversion point for foreign config-validation errors. It
// is applied ONLY at config-validation call sites (Bucket C); dependency and
// construction sites (Bucket D — DB open, schema init, blob.NewS3BlobStore)
// are NOT routed through it and
// therefore stay class-only. The contract is that every wrapped error's Error()
// is already operator-safe (names a config KEY, never echoes a secret value);
// blob ConfigError and WeakBootstrapKeyError both satisfy that.
//
// An error that is already a *workload.ConfigError passes through unchanged
// (no double wrapping); nil returns nil so the helper is safe at a success path.
func asStartupConfigError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := workload.AsConfigError(err); ok {
		return err
	}
	return workload.NewConfigError(err.Error())
}

func validatePublicAPIConfig(vaultKey string) error {
	return ValidateVaultKey(vaultKey)
}

func loadInternalPrincipalVerifierFromEnv(env Env) (*auth.InternalPrincipalVerifier, error) {
	raw := env.Getenv(envTetralAuthInternalPrincipalPublicKey)
	if raw == "" {
		return nil, workload.NewConfigError(envTetralAuthInternalPrincipalPublicKey + " is required")
	}
	verifier, err := auth.NewInternalPrincipalVerifierFromBase64(raw)
	if err != nil {
		return nil, asStartupConfigError(err)
	}
	return verifier, nil
}

func buildSkillReferenceResolver(runtimeClient *dbconnect.Client, pageTokenSecret []byte) agent.SkillReferenceResolver {
	skillStore := skill.NewPostgreSQLStore(runtimeClient, nil)
	return skill.NewService(skillStore, skill.WithPageTokenSecret(pageTokenSecret))
}

func buildFileIdentityService(runtimeClient *dbconnect.Client) session.FileIdentityService {
	fileStore := files.NewPostgreSQLStore(runtimeClient, nil)
	return files.NewService(fileStore)
}

func buildOptionalBlobStore(ctx context.Context, suppliedBlobStore blob.BlobStore) (blob.BlobStore, error) {
	if suppliedBlobStore != nil {
		return suppliedBlobStore, nil
	}
	if !blob.ConfigPresent() {
		return nil, nil
	}
	cfg, err := blob.LoadConfig()
	if err != nil {
		// Bucket C: config validation. Convert the foreign blob ConfigError to a
		// workload.ConfigError so startup classifies it config_error.
		return nil, asStartupConfigError(err)
	}
	if err := cfg.AssertProductionReady(); err != nil {
		return nil, asStartupConfigError(err)
	}
	blobStore, err := blob.NewS3BlobStore(ctx, cfg)
	if err != nil {
		// Bucket D: client construction. Stays class-only (not converted).
		return nil, fmt.Errorf("blob store: %w", err)
	}
	return blobStore, nil
}

func buildSkillHandler(ctx context.Context, runtimeClient *dbconnect.Client, dataDir string, suppliedBlobStore blob.BlobStore, pageTokenSecret []byte) (*httpapi.SkillHandler, error) {
	blobStore := suppliedBlobStore
	if blobStore == nil {
		return nil, nil
	}
	stageDir, err := skill.NewStageDir(dataDir)
	if err != nil {
		return nil, fmt.Errorf("skill upload stage directory: %w", err)
	}
	skillStore := skill.NewPostgreSQLStore(runtimeClient, blobStore)
	skillService := skill.NewService(
		skillStore,
		skill.WithPackageStageDir(stageDir),
		skill.WithPageTokenSecret(pageTokenSecret),
	)
	return httpapi.NewSkillHandler(skillService, stageDir), nil
}

func buildFileHandler(dataDir string, fileStore *files.PostgreSQLFileStore) (*httpapi.FileHandler, *files.Service, error) {
	if fileStore == nil {
		return nil, nil, nil
	}
	stageDir, err := files.NewStageDir(dataDir)
	if err != nil {
		return nil, nil, fmt.Errorf("upload stage directory: %w", err)
	}
	fileService := files.NewService(fileStore)
	return httpapi.NewFileHandler(fileService, stageDir, httpapi.FileHandlerLimits{}), fileService, nil
}

type runtimeControlConfig struct {
	SessionEventBodyByteCap int64
	MaxEventsPerRequest     int
	SessionEventLimits      sessionevent.Limits
}

func loadRuntimeControlConfigFromEnv(env Env) (runtimeControlConfig, error) {
	config := defaultRuntimeControlConfig()
	var err error
	if config.SessionEventBodyByteCap, err = loadPositiveInt64(env, envSessionEventBodyByteCap, config.SessionEventBodyByteCap); err != nil {
		return runtimeControlConfig{}, err
	}
	if config.MaxEventsPerRequest, err = loadPositiveInt(env, envSessionEventMaxEventsPerRequest, config.MaxEventsPerRequest); err != nil {
		return runtimeControlConfig{}, err
	}
	config.SessionEventLimits = sessionevent.Limits{
		MaxEventsPerRequest: config.MaxEventsPerRequest,
	}
	return config, nil
}

func loadDefaultEnvironmentArtifactRefFromEnv(env Env) (string, error) {
	if env == nil {
		env = osEnv{}
	}
	ref := env.Getenv(envDefaultEnvironmentArtifactRef)
	if ref == "" {
		return "", workload.NewConfigError(envDefaultEnvironmentArtifactRef + " is required")
	}
	return ref, nil
}

func defaultRuntimeControlConfig() runtimeControlConfig {
	limits := sessionevent.DefaultLimits()
	return runtimeControlConfig{
		SessionEventBodyByteCap: defaultSessionEventBodyByteCap,
		MaxEventsPerRequest:     limits.MaxEventsPerRequest,
		SessionEventLimits:      limits,
	}
}

func loadPositiveInt(env Env, key string, defaultValue int) (int, error) {
	raw := env.Getenv(key)
	if raw == "" {
		return defaultValue, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, workload.NewConfigError(fmt.Sprintf("%s must be a positive integer", key))
	}
	return value, nil
}

func loadRequiredPositiveInt(env Env, key string) (int, error) { //nolint:unused // Scanned by startup error inventory tests.
	raw := env.Getenv(key)
	if raw == "" {
		return 0, workload.NewConfigError(key + " is required")
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, workload.NewConfigError(fmt.Sprintf("%s must be a positive integer", key))
	}
	return value, nil
}

func loadPositiveInt64(env Env, key string, defaultValue int64) (int64, error) {
	raw := env.Getenv(key)
	if raw == "" {
		return defaultValue, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 {
		return 0, workload.NewConfigError(fmt.Sprintf("%s must be a positive integer", key))
	}
	return value, nil
}

// EnsureDataDir creates a service-owned data directory with owner-only permissions.
// The mode rejection is operator-misconfiguration: it names ENGINE_DATA_DIR and the
// offending mode (both safe static text) as a ConfigError so startup logs the
// diagnosis. The configured path itself is an operator-supplied value and is
// deliberately NOT interpolated into the message — a path can carry a secret-
// shaped segment, so only the env key name and the shape-validated mode appear.
// The MkdirAll/Stat failures are filesystem/dependency failures and stay class-only.
func EnsureDataDir(path string) error {
	if err := os.MkdirAll(path, 0700); err != nil { //nolint:gosec // path is service config, validated by deployment ownership.
		return fmt.Errorf("create data dir: %w", err)
	}
	info, err := os.Stat(path) //nolint:gosec // path is service config, validated by deployment ownership.
	if err != nil {
		return fmt.Errorf("stat data dir: %w", err)
	}
	if info.Mode().Perm()&0077 != 0 {
		return workload.NewConfigError(fmt.Sprintf("ENGINE_DATA_DIR directory has mode %04o; must not be group/other accessible", info.Mode().Perm()))
	}
	return nil
}

// ValidateVaultKey checks the AES-256-GCM key used for credential encryption.
// All rejections are ConfigErrors: their messages name ENGINE_VAULT_KEY and its
// required shape only, never the key material itself. The hex-decodability check
// runs here so a 64-character-but-non-hex key is rejected up front with safe
// static text, keeping the raw hex.DecodeString error inside
// encryption.NewAES256GCMEncryptor — which echoes the offending byte —
// unreachable for env-supplied keys.
func ValidateVaultKey(key string) error {
	if key == "" {
		return workload.NewConfigError("ENGINE_VAULT_KEY environment variable is required")
	}
	if len(key) != 64 {
		return workload.NewConfigError(fmt.Sprintf("ENGINE_VAULT_KEY must be 64 hex characters (32 bytes), got %d characters", len(key)))
	}
	if _, err := hex.DecodeString(key); err != nil {
		return workload.NewConfigError("ENGINE_VAULT_KEY must be 64 hexadecimal characters (32 bytes); it contains non-hexadecimal characters")
	}
	return nil
}

// AssertNoOldWorkerImports proves the public API composition no longer wires removed worker packages.
func AssertNoOldWorkerImports(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(dir, name), nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imported := range file.Imports {
			value := strings.Trim(imported.Path.Value, `"`)
			for _, forbidden := range []string{
				"github.com/tetral-ai/tetral/internal/" + "worker",
				"github.com/tetral-ai/tetral/internal/" + "workerlaunch",
			} {
				if strings.HasPrefix(value, forbidden) {
					return fmt.Errorf("%s imports removed worker package %s", name, value)
				}
			}
		}
	}
	return nil
}
