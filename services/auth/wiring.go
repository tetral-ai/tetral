package tetralauth

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const defaultShutdownTimeout = 10 * time.Second

type StartupDatabase struct {
	OpenResult dbconnect.OpenResult
	Client     StartupReadinessClient
}

type StartupReadinessClient interface {
	VerifySchema(context.Context) error
	VerifyRuntimeRole(context.Context) error
}

type StartupOpenFunc func(context.Context) (StartupDatabase, error)

type Application struct {
	Handler http.Handler
	Client  *dbconnect.Client
}

func (a *Application) Close() error {
	if a == nil || a.Client == nil {
		return nil
	}
	return a.Client.Close()
}

func OpenStartupDatabaseFromEnv(ctx context.Context) (StartupDatabase, error) {
	openResult, err := dbconnect.OpenPlainDSNFromEnv(ctx)
	if err != nil {
		return StartupDatabase{}, err
	}
	return StartupDatabase{OpenResult: openResult, Client: openResult.Client}, nil
}

func BuildApplication(ctx context.Context, cfg Config, open StartupOpenFunc, options ...ApplicationOption) (*Application, error) {
	opts := applicationOptions{}
	for _, option := range options {
		if option != nil {
			option(&opts)
		}
	}
	if open == nil {
		open = OpenStartupDatabaseFromEnv
	}
	database, err := prepareStartupDatabase(ctx, open)
	if err != nil {
		return nil, err
	}
	handler, err := BuildRouter(ctx, RouterBuildConfig{
		RawDatabase:    database.OpenResult.RawDatabaseForExcludedStores,
		Config:         cfg,
		Logger:         opts.logger,
		RequestMetrics: opts.requestMetrics,
	})
	if err != nil {
		_ = database.OpenResult.Client.Close()
		return nil, err
	}
	return &Application{Handler: handler, Client: database.OpenResult.Client}, nil
}

type RouterBuildConfig struct {
	RawDatabase    *sql.DB
	Config         Config
	Logger         *slog.Logger
	RequestMetrics httpapi.RequestMetricsRecorder
}

type ApplicationOption func(*applicationOptions)

type applicationOptions struct {
	logger         *slog.Logger
	requestMetrics httpapi.RequestMetricsRecorder
}

func WithLogger(logger *slog.Logger) ApplicationOption {
	return func(opts *applicationOptions) {
		opts.logger = logger
	}
}

func WithRequestMetrics(metrics httpapi.RequestMetricsRecorder) ApplicationOption {
	return func(opts *applicationOptions) {
		opts.requestMetrics = metrics
	}
}

func BuildRouter(ctx context.Context, cfg RouterBuildConfig) (http.Handler, error) {
	if cfg.RawDatabase == nil {
		return nil, fmt.Errorf("raw database is required")
	}
	signer, err := auth.NewInternalPrincipalSignerFromBase64(cfg.Config.InternalPrincipalPrivateKeyB64)
	if err != nil {
		return nil, err
	}
	store := auth.NewAPIKeyStore(cfg.RawDatabase)
	workspaceStore := workspace.NewStore(cfg.RawDatabase)
	if _, err := workspaceStore.Get(ctx, cfg.Config.BootstrapWorkspaceID); err != nil {
		return nil, err
	}
	if err := auth.RefreshBootstrap(ctx, store, cfg.Config.BootstrapWorkspaceID, cfg.Config.BootstrapAPIKey); err != nil {
		return nil, fmt.Errorf("bootstrap api key: %w", err)
	}
	return NewRouter(RouterConfig{
		Store:               store,
		Signer:              signer,
		PrincipalTTLSeconds: int(cfg.Config.InternalPrincipalTTL.Seconds()),
		Logger:              cfg.Logger,
		RequestMetrics:      cfg.RequestMetrics,
	}), nil
}

func prepareStartupDatabase(ctx context.Context, open StartupOpenFunc) (StartupDatabase, error) {
	database, err := open(ctx)
	if err != nil {
		return StartupDatabase{}, err
	}
	if database.Client == nil {
		_ = closeStartupDatabase(database)
		return StartupDatabase{}, fmt.Errorf("startup database readiness client is required")
	}
	if database.OpenResult.RawDatabaseForExcludedStores == nil {
		_ = closeStartupDatabase(database)
		return StartupDatabase{}, fmt.Errorf("raw database is required")
	}
	if err := database.Client.VerifySchema(ctx); err != nil {
		_ = closeStartupDatabase(database)
		return StartupDatabase{}, fmt.Errorf("schema verification: %w", err)
	}
	if err := database.Client.VerifyRuntimeRole(ctx); err != nil {
		_ = closeStartupDatabase(database)
		return StartupDatabase{}, err
	}
	return database, nil
}

func closeStartupDatabase(database StartupDatabase) error {
	if database.OpenResult.Client == nil {
		return nil
	}
	return database.OpenResult.Client.Close()
}

func WorkloadConfig(cfg Config, handler http.Handler, readiness *workload.Readiness, logger *slog.Logger) workload.Config {
	return workload.Config{
		ServiceName:           "auth",
		DeploymentEnvironment: cfg.DeploymentEnvironment,
		ServiceVersion:        cfg.ServiceVersion,
		ListenAddress:         cfg.HTTPAddress,
		ListenConfigKey:       EnvHTTPAddress,
		Handler:               handler,
		Readiness:             readiness,
		ShutdownTimeout:       defaultShutdownTimeout,
		Logger:                logger,
	}
}
