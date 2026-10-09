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

// DefaultShutdownTimeout bounds each Auth listener drain.
const DefaultShutdownTimeout = 10 * time.Second

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
	ExternalAuthorization *ExternalAuthorization
	Handler               http.Handler
	Client                *dbconnect.Client
	Verifier              *auth.AssertionVerifier
	Pruner                *auth.TokenPruner
	PruningMetrics        workload.MetricsCollector
	UsageRecorder         *auth.APIKeyUsageRecorder
	UsageMetrics          workload.MetricsCollector
}

// Close is idempotent. The usage recorder stops first, discarding unwritten
// samples, so neither shutdown nor pool close waits for best-effort usage.
func (a *Application) Close() error {
	if a == nil {
		return nil
	}
	a.UsageRecorder.Close()
	if a.Pruner != nil {
		a.Pruner.Close()
	}
	if a.Verifier != nil {
		a.Verifier.Close()
	}
	if a.Client == nil {
		return nil
	}
	return a.Client.Close()
}

func OpenStartupDatabaseFromEnv(ctx context.Context) (StartupDatabase, error) {
	openResult, err := dbconnect.OpenProtectedDSNFromEnv(ctx)
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
	verifier, err := auth.NewConfiguredAssertionVerifier(ctx, auth.AssertionVerifierConfig{KeyCacheTTL: cfg.JWKSCacheTTL})
	if err != nil {
		_ = database.OpenResult.Client.Close()
		return nil, err
	}
	// HTTP and Check share one resolver and therefore this one recorder. Its
	// worker starts only after startup validation, before any listener opens.
	usage := auth.NewAPIKeyUsageRecorder(database.OpenResult.RawDatabaseForExcludedStores, opts.logger)
	routerConfig, err := buildRouterConfig(ctx, RouterBuildConfig{
		RawDatabase:       database.OpenResult.RawDatabaseForExcludedStores,
		AssertionVerifier: verifier,
		APIKeyUsage:       usage,
		Config:            cfg,
		Logger:            opts.logger,
		RequestMetrics:    opts.requestMetrics,
	})
	if err != nil {
		verifier.Close()
		_ = database.OpenResult.Client.Close()
		return nil, err
	}
	adapter, err := NewExternalAuthorization(ExternalAuthorizationConfig{Authenticator: &auth.RequestAuthenticator{Resolver: routerConfig.Resolver}, Signer: routerConfig.Signer, PrincipalTTL: cfg.InternalPrincipalTTL, Logger: opts.logger})
	if err != nil {
		verifier.Close()
		_ = database.OpenResult.Client.Close()
		return nil, err
	}
	handler := NewRouter(routerConfig)
	usage.Start(ctx)
	pruner := auth.StartTokenPruner(ctx, routerConfig.Resolver, opts.logger)
	return &Application{ExternalAuthorization: adapter, Handler: handler, Client: database.OpenResult.Client, Verifier: verifier, Pruner: pruner, PruningMetrics: pruner.Collector(), UsageRecorder: usage, UsageMetrics: usage.Collector()}, nil
}

type RouterBuildConfig struct {
	AssertionVerifier *auth.AssertionVerifier
	RawDatabase       *sql.DB
	// APIKeyUsage receives committed API-key admissions through the router's
	// resolver. Its owner starts and closes it; BuildRouter never starts a
	// worker, and nil records no usage.
	APIKeyUsage    *auth.APIKeyUsageRecorder
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
	routerConfig, err := buildRouterConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return NewRouter(routerConfig), nil
}

func buildRouterConfig(ctx context.Context, cfg RouterBuildConfig) (RouterConfig, error) {
	if cfg.RawDatabase == nil {
		return RouterConfig{}, fmt.Errorf("raw database is required")
	}
	signer, err := auth.NewInternalPrincipalSignerFromBase64(cfg.Config.InternalPrincipalPrivateKeyB64)
	if err != nil {
		return RouterConfig{}, err
	}
	store := auth.NewAPIKeyStore(cfg.RawDatabase)
	workspaceStore := workspace.NewStore(cfg.RawDatabase)
	if _, err := workspaceStore.Get(ctx, cfg.Config.BootstrapWorkspaceID); err != nil {
		return RouterConfig{}, err
	}
	if err := auth.RefreshBootstrap(ctx, store, cfg.Config.BootstrapWorkspaceID, cfg.Config.BootstrapAPIKey); err != nil {
		return RouterConfig{}, fmt.Errorf("bootstrap api key: %w", err)
	}
	return RouterConfig{
		Store:             store,
		Resolver:          auth.NewAuthorityResolver(cfg.RawDatabase, cfg.Config.BootstrapWorkspaceID, cfg.APIKeyUsage),
		AssertionVerifier: cfg.AssertionVerifier,
		ExchangeLimits:    cfg.Config.ExchangeLimits,
		Signer:            signer,
		Logger:            cfg.Logger,
		RequestMetrics:    cfg.RequestMetrics,
	}, nil
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
		ShutdownTimeout:       DefaultShutdownTimeout,
		Logger:                logger,
	}
}
