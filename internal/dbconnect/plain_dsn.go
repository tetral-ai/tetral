package dbconnect

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

const envProductionDatabaseURL = "TETRAL_DATABASE_URL"

type OpenResult struct {
	Client *Client

	Provider   Provider
	Descriptor Descriptor

	RawDatabaseForExcludedStores *sql.DB
}

func OpenPlainDSNFromEnv(ctx context.Context) (OpenResult, error) {
	return OpenPlainDSN(ctx, envProductionDatabaseURL, os.Getenv(envProductionDatabaseURL))
}

func OpenPlainDSN(ctx context.Context, envVarName string, dsn string) (OpenResult, error) {
	if dsn == "" {
		return OpenResult{}, diagnostic(
			ProviderPlainDSN,
			unknownDescriptor(),
			PhaseParseConfig,
			KindInvalidConfig,
			"",
			envVarName+" is not set",
			errors.New("missing PostgreSQL DSN"),
		)
	}

	poolConfig, err := PoolConfigFromEnv(os.Getenv)
	if err != nil {
		return OpenResult{}, diagnostic(
			ProviderPlainDSN,
			unknownDescriptor(),
			PhaseParseConfig,
			KindInvalidConfig,
			"",
			"database pool config invalid",
			err,
		)
	}
	cfg, err := configurePlainDSN(dsn)
	if err != nil {
		return OpenResult{}, diagnostic(
			ProviderPlainDSN,
			unknownDescriptor(),
			PhaseParseConfig,
			KindInvalidConfig,
			"",
			envVarName+" parse failed",
			err,
		)
	}
	descriptor := descriptorFromConfig(cfg)
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = make(map[string]string)
	}
	cfg.RuntimeParams["statement_timeout"] = strconv.FormatInt(statementTimeoutMilliseconds(poolConfig.StatementTimeout), 10)
	db := sql.OpenDB(stdlib.GetConnector(*cfg))
	applyPoolConfig(db, poolConfig)
	client := newClient(db, ProviderPlainDSN, descriptor)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return OpenResult{}, diagnostic(
			ProviderPlainDSN,
			descriptor,
			PhasePing,
			classifyOpenOrPingKind(err),
			"",
			"ping failed",
			err,
		)
	}
	return OpenResult{
		Client:                       client,
		Provider:                     ProviderPlainDSN,
		Descriptor:                   descriptor,
		RawDatabaseForExcludedStores: db,
	}, nil
}

func configurePlainDSN(dsn string) (*pgx.ConnConfig, error) {
	return pgx.ParseConfig(dsn)
}

func descriptorFromConfig(cfg *pgx.ConnConfig) Descriptor {
	host := cfg.Host
	if host == "" {
		host = UnknownDescriptorField
	}
	database := cfg.Database
	if database == "" {
		database = UnknownDescriptorField
	}
	return Descriptor{Host: host, Database: database}
}

func unknownDescriptor() Descriptor {
	return Descriptor{Host: UnknownDescriptorField, Database: UnknownDescriptorField}
}

func classifyOpenOrPingKind(err error) Kind {
	switch {
	case errors.Is(err, context.Canceled):
		return KindCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return KindTimeout
	}
	lower := strings.ToLower(err.Error())
	switch {
	case strings.Contains(lower, "connection refused"),
		strings.Contains(lower, "no route to host"),
		strings.Contains(lower, "network is unreachable"),
		strings.Contains(lower, "connection reset"):
		return KindEndpointUnreachable
	case strings.Contains(lower, "password authentication failed"),
		strings.Contains(lower, "authentication failed"):
		return KindAuthenticationFailed
	case strings.Contains(lower, "tls"),
		strings.Contains(lower, "ssl"):
		return KindTLSFailed
	case strings.Contains(lower, "permission denied"):
		return KindPermissionDenied
	default:
		return KindInternalError
	}
}
