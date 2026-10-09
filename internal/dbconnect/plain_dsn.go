package dbconnect

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"os"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/tetral-ai/tetral/internal/transportsecurity"
)

const envProductionDatabaseURL = "TETRAL_DATABASE_URL"

type OpenResult struct {
	Client *Client

	Provider   Provider
	Descriptor Descriptor

	RawDatabaseForExcludedStores *sql.DB
}

// OpenPlainDSN is the fixture and local opener: the connection honors only the
// DSN's own sslmode. Production stores open through OpenProtectedDSNFromEnv.
func OpenPlainDSN(ctx context.Context, envVarName string, dsn string) (OpenResult, error) {
	return openDSN(ctx, envVarName, dsn, nil, "")
}

// OpenProtectedDSNFromEnv is the production store composition. Local fixtures
// deliberately use OpenPlainDSN; protected composition cannot downgrade a URL.
func OpenProtectedDSNFromEnv(ctx context.Context) (OpenResult, error) {
	return OpenProtectedDSN(ctx, os.Getenv(envProductionDatabaseURL), os.Getenv("TETRAL_DATABASE_TLS_CA_PATH"), os.Getenv("TETRAL_DATABASE_TLS_SERVER_NAME"))
}

func OpenProtectedDSN(ctx context.Context, dsn, caPath, serverName string) (OpenResult, error) {
	owner, err := transportsecurity.Open(ctx, transportsecurity.Config{CAPath: caPath, Purpose: "database"})
	if err != nil {
		return OpenResult{}, err
	}
	if _, err = owner.ClientTLSConfig(serverName, ""); err != nil {
		_ = owner.Close()
		return OpenResult{}, err
	}
	result, err := openDSN(ctx, envProductionDatabaseURL, dsn, owner, serverName)
	if err != nil {
		_ = owner.Close()
		return OpenResult{}, err
	}
	return result, nil
}

// OpenProtectedConfig owns verified pgx configuration for administrative
// compositions that need both pgx and database/sql connections. Before each
// later connection, refresh TLSConfig with the owner's ClientTLSConfig and keep
// Fallbacks nil. The caller must close the returned credential owner.
func OpenProtectedConfig(ctx context.Context, dsn, caPath, serverName string) (*pgx.ConnConfig, *transportsecurity.Owner, error) {
	owner, err := transportsecurity.Open(ctx, transportsecurity.Config{CAPath: caPath, Purpose: "database"})
	if err != nil {
		return nil, nil, err
	}
	config, err := configurePlainDSN(dsn)
	if err == nil && strings.HasPrefix(config.Host, "/") {
		err = errors.New("protected PostgreSQL requires a TCP endpoint")
	}
	if err == nil {
		config.TLSConfig, err = owner.ClientTLSConfig(serverName, "")
		config.Fallbacks = nil
	}
	if err != nil {
		_ = owner.Close()
		return nil, nil, errors.New("protected PostgreSQL configuration is invalid")
	}
	return config, owner, nil
}

func openDSN(ctx context.Context, envVarName string, dsn string, tlsOwner *transportsecurity.Owner, serverName string) (OpenResult, error) {
	provider := ProviderPlainDSN
	if tlsOwner != nil {
		provider = ProviderProtectedDSN
	}
	if dsn == "" {
		return OpenResult{}, diagnostic(
			provider,
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
			provider,
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
			provider,
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
	var connectorOptions []stdlib.OptionOpenDB
	if tlsOwner != nil {
		if strings.HasPrefix(cfg.Host, "/") {
			return OpenResult{}, errors.New("protected PostgreSQL requires a TCP endpoint")
		}
		cfg.TLSConfig, err = tlsOwner.ClientTLSConfig(serverName, "")
		if err != nil {
			return OpenResult{}, err
		}
		cfg.Fallbacks = nil
		connectorOptions = append(connectorOptions, stdlib.OptionResetSession(func(_ context.Context, conn *pgx.Conn) error {
			if !tlsOwner.IsCurrentTLSConfig(conn.Config().TLSConfig) {
				return driver.ErrBadConn
			}
			return nil
		}), stdlib.OptionBeforeConnect(func(_ context.Context, config *pgx.ConnConfig) error {
			currentTLS, currentErr := tlsOwner.ClientTLSConfig(serverName, "")
			config.TLSConfig = currentTLS
			config.Fallbacks = nil
			return currentErr
		}))
	}
	connector := stdlib.GetConnector(*cfg, connectorOptions...)
	if tlsOwner != nil {
		connector = protectedConnector{Connector: connector, owner: tlsOwner}
	}
	db := sql.OpenDB(connector)
	applyPoolConfig(db, poolConfig)
	client := newClient(db, provider, descriptor)
	if tlsOwner != nil {
		client.closeResources = tlsOwner.Close
		if err := tlsOwner.SetActivationObserver(func() {
			// database/sql closes currently idle connections under its own locks.
			// Active operations remain leased; the driver validator retires them at
			// return. Restore the operator idle bound immediately for fresh sockets.
			db.SetMaxIdleConns(0)
			db.SetMaxIdleConns(poolConfig.MaxIdleConns)
		}); err != nil {
			_ = db.Close()
			return OpenResult{}, err
		}
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return OpenResult{}, diagnostic(
			provider,
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
		Provider:                     provider,
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
