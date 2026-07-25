package dbconnect

import (
	"context"
	"testing"
	"time"
)

func TestPoolConfigFromEnvAppliesValidatedDefaultsAndOverrides(t *testing.T) {
	defaults, err := PoolConfigFromEnv(func(string) string { return "" })
	if err != nil {
		t.Fatalf("PoolConfigFromEnv defaults: %v", err)
	}
	if defaults.MaxOpenConns != 20 ||
		defaults.MaxIdleConns != 10 ||
		defaults.ConnMaxLifetime != 30*time.Minute ||
		defaults.ConnMaxIdleTime != 5*time.Minute ||
		defaults.StatementTimeout != time.Minute {
		t.Fatalf("default pool config = %#v", defaults)
	}

	values := map[string]string{
		EnvDBMaxOpenConns:     "40",
		EnvDBMaxIdleConns:     "15",
		EnvDBConnMaxLifetime:  "45m",
		EnvDBConnMaxIdleTime:  "7m",
		EnvDBStatementTimeout: "90s",
	}
	overrides, err := PoolConfigFromEnv(func(key string) string { return values[key] })
	if err != nil {
		t.Fatalf("PoolConfigFromEnv overrides: %v", err)
	}
	if overrides.MaxOpenConns != 40 ||
		overrides.MaxIdleConns != 15 ||
		overrides.ConnMaxLifetime != 45*time.Minute ||
		overrides.ConnMaxIdleTime != 7*time.Minute ||
		overrides.StatementTimeout != 90*time.Second {
		t.Fatalf("override pool config = %#v", overrides)
	}
}

func TestPoolConfigFromEnvRejectsNonPositiveAndInconsistentValues(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		key   string
		value string
	}{
		{name: "zero open", key: EnvDBMaxOpenConns, value: "0"},
		{name: "negative idle", key: EnvDBMaxIdleConns, value: "-1"},
		{name: "fractional open", key: EnvDBMaxOpenConns, value: "1.5"},
		{name: "zero lifetime", key: EnvDBConnMaxLifetime, value: "0s"},
		{name: "invalid idle time", key: EnvDBConnMaxIdleTime, value: "later"},
		{name: "zero statement timeout", key: EnvDBStatementTimeout, value: "0s"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := PoolConfigFromEnv(func(key string) string {
				if key == testCase.key {
					return testCase.value
				}
				return ""
			})
			if err == nil {
				t.Fatal("PoolConfigFromEnv accepted invalid pool config")
			}
		})
	}
}

func TestStatementTimeoutMillisecondsRoundsPositiveDurationsUpToPostgreSQLGranularity(t *testing.T) {
	if got := statementTimeoutMilliseconds(time.Nanosecond); got != 1 {
		t.Fatalf("statement timeout milliseconds = %d; want positive sub-millisecond duration rounded to 1", got)
	}
	if got := statementTimeoutMilliseconds(1500 * time.Microsecond); got != 2 {
		t.Fatalf("statement timeout milliseconds = %d; want 1.5ms rounded to 2", got)
	}
}

func TestOpenPlainDSNAppliesPoolBoundsAndStatementTimeout(t *testing.T) {
	dsn := requirePostgreSQLTestDSN(t)
	t.Setenv(EnvDBMaxOpenConns, "7")
	t.Setenv(EnvDBMaxIdleConns, "3")
	t.Setenv(EnvDBConnMaxLifetime, "11m")
	t.Setenv(EnvDBConnMaxIdleTime, "2m")
	t.Setenv(EnvDBStatementTimeout, "17s")

	result, err := OpenPlainDSN(context.Background(), "TETRAL_TEST_DATABASE_URL", dsn)
	if err != nil {
		t.Fatalf("OpenPlainDSN: %v", err)
	}
	defer func() { _ = result.Client.Close() }()

	stats := result.RawDatabaseForExcludedStores.Stats()
	if stats.MaxOpenConnections != 7 {
		t.Fatalf("MaxOpenConnections = %d; want 7", stats.MaxOpenConnections)
	}
	var statementTimeout string
	if err := result.RawDatabaseForExcludedStores.QueryRowContext(context.Background(), "SHOW statement_timeout").Scan(&statementTimeout); err != nil {
		t.Fatalf("SHOW statement_timeout: %v", err)
	}
	if statementTimeout != "17s" {
		t.Fatalf("statement_timeout = %q; want 17s", statementTimeout)
	}
}

func TestApplyPoolConfigSetsEveryBound(t *testing.T) {
	target := &recordingPoolConfigurator{}
	applyPoolConfig(target, PoolConfig{
		MaxOpenConns:    7,
		MaxIdleConns:    3,
		ConnMaxLifetime: 11 * time.Minute,
		ConnMaxIdleTime: 2 * time.Minute,
	})
	if target.maxOpen != 7 ||
		target.maxIdle != 3 ||
		target.maxLifetime != 11*time.Minute ||
		target.maxIdleTime != 2*time.Minute {
		t.Fatalf("applied pool config = %#v", target)
	}
}

type recordingPoolConfigurator struct {
	maxOpen     int
	maxIdle     int
	maxLifetime time.Duration
	maxIdleTime time.Duration
}

func (r *recordingPoolConfigurator) SetMaxOpenConns(value int) {
	r.maxOpen = value
}

func (r *recordingPoolConfigurator) SetMaxIdleConns(value int) {
	r.maxIdle = value
}

func (r *recordingPoolConfigurator) SetConnMaxLifetime(value time.Duration) {
	r.maxLifetime = value
}

func (r *recordingPoolConfigurator) SetConnMaxIdleTime(value time.Duration) {
	r.maxIdleTime = value
}
