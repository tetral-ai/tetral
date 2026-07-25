package dbconnect

import (
	"fmt"
	"strconv"
	"time"
)

const (
	EnvDBMaxOpenConns     = "TETRAL_DB_MAX_OPEN_CONNS"
	EnvDBMaxIdleConns     = "TETRAL_DB_MAX_IDLE_CONNS"
	EnvDBConnMaxLifetime  = "TETRAL_DB_CONN_MAX_LIFETIME"
	EnvDBConnMaxIdleTime  = "TETRAL_DB_CONN_MAX_IDLE_TIME"
	EnvDBStatementTimeout = "TETRAL_DB_STATEMENT_TIMEOUT"
)

type PoolConfig struct {
	MaxOpenConns     int
	MaxIdleConns     int
	ConnMaxLifetime  time.Duration
	ConnMaxIdleTime  time.Duration
	StatementTimeout time.Duration
}

type poolConfigurator interface {
	SetMaxOpenConns(int)
	SetMaxIdleConns(int)
	SetConnMaxLifetime(time.Duration)
	SetConnMaxIdleTime(time.Duration)
}

func PoolConfigFromEnv(getenv func(string) string) (PoolConfig, error) {
	if getenv == nil {
		return PoolConfig{}, fmt.Errorf("database pool environment reader is required")
	}
	maxOpen, err := positiveIntOrDefault(getenv(EnvDBMaxOpenConns), 20, EnvDBMaxOpenConns)
	if err != nil {
		return PoolConfig{}, err
	}
	maxIdle, err := positiveIntOrDefault(getenv(EnvDBMaxIdleConns), 10, EnvDBMaxIdleConns)
	if err != nil {
		return PoolConfig{}, err
	}
	maxLifetime, err := positiveDurationOrDefault(getenv(EnvDBConnMaxLifetime), 30*time.Minute, EnvDBConnMaxLifetime)
	if err != nil {
		return PoolConfig{}, err
	}
	maxIdleTime, err := positiveDurationOrDefault(getenv(EnvDBConnMaxIdleTime), 5*time.Minute, EnvDBConnMaxIdleTime)
	if err != nil {
		return PoolConfig{}, err
	}
	statementTimeout, err := positiveDurationOrDefault(getenv(EnvDBStatementTimeout), time.Minute, EnvDBStatementTimeout)
	if err != nil {
		return PoolConfig{}, err
	}
	return PoolConfig{
		MaxOpenConns:     maxOpen,
		MaxIdleConns:     maxIdle,
		ConnMaxLifetime:  maxLifetime,
		ConnMaxIdleTime:  maxIdleTime,
		StatementTimeout: statementTimeout,
	}, nil
}

func statementTimeoutMilliseconds(value time.Duration) int64 {
	milliseconds := value.Milliseconds()
	if value%time.Millisecond != 0 {
		milliseconds++
	}
	return milliseconds
}

func applyPoolConfig(target poolConfigurator, cfg PoolConfig) {
	target.SetMaxOpenConns(cfg.MaxOpenConns)
	target.SetMaxIdleConns(cfg.MaxIdleConns)
	target.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	target.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)
}

func positiveIntOrDefault(raw string, fallback int, name string) (int, error) {
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return value, nil
}

func positiveDurationOrDefault(raw string, fallback time.Duration, name string) (time.Duration, error) {
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", name)
	}
	return value, nil
}
