package tetralqueue

import (
	"strings"
	"testing"
	"time"
)

type configEnv map[string]string

func (e configEnv) Getenv(key string) string { return e[key] }

func TestConfigFromEnvPinsQueueRetryPolicy(t *testing.T) {
	cfg, err := ConfigFromEnv(configEnv{})
	if err != nil {
		t.Fatalf("ConfigFromEnv defaults: %v", err)
	}
	if cfg.RetryBaseDelay != time.Second || cfg.RetryMaxDelay != time.Minute || cfg.RetryMaxAttempts != 10 {
		t.Fatalf("default retry policy = (%s,%s,%d)", cfg.RetryBaseDelay, cfg.RetryMaxDelay, cfg.RetryMaxAttempts)
	}

	cfg, err = ConfigFromEnv(configEnv{
		EnvRetryBaseMS:      "250",
		EnvRetryCapMS:       "2000",
		EnvRetryMaxAttempts: "4",
	})
	if err != nil {
		t.Fatalf("ConfigFromEnv custom retry: %v", err)
	}
	if cfg.RetryBaseDelay != 250*time.Millisecond || cfg.RetryMaxDelay != 2*time.Second || cfg.RetryMaxAttempts != 4 {
		t.Fatalf("custom retry policy = (%s,%s,%d)", cfg.RetryBaseDelay, cfg.RetryMaxDelay, cfg.RetryMaxAttempts)
	}
}

func TestConfigFromEnvRejectsInvalidQueueRetryPolicy(t *testing.T) {
	for _, env := range []configEnv{
		{EnvRetryBaseMS: "0"},
		{EnvRetryCapMS: "500", EnvRetryBaseMS: "1000"},
		{EnvRetryMaxAttempts: "-1"},
	} {
		if _, err := ConfigFromEnv(env); err == nil || !strings.Contains(strings.ToLower(err.Error()), "retry") {
			t.Fatalf("ConfigFromEnv(%v) error = %v; want retry config error", env, err)
		}
	}
}
