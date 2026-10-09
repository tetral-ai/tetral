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

func TestQueueLifecycleConfigFitsPodApplicationAllocation(t *testing.T) {
	for _, tc := range []struct {
		profile, drain, join string
		valid                bool
	}{
		{"standard-routed", "2000", "3000", true}, {"standard-routed", "20000", "5000", true}, {"standard-routed", "25000", "5000", false}, {"standard-routed", "20001", "5000", false},
		{"hardened", "20000", "5000", true}, {"hardened", "20001", "5000", false}, {"hardened", "24000", "1000", true}, {"hardened", "2000", "0", false},
		{"standard-routed", "2000", "9223372036854775807", false},
		{"unknown", "2000", "3000", false},
	} {
		cfg, err := ConfigFromEnv(configEnv{"TETRAL_TRANSPORT_PROFILE": tc.profile, EnvDrainTimeoutMS: tc.drain, "TETRAL_CANCEL_JOIN_TIMEOUT_MS": tc.join})
		if (err == nil) != tc.valid {
			t.Fatalf("%+v error=%v", tc, err)
		}
		if tc.valid && tc.join == "3000" && cfg.CancelJoinTimeout != 3*time.Second {
			t.Fatal("join configuration lost")
		}
	}
}

func TestQueueDrainUsesTheSharedApplicationDrainKey(t *testing.T) {
	if EnvDrainTimeoutMS != "TETRAL_DRAIN_TIMEOUT_MS" {
		t.Fatalf("Queue drain key=%s", EnvDrainTimeoutMS)
	}
	cfg, err := ConfigFromEnv(configEnv{"TETRAL_DRAIN_TIMEOUT_MS": "2000"})
	if err != nil || cfg.DrainTimeout != 2*time.Second {
		t.Fatalf("shared drain key=%s/%v", cfg.DrainTimeout, err)
	}
	cfg, err = ConfigFromEnv(configEnv{"TETRAL_QUEUE_DRAIN_TIMEOUT_MS": "2000"})
	if err != nil || cfg.DrainTimeout != 10*time.Second {
		t.Fatalf("unrecognized drain key changed the default=%s/%v", cfg.DrainTimeout, err)
	}
}
