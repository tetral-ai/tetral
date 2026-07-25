package tetralsandbox

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/queue"
)

type configTestEnv map[string]string

func (e configTestEnv) Getenv(key string) string {
	return e[key]
}

func validSandboxConfigEnv() configTestEnv {
	return configTestEnv{ //nolint:gosec // Test env values are fixture credentials only.
		EnvPostgresDSN:         "postgres://tetral-sandbox@example.invalid/tetral",
		EnvSandboxDriver:       "daytona",
		EnvDaytonaAPIURL:       "https://daytona.example.invalid/api",
		EnvDaytonaAPIKey:       "daytona-test-key",
		EnvQueueGRPCAddress:    "queue:9090",
		EnvBlobEndpoint:        "https://blob.example.invalid",
		EnvBlobRegion:          "auto",
		EnvBlobBucket:          "tetral-files",
		EnvBlobAccessKey:       "blob-access-key",
		EnvBlobSecretKey:       "blob-secret-key",
		EnvR2AccountID:         "account_123",
		EnvR2ParentAPIToken:    "r2-parent-token",
		EnvR2ParentAccessKeyID: "r2-parent-access-key",
		EnvGitProxyHost:        "git.tetral.example.invalid",
	}
}

func TestConfigFromEnvRequiresResourceProjectionAndGitProxySurface(t *testing.T) {
	tests := []struct {
		name string
		key  string
	}{
		{name: "account id", key: EnvR2AccountID},
		{name: "parent api token", key: EnvR2ParentAPIToken},
		{name: "parent access key", key: EnvR2ParentAccessKeyID},
		{name: "git proxy host", key: EnvGitProxyHost},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := validSandboxConfigEnv()
			env[tc.key] = ""

			_, err := ConfigFromEnv(env)

			if err == nil {
				t.Fatalf("ConfigFromEnv succeeded without %s", tc.key)
			}
			if !strings.Contains(err.Error(), tc.key+" is required") {
				t.Fatalf("ConfigFromEnv error = %q; want missing %s", err.Error(), tc.key)
			}
			if strings.Contains(err.Error(), "r2-parent-token") || strings.Contains(err.Error(), "r2-parent-access-key") {
				t.Fatalf("ConfigFromEnv error leaked secret material: %q", err.Error())
			}
		})
	}
}

func TestConfigFromEnvCarriesGitProxyHost(t *testing.T) {
	cfg, err := ConfigFromEnv(validSandboxConfigEnv())
	if err != nil {
		t.Fatalf("ConfigFromEnv: %v", err)
	}
	if cfg.GitProxyHost != "git.tetral.example.invalid" {
		t.Fatalf("GitProxyHost = %q; want configured host", cfg.GitProxyHost)
	}
}

func TestConfigFromEnvRejectsEveryLeaseConcurrencyAboveTransportCapacity(t *testing.T) {
	for _, envName := range []string{
		EnvSandboxEnvironmentBuildConcurrency,
		EnvSandboxEnvironmentReadyFanoutConcurrency,
		EnvSandboxSessionPrepareConcurrency,
	} {
		t.Run(envName, func(t *testing.T) {
			env := validSandboxConfigEnv()
			env[envName] = strconv.Itoa(queue.MaxQueueLeaseJobs() + 1)
			if _, err := ConfigFromEnv(env); err == nil || !strings.Contains(err.Error(), envName) {
				t.Fatalf("ConfigFromEnv oversized %s error = %v; want knob-owned startup validation", envName, err)
			}
		})
	}
}

func TestConfigFromEnvPinsCleanupAttemptBudget(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		cfg, err := ConfigFromEnv(validSandboxConfigEnv())
		if err != nil {
			t.Fatalf("ConfigFromEnv: %v", err)
		}
		if cfg.CleanupMaxAttempts != 20 {
			t.Fatalf("CleanupMaxAttempts = %d; want 20", cfg.CleanupMaxAttempts)
		}
	})

	t.Run("configured", func(t *testing.T) {
		env := validSandboxConfigEnv()
		env[EnvSandboxCleanupMaxAttempts] = "7"
		cfg, err := ConfigFromEnv(env)
		if err != nil {
			t.Fatalf("ConfigFromEnv: %v", err)
		}
		if cfg.CleanupMaxAttempts != 7 {
			t.Fatalf("CleanupMaxAttempts = %d; want 7", cfg.CleanupMaxAttempts)
		}
	})

	t.Run("rejects nonpositive", func(t *testing.T) {
		env := validSandboxConfigEnv()
		env[EnvSandboxCleanupMaxAttempts] = "0"
		if _, err := ConfigFromEnv(env); err == nil {
			t.Fatal("ConfigFromEnv accepted zero cleanup attempt budget")
		}
	})
}

func TestConfigFromEnvPinsStartupCleanupLease(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		cfg, err := ConfigFromEnv(validSandboxConfigEnv())
		if err != nil {
			t.Fatalf("ConfigFromEnv: %v", err)
		}
		if cfg.CleanupLeaseDuration != 120*time.Second {
			t.Fatalf("CleanupLeaseDuration = %s; want 120s", cfg.CleanupLeaseDuration)
		}
	})

	t.Run("configured", func(t *testing.T) {
		env := validSandboxConfigEnv()
		env[EnvSandboxCleanupLeaseDuration] = "75s"
		env[EnvSandboxDaytonaStopTimeout] = "30s"
		env[EnvSandboxDaytonaStopForceAfter] = "20s"
		cfg, err := ConfigFromEnv(env)
		if err != nil {
			t.Fatalf("ConfigFromEnv: %v", err)
		}
		if cfg.CleanupLeaseDuration != 75*time.Second {
			t.Fatalf("CleanupLeaseDuration = %s; want 75s", cfg.CleanupLeaseDuration)
		}
	})

	t.Run("rejects nonpositive", func(t *testing.T) {
		env := validSandboxConfigEnv()
		env[EnvSandboxCleanupLeaseDuration] = "0s"
		if _, err := ConfigFromEnv(env); err == nil {
			t.Fatal("ConfigFromEnv accepted zero cleanup lease duration")
		}
	})
}

func TestConfigFromEnvRequiresStartupCleanupDeadlineToExceedConfiguredStopLegs(t *testing.T) {
	tests := []struct {
		name       string
		lease      string
		stop       string
		forceAfter string
		wantErr    bool
	}{
		{name: "strictly greater without force", lease: "41s", stop: "30s"},
		{name: "equality rejected", lease: "40s", stop: "30s", wantErr: true},
		{name: "force after included", lease: "61s", stop: "30s", forceAfter: "20s"},
		{name: "force after equality rejected", lease: "60s", stop: "30s", forceAfter: "20s", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := validSandboxConfigEnv()
			env[EnvSandboxCleanupLeaseDuration] = tc.lease
			env[EnvSandboxDaytonaStopTimeout] = tc.stop
			env[EnvSandboxDaytonaStopForceAfter] = tc.forceAfter
			cfg, err := ConfigFromEnv(env)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ConfigFromEnv = %+v, nil; want cleanup lease floor error", cfg)
				}
				return
			}
			if err != nil {
				t.Fatalf("ConfigFromEnv: %v", err)
			}
		})
	}
}

func TestConfigFromEnvRejectsOverflowingStartupCleanupStopBudget(t *testing.T) {
	env := validSandboxConfigEnv()
	env[EnvSandboxCleanupLeaseDuration] = "1m"
	env[EnvSandboxDaytonaStopTimeout] = "2562047h47m16.854775807s"
	env[EnvSandboxDaytonaStopForceAfter] = "1s"
	if _, err := ConfigFromEnv(env); err == nil {
		t.Fatal("ConfigFromEnv accepted a stop budget that exceeds the cleanup lease")
	}
}

func TestConfigFromEnvCarriesLifecyclePolicyIntoDaytonaDriver(t *testing.T) {
	env := validSandboxConfigEnv()
	env[EnvSandboxDaytonaStopTimeout] = "45s"
	env[EnvSandboxDaytonaStopForceAfter] = "3m"
	env[EnvSandboxCleanupLeaseDuration] = "4m"
	env[EnvSandboxAutoStopInterval] = "31m"
	env[EnvSandboxAutoArchiveInterval] = "25h"
	env[EnvSandboxAutoDeleteInterval] = "720h"

	cfg, err := ConfigFromEnv(env)
	if err != nil {
		t.Fatalf("ConfigFromEnv: %v", err)
	}
	policy := cfg.Daytona.Lifecycle
	if policy.StopTimeout != 45*time.Second || policy.StopForceAfter != 3*time.Minute ||
		policy.AutoStopInterval != 31*time.Minute || policy.AutoArchiveInterval != 25*time.Hour ||
		policy.AutoDeleteInterval != 720*time.Hour {
		t.Fatalf("Daytona lifecycle policy = %+v; want configured service values", policy)
	}
}

func TestConfigFromEnvPinsLateCommandFenceDefaults(t *testing.T) {
	cfg, err := ConfigFromEnv(validSandboxConfigEnv())
	if err != nil {
		t.Fatalf("ConfigFromEnv: %v", err)
	}
	if cfg.SessionPrepareLeaseDuration != 120*time.Second || cfg.PreparationCommandTimeout != 45*time.Second || cfg.LateCommandMargin != 30*time.Second {
		t.Fatalf("late-command defaults = lease %s timeout %s margin %s", cfg.SessionPrepareLeaseDuration, cfg.PreparationCommandTimeout, cfg.LateCommandMargin)
	}
	if cfg.Daytona.PreparationCommandTimeout != cfg.PreparationCommandTimeout {
		t.Fatalf("Daytona timeout = %s; want %s", cfg.Daytona.PreparationCommandTimeout, cfg.PreparationCommandTimeout)
	}
}

func TestConfigFromEnvValidatesLateCommandFenceInWholeSeconds(t *testing.T) {
	tests := []struct {
		name    string
		lease   string
		heart   string
		timeout string
		margin  string
		wantErr bool
	}{
		{name: "equality", lease: "90s", heart: "15s", timeout: "45s", margin: "30s"},
		{name: "one second short", lease: "89s", heart: "15s", timeout: "45s", margin: "30s", wantErr: true},
		{name: "sub-second lease", lease: "120.5s", heart: "15s", timeout: "45s", margin: "30s", wantErr: true},
		{name: "sub-second timeout", lease: "120s", heart: "15s", timeout: "500ms", margin: "30s", wantErr: true},
		{name: "non-integral margin", lease: "120s", heart: "15s", timeout: "45s", margin: "30.1s", wantErr: true},
		{name: "zero", lease: "120s", heart: "15s", timeout: "0s", margin: "30s", wantErr: true},
		{name: "timeout exceeds wire", lease: "700000h", heart: "15s", timeout: "596524h", margin: "1s", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := validSandboxConfigEnv()
			env[EnvSandboxSessionPrepareLeaseDuration] = tc.lease
			env[EnvSandboxLeaseHeartbeatInterval] = tc.heart
			env[EnvSandboxPreparationCommandTimeout] = tc.timeout
			env[EnvSandboxLateCommandMargin] = tc.margin
			cfg, err := ConfigFromEnv(env)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ConfigFromEnv = %+v, nil; want startup error", cfg)
				}
				return
			}
			if err != nil {
				t.Fatalf("ConfigFromEnv: %v", err)
			}
			if cfg.SessionPrepareLeaseDuration != 90*time.Second {
				t.Fatalf("lease = %s; want equality boundary 90s", cfg.SessionPrepareLeaseDuration)
			}
		})
	}
}

func TestQueueRunnerLeaseDurationsPromoteOnlySessionPrepare(t *testing.T) {
	cfg := Config{LeaseHeartbeatInterval: 15 * time.Second, SessionPrepareLeaseDuration: 120 * time.Second}
	if got := EnvironmentQueueLeaseDuration(cfg); got != 60*time.Second {
		t.Fatalf("environment runner lease = %s; want heartbeat*4 = 60s", got)
	}
	if got := SessionPrepareQueueLeaseDuration(cfg); got != 120*time.Second {
		t.Fatalf("session_prepare runner lease = %s; want explicit 120s", got)
	}
}

func TestConfigFromEnvRejectsAutoDeleteBelowThirtyDays(t *testing.T) {
	env := validSandboxConfigEnv()
	env[EnvSandboxAutoDeleteInterval] = "719h59m"
	if _, err := ConfigFromEnv(env); err == nil {
		t.Fatal("ConfigFromEnv short auto-delete interval = nil; want validation error")
	}
}

func TestConfigFromEnvCarriesResourceCredentialTTL(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		cfg, err := ConfigFromEnv(validSandboxConfigEnv())
		if err != nil {
			t.Fatalf("ConfigFromEnv: %v", err)
		}
		if cfg.ResourceCredentialTTL != 24*time.Hour {
			t.Fatalf("ResourceCredentialTTL = %s; want 24h", cfg.ResourceCredentialTTL)
		}
	})

	t.Run("configured", func(t *testing.T) {
		env := validSandboxConfigEnv()
		env[EnvResourceCredentialTTL] = "48h"
		cfg, err := ConfigFromEnv(env)
		if err != nil {
			t.Fatalf("ConfigFromEnv: %v", err)
		}
		if cfg.ResourceCredentialTTL != 48*time.Hour {
			t.Fatalf("ResourceCredentialTTL = %s; want 48h", cfg.ResourceCredentialTTL)
		}
	})

	t.Run("rejects nonpositive", func(t *testing.T) {
		env := validSandboxConfigEnv()
		env[EnvResourceCredentialTTL] = "0s"
		_, err := ConfigFromEnv(env)
		if err == nil || !strings.Contains(err.Error(), EnvResourceCredentialTTL+" must be a positive duration") {
			t.Fatalf("ConfigFromEnv error = %v; want positive duration validation", err)
		}
	})
}

func TestConfigFromEnvCarriesResourceCredentialRefreshMargin(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		cfg, err := ConfigFromEnv(validSandboxConfigEnv())
		if err != nil {
			t.Fatalf("ConfigFromEnv: %v", err)
		}
		if cfg.ResourceCredentialRefreshMargin != 30*time.Minute {
			t.Fatalf("ResourceCredentialRefreshMargin = %s; want 30m", cfg.ResourceCredentialRefreshMargin)
		}
	})

	t.Run("configured", func(t *testing.T) {
		env := validSandboxConfigEnv()
		env[EnvResourceCredentialRefreshMargin] = "45m"
		cfg, err := ConfigFromEnv(env)
		if err != nil {
			t.Fatalf("ConfigFromEnv: %v", err)
		}
		if cfg.ResourceCredentialRefreshMargin != 45*time.Minute {
			t.Fatalf("ResourceCredentialRefreshMargin = %s; want 45m", cfg.ResourceCredentialRefreshMargin)
		}
	})

	t.Run("rejects nonpositive", func(t *testing.T) {
		env := validSandboxConfigEnv()
		env[EnvResourceCredentialRefreshMargin] = "0s"
		_, err := ConfigFromEnv(env)
		if err == nil || !strings.Contains(err.Error(), EnvResourceCredentialRefreshMargin+" must be a positive duration") {
			t.Fatalf("ConfigFromEnv error = %v; want positive duration validation", err)
		}
	})
}

func TestConfigFromEnvCarriesRcloneCacheKnobs(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		cfg, err := ConfigFromEnv(validSandboxConfigEnv())
		if err != nil {
			t.Fatalf("ConfigFromEnv: %v", err)
		}
		if cfg.RcloneVFSCacheMaxSize != "2G" || cfg.RcloneVFSMinFree != "1G" {
			t.Fatalf("rclone knobs = %q/%q; want 2G/1G", cfg.RcloneVFSCacheMaxSize, cfg.RcloneVFSMinFree)
		}
	})

	t.Run("configured", func(t *testing.T) {
		env := validSandboxConfigEnv()
		env[EnvRcloneVFSCacheMaxSize] = "4G"
		env[EnvRcloneVFSMinFree] = "2G"
		cfg, err := ConfigFromEnv(env)
		if err != nil {
			t.Fatalf("ConfigFromEnv: %v", err)
		}
		if cfg.RcloneVFSCacheMaxSize != "4G" || cfg.RcloneVFSMinFree != "2G" {
			t.Fatalf("rclone knobs = %q/%q; want configured values", cfg.RcloneVFSCacheMaxSize, cfg.RcloneVFSMinFree)
		}
	})
}
