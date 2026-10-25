package computerhub

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/workload"
)

type mapEnv map[string]string

func (m mapEnv) Getenv(key string) string { return m[key] }

func TestConfigFromEnv(t *testing.T) {
	public, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	valid := func() mapEnv {
		return mapEnv{EnvDatabaseURL: "postgres://hub@db/tetral", EnvInternalPrincipalPublicKey: base64.StdEncoding.EncodeToString(public), EnvPageTokenKey: strings.Repeat("ab", 32), EnvPodName: "computer-hub-0"}
	}
	for _, tc := range []struct {
		name   string
		change func(mapEnv)
		check  func(Config) bool
		reject string
	}{
		{name: "required settings", change: func(mapEnv) {}, check: func(c Config) bool {
			return c.DatabaseURL == "postgres://hub@db/tetral" && c.PrincipalVerifier != nil && string(c.PageTokenKey) == strings.Repeat("\xab", 32)
		}},
		{name: "maximum credential lifetime", change: func(e mapEnv) { e[EnvMaxCredentialLifetime] = "1h" }, check: func(c Config) bool {
			return c.MaxCredentialLifetime == time.Hour
		}},
		{name: "addresses and database TLS", change: func(e mapEnv) {
			e[EnvHTTPAddress], e[EnvMetricsAddress], e[EnvDatabaseTLSCAPath], e[EnvDatabaseTLSServerName] = ":9080", ":9081", "/ca.pem", "db.internal"
		}, check: func(c Config) bool {
			return c.HTTPAddress == ":9080" && c.MetricsAddress == ":9081" && c.DatabaseTLSCAPath == "/ca.pem" && c.DatabaseTLSServerName == "db.internal"
		}},
		{name: "metrics on the public address", change: func(e mapEnv) { e[EnvMetricsAddress] = ":8080" }, reject: EnvMetricsAddress},
		{name: "no database", change: func(e mapEnv) { delete(e, EnvDatabaseURL) }, reject: EnvDatabaseURL},
		{name: "no principal key", change: func(e mapEnv) { delete(e, EnvInternalPrincipalPublicKey) }, reject: EnvInternalPrincipalPublicKey},
		{name: "principal key not Ed25519", change: func(e mapEnv) { e[EnvInternalPrincipalPublicKey] = base64.StdEncoding.EncodeToString([]byte("short")) }, reject: EnvInternalPrincipalPublicKey},
		{name: "lifetime not a duration", change: func(e mapEnv) { e[EnvMaxCredentialLifetime] = "30 days" }, reject: EnvMaxCredentialLifetime},
		{name: "lifetime zero", change: func(e mapEnv) { e[EnvMaxCredentialLifetime] = "0s" }, reject: EnvMaxCredentialLifetime},
		{name: "no page token key", change: func(e mapEnv) { delete(e, EnvPageTokenKey) }, reject: EnvPageTokenKey},
		{name: "page token key of 31 bytes", change: func(e mapEnv) { e[EnvPageTokenKey] = strings.Repeat("ab", 31) }, reject: EnvPageTokenKey},
		{name: "page token key not hexadecimal", change: func(e mapEnv) { e[EnvPageTokenKey] = strings.Repeat("zz", 32) }, reject: EnvPageTokenKey},
		{name: "CLI versions and pod name", change: func(e mapEnv) {
			e[EnvMinCLIVersion], e[EnvRecommendedCLIVersion], e[EnvPodName] = "1.0.0", "1.2.0-rc.1+build.5", "computer-hub-7d9f"
		}, check: func(c Config) bool {
			return c.MinCLIVersion == "1.0.0" && c.RecommendedCLIVersion == "1.2.0-rc.1+build.5" && c.PodName == "computer-hub-7d9f"
		}},
		{name: "minimum version shorthand", change: func(e mapEnv) { e[EnvMinCLIVersion] = "1.2" }, reject: EnvMinCLIVersion},
		{name: "recommended version with v prefix", change: func(e mapEnv) { e[EnvRecommendedCLIVersion] = "v1.2.0" }, reject: EnvRecommendedCLIVersion},
		{name: "recommended below minimum by precedence", change: func(e mapEnv) {
			e[EnvMinCLIVersion], e[EnvRecommendedCLIVersion] = "1.10.0", "1.9.0"
		}, reject: EnvRecommendedCLIVersion},
		{name: "no pod name", change: func(e mapEnv) { delete(e, EnvPodName) }, reject: EnvPodName},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := valid()
			tc.change(env)
			cfg, err := ConfigFromEnv(env)
			if tc.reject != "" {
				configErr, ok := workload.AsConfigError(err)
				if !ok || !strings.Contains(configErr.Error(), tc.reject) {
					t.Fatalf("error = %v; want a config error naming %s", err, tc.reject)
				}
				return
			}
			if err != nil || !tc.check(cfg) {
				t.Fatalf("config = %+v, %v", cfg, err)
			}
		})
	}
}
