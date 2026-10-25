package computerhub

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"strings"
	"testing"

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
		return mapEnv{EnvDatabaseURL: "postgres://hub@db/tetral", EnvInternalPrincipalPublicKey: base64.StdEncoding.EncodeToString(public)}
	}
	for _, tc := range []struct {
		name   string
		change func(mapEnv)
		check  func(Config) bool
		reject string
	}{
		{name: "defaults", change: func(mapEnv) {}, check: func(c Config) bool {
			return c.HTTPAddress == ":8080" && c.MetricsAddress == ":8081" && c.DatabaseURL == "postgres://hub@db/tetral" && c.PrincipalVerifier != nil
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
