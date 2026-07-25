package tetralapi_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workload"
	tetralapi "github.com/tetral-ai/tetral/services/api"
)

// startupConfigSurface drives one Bucket-C config-validation surface through the
// real BuildRouter composition path with an invalid env value and asserts the
// returned error classifies as config_error, names the exact offending env key,
// and never echoes the secret-shaped value-position sentinel (A.3).
type startupConfigSurface struct {
	// inventoryID labels the surface; the subtest name equals it so a reviewer
	// maps inventory member -> A.3 case mechanically.
	inventoryID string
	// envKey is the exact env key the surfaced message must name.
	envKey string
	// valueSentinel is the secret-shaped value placed in the offending value
	// position; it must be absent from the message and the captured log.
	valueSentinel string
	// configure mutates the router config / process env to drive the failure.
	configure func(t *testing.T, env *envFixture, config *tetralapi.RouterConfig)
}

// envFixture is an injectable tetralapi.Env that returns only the
// values the surface under test sets, so unrelated process env never bleeds in.
type envFixture map[string]string

func (e envFixture) Getenv(key string) string { return e[key] }

func TestBuildRouterConfigValidationSurfacesClassifyAndRedact(t *testing.T) {
	if os.Getenv(storagetest.EnvTestDatabaseURL) == "" {
		t.Skip(storagetest.EnvTestDatabaseURL + " is not set")
	}
	for _, surface := range []startupConfigSurface{
		{
			inventoryID:   "environment default artifact ref",
			envKey:        "TETRAL_DEFAULT_ENVIRONMENT_ARTIFACT_REF",
			valueSentinel: "tetral_sk_default_environment_artifact_sentinel",
			configure: func(t *testing.T, env *envFixture, _ *tetralapi.RouterConfig) {
				delete(*env, "TETRAL_DEFAULT_ENVIRONMENT_ARTIFACT_REF")
			},
		},
		{
			inventoryID:   "blob.LoadConfig (buildOptionalBlobStore)",
			envKey:        blob.EnvEndpoint,
			valueSentinel: "tetral_sk_blob_endpoint_sentinel",
			configure: func(t *testing.T, _ *envFixture, _ *tetralapi.RouterConfig) {
				// Blob config is read via os.Getenv, so drive it through process env.
				// Region/bucket/keys are valid; the endpoint embeds credentials with a
				// secret-shaped sentinel in the userinfo (value) position, which the
				// blob validator rejects naming TETRAL_BLOB_ENDPOINT without echoing it.
				t.Setenv(blob.EnvRegion, "us-east-1")
				t.Setenv(blob.EnvBucket, "tetral-skills")
				t.Setenv(blob.EnvAccessKey, "AKIDEXAMPLE")
				t.Setenv(blob.EnvSecretKey, "secret-not-leaked")
				t.Setenv(blob.EnvEndpoint, "https://tetral_sk_blob_endpoint_sentinel@blob.example.com")
			},
		},
	} {
		t.Run(surface.inventoryID, func(t *testing.T) {
			runtimeDB, adminDB := storagetest.NewPostgreSQLDBWithAdmin(t)
			env := envFixture{"TETRAL_DEFAULT_ENVIRONMENT_ARTIFACT_REF": "artifact_default_test"}
			config := tetralapi.RouterConfig{
				RuntimeClient:     dbconnect.NewClientForTesting(runtimeDB),
				RawDatabase:       adminDB,
				VaultKey:          tetralAPITestVaultKey,
				DataDir:           secureTetralAPIDataDir(t),
				Env:               env,
				PrincipalVerifier: tetralAPITestPrincipalVerifier(t),
			}
			surface.configure(t, &env, &config)
			config.Env = env

			_, err := tetralapi.BuildRouter(context.Background(), config)
			if err == nil {
				t.Fatalf("%s: BuildRouter accepted an invalid config value", surface.inventoryID)
			}
			configErr, ok := workload.AsConfigError(err)
			if !ok {
				t.Fatalf("%s: BuildRouter returned %T; want a workload.ConfigError (error.class=config_error)", surface.inventoryID, err)
			}
			message := configErr.Error()
			if !strings.Contains(message, surface.envKey) {
				t.Fatalf("%s: config error %q does not name the offending key %q", surface.inventoryID, message, surface.envKey)
			}
			// Drive the same error through the workload startup-failure classifier
			// to prove the captured log line carries config_error + the safe message
			// and never the value-position sentinel.
			class, logMessage, hasMessage := workload.StartupFailureFields(err)
			if class != "config_error" || !hasMessage {
				t.Fatalf("%s: startup classification = (%q, hasMessage=%v); want config_error with a message", surface.inventoryID, class, hasMessage)
			}
			for _, sink := range []string{message, logMessage} {
				if strings.Contains(sink, surface.valueSentinel) {
					t.Fatalf("%s: surfaced text leaked value-position sentinel %q: %s", surface.inventoryID, surface.valueSentinel, sink)
				}
			}
		})
	}
}
