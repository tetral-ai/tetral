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

// startupConfigSurface drives one configuration surface through the real
// BuildRouter composition path with an invalid value and asserts the returned
// error classifies as config_error, names the exact offending key, and never
// echoes the secret-shaped value. A new surface gets a row here.
type startupConfigSurface struct {
	// inventoryID names the surface and its subtest.
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
			inventoryID:   "drive upload budget",
			envKey:        "TETRAL_API_DRIVE_UPLOAD_BUDGET_BYTES_PER_MINUTE",
			valueSentinel: "tetral_sk_upload_budget_sentinel",
			configure: func(t *testing.T, env *envFixture, _ *tetralapi.RouterConfig) {
				(*env)["TETRAL_API_DRIVE_UPLOAD_BUDGET_BYTES_PER_MINUTE"] = "tetral_sk_upload_budget_sentinel"
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
			// The startup-failure record classifies it as config_error and takes
			// the message as its cause, so the message never holds the
			// value-position sentinel.
			if code := workload.StartupFailureCode(err); code != "config_error" {
				t.Fatalf("%s: startup code = %q; want config_error", surface.inventoryID, code)
			}
			if strings.Contains(message, surface.valueSentinel) {
				t.Fatalf("%s: surfaced text leaked value-position sentinel %q: %s", surface.inventoryID, surface.valueSentinel, message)
			}
		})
	}
}
