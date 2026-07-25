package environment_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/tetral-ai/tetral/internal/environment"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestSDKCompatibilityPackageProjectionDoesNotExpandStoredJSONOrBuildDecisions(t *testing.T) {
	runtime, admin := newPGEnvEnv(t)
	store := newPGEnvStore(runtime)
	ctx := context.Background()

	minimal, err := store.Create(ctx, workspace.DefaultID, environment.CreateEnvironmentRequest{
		Name: "minimal",
		Config: environment.EnvironmentConfig{
			Type:       "cloud",
			Networking: &environment.NetworkingConfig{Type: "unrestricted"},
			Packages:   environment.PackageMap{},
		},
	})
	if err != nil {
		t.Fatalf("Create minimal environment: %v", err)
	}
	assertEnvironmentPublicSixPackageKeys(t, minimal)
	if count := countEnvironmentBuildJobs(t, admin, workspace.DefaultID); count != 0 {
		t.Fatalf("minimal build rows = %d; want 0", count)
	}

	packaged, err := store.Create(ctx, workspace.DefaultID, environment.CreateEnvironmentRequest{
		Name: "packaged",
		Config: environment.EnvironmentConfig{
			Type:       "cloud",
			Networking: &environment.NetworkingConfig{Type: "unrestricted"},
			Packages:   environment.PackageMap{"pip": {"pandas==2.2.0"}},
		},
	})
	if err != nil {
		t.Fatalf("Create packaged environment: %v", err)
	}
	assertEnvironmentPublicSixPackageKeys(t, packaged)
	if count := countEnvironmentBuildJobs(t, admin, workspace.DefaultID); count != 1 {
		t.Fatalf("packaged build rows = %d; want 1", count)
	}

	var configJSON string
	if err := admin.QueryRow(`SELECT config_json FROM environments WHERE id = $1`, packaged.ID).Scan(&configJSON); err != nil {
		t.Fatalf("read stored config_json: %v", err)
	}
	var stored struct {
		Packages map[string][]string `json:"packages"`
	}
	if err := json.Unmarshal([]byte(configJSON), &stored); err != nil {
		t.Fatalf("decode stored config_json %s: %v", configJSON, err)
	}
	if len(stored.Packages) != 1 || len(stored.Packages["pip"]) != 1 || stored.Packages["pip"][0] != "pandas==2.2.0" {
		t.Fatalf("stored packages = %#v; want sparse canonical pip only", stored.Packages)
	}
}

func assertEnvironmentPublicSixPackageKeys(t *testing.T, value *environment.Environment) {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("Marshal Environment: %v", err)
	}
	var response struct {
		Config struct {
			Packages map[string][]string `json:"packages"`
		} `json:"config"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("Unmarshal Environment response: %v", err)
	}
	if len(response.Config.Packages) != 6 {
		t.Fatalf("public packages = %#v; want six keys", response.Config.Packages)
	}
	for _, key := range []string{"apt", "cargo", "gem", "go", "npm", "pip"} {
		if values, ok := response.Config.Packages[key]; !ok || values == nil {
			t.Fatalf("public packages[%q] = %#v (present=%v); want [] or values", key, values, ok)
		}
	}
}
