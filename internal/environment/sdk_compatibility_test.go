package environment

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestSDKCompatibilityPackageMapAlwaysMarshalsExactlySixManagers(t *testing.T) {
	for _, packages := range []PackageMap{
		nil,
		{},
		{"pip": {"pandas==2.2.0"}, "npm": nil},
	} {
		body, err := json.Marshal(packages)
		if err != nil {
			t.Fatalf("Marshal PackageMap: %v", err)
		}
		var got map[string][]string
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatalf("Unmarshal PackageMap JSON %s: %v", body, err)
		}
		wantKeys := []string{"apt", "cargo", "gem", "go", "npm", "pip"}
		if len(got) != len(wantKeys) {
			t.Fatalf("PackageMap keys = %#v; want exactly %v", got, wantKeys)
		}
		for _, key := range wantKeys {
			values, ok := got[key]
			if !ok {
				t.Fatalf("PackageMap missing %q in %s", key, body)
			}
			if values == nil {
				t.Fatalf("PackageMap[%q] = null; want [] in %s", key, body)
			}
		}
		if packages != nil && len(packages["pip"]) > 0 && !reflect.DeepEqual(got["pip"], packages["pip"]) {
			t.Fatalf("pip = %#v; want %#v", got["pip"], packages["pip"])
		}
	}
}

func TestSDKCompatibilityEnvironmentUpdateNullsClearToDefaults(t *testing.T) {
	patch, err := DecodeUpdateEnvironmentRequest([]byte(`{"name":null,"description":null,"config":null}`))
	if err != nil {
		t.Fatalf("DecodeUpdateEnvironmentRequest explicit nulls: %v", err)
	}
	materialized, err := patch.Materialize(Environment{
		Name:        "before",
		Description: "before description",
		Config: EnvironmentConfig{
			Type:       "cloud",
			Networking: &NetworkingConfig{Type: "blocked"},
			Packages:   PackageMap{"pip": {"pandas==2.2.0"}},
		},
		Metadata: StringMap{"keep": "yes"},
	})
	if err != nil {
		t.Fatalf("Materialize explicit nulls: %v", err)
	}
	if materialized.Name != "" || materialized.Description != "" {
		t.Fatalf("name/description = %q/%q; want cleared", materialized.Name, materialized.Description)
	}
	if materialized.Config.Type != "cloud" || materialized.Config.Networking == nil || materialized.Config.Networking.Type != "unrestricted" {
		t.Fatalf("config = %+v; want default cloud/unrestricted", materialized.Config)
	}
	if len(materialized.Config.Packages) != 0 {
		t.Fatalf("packages = %#v; want cleared default", materialized.Config.Packages)
	}
	if !reflect.DeepEqual(materialized.Metadata, StringMap{"keep": "yes"}) {
		t.Fatalf("metadata = %#v; want omitted metadata unchanged", materialized.Metadata)
	}
}

func TestSDKCompatibilityEnvironmentCreateConfigNullRemainsNamedRejection(t *testing.T) {
	_, err := DecodeCreateEnvironmentRequest([]byte(`{"name":"env","config":null}`))
	if err == nil || err.Error() != "config cannot be null" {
		t.Fatalf("error = %v; want config cannot be null", err)
	}
}

func TestSDKCompatibilityEnvironmentCreateNullableNamesClearToDefaults(t *testing.T) {
	request, err := DecodeCreateEnvironmentRequest([]byte(`{"name":null,"description":null}`))
	if err != nil {
		t.Fatalf("DecodeCreateEnvironmentRequest nullable nulls: %v", err)
	}
	if request.Name != "" || request.Description != "" {
		t.Fatalf("name/description = %q/%q; want cleared defaults", request.Name, request.Description)
	}
	if request.Config.Type != "cloud" || request.Config.Networking == nil || request.Config.Networking.Type != "unrestricted" {
		t.Fatalf("config = %+v; want default cloud config", request.Config)
	}
}
