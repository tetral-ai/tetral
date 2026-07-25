package environment

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestDecodeCreateEnvironmentRequestNormalizesMinimalBody(t *testing.T) {
	request, err := DecodeCreateEnvironmentRequest([]byte(`{"name":"python-dev"}`))
	if err != nil {
		t.Fatalf("DecodeCreateEnvironmentRequest: %v", err)
	}
	if request.Name != "python-dev" {
		t.Errorf("Name = %q; want python-dev", request.Name)
	}
	if request.Description != "" {
		t.Errorf("Description = %q; want empty", request.Description)
	}
	assertCompleteDefaultConfig(t, request.Config)
	if len(request.Metadata) != 0 {
		t.Errorf("Metadata = %v; want empty", request.Metadata)
	}
}

func TestDecodeCreateEnvironmentRequestRejectsUnknownAndInvalidShape(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"top level null", `null`},
		{"top level array", `[]`},
		{"unknown top level", `{"name":"env","version":1}`},
		{"metadata null", `{"name":"env","metadata":null}`},
		{"metadata object value", `{"name":"env","metadata":{"nested":{"x":"y"}}}`},
		{"metadata array value", `{"name":"env","metadata":{"items":[]}}`},
		{"metadata number value", `{"name":"env","metadata":{"n":1}}`},
		{"metadata boolean value", `{"name":"env","metadata":{"b":true}}`},
		{"metadata null value", `{"name":"env","metadata":{"k":null}}`},
		{"empty config type", `{"name":"env","config":{"type":""}}`},
		{"unsupported config type", `{"name":"env","config":{"type":"self_hosted"}}`},
		{"unknown config field", `{"name":"env","config":{"type":"cloud","image":"x"}}`},
		{"empty networking type", `{"name":"env","config":{"networking":{"type":""}}}`},
		{"unsupported networking type", `{"name":"env","config":{"networking":{"type":"private"}}}`},
		{"unknown networking field", `{"name":"env","config":{"networking":{"type":"unrestricted","proxy":"x"}}}`},
		{"old limited networking", `{"name":"env","config":{"networking":{"type":"limited","allowed_hosts":["https://api.example.com"]}}}`},
		{"old allowed hosts field", `{"name":"env","config":{"networking":{"type":"unrestricted","allowed_hosts":["https://api.example.com"]}}}`},
		{"old mcp servers field", `{"name":"env","config":{"networking":{"type":"unrestricted","allow_mcp_servers":false}}}`},
		{"old package managers field", `{"name":"env","config":{"networking":{"type":"unrestricted","allow_package_managers":false}}}`},
		{"unrestricted network allow list", `{"name":"env","config":{"networking":{"type":"unrestricted","network_allow_list":"10.0.0.0/8"}}}`},
		{"blocked network allow list", `{"name":"env","config":{"networking":{"type":"blocked","network_allow_list":"10.0.0.0/8"}}}`},
		{"cidr missing allow list", `{"name":"env","config":{"networking":{"type":"cidr_allow_list"}}}`},
		{"cidr host", `{"name":"env","config":{"networking":{"type":"cidr_allow_list","network_allow_list":"github.com"}}}`},
		{"cidr ip without prefix", `{"name":"env","config":{"networking":{"type":"cidr_allow_list","network_allow_list":"10.0.0.1"}}}`},
		{"cidr empty element", `{"name":"env","config":{"networking":{"type":"cidr_allow_list","network_allow_list":"10.0.0.0/8,"}}}`},
		{"cidr control character", `{"name":"env","config":{"networking":{"type":"cidr_allow_list","network_allow_list":"10.0.0.0/8\u0000"}}}`},
		{"unknown package manager", `{"name":"env","config":{"packages":{"brew":["jq"]}}}`},
		{"unknown packages marker", `{"name":"env","config":{"packages":{"type":"other"}}}`},
		{"package manager non array", `{"name":"env","config":{"packages":{"pip":"pandas"}}}`},
		{"package entry non string", `{"name":"env","config":{"packages":{"pip":[1]}}}`},
		{"package entry null", `{"name":"env","config":{"packages":{"pip":[null]}}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeCreateEnvironmentRequest([]byte(tc.body))
			if err == nil {
				t.Fatal("expected validation error")
			}
			var validation *ValidationError
			if !errors.As(err, &validation) {
				t.Fatalf("error = %T %v; want *ValidationError", err, err)
			}
		})
	}
}

func TestDecodeCreateEnvironmentRequestValidatesMetadataLimitsByRune(t *testing.T) {
	tooManyPairs := make(map[string]string)
	for i := 0; i < 17; i++ {
		tooManyPairs[string(rune('a'+i))] = "v"
	}
	assertCreateMetadataRejected(t, tooManyPairs)
	assertCreateMetadataRejected(t, map[string]string{strings.Repeat("界", 65): "v"})
	assertCreateMetadataRejected(t, map[string]string{"key": strings.Repeat("界", 513)})
	assertCreateMetadataAccepted(t, map[string]string{strings.Repeat("界", 64): strings.Repeat("界", 512)})
}

func TestDecodeCreateEnvironmentRequestNormalizesCIDRNetworking(t *testing.T) {
	request, err := DecodeCreateEnvironmentRequest([]byte(`{
		"name":"env",
		"config":{
			"networking":{"type":"cidr_allow_list","network_allow_list":"10.0.0.0/8, 2001:db8::/32"},
			"packages":{"type":"packages","pip":["pandas==2.2.0"]}
		}
	}`))
	if err != nil {
		t.Fatalf("DecodeCreateEnvironmentRequest: %v", err)
	}
	if request.Config.Networking.Type != "cidr_allow_list" {
		t.Fatalf("Networking.Type = %q; want cidr_allow_list", request.Config.Networking.Type)
	}
	if got, want := request.Config.Networking.NetworkAllowList, "10.0.0.0/8,2001:db8::/32"; got != want {
		t.Fatalf("NetworkAllowList = %q; want %q", got, want)
	}
	if got := request.Config.Packages["pip"]; len(got) != 1 || got[0] != "pandas==2.2.0" {
		t.Fatalf("Packages = %v; want pip package and dropped type marker", request.Config.Packages)
	}
	if _, ok := request.Config.Packages["type"]; ok {
		t.Fatalf("packages type marker leaked into PackageMap: %v", request.Config.Packages)
	}
}

func TestDecodeEnvironmentRequestRejectsNetworkAllowListOutsideCIDRMode(t *testing.T) {
	cases := []struct {
		name       string
		networking string
	}{
		{
			name:       "unrestricted",
			networking: `{"type":"unrestricted","network_allow_list":"10.0.0.0/8"}`,
		},
		{
			name:       "blocked",
			networking: `{"type":"blocked","network_allow_list":"10.0.0.0/8"}`,
		},
	}
	for _, tc := range cases {
		t.Run("create "+tc.name, func(t *testing.T) {
			body := []byte(`{"name":"env","config":{"networking":` + tc.networking + `}}`)
			if _, err := DecodeCreateEnvironmentRequest(body); err == nil {
				t.Fatal("expected create validation error")
			}
		})
		t.Run("update "+tc.name, func(t *testing.T) {
			body := []byte(`{"config":{"networking":` + tc.networking + `}}`)
			if _, err := DecodeUpdateEnvironmentRequest(body); err == nil {
				t.Fatal("expected update validation error")
			}
		})
	}
}

func TestDecodeCreateEnvironmentRequestNormalizesUnrestrictedNetworkingShape(t *testing.T) {
	request, err := DecodeCreateEnvironmentRequest([]byte(`{"name":"env","config":{"networking":{"type":"unrestricted"}}}`))
	if err != nil {
		t.Fatalf("DecodeCreateEnvironmentRequest: %v", err)
	}
	body, err := json.Marshal(request.Config.Networking)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != `{"type":"unrestricted"}` {
		t.Fatalf("networking JSON = %s; want unrestricted type only", body)
	}
}

func TestNormalizeEnvironmentConfigRejectsNetworkAllowListOutsideCIDRMode(t *testing.T) {
	cases := []struct {
		name       string
		networking NetworkingConfig
	}{
		{
			name:       "unrestricted",
			networking: NetworkingConfig{Type: "unrestricted", NetworkAllowList: "10.0.0.0/8"},
		},
		{
			name:       "blocked",
			networking: NetworkingConfig{Type: "blocked", NetworkAllowList: "10.0.0.0/8"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NormalizeEnvironmentConfig(EnvironmentConfig{
				Type:       "cloud",
				Networking: &tc.networking,
			})
			if err == nil {
				t.Fatal("expected unrestricted networking validation error")
			}
		})
	}
}

func TestCreateEnvironmentRequestJSONMarshalUsesPublicFieldNames(t *testing.T) {
	body, err := json.Marshal(CreateEnvironmentRequest{Name: "env", Config: EnvironmentConfig{}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `"name"`) || strings.Contains(string(body), `"Name"`) {
		t.Fatalf("CreateEnvironmentRequest marshaled as %s; want lowercase public fields", body)
	}
	request, err := DecodeCreateEnvironmentRequest(body)
	if err != nil {
		t.Fatalf("DecodeCreateEnvironmentRequest marshaled DTO: %v", err)
	}
	if request.Name != "env" {
		t.Errorf("Name = %q; want env", request.Name)
	}
}

func TestEnvironmentPatchMaterializePreservesAndReplacesFields(t *testing.T) {
	createdAt := time.Date(2026, 4, 7, 14, 0, 0, 0, time.UTC)
	current := Environment{
		ID:          "env_current",
		Type:        "environment",
		Name:        "current",
		Description: "existing",
		Config: EnvironmentConfig{
			Type:       "cloud",
			Networking: &NetworkingConfig{Type: "unrestricted"},
			Packages:   PackageMap{"pip": []string{"pandas==2.2.0"}},
		},
		Metadata:  StringMap{"keep": "yes", "delete_null": "x", "delete_empty": "x"},
		CreatedAt: createdAt,
		UpdatedAt: createdAt,
	}

	patch, err := DecodeUpdateEnvironmentRequest([]byte(`{
		"name":"updated",
		"config":{"networking":{"type":"cidr_allow_list","network_allow_list":"10.0.0.0/8"}},
		"metadata":{"new":"value","delete_null":null,"delete_empty":""}
	}`))
	if err != nil {
		t.Fatalf("DecodeUpdateEnvironmentRequest: %v", err)
	}
	materialized, err := patch.Materialize(current)
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	if materialized.Name != "updated" {
		t.Errorf("Name = %q; want updated", materialized.Name)
	}
	if materialized.Description != "existing" {
		t.Errorf("Description = %q; want existing", materialized.Description)
	}
	if materialized.Config.Networking.Type != "cidr_allow_list" {
		t.Errorf("Networking.Type = %q; want cidr_allow_list", materialized.Config.Networking.Type)
	}
	if got := materialized.Config.Networking.NetworkAllowList; got != "10.0.0.0/8" {
		t.Errorf("NetworkAllowList = %q; want 10.0.0.0/8", got)
	}
	if _, ok := materialized.Config.Packages["pip"]; !ok {
		t.Errorf("Packages = %v; want preserved pip", materialized.Config.Packages)
	}
	if materialized.Metadata["keep"] != "yes" || materialized.Metadata["new"] != "value" {
		t.Errorf("Metadata = %v; want preserved keep and upserted new", materialized.Metadata)
	}
	if _, ok := materialized.Metadata["delete_null"]; ok {
		t.Errorf("Metadata delete_null still present: %v", materialized.Metadata)
	}
	if _, ok := materialized.Metadata["delete_empty"]; ok {
		t.Errorf("Metadata delete_empty still present: %v", materialized.Metadata)
	}
}

func TestEnvironmentPatchMaterializePreservesOmittedTopLevelFields(t *testing.T) {
	current := Environment{
		ID:          "env_current",
		Type:        "environment",
		Name:        "current",
		Description: "existing",
		Config: EnvironmentConfig{
			Type:       "cloud",
			Networking: &NetworkingConfig{Type: "cidr_allow_list", NetworkAllowList: "10.0.0.0/8"},
			Packages:   PackageMap{"pip": []string{"pandas==2.2.0"}},
		},
		Metadata: StringMap{"keep": "yes"},
	}

	patch, err := DecodeUpdateEnvironmentRequest([]byte(`{"description":"changed"}`))
	if err != nil {
		t.Fatalf("DecodeUpdateEnvironmentRequest: %v", err)
	}
	materialized, err := patch.Materialize(current)
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	if materialized.Name != "current" {
		t.Errorf("Name = %q; want current", materialized.Name)
	}
	if materialized.Description != "changed" {
		t.Errorf("Description = %q; want changed", materialized.Description)
	}
	if materialized.Config.Networking == nil || materialized.Config.Networking.Type != "cidr_allow_list" {
		t.Fatalf("Networking = %+v; want preserved cidr_allow_list", materialized.Config.Networking)
	}
	if got := materialized.Config.Networking.NetworkAllowList; got != "10.0.0.0/8" {
		t.Errorf("NetworkAllowList = %q; want preserved allow list", got)
	}
	if got := materialized.Config.Packages["pip"]; len(got) != 1 || got[0] != "pandas==2.2.0" {
		t.Errorf("Packages = %v; want preserved pip", materialized.Config.Packages)
	}
	if materialized.Metadata["keep"] != "yes" {
		t.Errorf("Metadata = %v; want preserved keep", materialized.Metadata)
	}
}

func TestDecodeUpdateEnvironmentRequestRejectsInvalidPatch(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"top level null", `null`},
		{"top level array", `[]`},
		{"top level string", `"name"`},
		{"top level number", `1`},
		{"top level boolean", `true`},
		{"version rejected", `{"version":1}`},
		{"unknown top level", `{"unknown":true}`},
		{"metadata top null", `{"metadata":null}`},
		{"metadata object", `{"metadata":{"k":{"nested":"v"}}}`},
		{"metadata array", `{"metadata":{"k":[]}}`},
		{"metadata number", `{"metadata":{"k":1}}`},
		{"metadata boolean", `{"metadata":{"k":true}}`},
		{"empty config type", `{"config":{"type":""}}`},
		{"unsupported config type", `{"config":{"type":"docker"}}`},
		{"unknown config field", `{"config":{"unknown":true}}`},
		{"empty networking type", `{"config":{"networking":{"type":""}}}`},
		{"old limited networking", `{"config":{"networking":{"type":"limited","allowed_hosts":["https://api.example.com"]}}}`},
		{"old allowed hosts field", `{"config":{"networking":{"type":"unrestricted","allowed_hosts":["http://localhost"]}}}`},
		{"old allow mcp servers field", `{"config":{"networking":{"type":"unrestricted","allow_mcp_servers":false}}}`},
		{"old allow package managers field", `{"config":{"networking":{"type":"unrestricted","allow_package_managers":false}}}`},
		{"unknown networking field", `{"config":{"networking":{"type":"cidr_allow_list","network_allow_list":"10.0.0.0/8","extra":true}}}`},
		{"unrestricted network allow list", `{"config":{"networking":{"type":"unrestricted","network_allow_list":"10.0.0.0/8"}}}`},
		{"blocked network allow list", `{"config":{"networking":{"type":"blocked","network_allow_list":"10.0.0.0/8"}}}`},
		{"cidr missing allow list", `{"config":{"networking":{"type":"cidr_allow_list"}}}`},
		{"cidr host", `{"config":{"networking":{"type":"cidr_allow_list","network_allow_list":"github.com"}}}`},
		{"cidr ip without prefix", `{"config":{"networking":{"type":"cidr_allow_list","network_allow_list":"10.0.0.1"}}}`},
		{"cidr empty element", `{"config":{"networking":{"type":"cidr_allow_list","network_allow_list":"10.0.0.0/8,"}}}`},
		{"cidr control character", `{"config":{"networking":{"type":"cidr_allow_list","network_allow_list":"10.0.0.0/8\u0000"}}}`},
		{"unknown package field", `{"config":{"packages":{"unknown":[]}}}`},
		{"unknown packages marker", `{"config":{"packages":{"type":"other"}}}`},
		{"package manager non array", `{"config":{"packages":{"pip":"pandas"}}}`},
		{"package entry non string", `{"config":{"packages":{"pip":[1]}}}`},
		{"package entry null", `{"config":{"packages":{"pip":[null]}}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeUpdateEnvironmentRequest([]byte(tc.body))
			if err == nil {
				t.Fatal("expected validation error")
			}
			var validation *ValidationError
			if !errors.As(err, &validation) {
				t.Fatalf("error = %T %v; want *ValidationError", err, err)
			}
		})
	}
}

func TestDecodeUpdateEnvironmentRequestAcceptsEmptyObjectPatch(t *testing.T) {
	current := Environment{
		Name:        "env",
		Description: "kept",
		Config: EnvironmentConfig{
			Type:       "cloud",
			Networking: &NetworkingConfig{Type: "unrestricted"},
			Packages:   PackageMap{},
		},
		Metadata: StringMap{"keep": "yes"},
	}
	patch, err := DecodeUpdateEnvironmentRequest([]byte(`{}`))
	if err != nil {
		t.Fatalf("DecodeUpdateEnvironmentRequest: %v", err)
	}
	materialized, err := patch.Materialize(current)
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	if materialized.Name != current.Name || materialized.Description != current.Description {
		t.Fatalf("materialized = %+v; want top-level fields preserved", materialized)
	}
	if materialized.Metadata["keep"] != "yes" {
		t.Fatalf("metadata = %v; want preserved keep", materialized.Metadata)
	}
}

func TestEnvironmentPatchMaterializeRejectsMetadataLimits(t *testing.T) {
	current := Environment{
		Name:      "env",
		Config:    EnvironmentConfig{Type: "cloud", Networking: &NetworkingConfig{Type: "unrestricted"}},
		Metadata:  StringMap{},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	metadata := make(map[string]string)
	for i := 0; i < 17; i++ {
		metadata[string(rune('a'+i))] = "v"
	}
	body, err := json.Marshal(map[string]any{"metadata": metadata})
	if err != nil {
		t.Fatal(err)
	}
	patch, err := DecodeUpdateEnvironmentRequest(body)
	if err != nil {
		t.Fatalf("DecodeUpdateEnvironmentRequest: %v", err)
	}
	if _, err := patch.Materialize(current); err == nil {
		t.Fatal("expected metadata pair limit error")
	}

	patch, err = DecodeUpdateEnvironmentRequest([]byte(`{"metadata":{"` + strings.Repeat("界", 65) + `":"v"}}`))
	if err != nil {
		t.Fatalf("DecodeUpdateEnvironmentRequest key length: %v", err)
	}
	if _, err := patch.Materialize(current); err == nil {
		t.Fatal("expected metadata key length error")
	}

	patch, err = DecodeUpdateEnvironmentRequest([]byte(`{"metadata":{"k":"` + strings.Repeat("界", 513) + `"}}`))
	if err != nil {
		t.Fatalf("DecodeUpdateEnvironmentRequest value length: %v", err)
	}
	if _, err := patch.Materialize(current); err == nil {
		t.Fatal("expected metadata value length error")
	}
}

func assertCreateMetadataRejected(t *testing.T, metadata map[string]string) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"name": "env", "metadata": metadata})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeCreateEnvironmentRequest(body); err == nil {
		t.Fatalf("metadata %v accepted; want rejection", metadata)
	}
}

func assertCreateMetadataAccepted(t *testing.T, metadata map[string]string) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"name": "env", "metadata": metadata})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeCreateEnvironmentRequest(body); err != nil {
		t.Fatalf("metadata %v rejected: %v", metadata, err)
	}
}

func assertCompleteDefaultConfig(t *testing.T, cfg EnvironmentConfig) {
	t.Helper()
	if cfg.Type != "cloud" {
		t.Errorf("Config.Type = %q; want cloud", cfg.Type)
	}
	if cfg.Networking == nil {
		t.Fatal("Networking is nil; want unrestricted object")
	}
	if cfg.Networking.Type != "unrestricted" {
		t.Errorf("Networking.Type = %q; want unrestricted", cfg.Networking.Type)
	}
	if cfg.Networking.NetworkAllowList != "" {
		t.Errorf("Networking = %+v; want unrestricted with no network allow list", cfg.Networking)
	}
	if len(cfg.Packages) != 0 {
		t.Errorf("Packages = %v; want empty", cfg.Packages)
	}
}
