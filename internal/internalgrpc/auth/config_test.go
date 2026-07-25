package auth

import (
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/workload"
)

func TestParseServiceAccountEnforcesDNS1123Labels(t *testing.T) {
	longLabel := strings.Repeat("a", 64)
	for _, test := range []struct {
		name string
		raw  string
		want bool
	}{
		{name: "valid lowercase", raw: "tetral-agent-runtime/agent-runtime", want: true},
		{name: "valid single char parts", raw: "a/b", want: true},
		{name: "valid digits", raw: "ns1/sa2", want: true},
		{name: "uppercase namespace", raw: "Tetral/agent-runtime"},
		{name: "uppercase name", raw: "tetral/Agent"},
		{name: "embedded space namespace", raw: "tetral runtime/agent"},
		{name: "embedded space name", raw: "tetral/agent runtime"},
		{name: "punctuation underscore", raw: "tetral_runtime/agent"},
		{name: "punctuation dot", raw: "tetral.runtime/agent"},
		{name: "leading dash namespace", raw: "-tetral/agent"},
		{name: "trailing dash namespace", raw: "tetral-/agent"},
		{name: "leading dash name", raw: "tetral/-agent"},
		{name: "trailing dash name", raw: "tetral/agent-"},
		{name: "empty namespace", raw: "/agent"},
		{name: "empty name", raw: "tetral/"},
		{name: "blank", raw: ""},
		{name: "too long namespace", raw: longLabel + "/agent"},
		{name: "too long name", raw: "tetral/" + longLabel},
		{name: "extra separator", raw: "tetral/runtime/agent"},
		{name: "wildcard", raw: "*/agent"},
	} {
		t.Run(test.name, func(t *testing.T) {
			account, err := ParseServiceAccount(test.raw)
			if test.want {
				if err != nil {
					t.Fatalf("ParseServiceAccount(%q) error = %v; want valid", test.raw, err)
				}
				if account.String() != strings.TrimSpace(test.raw) {
					t.Fatalf("ParseServiceAccount(%q) = %q; want round-trip", test.raw, account.String())
				}
				return
			}
			if err == nil {
				t.Fatalf("ParseServiceAccount(%q) accepted a malformed identity", test.raw)
			}
		})
	}
}

func TestLoadConfigRejectsMalformedAllowlistAndNamesEnvKey(t *testing.T) {
	for _, test := range []struct {
		name      string
		allowlist string
	}{
		{name: "uppercase", allowlist: "Tetral/agent"},
		{name: "embedded space", allowlist: "tetral/agent runtime"},
		{name: "punctuation", allowlist: "tetral/agent.runtime"},
		{name: "trailing dash", allowlist: "tetral/agent-"},
		{name: "extra separator", allowlist: "tetral/runtime/agent"},
		{name: "mixed valid and malformed", allowlist: "tetral/agent,Tetral/Other"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := LoadConfig(envMap{
				"TETRAL_INTERNAL_GRPC_AUDIENCE":            Audience,
				"TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS": test.allowlist,
			})
			if err == nil {
				t.Fatalf("LoadConfig accepted malformed allowlist %q", test.allowlist)
			}
			if !strings.Contains(err.Error(), "TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS") {
				t.Fatalf("error %q does not name the env key", err.Error())
			}
			if _, ok := workload.AsConfigError(err); !ok {
				t.Fatalf("malformed allowlist %q returned %T; want a workload.ConfigError", test.allowlist, err)
			}
		})
	}
}

func TestLoadConfigRejectsWrongAudienceAsConfigError(t *testing.T) {
	_, err := LoadConfig(envMap{
		"TETRAL_INTERNAL_GRPC_AUDIENCE":            "wrong",
		"TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS": "tetral-agent-runtime/agent-runtime",
	})
	if err == nil {
		t.Fatal("LoadConfig accepted a wrong audience")
	}
	configErr, ok := workload.AsConfigError(err)
	if !ok {
		t.Fatalf("wrong audience returned %T; want a workload.ConfigError", err)
	}
	if !strings.Contains(configErr.Error(), "TETRAL_INTERNAL_GRPC_AUDIENCE") {
		t.Fatalf("config error %q does not name the audience env key", configErr.Error())
	}
}

func TestLoadConfigAcceptsValidLowercaseAllowlist(t *testing.T) {
	cfg, err := LoadConfig(envMap{
		"TETRAL_INTERNAL_GRPC_AUDIENCE":            Audience,
		"TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS": "tetral-agent-runtime/agent-runtime,other-ns/other-sa",
	})
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if len(cfg.AllowedServiceAccounts) != 2 {
		t.Fatalf("AllowedServiceAccounts = %#v; want two valid identities", cfg.AllowedServiceAccounts)
	}
}
