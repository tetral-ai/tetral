package blob_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/blob"
)

const (
	testSecretSentinel = "test-secret-do-not-leak-AKIAEXAMPLEZZZZZZ"
	testAccessSentinel = "test-access-do-not-leak-EXAMPLEKEYABCDEF"
)

func validConfigEnv(t *testing.T) {
	t.Helper()
	t.Setenv(blob.EnvRegion, "us-east-1")
	t.Setenv(blob.EnvBucket, "my-bucket")
	t.Setenv(blob.EnvAccessKey, testAccessSentinel)
	t.Setenv(blob.EnvSecretKey, testSecretSentinel)
}

func TestLoadConfigAcceptsValidEnv(t *testing.T) {
	validConfigEnv(t)
	t.Setenv(blob.EnvEndpoint, "https://s3.example.com")
	cfg, err := blob.LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Region != "us-east-1" || cfg.Bucket != "my-bucket" {
		t.Errorf("cfg = %+v", cfg)
	}
}

func TestLoadConfigDefaultEndpointAccepted(t *testing.T) {
	validConfigEnv(t)
	t.Setenv(blob.EnvEndpoint, "")
	cfg, err := blob.LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig with default endpoint: %v", err)
	}
	if cfg.Endpoint != "" {
		t.Errorf("Endpoint = %q; want empty (AWS default)", cfg.Endpoint)
	}
}

func TestLoadConfigRejectsMissingRegion(t *testing.T) {
	t.Setenv(blob.EnvBucket, "b")
	t.Setenv(blob.EnvAccessKey, testAccessSentinel)
	t.Setenv(blob.EnvSecretKey, testSecretSentinel)
	t.Setenv(blob.EnvRegion, "")
	_, err := blob.LoadConfig()
	if err == nil {
		t.Fatal("expected reject")
	}
	assertConfigErrorNoSecretLeak(t, err)
}

func TestLoadConfigRejectsMissingBucket(t *testing.T) {
	t.Setenv(blob.EnvRegion, "us-east-1")
	t.Setenv(blob.EnvAccessKey, testAccessSentinel)
	t.Setenv(blob.EnvSecretKey, testSecretSentinel)
	t.Setenv(blob.EnvBucket, "")
	_, err := blob.LoadConfig()
	if err == nil {
		t.Fatal("expected reject")
	}
	assertConfigErrorNoSecretLeak(t, err)
}

func TestLoadConfigRejectsMissingAccessKey(t *testing.T) {
	t.Setenv(blob.EnvRegion, "us-east-1")
	t.Setenv(blob.EnvBucket, "b")
	t.Setenv(blob.EnvSecretKey, testSecretSentinel)
	t.Setenv(blob.EnvAccessKey, "")
	_, err := blob.LoadConfig()
	if err == nil {
		t.Fatal("expected reject")
	}
	assertConfigErrorNoSecretLeak(t, err)
}

func TestLoadConfigRejectsMissingSecretKey(t *testing.T) {
	t.Setenv(blob.EnvRegion, "us-east-1")
	t.Setenv(blob.EnvBucket, "b")
	t.Setenv(blob.EnvAccessKey, testAccessSentinel)
	t.Setenv(blob.EnvSecretKey, "")
	_, err := blob.LoadConfig()
	if err == nil {
		t.Fatal("expected reject")
	}
	assertConfigErrorNoSecretLeak(t, err)
}

func TestLoadConfigRejectsHTTPEndpointWithoutOptIn(t *testing.T) {
	validConfigEnv(t)
	t.Setenv(blob.EnvEndpoint, "http://insecure.example.com")
	_, err := blob.LoadConfig()
	if err == nil {
		t.Fatal("expected reject for http:// endpoint without opt-in")
	}
	assertConfigErrorNoSecretLeak(t, err)
	if !strings.Contains(err.Error(), "https") {
		t.Errorf("error must mention https requirement: %q", err.Error())
	}
}

func TestLoadConfigRejectsInsecureWithoutLocalTestMode(t *testing.T) {
	validConfigEnv(t)
	t.Setenv(blob.EnvEndpoint, "http://localhost:9000")
	t.Setenv(blob.EnvAllowInsecure, "true")
	t.Setenv(blob.EnvLocalTestMode, "")
	_, err := blob.LoadConfig()
	if err == nil {
		t.Fatal("expected reject when AllowInsecure is set without LocalTestMode")
	}
	assertConfigErrorNoSecretLeak(t, err)
	if !strings.Contains(err.Error(), blob.EnvLocalTestMode) {
		t.Errorf("error must mention LocalTestMode requirement: %q", err.Error())
	}
}

func TestLoadConfigAcceptsHTTPEndpointWithLocalTestOptIn(t *testing.T) {
	validConfigEnv(t)
	t.Setenv(blob.EnvEndpoint, "http://localhost:9000")
	t.Setenv(blob.EnvAllowInsecure, "true")
	t.Setenv(blob.EnvLocalTestMode, "true")
	cfg, err := blob.LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig with local-test opt-in: %v", err)
	}
	if !cfg.AllowInsecure || !cfg.LocalTestMode {
		t.Errorf("cfg = %+v; expected AllowInsecure=true and LocalTestMode=true", cfg)
	}
}

func TestLoadConfigRejectsCredentialBearingURL(t *testing.T) {
	validConfigEnv(t)
	t.Setenv(blob.EnvEndpoint, "https://user:pass@example.com")
	_, err := blob.LoadConfig()
	if err == nil {
		t.Fatal("expected reject for credential-bearing URL")
	}
	assertConfigErrorNoSecretLeak(t, err)
	assertConfigErrorExcludes(t, err, "user", "pass", "user:pass@example.com")
	if !strings.Contains(err.Error(), "credentials") {
		t.Errorf("error must reflect credential rejection: %q", err.Error())
	}
}

func TestLoadConfigRejectsQueryString(t *testing.T) {
	validConfigEnv(t)
	t.Setenv(blob.EnvEndpoint, "https://example.com?x=1")
	_, err := blob.LoadConfig()
	if err == nil {
		t.Fatal("expected reject for query string in endpoint")
	}
	assertConfigErrorNoSecretLeak(t, err)
	assertConfigErrorExcludes(t, err, "x=1")
}

func TestLoadConfigRejectsFragment(t *testing.T) {
	validConfigEnv(t)
	t.Setenv(blob.EnvEndpoint, "https://example.com#x")
	_, err := blob.LoadConfig()
	if err == nil {
		t.Fatal("expected reject for fragment in endpoint")
	}
	assertConfigErrorNoSecretLeak(t, err)
	assertConfigErrorExcludes(t, err, "#x")
}

func TestLoadConfigRejectsUnknownScheme(t *testing.T) {
	validConfigEnv(t)
	t.Setenv(blob.EnvEndpoint, "ftp://example.com")
	_, err := blob.LoadConfig()
	if err == nil {
		t.Fatal("expected reject for non-http(s) scheme")
	}
	assertConfigErrorNoSecretLeak(t, err)
	assertConfigErrorExcludes(t, err, "ftp://example.com")
}

func TestLoadConfigRejectsUnsupportedSchemeWithoutLeakingEndpointFragments(t *testing.T) {
	const (
		bucketSentinel   = "bucket-do-not-leak-prod-archive"
		schemeSentinel   = "test-access-do-not-leak-unsupported-scheme"
		endpointSentinel = schemeSentinel + "://s3.example.com"
	)
	t.Setenv(blob.EnvRegion, "us-east-1")
	t.Setenv(blob.EnvBucket, bucketSentinel)
	t.Setenv(blob.EnvAccessKey, testAccessSentinel)
	t.Setenv(blob.EnvSecretKey, testSecretSentinel)
	t.Setenv(blob.EnvEndpoint, endpointSentinel)

	_, err := blob.LoadConfig()
	if err == nil {
		t.Fatal("expected reject for unsupported endpoint scheme")
	}
	assertConfigErrorNoSecretLeak(t, err)
	for _, rendered := range []string{
		err.Error(),
		fmt.Sprintf("%v", err),
		fmt.Sprintf("%+v", err),
		fmt.Sprintf("engine: blob configuration: %v", err),
	} {
		if !strings.Contains(rendered, blob.EnvEndpoint) {
			t.Errorf("error must mention endpoint env key: %q", rendered)
		}
		if !strings.Contains(rendered, "https") {
			t.Errorf("error must mention endpoint scheme rule: %q", rendered)
		}
		for _, forbidden := range []string{
			schemeSentinel,
			endpointSentinel,
			bucketSentinel,
			testAccessSentinel,
			testSecretSentinel,
		} {
			if strings.Contains(rendered, forbidden) {
				t.Errorf("error leaked forbidden config value %q: %q", forbidden, rendered)
			}
		}
	}
}

func TestAssertProductionReadyRejectsInsecureOutsideLocal(t *testing.T) {
	cfg := &blob.Config{
		Endpoint:      "https://s3.example.com",
		Region:        "us-east-1",
		Bucket:        "b",
		AccessKey:     testAccessSentinel,
		SecretKey:     testSecretSentinel,
		AllowInsecure: true,
		LocalTestMode: false,
	}
	if err := cfg.AssertProductionReady(); err == nil {
		t.Fatal("expected production reject for AllowInsecure without LocalTestMode")
	} else {
		assertConfigErrorNoSecretLeak(t, err)
	}
}

func TestAssertProductionReadyAcceptsHTTPSDefault(t *testing.T) {
	cfg := &blob.Config{
		Region:    "us-east-1",
		Bucket:    "b",
		AccessKey: testAccessSentinel,
		SecretKey: testSecretSentinel,
	}
	if err := cfg.AssertProductionReady(); err != nil {
		t.Fatalf("AssertProductionReady: %v", err)
	}
}

func TestConfigPresentDetectsAnyVar(t *testing.T) {
	t.Setenv(blob.EnvRegion, "")
	t.Setenv(blob.EnvBucket, "")
	t.Setenv(blob.EnvAccessKey, "")
	t.Setenv(blob.EnvSecretKey, "")
	t.Setenv(blob.EnvEndpoint, "")
	if blob.ConfigPresent() {
		t.Error("ConfigPresent must return false when no env vars set")
	}
	t.Setenv(blob.EnvRegion, "us-east-1")
	if !blob.ConfigPresent() {
		t.Error("ConfigPresent must return true when at least one var set")
	}
}

// assertConfigErrorNoSecretLeak pins that LoadConfig error strings
// never echo the actual access key or secret key values, even when
// rejecting the env that contains them. The HTTP error layer relies
// on this property to keep operator-readable but secret-safe text.
func assertConfigErrorNoSecretLeak(t *testing.T, err error) {
	t.Helper()
	var cfgErr *blob.ConfigError
	if !errors.As(err, &cfgErr) {
		t.Fatalf("expected *blob.ConfigError; got %T (%v)", err, err)
	}
	msg := err.Error()
	if strings.Contains(msg, testAccessSentinel) {
		t.Errorf("error leaked access key value: %q", msg)
	}
	if strings.Contains(msg, testSecretSentinel) {
		t.Errorf("error leaked secret key value: %q", msg)
	}
}

func assertConfigErrorExcludes(t *testing.T, err error, forbidden ...string) {
	t.Helper()
	msg := err.Error()
	for _, value := range forbidden {
		if value != "" && strings.Contains(msg, value) {
			t.Errorf("error leaked forbidden config fragment %q: %q", value, msg)
		}
	}
}
