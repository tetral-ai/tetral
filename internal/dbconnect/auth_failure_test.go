package dbconnect

import (
	"context"
	"errors"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestOpenPlainDSNClassifiesAuthenticationFailure(t *testing.T) {
	dsn := requirePostgreSQLTestDSN(t)
	invalidDSN, invalidPassword := invalidPasswordPlainDSN(t, dsn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := OpenPlainDSN(ctx, "TETRAL_TEST_DATABASE_URL", invalidDSN)

	var diagnostic *DiagnosticError
	if !errors.As(err, &diagnostic) {
		t.Fatalf("expected DiagnosticError, got %T (%v)", err, err)
	}
	if diagnostic.Provider != ProviderPlainDSN {
		t.Fatalf("provider = %q; want %q", diagnostic.Provider, ProviderPlainDSN)
	}
	if diagnostic.Phase != PhasePing {
		t.Fatalf("phase = %q; want %q", diagnostic.Phase, PhasePing)
	}
	if diagnostic.Kind != KindAuthenticationFailed {
		t.Fatalf("kind = %q; want %q", diagnostic.Kind, KindAuthenticationFailed)
	}
	if diagnostic.Cause == nil {
		t.Fatal("authentication diagnostic must preserve original cause")
	}
	if errors.Unwrap(diagnostic) != diagnostic.Cause {
		t.Fatalf("Unwrap() = %v; want preserved cause %v", errors.Unwrap(diagnostic), diagnostic.Cause)
	}
	assertPublicSafe(t, diagnostic, invalidDSN, dsn, invalidPassword)
}

func invalidPasswordPlainDSN(t testing.TB, dsn string) (string, string) {
	t.Helper()
	cfg, err := configurePlainDSN(dsn)
	if err != nil {
		t.Fatalf("configure test DSN: %v", err)
	}
	invalidPassword := sensitivePlainDSNSentinel + "-auth-failure"
	if cfg.Password == invalidPassword {
		invalidPassword += "-different"
	}

	if parsed, err := url.Parse(dsn); err == nil && (parsed.Scheme == "postgres" || parsed.Scheme == "postgresql") {
		user := cfg.User
		if parsed.User != nil && parsed.User.Username() != "" {
			user = parsed.User.Username()
		}
		if user == "" {
			t.Fatal("test DSN must include a user to derive an invalid-password DSN")
		}
		parsed.User = url.UserPassword(user, invalidPassword)
		return parsed.String(), invalidPassword
	}

	var parts []string
	appendKeywordDSNPart := func(key string, value string) {
		if value == "" {
			return
		}
		parts = append(parts, key+"="+quoteKeywordDSNValue(value))
	}
	appendKeywordDSNPart("host", cfg.Host)
	if cfg.Port != 0 {
		appendKeywordDSNPart("port", strconv.FormatUint(uint64(cfg.Port), 10))
	}
	appendKeywordDSNPart("user", cfg.User)
	appendKeywordDSNPart("password", invalidPassword)
	appendKeywordDSNPart("dbname", cfg.Database)
	if cfg.ConnectTimeout > 0 {
		connectTimeoutSeconds := int64(cfg.ConnectTimeout / time.Second)
		if connectTimeoutSeconds == 0 {
			connectTimeoutSeconds = 1
		}
		appendKeywordDSNPart("connect_timeout", strconv.FormatInt(connectTimeoutSeconds, 10))
	}
	runtimeParamKeys := make([]string, 0, len(cfg.RuntimeParams))
	for key := range cfg.RuntimeParams {
		runtimeParamKeys = append(runtimeParamKeys, key)
	}
	sort.Strings(runtimeParamKeys)
	for _, key := range runtimeParamKeys {
		appendKeywordDSNPart(key, cfg.RuntimeParams[key])
	}
	if len(parts) == 0 {
		t.Fatal("test DSN did not produce structured connection fields")
	}
	return strings.Join(parts, " "), invalidPassword
}

func quoteKeywordDSNValue(value string) string {
	escaped := strings.ReplaceAll(value, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `'`, `\'`)
	return "'" + escaped + "'"
}
