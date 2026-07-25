package storage

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// sensitivePasswordSentinel is a fixed string injected into DSNs for the
// secret-redaction tests. The redaction proof asserts this exact string
// never appears in any returned error text or returned error wrapper
// chain.
const sensitivePasswordSentinel = "do-not-leak-this"

// requirePostgreSQLTestDSN returns the configured TETRAL_TEST_DATABASE_URL
// or fails the test with a clear message. PostgreSQL-backed tests must not
// skip silently when their required database is unavailable.
func requirePostgreSQLTestDSN(t testing.TB) string {
	t.Helper()
	dsn := os.Getenv("TETRAL_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("TETRAL_TEST_DATABASE_URL is required: set it to a real PostgreSQL test DSN. CI provides it automatically.")
	}
	return dsn
}

func TestOpenPostgreSQLDatabaseReturnsErrorWhenDSNEmpty(t *testing.T) {
	ctx := context.Background()
	_, err := OpenPostgreSQLDatabase(ctx, "TETRAL_DATABASE_URL", "")
	if err == nil {
		t.Fatal("expected error for empty DSN, got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "TETRAL_DATABASE_URL") {
		t.Errorf("error message must name the env var, got %q", msg)
	}
	// Belt-and-braces: an empty DSN must not surface anything that looks
	// like a connection string in the error.
	if strings.Contains(msg, "postgres://") || strings.Contains(msg, "postgresql://") {
		t.Errorf("error message must not contain a connection string, got %q", msg)
	}
}

// redactionCases enumerates every sensitive location the contract pins:
// userinfo password, sslpassword query, passfile query, an arbitrary
// query parameter, and the full raw DSN string.
type redactionCase struct {
	name string
	dsn  string
}

func sensitiveDSNRedactionCases(host string) []redactionCase {
	return []redactionCase{
		{
			name: "userinfo_password",
			dsn:  "postgres://tetral:" + sensitivePasswordSentinel + "@" + host + "/tetral?connect_timeout=1",
		},
		{
			name: "sslpassword_query",
			dsn:  "postgres://tetral@" + host + "/tetral?sslpassword=" + sensitivePasswordSentinel + "&connect_timeout=1",
		},
		{
			name: "passfile_query",
			dsn:  "postgres://tetral@" + host + "/tetral?passfile=" + sensitivePasswordSentinel + "&connect_timeout=1",
		},
		{
			name: "arbitrary_query_parameter",
			dsn:  "postgres://tetral@" + host + "/tetral?application_name=" + sensitivePasswordSentinel + "&connect_timeout=1",
		},
		{
			name: "full_raw_dsn",
			dsn:  "postgres://tetral:" + sensitivePasswordSentinel + "@" + host + "/tetral?sslpassword=" + sensitivePasswordSentinel + "&passfile=" + sensitivePasswordSentinel + "&application_name=" + sensitivePasswordSentinel + "&connect_timeout=1",
		},
	}
}

// assertRedacted verifies that an error message contains neither the
// sensitive sentinel nor the raw DSN string. The contract's redaction
// rule pins both checks.
func assertRedacted(t *testing.T, err error, dsn string) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	msg := err.Error()
	if strings.Contains(msg, sensitivePasswordSentinel) {
		t.Errorf("error message must not contain sentinel password %q, got %q", sensitivePasswordSentinel, msg)
	}
	if strings.Contains(msg, dsn) {
		t.Errorf("error message must not contain raw DSN, got %q", msg)
	}
	// A complete connection string after the scheme should not be
	// echoed either; pgx-generated errors may otherwise embed the DSN.
	for _, scheme := range []string{"postgres://", "postgresql://"} {
		if idx := strings.Index(msg, scheme); idx >= 0 {
			tail := msg[idx:]
			if strings.Contains(tail, "@") {
				t.Errorf("error message must not contain a userinfo-bearing connection string fragment, got %q", msg)
			}
		}
	}
}

func TestOpenPostgreSQLDatabaseRedactsMalformedDSN(t *testing.T) {
	ctx := context.Background()
	// "@@@" is malformed for both the URL parser and pgx's keyword/value
	// parser, forcing the parse-stage redaction path.
	for _, tc := range sensitiveDSNRedactionCases("@@@notahost@@@") {
		t.Run(tc.name, func(t *testing.T) {
			_, err := OpenPostgreSQLDatabase(ctx, "TETRAL_TEST_DATABASE_URL", tc.dsn)
			assertRedacted(t, err, tc.dsn)
			if !strings.Contains(err.Error(), "TETRAL_TEST_DATABASE_URL") {
				t.Errorf("error must name supplied env var, got %q", err.Error())
			}
		})
	}
}

func TestOpenPostgreSQLDatabaseRedactsUnreachableDSN(t *testing.T) {
	// 127.0.0.1:1 is reserved + unbound on test hosts → fast deterministic
	// "connection refused" without external dependencies.
	const unreachableHost = "127.0.0.1:1"
	for _, tc := range sensitiveDSNRedactionCases(unreachableHost) {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := OpenPostgreSQLDatabase(ctx, "TETRAL_TEST_DATABASE_URL", tc.dsn)
			assertRedacted(t, err, tc.dsn)
		})
	}
}

func TestOpenPostgreSQLDatabaseOpensValidConnection(t *testing.T) {
	dsn := requirePostgreSQLTestDSN(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	db, err := OpenPostgreSQLDatabase(ctx, "TETRAL_TEST_DATABASE_URL", dsn)
	if err != nil {
		t.Fatalf("OpenPostgreSQLDatabase: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext after open: %v", err)
	}
}

func TestOpenPostgreSQLDatabaseFromEnvReadsTetralDatabaseURL(t *testing.T) {
	dsn := requirePostgreSQLTestDSN(t)
	t.Setenv("TETRAL_DATABASE_URL", dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	db, err := OpenPostgreSQLDatabaseFromEnv(ctx)
	if err != nil {
		t.Fatalf("OpenPostgreSQLDatabaseFromEnv: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
}

func TestOpenPostgreSQLDatabaseFromEnvReportsMissingTetralDatabaseURL(t *testing.T) {
	t.Setenv("TETRAL_DATABASE_URL", "")
	ctx := context.Background()
	_, err := OpenPostgreSQLDatabaseFromEnv(ctx)
	if err == nil {
		t.Fatal("expected error when TETRAL_DATABASE_URL is unset, got nil")
	}
	if !strings.Contains(err.Error(), "TETRAL_DATABASE_URL") {
		t.Errorf("missing-config error must name TETRAL_DATABASE_URL, got %q", err.Error())
	}
}

func TestOpenPostgreSQLDatabaseFromEnvDoesNotFallBackToTestVar(t *testing.T) {
	// Setting only the test variable must not satisfy the production
	// path. The contract makes the env-var boundary load-bearing.
	t.Setenv("TETRAL_DATABASE_URL", "")
	t.Setenv("TETRAL_TEST_DATABASE_URL", "postgres://tetral:should-not-be-read@127.0.0.1:1/tetral")
	ctx := context.Background()
	_, err := OpenPostgreSQLDatabaseFromEnv(ctx)
	if err == nil {
		t.Fatal("expected missing-config error, got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "TETRAL_DATABASE_URL") {
		t.Errorf("missing-config error must name TETRAL_DATABASE_URL, got %q", msg)
	}
	if strings.Contains(msg, "TETRAL_TEST_DATABASE_URL") {
		t.Errorf("missing-config error must not echo TETRAL_TEST_DATABASE_URL, got %q", msg)
	}
}

func TestConfigurePostgreSQLConnectionPreservesSecureSslmode(t *testing.T) {
	cases := []struct {
		name    string
		sslmode string
	}{
		{"sslmode_require", "require"},
		{"sslmode_verify_full", "verify-full"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dsn := "postgres://tetral@example.invalid/tetral?sslmode=" + tc.sslmode
			cfg, err := configurePostgreSQLConnection(dsn)
			if err != nil {
				t.Fatalf("configurePostgreSQLConnection(%q): %v", dsn, err)
			}
			// pgx exposes the resolved TLS state via TLSConfig: nil
			// means sslmode=disable, non-nil means TLS is required and
			// the configured sslmode was honored.
			if cfg.TLSConfig == nil {
				t.Errorf("sslmode=%s parsed without TLS — must not be downgraded to sslmode=disable", tc.sslmode)
			}
		})
	}
}

func TestPostgreSQLProductionSourceContainsNoSslmodeDisable(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	dir := filepath.Dir(thisFile)
	productionFile := filepath.Join(dir, "postgresql_database.go")
	content, err := os.ReadFile(productionFile) //nolint:gosec // test-only static read of repo source
	if err != nil {
		t.Fatalf("read %s: %v", productionFile, err)
	}
	if strings.Contains(string(content), "sslmode=disable") {
		t.Errorf("%s must not contain sslmode=disable; production code must honor caller TLS settings", productionFile)
	}
}

func TestPgxDirectDependencyAtSecurityFloor(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	dir := filepath.Dir(thisFile)
	goMod := filepath.Join(dir, "..", "..", "go.mod")
	content, err := os.ReadFile(goMod) //nolint:gosec // test-only static read of repo go.mod
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	version := pgxVersionFromGoMod(t, string(content))
	if compareSemver(version, "v5.9.0") < 0 {
		t.Errorf("github.com/jackc/pgx/v5 version %q is below the v5.9.0 floor required by GO-2026-4772 / CVE-2026-33816", version)
	}
}

// pgxVersionFromGoMod scans go.mod text for the
// `github.com/jackc/pgx/v5 vX.Y.Z` token and returns the version. Fails
// the test if the dependency is absent or no version follows it.
func pgxVersionFromGoMod(t *testing.T, goMod string) string {
	t.Helper()
	const module = "github.com/jackc/pgx/v5"
	for _, line := range strings.Split(goMod, "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		// Module line shape inside require blocks: `module vX.Y.Z` or
		// `module vX.Y.Z // indirect`.
		for i := 0; i+1 < len(fields); i++ {
			if fields[i] == module && strings.HasPrefix(fields[i+1], "v") {
				return fields[i+1]
			}
		}
	}
	t.Fatalf("github.com/jackc/pgx/v5 not found in go.mod")
	return ""
}

// compareSemver compares two `vMAJOR.MINOR.PATCH` strings. Returns -1, 0,
// or 1. Pre-release suffixes are treated as part of the patch token; that
// is good enough for the v5.9.x floor check.
func compareSemver(a, b string) int {
	parse := func(v string) [3]int {
		v = strings.TrimPrefix(v, "v")
		// Trim a possible "+meta" or "-pre" suffix from the patch.
		if idx := strings.IndexAny(v, "-+"); idx >= 0 {
			v = v[:idx]
		}
		parts := strings.SplitN(v, ".", 3)
		var out [3]int
		for i := 0; i < len(parts) && i < 3; i++ {
			n := 0
			for _, r := range parts[i] {
				if r < '0' || r > '9' {
					break
				}
				n = n*10 + int(r-'0')
			}
			out[i] = n
		}
		return out
	}
	ax, bx := parse(a), parse(b)
	for i := 0; i < 3; i++ {
		if ax[i] < bx[i] {
			return -1
		}
		if ax[i] > bx[i] {
			return 1
		}
	}
	return 0
}
