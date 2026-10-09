// tetral-auth-policy applies an explicit Auth policy change set through a
// protected administrative connection. It never migrates schema or seeds policy.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage"
)

func main() {
	if err := run(context.Background(), os.Getenv, os.Stdin, os.Stdout); err != nil {
		// Driver and JSON errors can contain credentials or untrusted policy fields.
		_, _ = io.WriteString(os.Stderr, "Auth policy import failed; check the policy document, administrative privileges, schema readiness, and protected PostgreSQL connection.\n")
		os.Exit(1)
	}
}

func run(ctx context.Context, getenv func(string) string, input io.Reader, output io.Writer) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	raw, err := io.ReadAll(io.LimitReader(input, 1024*1024+1))
	if err != nil {
		return err
	}
	document, err := auth.DecodePolicyDocument(raw)
	if err != nil {
		return err
	}
	config, owner, err := dbconnect.OpenProtectedConfig(ctx, getenv("TETRAL_DATABASE_ADMIN_URL"), getenv("TETRAL_DATABASE_TLS_CA_PATH"), getenv("TETRAL_DATABASE_TLS_SERVER_NAME"))
	if err != nil {
		return err
	}
	defer func() { _ = owner.Close() }()
	name := getenv("TETRAL_DATABASE_TLS_SERVER_NAME")
	db := sql.OpenDB(stdlib.GetConnector(*config, stdlib.OptionBeforeConnect(func(_ context.Context, next *pgx.ConnConfig) error {
		var refreshErr error
		next.TLSConfig, refreshErr = owner.ClientTLSConfig(name, "")
		next.Fallbacks = nil
		return refreshErr
	})))
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)
	if err := storage.VerifySchema(ctx, db); err != nil {
		return err
	}
	changes, err := auth.NewPolicyStore(db).Apply(ctx, document)
	if err != nil {
		return err
	}
	// No security_config, assertion, opaque token, connection string or digest
	// is ever emitted. Empty/no-op imports report an empty changes array.
	if changes == nil {
		changes = []auth.PolicyChange{}
	}
	return json.NewEncoder(output).Encode(struct {
		Changes []auth.PolicyChange `json:"changes"`
	}{Changes: changes})
}
