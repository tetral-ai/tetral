package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const recommendedWorkspaceIDBytes = 20

type bootstrapConfig struct {
	workspaceID workspace.ID
	name        string
}

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout); err != nil {
		if _, printErr := fmt.Fprintf(os.Stdout, "tetral-bootstrap: %v\n", err); printErr != nil {
			os.Exit(1)
		}
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, output io.Writer) error {
	config, err := parseBootstrapConfig(args)
	if err != nil {
		return err
	}
	openResult, err := dbconnect.OpenPlainDSNFromEnv(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = openResult.Client.Close() }()
	return seedBootstrapWorkspace(ctx, openResult.RawDatabaseForExcludedStores, config, output)
}

func parseBootstrapConfig(args []string) (bootstrapConfig, error) {
	flags := flag.NewFlagSet("tetral-bootstrap", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var workspaceID string
	var name string
	flags.StringVar(&workspaceID, "workspace-id", "", "workspace identifier")
	flags.StringVar(&name, "name", "", "workspace display name")
	if err := flags.Parse(args); err != nil {
		return bootstrapConfig{}, err
	}
	if flags.NArg() != 0 {
		return bootstrapConfig{}, fmt.Errorf("unexpected positional arguments")
	}
	if err := validateWorkspaceID(workspaceID); err != nil {
		return bootstrapConfig{}, err
	}
	if name == "" {
		name = workspaceID
	}
	return bootstrapConfig{workspaceID: workspace.ID(workspaceID), name: name}, nil
}

func validateWorkspaceID(id string) error {
	if strings.TrimSpace(id) == "" {
		return fmt.Errorf("--workspace-id is required")
	}
	if len(id) > workspace.MaxWorkspaceIDBytes {
		return fmt.Errorf("--workspace-id must be at most %d bytes", workspace.MaxWorkspaceIDBytes)
	}
	if strings.Contains(id, "/") {
		return fmt.Errorf("--workspace-id must not contain '/'")
	}
	if strings.IndexFunc(id, unicode.IsSpace) >= 0 {
		return fmt.Errorf("--workspace-id must not contain whitespace")
	}
	return nil
}

func seedBootstrapWorkspace(ctx context.Context, db *sql.DB, config bootstrapConfig, output io.Writer) error {
	if err := storage.VerifySchema(ctx, db); err != nil {
		return err
	}
	if utf8.RuneCountInString(string(config.workspaceID)) > recommendedWorkspaceIDBytes {
		if _, err := fmt.Fprintln(output, "warning: workspace id exceeds 20 characters and consumes the sandbox snapshot-name budget"); err != nil {
			return err
		}
	}
	created, err := workspace.NewSeeder(db).Seed(ctx, config.workspaceID, config.name)
	if err != nil {
		return err
	}
	if created {
		_, err = fmt.Fprintf(output, "workspace %q created\n", config.workspaceID)
		return err
	}
	_, err = fmt.Fprintf(output, "workspace %q already present\n", config.workspaceID)
	return err
}
