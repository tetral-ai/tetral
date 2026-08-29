package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/tetral-ai/tetral/internal/testinfra"
)

type ledger struct {
	Version int                         `json:"version"`
	Rows    []testinfra.ShadowLedgerRow `json:"rows"`
}

func main() {
	input := flag.String("input", "", "captured GitHub API snapshot; omit for read-only live collection")
	repository := flag.String("repository", "", "owner/repository for read-only live collection")
	pullRequest := flag.Int("pull-request", 0, "pull request number for read-only live collection")
	forkPendingAt := flag.String("fork-pending-observed-at", "", "RFC3339 time when an external fork run was observed pending approval")
	forkApprovedAt := flag.String("fork-approved-at", "", "RFC3339 time when the external fork run was approved")
	output := flag.String("output", "shadow-ledger.json", "normalized append-only ledger")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	var snapshot testinfra.ShadowSnapshot
	var err error
	if *input != "" {
		snapshot, err = readSnapshot(*input)
	} else if *repository != "" && *pullRequest > 0 {
		snapshot, err = testinfra.CollectLiveShadowSnapshot(ctx, *repository, *pullRequest)
	} else {
		err = fmt.Errorf("provide --input or both --repository and --pull-request")
	}
	if err != nil {
		fatal(err)
	}
	if *forkPendingAt != "" || *forkApprovedAt != "" {
		pending, pendingErr := time.Parse(time.RFC3339, *forkPendingAt)
		approved, approvedErr := time.Parse(time.RFC3339, *forkApprovedAt)
		if pendingErr != nil || approvedErr != nil || !approved.After(pending) {
			fatal(fmt.Errorf("fork approval evidence must contain ordered RFC3339 timestamps"))
		}
		snapshot.ForkApproval = &testinfra.ShadowForkApproval{PendingObservedAt: pending, ApprovedAt: approved}
	}
	row, err := testinfra.NormalizeShadowSnapshot(snapshot)
	if err != nil {
		fatal(err)
	}
	snapshotPath := filepath.Join(filepath.Dir(*output), "shadow-snapshots", strings.TrimPrefix(row.SnapshotDigest, "sha256:")+".json")
	if err := writeJSON(snapshotPath, snapshot); err != nil {
		fatal(err)
	}
	value, err := readLedger(*output)
	if err != nil {
		fatal(err)
	}
	for _, existing := range value.Rows {
		if existing.LegacyRunID == row.LegacyRunID && existing.LegacyRunAttempt == row.LegacyRunAttempt &&
			existing.ShadowRunID == row.ShadowRunID && existing.ShadowRunAttempt == row.ShadowRunAttempt {
			fatal(fmt.Errorf("shadow execution pair already exists in the ledger"))
		}
	}
	value.Rows = append(value.Rows, row)
	if err := writeJSON(*output, value); err != nil {
		fatal(err)
	}
}

func readSnapshot(path string) (testinfra.ShadowSnapshot, error) {
	// The path is an explicit operator-supplied immutable capture.
	//nolint:gosec
	body, err := os.ReadFile(path)
	if err != nil {
		return testinfra.ShadowSnapshot{}, err
	}
	var snapshot testinfra.ShadowSnapshot
	if err := json.Unmarshal(body, &snapshot); err != nil {
		return testinfra.ShadowSnapshot{}, fmt.Errorf("decode shadow snapshot: %w", err)
	}
	return snapshot, nil
}

func readLedger(path string) (ledger, error) {
	// The path is the explicit operator-owned output ledger.
	//nolint:gosec
	body, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return ledger{Version: 1}, nil
	}
	if err != nil {
		return ledger{}, err
	}
	var value ledger
	if err := json.Unmarshal(body, &value); err != nil {
		return ledger{}, fmt.Errorf("decode shadow ledger: %w", err)
	}
	if value.Version != 1 {
		return ledger{}, fmt.Errorf("unsupported shadow ledger version")
	}
	return value, nil
}

func writeJSON(path string, value any) error {
	body, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	if directory := filepath.Dir(path); directory != "." {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return err
		}
	}
	return os.WriteFile(path, append(body, '\n'), 0o600)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
