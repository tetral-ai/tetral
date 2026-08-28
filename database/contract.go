// Package database owns the machine-readable PostgreSQL tenant and
// policy contract shared by Go services, Bun services, and test infrastructure.
package database

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

//go:embed postgresql.json
var postgresqlJSON []byte

func PostgreSQLContractDigest() string {
	digest := sha256.Sum256(postgresqlJSON)
	return hex.EncodeToString(digest[:])
}

type PostgreSQL struct {
	Version                  int      `json:"version"`
	WorkspaceTables          []string `json:"workspace_tables"`
	AppendOnlyWorkspaceTable string   `json:"append_only_workspace_table"`
	GlobalTables             []string `json:"global_tables"`
	SpecialPolicies          []Policy `json:"special_policies"`
}

type Policy struct {
	Table   string `json:"table"`
	Name    string `json:"name"`
	Command string `json:"command"`
	Using   string `json:"using"`
	Check   string `json:"check"`
}

func LoadPostgreSQL() (PostgreSQL, error) {
	var contract PostgreSQL
	if err := json.Unmarshal(postgresqlJSON, &contract); err != nil {
		return PostgreSQL{}, fmt.Errorf("decode PostgreSQL contract: %w", err)
	}
	if contract.Version != 1 || contract.AppendOnlyWorkspaceTable == "" || len(contract.WorkspaceTables) == 0 {
		return PostgreSQL{}, fmt.Errorf("invalid PostgreSQL contract")
	}
	if duplicate(contract.WorkspaceTables) || duplicate(contract.GlobalTables) {
		return PostgreSQL{}, fmt.Errorf("PostgreSQL contract contains duplicate tables")
	}
	tables := map[string]string{}
	for _, table := range contract.WorkspaceTables {
		tables[table] = "workspace"
	}
	if previous := tables[contract.AppendOnlyWorkspaceTable]; previous != "" {
		return PostgreSQL{}, fmt.Errorf("PostgreSQL contract table %q has multiple ownership classes", contract.AppendOnlyWorkspaceTable)
	}
	tables[contract.AppendOnlyWorkspaceTable] = "append-only workspace"
	for _, table := range contract.GlobalTables {
		if previous := tables[table]; previous != "" {
			return PostgreSQL{}, fmt.Errorf("PostgreSQL contract table %q has multiple ownership classes", table)
		}
		tables[table] = "global"
	}
	policies := map[string]bool{}
	for _, policy := range contract.SpecialPolicies {
		if tables[policy.Table] == "" || policy.Name == "" || policy.Command == "" {
			return PostgreSQL{}, fmt.Errorf("PostgreSQL contract contains an invalid special policy")
		}
		key := policy.Table + "\x00" + policy.Name
		if policies[key] {
			return PostgreSQL{}, fmt.Errorf("PostgreSQL contract contains duplicate policy %q", policy.Name)
		}
		policies[key] = true
	}
	return contract, nil
}

func duplicate(values []string) bool {
	copyValues := append([]string(nil), values...)
	sort.Strings(copyValues)
	for index := 1; index < len(copyValues); index++ {
		if copyValues[index] == copyValues[index-1] {
			return true
		}
	}
	return false
}
