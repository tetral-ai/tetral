package database

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

//go:embed roles.json
var rolesJSON []byte

func RoleContractDigest() string {
	digest := sha256.Sum256(rolesJSON)
	return hex.EncodeToString(digest[:])
}

type RoleContract struct {
	Version        int                     `json:"version"`
	MigrationOwner string                  `json:"migration_owner"`
	Workloads      map[string]WorkloadRole `json:"workloads"`
}

type WorkloadRole struct {
	Tables    map[string][]string `json:"tables"`
	Sequences []string            `json:"sequences,omitempty"`
}

func LoadRoleContract() (RoleContract, error) {
	var contract RoleContract
	if err := json.Unmarshal(rolesJSON, &contract); err != nil {
		return RoleContract{}, fmt.Errorf("decode PostgreSQL role contract: %w", err)
	}
	if contract.Version != 1 || contract.MigrationOwner == "" || contract.Workloads[contract.MigrationOwner].Tables != nil {
		return RoleContract{}, fmt.Errorf("invalid PostgreSQL role contract")
	}
	postgresql, err := LoadPostgreSQL()
	if err != nil {
		return RoleContract{}, err
	}
	knownTables := map[string]bool{postgresql.AppendOnlyWorkspaceTable: true}
	for _, table := range append(append([]string(nil), postgresql.WorkspaceTables...), postgresql.GlobalTables...) {
		knownTables[table] = true
	}
	allowedPrivileges := map[string]bool{"SELECT": true, "INSERT": true, "UPDATE": true, "DELETE": true}
	for workload, role := range contract.Workloads {
		if workload == "" || len(role.Tables) == 0 {
			return RoleContract{}, fmt.Errorf("invalid PostgreSQL workload role")
		}
		for table, privileges := range role.Tables {
			if !knownTables[table] || len(privileges) == 0 {
				return RoleContract{}, fmt.Errorf("invalid table grant for workload %q", workload)
			}
			seen := map[string]bool{}
			for _, privilege := range privileges {
				if !allowedPrivileges[privilege] || seen[privilege] {
					return RoleContract{}, fmt.Errorf("invalid table privilege for workload %q", workload)
				}
				seen[privilege] = true
			}
		}
		if duplicate(role.Sequences) {
			return RoleContract{}, fmt.Errorf("duplicate sequence grant for workload %q", workload)
		}
	}
	return contract, nil
}

func (c RoleContract) WorkloadNames() []string {
	names := make([]string, 0, len(c.Workloads))
	for name := range c.Workloads {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
