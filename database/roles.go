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
	Functions []string            `json:"functions,omitempty"`
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
	knownTables := map[string]bool{}
	for _, table := range append(append(append([]string(nil), postgresql.WorkspaceTables...), postgresql.AppendOnlyWorkspaceTables...), postgresql.GlobalTables...) {
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
		for _, function := range role.Functions {
			allowed := (function == "tetral_lock_runtime_process(text, text, text)" && (workload == "bridge" || workload == "job_runner")) ||
				(function == "tetral_lock_runtime_process_liveness(text, text, text)" && workload == "job_runner") ||
				(function == "tetral_job_runner_binding_upper()" && workload == "job_runner") ||
				(function == "tetral_job_runner_binding_page(text, text, text, text, integer)" && workload == "job_runner") ||
				(function == "tetral_cleanup_due_sessions(timestamptz, timestamptz, text, integer)" && workload == "cleanup")
			if workload == "auth" {
				switch function {
				case "tetral_auth_lookup_key(bytea)", "tetral_auth_lookup_token(bytea)", "tetral_auth_lookup_grants(text, text)", "tetral_auth_lock_authority(text, text, text, text)", "tetral_auth_prune_tokens(integer)":
					allowed = true
				}
			}
			if !allowed {
				return RoleContract{}, fmt.Errorf("invalid function grant for workload %q", workload)
			}
		}
		if duplicate(role.Functions) {
			return RoleContract{}, fmt.Errorf("duplicate function grant for workload %q", workload)
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
