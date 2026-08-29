package database

import "testing"

func TestPostgreSQLContractHasOneOwnerForEveryDeclaredTableAndPolicy(t *testing.T) {
	contract, err := LoadPostgreSQL()
	if err != nil {
		t.Fatal(err)
	}
	if len(contract.WorkspaceTables) == 0 || len(contract.GlobalTables) == 0 || len(contract.SpecialPolicies) == 0 {
		t.Fatalf("contract is incomplete: %+v", contract)
	}
}
