package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestPolicyCommandRejectsInvalidInputBeforeDatabaseAccess(t *testing.T) {
	for _, input := range []string{
		`{"workspace_grants":[{"id":"g","identity_id":"i","workspace_id":"a","WORKSPACE_ID":"b","role":"workspace_full_access","enabled":true}]}`,
		`{"unknown":"sensitive-input"}`, `null`, `[]`, `{}`, strings.Repeat(" ", 1024*1024+1),
	} {
		var output bytes.Buffer
		accesses := 0
		getenv := func(string) string { accesses++; return "" }
		err := run(context.Background(), getenv, strings.NewReader(input), &output)
		if err == nil {
			t.Fatal("invalid input or absent protected connection accepted")
		}
		if output.Len() != 0 {
			t.Fatal("failed import emitted output")
		}
		if input != "{}" && accesses != 0 {
			t.Fatal("invalid document attempted a database connection")
		}
	}
}
