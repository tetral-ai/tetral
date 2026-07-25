package dbconnect

import (
	"context"
	"strings"
	"testing"
)

func TestResourceIDPrefixesAreRejectedSafelyAcrossDispatchPathsWithoutDatabase(t *testing.T) {
	client := newTestClient(nil)
	for _, prefix := range []string{
		"workspace_",
		"agent_",
		"agv_",
		"skill_",
		"skill_version_",
		"skv_",
		"env_",
		"vlt_",
		"cred_",
		"ak_",
		"req_",
		"run_",
		"sevt_",
		"sesn_",
		"sesrsc_",
		"sub_",
		"file_",
		"fobj_",
		"memstore_",
		"mem_",
		"memver_",
	} {
		operation := "dbconnect." + prefix + "operation_label_probe"
		t.Run(prefix, func(t *testing.T) {
			assertOperationRejectedBeforeDispatch(t, client, operation)
		})
	}
}

func TestEmbeddedResourceIDShapesAreRejectedSafelyAcrossDispatchPathsWithoutDatabase(t *testing.T) {
	client := newTestClient(nil)
	for _, operation := range []string{
		"dbconnect.delete_file_0000000000000000",
		"dbconnect.cleanup_fobj_0000000000000000",
		"memory.audit_memstore_0000000000000000",
		"memory.audit_mem_0000000000000000",
		"skill.audit_skill_version_0000000000000000",
		"agent.audit_agent_0123456789abcdef",
		"run.audit_run_0123456789abcdef",
		"sessionevent.audit_sevt_0123456789abcdef",
		"session.audit_sesn_0123456789abcdef",
		"session.audit_sesrsc_0123456789abcdef",
	} {
		t.Run(operation, func(t *testing.T) {
			assertOperationRejectedBeforeDispatch(t, client, operation)
		})
	}
}

func TestOperationLabelValidationAllowsStaticMemoryOperationLabels(t *testing.T) {
	for _, operation := range []string{
		"memory.create_store",
		"memory.update",
		"memory.profile_update",
		"memory.audit_memstore_operation",
		"files.create",
		"dbconnect.profile_update",
		"skill.audit_skill_version_operation",
	} {
		if err := validateOperationLabel(operation); err != nil {
			t.Fatalf("validateOperationLabel(%q) = %v; want nil", operation, err)
		}
	}
}

func assertOperationRejectedBeforeDispatch(t *testing.T, client *Client, operation string) {
	t.Helper()
	ctx := context.Background()

	_, err := client.Exec(ctx, operation, `SELECT 1`)
	assertInvalidOperationRejectedSafely(t, err, operation)

	_, err = client.Query(ctx, operation, `SELECT 1`)
	assertInvalidOperationRejectedSafely(t, err, operation)

	err = client.QueryRow(ctx, operation, `SELECT 1`).Scan(new(int))
	assertInvalidOperationRejectedSafely(t, err, operation)

	err = client.WithTx(ctx, operation, nil, func(_ *Tx) error {
		t.Fatal("WithTx callback must not run for invalid operation")
		return nil
	})
	assertInvalidOperationRejectedSafely(t, err, operation)

	err = client.WithWorkspaceTx(ctx, "default", operation, func(_ *Tx) error {
		t.Fatal("WithWorkspaceTx callback must not run for invalid operation")
		return nil
	})
	assertInvalidOperationRejectedSafely(t, err, operation)

	err = client.WithWorkspaceTxAndCleanup(ctx, "default", operation, func(_ *Tx) error {
		t.Fatal("WithWorkspaceTxAndCleanup callback must not run for invalid operation")
		return nil
	}, func() {
		t.Fatal("cleanup must not run for invalid operation")
	})
	assertInvalidOperationRejectedSafely(t, err, operation)
}

func assertInvalidOperationRejectedSafely(t *testing.T, err error, operation string) {
	t.Helper()
	diagnostic := assertDiagnostic(t, err, PhaseRuntimeQuery, KindInternalError)
	if diagnostic.Operation != "" {
		t.Fatalf("invalid operation diagnostic operation = %q; want empty", diagnostic.Operation)
	}
	if !strings.Contains(diagnostic.Message, "invalid operation label") {
		t.Fatalf("invalid operation diagnostic message = %q; want invalid operation label", diagnostic.Message)
	}
	assertPublicSafe(t, diagnostic, operation)
}
