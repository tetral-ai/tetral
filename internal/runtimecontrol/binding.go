package runtimecontrol

import (
	"context"
	"net/netip"

	"github.com/tetral-ai/tetral/internal/dbconnect"
)

type Binding struct {
	BindingID         string
	BindingGeneration int64
	Namespace         string
	PodName           string
	PodUID            string
	PodIP             string
}

func ReadRuntimeBindingForDeliveryTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string) (Binding, error) {
	binding, found, err := ReadOptionalRuntimeBindingForDeliveryTx(ctx, tx, workspaceID, sessionID)
	if err != nil {
		return Binding{}, err
	}
	if !found {
		return Binding{}, PreparationError{Kind: "runtime_binding_unavailable", Message: "runtime binding is unavailable", Retryable: true}
	}
	return binding, nil
}

func ReadOptionalRuntimeBindingForDeliveryTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string) (Binding, bool, error) {
	row := tx.QueryRow(ctx,
		`SELECT binding_id, binding_generation, agent_runtime_namespace, agent_runtime_pod_name, agent_runtime_pod_uid, agent_runtime_pod_ip
		   FROM session_runtime_bindings
		  WHERE workspace_id = $1 AND session_id = $2
		  FOR UPDATE`,
		workspaceID,
		sessionID,
	)
	var binding Binding
	if err := row.Scan(&binding.BindingID, &binding.BindingGeneration, &binding.Namespace, &binding.PodName, &binding.PodUID, &binding.PodIP); dbconnect.IsNoRows(err) {
		return Binding{}, false, nil
	} else if err != nil {
		return Binding{}, false, err
	}
	if binding.BindingID == "" || binding.BindingGeneration <= 0 || binding.Namespace == "" || binding.PodName == "" || binding.PodUID == "" || binding.PodIP == "" {
		return Binding{}, false, PreparationError{Kind: "runtime_binding_invalid", Message: "runtime binding is invalid", Retryable: true}
	}
	if _, err := netip.ParseAddr(binding.PodIP); err != nil {
		return Binding{}, false, PreparationError{Kind: "runtime_binding_invalid", Message: "runtime binding pod ip is invalid", Retryable: true}
	}
	return binding, true, nil
}
