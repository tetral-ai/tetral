package agentruntimebridge

import (
	"context"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func (s *PostgreSQLBridgeAPIStore) ReleaseRuntimeBinding(ctx context.Context, request *bridgev1.ReleaseRuntimeBindingRequest) (*bridgev1.ReleaseRuntimeBindingResponse, error) {
	identity, err := authenticatedRuntimeProcess(ctx, request.GetRuntimeProcessId())
	if err != nil {
		return nil, err
	}
	if s == nil || s.Client == nil {
		return nil, status.Error(codes.Unavailable, "runtime release store is unavailable")
	}
	timeout := s.lifecyclePolicy().ReleaseTimeout
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var response *bridgev1.ReleaseRuntimeBindingResponse
	var handedBack int
	err = s.Client.WithWorkspaceTx(ctx, request.GetWorkspaceId(), "agentruntimebridge.release_runtime_binding", func(tx *dbconnect.Tx) error {
		var err error
		response, handedBack, err = runtimecontrol.ReleaseBindingTx(ctx, tx, identity, request, s.now())
		return err
	})
	if err != nil {
		err = bridgeContextError(ctx, err)
		if s.Logger != nil {
			s.Logger.Error("runtime.binding.release_rejected", slog.String("operation", "release_runtime_binding"), slog.String("workspace.id", request.GetWorkspaceId()), slog.String("session.id", request.GetSessionId()), slog.String("binding.id", request.GetBindingId()), slog.Int64("binding.generation", request.GetBindingGeneration()), slog.String("runtime.process.id", identity.ID), slog.String("operation.id", request.GetOperationId()), slog.String("grpc.code", status.Code(err).String()))
		}
		return nil, bridgeContextError(ctx, err)
	}
	if s.Logger != nil {
		s.Logger.Info("runtime.binding.released", slog.String("operation", "release_runtime_binding"), slog.String("workspace.id", request.GetWorkspaceId()), slog.String("session.id", request.GetSessionId()), slog.String("binding.id", request.GetBindingId()), slog.Int64("binding.generation", request.GetBindingGeneration()), slog.String("runtime.process.id", identity.ID), slog.String("operation.id", request.GetOperationId()), slog.String("handoff.id", response.GetHandoffId()), slog.Int("target.count", len(response.GetThreads())), slog.Int("input.count", handedBack))
	}
	return response, nil
}

func (s BridgeAPIServer) ReleaseRuntimeBinding(ctx context.Context, request *bridgev1.ReleaseRuntimeBindingRequest) (*bridgev1.ReleaseRuntimeBindingResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.ReleaseRuntimeBinding(ctx, request)
}
