package agentruntimebridge

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func authenticatedRuntimeProcess(ctx context.Context, processID string) (runtimecontrol.ProcessIdentity, error) {
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.ServiceAccount.String() != runtimePodServiceAccount || identity.KubernetesPodUID == "" {
		return runtimecontrol.ProcessIdentity{}, status.Error(codes.PermissionDenied, "runtime process requires authenticated Pod identity")
	}
	process := runtimecontrol.ProcessIdentity{Namespace: identity.ServiceAccount.Namespace, PodUID: identity.KubernetesPodUID, ID: processID}
	return process, runtimecontrol.ValidateProcessIdentity(process)
}

func (s *PostgreSQLBridgeAPIStore) RegisterRuntimeProcess(ctx context.Context, request *bridgev1.RegisterRuntimeProcessRequest) (*bridgev1.RegisterRuntimeProcessResponse, error) {
	if err := s.ProcessPolicy.Validate(); err != nil {
		return nil, status.Error(codes.FailedPrecondition, "runtime process policy is unavailable")
	}
	ctx, cancelAttempt := context.WithTimeout(ctx, s.ProcessPolicy.RegistrationTimeout)
	defer cancelAttempt()
	identity, err := authenticatedRuntimeProcess(ctx, request.GetRuntimeProcessId())
	if err != nil {
		return nil, err
	}
	process, err := runtimecontrol.RegisterProcess(ctx, s.Client, identity)
	if err != nil {
		return nil, bridgeContextError(ctx, err)
	}
	return &bridgev1.RegisterRuntimeProcessResponse{RuntimeProcessId: process.ID, RegistrationOrder: process.RegistrationOrder, RegistrationReceipt: process.RegistrationReceipt}, nil
}

func (s *PostgreSQLBridgeAPIStore) ReportRuntimeProcess(ctx context.Context, request *bridgev1.ReportRuntimeProcessRequest) (*bridgev1.ReportRuntimeProcessResponse, error) {
	if err := s.ProcessPolicy.Validate(); err != nil {
		return nil, status.Error(codes.FailedPrecondition, "runtime process policy is unavailable")
	}
	ctx, cancelAttempt := context.WithTimeout(ctx, s.ProcessPolicy.ReportTimeout)
	defer cancelAttempt()
	identity, err := authenticatedRuntimeProcess(ctx, request.GetRuntimeProcessId())
	if err != nil {
		return nil, err
	}
	var phase string
	switch request.GetPhase() {
	case bridgev1.RuntimeProcessPhase_RUNTIME_PROCESS_PHASE_ACCEPTING:
		phase = runtimecontrol.ProcessAccepting
	case bridgev1.RuntimeProcessPhase_RUNTIME_PROCESS_PHASE_DRAINING:
		phase = runtimecontrol.ProcessDraining
	default:
		return nil, status.Error(codes.InvalidArgument, "runtime report phase is malformed")
	}
	process, promoted, err := runtimecontrol.ReportProcess(ctx, s.Client, identity, request.GetRegistrationReceipt(), phase)
	if err != nil {
		return nil, bridgeContextError(ctx, err)
	}
	if promoted {
		// This wake is after promotion commit. Reconciliation takes its own Session
		// transactions in Runner; notification loss is covered by persisted census.
		wakeCtx, cancel := context.WithTimeout(ctx, s.ProcessPolicy.ReportTimeout)
		_ = s.Client.WithTx(wakeCtx, "runtimecontrol.notify_promotion", nil, func(tx *dbconnect.Tx) error {
			_, err := tx.Exec(wakeCtx, `SELECT pg_notify($1,$2)`, queue.NotificationChannel, queue.ConsumerClassJobRunner)
			return err
		})
		cancel()
	}
	return &bridgev1.ReportRuntimeProcessResponse{RuntimeProcessId: process.ID, Phase: request.GetPhase(), Current: process.Current}, nil
}

func (s BridgeAPIServer) RegisterRuntimeProcess(ctx context.Context, request *bridgev1.RegisterRuntimeProcessRequest) (*bridgev1.RegisterRuntimeProcessResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.RegisterRuntimeProcess(ctx, request)
}
func (s BridgeAPIServer) ReportRuntimeProcess(ctx context.Context, request *bridgev1.ReportRuntimeProcessRequest) (*bridgev1.ReportRuntimeProcessResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.ReportRuntimeProcess(ctx, request)
}
