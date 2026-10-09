package jobrunner

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tetral-ai/tetral/internal/mcpmanifest"

	"github.com/tetral-ai/tetral/internal/runtimecontrol"

	"github.com/tetral-ai/tetral/internal/runtimeconfig"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/childcontrol"
	"github.com/tetral-ai/tetral/internal/storage"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	internalgrpc "github.com/tetral-ai/tetral/internal/internalgrpc"
	internalgrpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sessioneventwrite"
	"github.com/tetral-ai/tetral/internal/sessionrpc"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type RuntimeDeliveryStore interface {
	PrepareRuntimeCommand(context.Context, RuntimeJob) (RuntimeCommandPlan, error)
	MarkRuntimeInputAccepted(context.Context, RuntimeJob, RuntimeAttemptedBinding) (bool, error)
	PrepareRuntimeInputRejection(context.Context, RuntimeJob, RuntimeDeliveryResult) (bool, error)
}

type RuntimeRecoveryActivationStore interface {
	ActivateRuntimeRecovery(context.Context, RuntimeJob) (RuntimeCommandPlan, error)
}

type RuntimeInterruptDeliveryAuthorityStore interface {
	InterruptDeliveryAuthority(context.Context, RuntimeJob) (RuntimeInterruptDeliveryAuthority, error)
}

type RuntimeInputDeliveryAuthorityStore interface {
	RuntimeInputDeliveryAuthority(context.Context, RuntimeJob) (RuntimeInputDeliveryAuthority, error)
}

type RuntimeInputDeliveryAuthority struct {
	Active            bool
	QueueLeaseSettled bool
}

type RuntimeInterruptDeliveryAuthority struct {
	Active            bool
	QueueLeaseSettled bool
}

type RuntimeCleanupFinalizer interface {
	FinalizeRuntimeCleanup(context.Context, RuntimeJob) (RuntimeDeliveryResult, error)
}

type RuntimeCleanupDeliveryStore interface {
	RuntimeCleanupDeliveryAuthority(context.Context, RuntimeJob) (RuntimeCleanupDeliveryAuthority, error)
	RescheduleBusyRuntimeCleanup(context.Context, RuntimeJob) (RuntimeDeliveryResult, error)
	FinalizeRuntimeCleanupExhaustion(context.Context, RuntimeJob, RuntimeDeliveryResult) (RuntimeDeliveryResult, error)
}

type RuntimeDeliveryFinalizationStore interface {
	FinalizeRuntimeDelivery(context.Context, RuntimeJob, RuntimeDeliveryResult) (RuntimeDeliveryResult, error)
	ReplayRuntimeDeliveryFinalization(context.Context, RuntimeJob) (RuntimeDeliveryResult, bool, error)
}

// MalformedRuntimeInputLease carries only Queue-owned lease identity. Payload
// fields are intentionally absent: durable Queue keys and the canonical Inbox
// relation select any business owner.
type MalformedRuntimeInputLease struct {
	WorkspaceID  string
	JobID        string
	LeaseToken   string
	Kind         string
	PartitionKey string
	DedupeKey    string
}

type MalformedRuntimeInputCustodyResult struct {
	Handled               bool
	QueueLeaseSettled     bool
	Retry                 bool
	InterruptTerminalized bool
	CanonicalReplacement  bool
}

type RuntimeTargetResolver interface {
	ResolveRuntimeTarget(context.Context, *dbconnect.Tx, RuntimeJob) (runtimecontrol.Binding, error)
}

type RuntimeCleanupTargetProver interface {
	CleanupTargetProvenGone(context.Context, *dbconnect.Tx, RuntimeJob, runtimecontrol.Binding) (bool, error)
}

type RuntimeCommandSender interface {
	AcceptInput(context.Context, RuntimePodTarget, *agentruntimev1.AcceptInputRequest) (*agentruntimev1.AcceptInputResponse, error)
	AcceptAgentMail(context.Context, RuntimePodTarget, *agentruntimev1.AcceptAgentMailRequest) (*agentruntimev1.AcceptAgentMailResponse, error)
	AcceptTaskNotification(context.Context, RuntimePodTarget, *agentruntimev1.AcceptTaskNotificationRequest) (*agentruntimev1.AcceptTaskNotificationResponse, error)
	Interrupt(context.Context, RuntimePodTarget, *agentruntimev1.InterruptRequest) (*agentruntimev1.InterruptResponse, error)
	ResolveToolConfirmation(context.Context, RuntimePodTarget, *agentruntimev1.ResolveToolConfirmationRequest) (*agentruntimev1.ResolveToolConfirmationResponse, error)
	ApplyRuntimeConfig(context.Context, RuntimePodTarget, *agentruntimev1.ApplyRuntimeConfigRequest) (*agentruntimev1.ApplyRuntimeConfigResponse, error)
	CleanupSession(context.Context, RuntimePodTarget, *agentruntimev1.CleanupSessionRequest) (*agentruntimev1.CleanupSessionResponse, error)
}

type RuntimeRecoveryCommandSender interface {
	RecoverThread(context.Context, RuntimePodTarget, *agentruntimev1.RecoverThreadRequest) (*agentruntimev1.RecoverThreadResponse, error)
}

// initialMCPManifestListTimeout is the per-call deadline for initial manifest
// capture outside queued user messages; a queued user message uses
// min(this, mcpInputDiscoveryBudget) as its whole-discovery deadline.
const initialMCPManifestListTimeout = 180 * time.Second

type RuntimeCommandPlan struct {
	placement             *runtimePlacementChoice
	StaleAccepted         bool
	DeliveryAuthorityLost bool
	SettledAccepted       bool
	QueueLeaseSettled     bool
	CleanupTargetGone     bool
	RecoveryPrepared      bool
	Target                RuntimePodTarget
	AttemptedBinding      RuntimeAttemptedBinding
	AcceptInput           *agentruntimev1.AcceptInputRequest
	AcceptAgentMail       *agentruntimev1.AcceptAgentMailRequest
	AcceptTask            *agentruntimev1.AcceptTaskNotificationRequest
	Interrupt             *agentruntimev1.InterruptRequest
	ToolConfirmation      *agentruntimev1.ResolveToolConfirmationRequest
	RuntimeConfig         *agentruntimev1.ApplyRuntimeConfigRequest
	MCPBeforeInput        []*agentruntimev1.ApplyRuntimeConfigRequest
	CleanupSession        *agentruntimev1.CleanupSessionRequest
	RecoverThread         *agentruntimev1.RecoverThreadRequest
	TaskNotification      *RuntimeTaskNotificationPlan
}

type RuntimeAttemptedBinding struct {
	BindingID        string
	Generation       int64
	TargetPodUID     string
	RuntimeProcessID string
}

func (p RuntimeCommandPlan) hasCommand() bool {
	count := 0
	for _, present := range []bool{
		p.AcceptInput != nil, p.AcceptAgentMail != nil, p.AcceptTask != nil,
		p.Interrupt != nil, p.ToolConfirmation != nil, p.RuntimeConfig != nil, p.CleanupSession != nil, p.RecoverThread != nil,
	} {
		if present {
			count++
		}
	}
	return count == 1
}

func (p RuntimeCommandPlan) send(ctx context.Context, sender RuntimeCommandSender) (RuntimeDeliveryResult, error) {
	for _, config := range p.MCPBeforeInput {
		response, err := sender.ApplyRuntimeConfig(ctx, p.Target, config)
		result := runtimeResultFromRuntimeConfig(response)
		if err != nil || (result.Status != RuntimeDeliveryAccepted && result.Status != RuntimeDeliveryDuplicate) {
			return result, err
		}
	}
	switch {
	case p.RecoverThread != nil:
		recoverySender, ok := sender.(RuntimeRecoveryCommandSender)
		if !ok {
			return RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, Retryable: true, ErrorKind: "runtime_transport_unavailable", ErrorMessage: "runtime recovery sender is unavailable"}, nil
		}
		response, err := recoverySender.RecoverThread(ctx, p.Target, p.RecoverThread)
		return runtimeResultFromRecoverThread(response), err
	case p.AcceptInput != nil:
		response, err := sender.AcceptInput(ctx, p.Target, p.AcceptInput)
		return runtimeResultFromAcceptInput(response), err
	case p.AcceptAgentMail != nil:
		response, err := sender.AcceptAgentMail(ctx, p.Target, p.AcceptAgentMail)
		return runtimeResultFromAcceptAgentMail(response), err
	case p.AcceptTask != nil:
		response, err := sender.AcceptTaskNotification(ctx, p.Target, p.AcceptTask)
		return runtimeResultFromAcceptTask(response), err
	case p.Interrupt != nil:
		// The command client applies the Interrupt attempt deadline from its method policy.
		response, err := sender.Interrupt(ctx, p.Target, p.Interrupt)
		return runtimeResultFromInterrupt(response), err
	case p.ToolConfirmation != nil:
		response, err := sender.ResolveToolConfirmation(ctx, p.Target, p.ToolConfirmation)
		return runtimeResultFromToolConfirmation(response), err
	case p.RuntimeConfig != nil:
		response, err := sender.ApplyRuntimeConfig(ctx, p.Target, p.RuntimeConfig)
		return runtimeResultFromRuntimeConfig(response), err
	case p.CleanupSession != nil:
		response, err := sender.CleanupSession(ctx, p.Target, p.CleanupSession)
		return runtimeResultFromCleanup(response), err
	default:
		return RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, ErrorKind: "runtime_command_plan_invalid", ErrorMessage: "runtime command request is missing"}, nil
	}
}

func runtimeResultFromRecoverThread(response *agentruntimev1.RecoverThreadResponse) RuntimeDeliveryResult {
	if response == nil {
		return invalidRuntimeResponse()
	}
	if response.GetAccepted() != nil {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}
	}
	if response.GetDuplicate() != nil {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}
	}
	if rejected := response.GetRejected(); rejected != nil {
		return rejectedRuntimeResponse(runtimeFailureKind(rejected.GetReason().String()), rejected.GetRetryable())
	}
	return invalidRuntimeResponse()
}

func invalidRuntimeResponse() RuntimeDeliveryResult {
	return RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, ErrorKind: "invalid_runtime_response", ErrorMessage: "runtime response is missing or invalid"}
}

func rejectedRuntimeResponse(reason string, retryable bool) RuntimeDeliveryResult {
	return RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, Retryable: retryable, ErrorKind: reason, ErrorMessage: "runtime rejected operation"}
}

func runtimeFailureKind(value string) string {
	value = strings.ToLower(value)
	if index := strings.LastIndex(value, "_failure_"); index >= 0 {
		value = value[index+len("_failure_"):]
	}
	return value
}

func runtimeResultFromAcceptInput(response *agentruntimev1.AcceptInputResponse) RuntimeDeliveryResult {
	if response == nil {
		return invalidRuntimeResponse()
	}
	if response.GetAccepted() != nil {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}
	}
	if response.GetDuplicate() != nil {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}
	}
	if rejected := response.GetRejected(); rejected != nil {
		if rejected.GetReason() == agentruntimev1.AcceptInputFailure_ACCEPT_INPUT_FAILURE_SESSION_INTERRUPT_BARRIER_STALE {
			return RuntimeDeliveryResult{Status: RuntimeDeliveryBarrierStale}
		}
		return rejectedRuntimeResponse(runtimeFailureKind(rejected.GetReason().String()), rejected.GetRetryable())
	}
	return invalidRuntimeResponse()
}

func runtimeResultFromAcceptAgentMail(response *agentruntimev1.AcceptAgentMailResponse) RuntimeDeliveryResult {
	if response == nil {
		return invalidRuntimeResponse()
	}
	if response.GetAccepted() != nil {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}
	}
	if response.GetDuplicate() != nil {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}
	}
	if rejected := response.GetRejected(); rejected != nil {
		if rejected.GetReason() == agentruntimev1.AcceptAgentMailFailure_ACCEPT_AGENT_MAIL_FAILURE_SESSION_INTERRUPT_BARRIER_STALE {
			return RuntimeDeliveryResult{Status: RuntimeDeliveryBarrierStale}
		}
		return rejectedRuntimeResponse(runtimeFailureKind(rejected.GetReason().String()), rejected.GetRetryable())
	}
	return invalidRuntimeResponse()
}

func runtimeResultFromAcceptTask(response *agentruntimev1.AcceptTaskNotificationResponse) RuntimeDeliveryResult {
	if response == nil {
		return invalidRuntimeResponse()
	}
	if response.GetAccepted() != nil {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}
	}
	if response.GetDuplicate() != nil {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}
	}
	if rejected := response.GetRejected(); rejected != nil {
		if rejected.GetReason() == agentruntimev1.AcceptTaskNotificationFailure_ACCEPT_TASK_NOTIFICATION_FAILURE_SESSION_INTERRUPT_BARRIER_STALE {
			return RuntimeDeliveryResult{Status: RuntimeDeliveryBarrierStale}
		}
		return rejectedRuntimeResponse(runtimeFailureKind(rejected.GetReason().String()), rejected.GetRetryable())
	}
	return invalidRuntimeResponse()
}

func runtimeResultFromInterrupt(response *agentruntimev1.InterruptResponse) RuntimeDeliveryResult {
	if response == nil {
		return invalidRuntimeResponse()
	}
	if response.GetAccepted() != nil {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}
	}
	if response.GetDuplicate() != nil {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}
	}
	if rejected := response.GetRejected(); rejected != nil {
		return rejectedRuntimeResponse(runtimeFailureKind(rejected.GetReason().String()), rejected.GetRetryable())
	}
	return invalidRuntimeResponse()
}

func runtimeResultFromToolConfirmation(response *agentruntimev1.ResolveToolConfirmationResponse) RuntimeDeliveryResult {
	if response == nil {
		return invalidRuntimeResponse()
	}
	if response.GetAccepted() != nil {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}
	}
	if response.GetDuplicate() != nil {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}
	}
	if response.GetStale() != nil {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}
	}
	if rejected := response.GetRejected(); rejected != nil {
		if rejected.GetReason() == agentruntimev1.ResolveToolConfirmationFailure_RESOLVE_TOOL_CONFIRMATION_FAILURE_SESSION_INTERRUPT_BARRIER_STALE {
			return RuntimeDeliveryResult{Status: RuntimeDeliveryBarrierStale}
		}
		return rejectedRuntimeResponse(runtimeFailureKind(rejected.GetReason().String()), rejected.GetRetryable())
	}
	return invalidRuntimeResponse()
}

func runtimeResultFromRuntimeConfig(response *agentruntimev1.ApplyRuntimeConfigResponse) RuntimeDeliveryResult {
	if response == nil {
		return invalidRuntimeResponse()
	}
	if response.GetApplied() != nil || response.GetNoResidency() != nil {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}
	}
	if response.GetDuplicate() != nil {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}
	}
	if rejected := response.GetRejected(); rejected != nil {
		return rejectedRuntimeResponse(runtimeFailureKind(rejected.GetReason().String()), rejected.GetRetryable())
	}
	return invalidRuntimeResponse()
}

func runtimeResultFromCleanup(response *agentruntimev1.CleanupSessionResponse) RuntimeDeliveryResult {
	if response == nil {
		return invalidRuntimeResponse()
	}
	if response.GetCompleted() != nil {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}
	}
	if response.GetDuplicate() != nil {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}
	}
	if rejected := response.GetRejected(); rejected != nil {
		if rejected.GetReason() == agentruntimev1.CleanupSessionFailure_CLEANUP_SESSION_FAILURE_SESSION_BUSY {
			return RuntimeDeliveryResult{
				Status:       RuntimeDeliveryRejected,
				Retryable:    rejected.GetRetryable(),
				ErrorKind:    "cleanup_session_busy",
				ErrorMessage: "runtime session is busy",
				CleanupBusy:  true,
			}
		}
		return rejectedRuntimeResponse(runtimeFailureKind(rejected.GetReason().String()), rejected.GetRetryable())
	}
	return invalidRuntimeResponse()
}

type RuntimeTaskNotificationPlan struct {
	TaskID               string
	SourceToolUseEventID string
	ResultJSON           string
}

type RuntimePodTarget struct {
	Namespace        string
	PodName          string
	PodUID           string
	RuntimeProcessID string
	PodIP            string
	Port             int
}

type RuntimePodDirectDeliverer struct {
	Store  RuntimeDeliveryStore
	Sender RuntimeCommandSender
}

func (d RuntimePodDirectDeliverer) ReplaceMalformedRuntimeInputCustody(ctx context.Context, job RuntimeJob) (queue.ReplaceMalformedRuntimeInputCustodyResult, error) {
	replacer, ok := d.Store.(interface {
		ReplaceMalformedRuntimeInputCustody(context.Context, RuntimeJob) (queue.ReplaceMalformedRuntimeInputCustodyResult, error)
	})
	if !ok || replacer == nil {
		return queue.ReplaceMalformedRuntimeInputCustodyResult{}, errors.New("runtime delivery store is unavailable")
	}
	return replacer.ReplaceMalformedRuntimeInputCustody(ctx, job)
}

func (d RuntimePodDirectDeliverer) FinalizeMalformedRuntimeInputCustody(ctx context.Context, lease MalformedRuntimeInputLease) (MalformedRuntimeInputCustodyResult, error) {
	finalizer, ok := d.Store.(interface {
		FinalizeMalformedRuntimeInputCustody(context.Context, MalformedRuntimeInputLease) (MalformedRuntimeInputCustodyResult, error)
	})
	if !ok || finalizer == nil {
		return MalformedRuntimeInputCustodyResult{}, errors.New("runtime delivery store is unavailable")
	}
	return finalizer.FinalizeMalformedRuntimeInputCustody(ctx, lease)
}

func (d RuntimePodDirectDeliverer) FinalizeRuntimeDelivery(ctx context.Context, job RuntimeJob, result RuntimeDeliveryResult) (RuntimeDeliveryResult, error) {
	finalizer, ok := d.Store.(RuntimeDeliveryFinalizationStore)
	if !ok || finalizer == nil {
		return RuntimeDeliveryResult{}, errors.New("runtime delivery finalizer is unavailable")
	}
	return finalizer.FinalizeRuntimeDelivery(ctx, job, result)
}

func (d RuntimePodDirectDeliverer) ReplayRuntimeDeliveryFinalization(ctx context.Context, job RuntimeJob) (RuntimeDeliveryResult, bool, error) {
	replayer, ok := d.Store.(RuntimeDeliveryFinalizationStore)
	if !ok || replayer == nil {
		return RuntimeDeliveryResult{}, false, errors.New("runtime delivery finalization replayer is unavailable")
	}
	return replayer.ReplayRuntimeDeliveryFinalization(ctx, job)
}

func (d RuntimePodDirectDeliverer) FinalizeRuntimeCleanupExhaustion(ctx context.Context, job RuntimeJob, result RuntimeDeliveryResult) (RuntimeDeliveryResult, error) {
	store, ok := d.Store.(RuntimeCleanupDeliveryStore)
	if !ok || store == nil {
		return RuntimeDeliveryResult{}, errors.New("runtime cleanup exhaustion finalizer is unavailable")
	}
	return store.FinalizeRuntimeCleanupExhaustion(ctx, job, result)
}

func (d RuntimePodDirectDeliverer) RuntimeCleanupDeliveryAuthority(ctx context.Context, job RuntimeJob) (RuntimeCleanupDeliveryAuthority, error) {
	store, ok := d.Store.(RuntimeCleanupDeliveryStore)
	if !ok || store == nil {
		return RuntimeCleanupDeliveryAuthority{}, errors.New("runtime cleanup authority store is unavailable")
	}
	return store.RuntimeCleanupDeliveryAuthority(ctx, job)
}

func (d RuntimePodDirectDeliverer) DeliverRuntimeJob(ctx context.Context, job RuntimeJob) (RuntimeDeliveryResult, error) {
	if d.Store == nil {
		return RuntimeDeliveryResult{
			Status:       RuntimeDeliveryRejected,
			Retryable:    true,
			ErrorKind:    "runtime_reconcile_unavailable",
			ErrorMessage: "runtime delivery store is unavailable",
		}, nil
	}
	plan, err := d.Store.PrepareRuntimeCommand(ctx, job)
	if err != nil {
		return runtimeDeliveryResultFromPrepareError(err), nil
	}
	if plan.SettledAccepted {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted, QueueLeaseSettled: plan.QueueLeaseSettled}, nil
	}
	if plan.StaleAccepted {
		status := RuntimeDeliveryDuplicate
		if plan.DeliveryAuthorityLost {
			status = RuntimeDeliveryAuthorityLost
		}
		return RuntimeDeliveryResult{Status: status, QueueLeaseSettled: plan.QueueLeaseSettled}, nil
	}
	if (job.Kind == queue.KindCleanupSession || job.Kind == queue.KindSessionDeleteCleanup) && plan.CleanupTargetGone {
		finalizer, ok := d.Store.(RuntimeCleanupFinalizer)
		if !ok {
			return RuntimeDeliveryResult{
				Status:       RuntimeDeliveryRejected,
				Retryable:    true,
				ErrorKind:    "cleanup_finalizer_unavailable",
				ErrorMessage: "cleanup finalizer is unavailable",
			}, nil
		}
		return finalizer.FinalizeRuntimeCleanup(ctx, job)
	}
	if plan.RecoveryPrepared {
		activator, ok := d.Store.(RuntimeRecoveryActivationStore)
		if !ok || activator == nil {
			return RuntimeDeliveryResult{
				Status: RuntimeDeliveryRejected, Retryable: true,
				ErrorKind: "runtime_recovery_authority_unavailable", ErrorMessage: "runtime recovery authority is unavailable",
			}, nil
		}
		plan, err = activator.ActivateRuntimeRecovery(ctx, job)
		if err != nil {
			return runtimeDeliveryResultFromPrepareError(err), nil
		}
		if plan.StaleAccepted || plan.DeliveryAuthorityLost {
			resultStatus := RuntimeDeliveryDuplicate
			if plan.DeliveryAuthorityLost {
				resultStatus = RuntimeDeliveryAuthorityLost
			}
			return RuntimeDeliveryResult{Status: resultStatus, QueueLeaseSettled: plan.QueueLeaseSettled}, nil
		}
	}
	if job.Kind == queue.KindRuntimeInput && job.InputKind == "interrupt_control" {
		authorizer, ok := d.Store.(RuntimeInterruptDeliveryAuthorityStore)
		if !ok || authorizer == nil {
			return RuntimeDeliveryResult{
				Status:       RuntimeDeliveryRejected,
				Retryable:    true,
				ErrorKind:    "runtime_interrupt_authority_unavailable",
				ErrorMessage: "runtime interrupt delivery authority is unavailable",
			}, nil
		}
		authority, err := authorizer.InterruptDeliveryAuthority(ctx, job)
		if err != nil {
			return runtimeDeliveryResultFromPrepareError(err), nil
		}
		if !authority.Active {
			status := RuntimeDeliveryDuplicate
			if !authority.QueueLeaseSettled {
				status = RuntimeDeliveryAuthorityLost
			}
			return RuntimeDeliveryResult{Status: status, QueueLeaseSettled: authority.QueueLeaseSettled}, nil
		}
	}
	if d.Sender == nil {
		return runtimeDeliveryResultWithAttemptedBinding(RuntimeDeliveryResult{
			Status:       RuntimeDeliveryRejected,
			Retryable:    true,
			ErrorKind:    "runtime_transport_unavailable",
			ErrorMessage: "runtime command sender is unavailable",
		}, plan.AttemptedBinding), nil
	}
	if !plan.hasCommand() {
		return RuntimeDeliveryResult{
			Status:       RuntimeDeliveryRejected,
			Retryable:    false,
			ErrorKind:    "runtime_command_plan_invalid",
			ErrorMessage: "runtime command request is missing",
		}, nil
	}
	if job.Kind == queue.KindRuntimeInput && job.InputKind == "agent_mail" {
		authorizer, ok := d.Store.(RuntimeInputDeliveryAuthorityStore)
		if !ok || authorizer == nil {
			return RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, Retryable: true, ErrorKind: "runtime_input_authority_unavailable", ErrorMessage: "runtime input delivery authority is unavailable"}, nil
		}
		authority, err := authorizer.RuntimeInputDeliveryAuthority(ctx, job)
		if err != nil {
			return runtimeDeliveryResultFromPrepareError(err), nil
		}
		if !authority.Active {
			status := RuntimeDeliveryAuthorityLost
			if authority.QueueLeaseSettled {
				status = RuntimeDeliveryDuplicate
			}
			return RuntimeDeliveryResult{Status: status, QueueLeaseSettled: authority.QueueLeaseSettled}, nil
		}
	}
	if job.Kind == queue.KindCleanupSession {
		store, ok := d.Store.(RuntimeCleanupDeliveryStore)
		if !ok || store == nil {
			return RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, Retryable: true, ErrorKind: "cleanup_authority_unavailable", ErrorMessage: "runtime cleanup authority is unavailable"}, nil
		}
		authority, err := store.RuntimeCleanupDeliveryAuthority(ctx, job)
		if err != nil {
			return runtimeDeliveryResultFromPrepareError(err), nil
		}
		if !authority.Active {
			return RuntimeDeliveryResult{Status: RuntimeDeliveryAuthorityLost}, nil
		}
	}
	result, err := plan.send(ctx, d.Sender)
	if err != nil {
		result, deliveryErr := runtimeDeliveryResultFromSendError(err)
		result = runtimeDeliveryResultWithAttemptedBinding(result, plan.AttemptedBinding)
		if deliveryErr != nil {
			return result, deliveryErr
		}
		converted, err := d.prepareRuntimeInputRejection(ctx, job, result)
		if err != nil {
			return runtimeDeliveryResultFromPrepareError(err), nil
		}
		if converted {
			return d.DeliverRuntimeJob(ctx, job)
		}
		return result, nil
	}
	result = runtimeDeliveryResultWithAttemptedBinding(result, plan.AttemptedBinding)
	converted, err := d.prepareRuntimeInputRejection(ctx, job, result)
	if err != nil {
		return runtimeDeliveryResultFromPrepareError(err), nil
	}
	if converted {
		return d.DeliverRuntimeJob(ctx, job)
	}
	if job.Kind == queue.KindCleanupSession && result.CleanupBusy {
		store, ok := d.Store.(RuntimeCleanupDeliveryStore)
		if !ok || store == nil {
			return RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, Retryable: true, ErrorKind: "cleanup_reschedule_unavailable", ErrorMessage: "runtime cleanup reschedule is unavailable"}, nil
		}
		return store.RescheduleBusyRuntimeCleanup(ctx, job)
	}
	if job.Kind == queue.KindRuntimeInput && job.InputKind == "interrupt_control" &&
		(result.Status == RuntimeDeliveryAccepted || result.Status == RuntimeDeliveryDuplicate) {
		replayer, ok := d.Store.(RuntimeDeliveryFinalizationReplayer)
		if !ok {
			return RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, Retryable: true, ErrorKind: "interrupt_closeout_unavailable", ErrorMessage: "interrupt closeout receipt is unavailable"}, nil
		}
		replayed, found, replayErr := replayer.ReplayRuntimeDeliveryFinalization(ctx, job)
		if replayErr != nil {
			return runtimeDeliveryResultWithAttemptedBinding(runtimeDeliveryResultFromPrepareError(replayErr), plan.AttemptedBinding), nil
		}
		if !found {
			return runtimeDeliveryResultWithAttemptedBinding(RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, Retryable: true, ErrorKind: "interrupt_closeout_pending", ErrorMessage: "interrupt closeout receipt is not committed"}, plan.AttemptedBinding), nil
		}
		return replayed, nil
	}
	if job.Kind == queue.KindRuntimeInput && (result.Status == RuntimeDeliveryAccepted || result.Status == RuntimeDeliveryDuplicate) {
		queueLeaseSettled, err := d.Store.MarkRuntimeInputAccepted(ctx, job, plan.AttemptedBinding)
		if err != nil {
			return runtimeDeliveryResultWithAttemptedBinding(runtimeDeliveryResultFromPrepareError(err), plan.AttemptedBinding), nil
		}
		if queueLeaseSettled {
			result.QueueLeaseSettled = true
		}
	}
	if (job.Kind == queue.KindCleanupSession || job.Kind == queue.KindSessionDeleteCleanup) && (result.Status == RuntimeDeliveryAccepted || result.Status == RuntimeDeliveryDuplicate) {
		finalizer, ok := d.Store.(RuntimeCleanupFinalizer)
		if !ok {
			return RuntimeDeliveryResult{
				Status:       RuntimeDeliveryRejected,
				Retryable:    true,
				ErrorKind:    "cleanup_finalizer_unavailable",
				ErrorMessage: "cleanup finalizer is unavailable",
			}, nil
		}
		return finalizer.FinalizeRuntimeCleanup(ctx, job)
	}
	return result, nil
}

// RuntimeInputDeliveryAuthority is the final exact-lease fence immediately
// before an agent-mail command crosses the Runtime transport boundary.
func (s *PostgreSQLRuntimeDeliveryStore) RuntimeInputDeliveryAuthority(ctx context.Context, job RuntimeJob) (RuntimeInputDeliveryAuthority, error) {
	if s == nil || s.Client == nil {
		return RuntimeInputDeliveryAuthority{}, runtimecontrol.PreparationError{Kind: "runtime_reconcile_unavailable", Message: "runtime delivery store is unavailable", Retryable: true}
	}
	if job.Kind != queue.KindRuntimeInput || job.InputKind != "agent_mail" || job.WorkspaceID == "" || job.SessionID == "" ||
		job.RuntimeInputID == "" || job.JobID == "" || job.LeaseToken == "" || job.PartitionKey == "" || job.DedupeKey == "" {
		return RuntimeInputDeliveryAuthority{}, runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "agent mail delivery authority is incomplete", Retryable: false}
	}
	workspaceID := workspace.ID(job.WorkspaceID)
	if job.PartitionKey != queue.FormatSessionPartitionKey(workspaceID, job.SessionID) ||
		job.DedupeKey != queue.FormatRuntimeInputDedupeKey(workspaceID, job.SessionID, job.RuntimeInputID) {
		return RuntimeInputDeliveryAuthority{}, runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "agent mail delivery authority binding is invalid", Retryable: false}
	}
	authority := RuntimeInputDeliveryAuthority{}
	err := s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "jobrunner.authorize_runtime_input_delivery", func(tx *dbconnect.Tx) error {
		if err := runtimecontrol.LockRuntimeMutationSessionTx(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
			return err
		}
		active, err := queue.AssertExactLeaseTx(ctx, tx, queue.ExactLeaseRequest{
			WorkspaceID: workspaceID, JobID: job.JobID, LeaseToken: job.LeaseToken,
			Kind: job.Kind, PartitionKey: job.PartitionKey, DedupeKey: job.DedupeKey,
		})
		if err != nil || !active {
			return err
		}
		replayed, found, err := replayAgentMailDeliveryFinalizationTx(ctx, tx, job)
		if err != nil {
			return err
		}
		if found {
			authority.QueueLeaseSettled = replayed.QueueLeaseSettled
			return nil
		}
		authority.Active = true
		return nil
	})
	return authority, err
}

func runtimeDeliveryResultWithAttemptedBinding(
	result RuntimeDeliveryResult,
	attempt RuntimeAttemptedBinding,
) RuntimeDeliveryResult {
	if attempt.BindingID == "" {
		return result
	}
	result.AttemptedBindingID = attempt.BindingID
	result.AttemptedBindingGeneration = attempt.Generation
	result.AttemptedTargetPodUID = attempt.TargetPodUID
	result.AttemptedRuntimeProcessID = attempt.RuntimeProcessID
	return result
}

func (d RuntimePodDirectDeliverer) prepareRuntimeInputRejection(ctx context.Context, job RuntimeJob, result RuntimeDeliveryResult) (bool, error) {
	if job.Kind != queue.KindRuntimeInput || result.Status != RuntimeDeliveryRejected || result.Retryable {
		return false, nil
	}
	return d.Store.PrepareRuntimeInputRejection(ctx, job, result)
}

// runtimeDeliveryResultFromSendError separates two RESOURCE_EXHAUSTED-shaped
// conditions that must never be conflated. A client's OWN send-cap rejection,
// detected LOCALLY before transmission (runtimeCommandPayloadTooLargeError, the
// transport fuse), is a deterministic per-input terminal: it dead-letters under a
// distinct error kind and is NEVER the retryable transport arm. Only a REMOTE
// RESOURCE_EXHAUSTED — the pod reporting itself at capacity — stays retryable
// (returned as a bare error below alongside DeadlineExceeded/Unavailable).
func runtimeDeliveryResultFromSendError(err error) (RuntimeDeliveryResult, error) {
	var tooLarge *runtimeCommandPayloadTooLargeError
	if errors.As(err, &tooLarge) {
		return RuntimeDeliveryResult{
			Status:       RuntimeDeliveryRejected,
			Retryable:    false,
			ErrorKind:    "runtime_command_payload_too_large",
			ErrorMessage: "runtime command exceeds the transport fuse",
		}, nil
	}
	switch status.Code(err) {
	case codes.InvalidArgument:
		return RuntimeDeliveryResult{
			Status:       RuntimeDeliveryRejected,
			Retryable:    false,
			ErrorKind:    "runtime_command_invalid_argument",
			ErrorMessage: "runtime pod rejected an invalid command request",
		}, nil
	case codes.Internal:
		return RuntimeDeliveryResult{
			Status:       RuntimeDeliveryRejected,
			Retryable:    false,
			ErrorKind:    "runtime_command_internal_invariant",
			ErrorMessage: "runtime pod reported a terminal command invariant failure",
		}, nil
	default:
		return RuntimeDeliveryResult{}, err
	}
}

type PostgreSQLRuntimeDeliveryStore struct {
	Client              *dbconnect.Client
	Logger              *slog.Logger
	PlacementMetrics    *RuntimePlacementMetrics
	RuntimeGRPCPort     int
	TargetResolver      RuntimeTargetResolver
	MCPManifestLister   mcpmanifest.Lister
	AttachmentBlobStore blob.BlobStore
	Clock               func() time.Time
}

// NewPostgreSQLRuntimeDeliveryStore builds a delivery store around the given
// target resolver. Every delivery, cleanup and loss decision goes through that
// resolver's process-aware classifier; a store without one fails closed with
// runtime_visibility_unavailable instead of reading the binding directly.
// Production Job Runner assembly uses NewJobRunnerRuntimeDeliveryStore so every
// delivery dependency is installed.
func NewPostgreSQLRuntimeDeliveryStore(client *dbconnect.Client, runtimeGRPCPort int, resolver RuntimeTargetResolver) *PostgreSQLRuntimeDeliveryStore {
	return &PostgreSQLRuntimeDeliveryStore{
		Client:          client,
		RuntimeGRPCPort: runtimeGRPCPort,
		TargetResolver:  resolver,
		Clock:           func() time.Time { return storage.Now() },
	}
}

// NewJobRunnerRuntimeDeliveryStore assembles the complete production delivery
// store, including the MCP manifest path needed before a session's first run.
func NewJobRunnerRuntimeDeliveryStore(
	client *dbconnect.Client,
	logger *slog.Logger,
	cfg JobRunnerConfig,
	bindingSnapshot func() enginekubernetes.BindingVisibilitySnapshot,
) *PostgreSQLRuntimeDeliveryStore {
	metrics := &RuntimePlacementMetrics{}
	store := NewPostgreSQLRuntimeDeliveryStore(client, cfg.AgentRuntimeGRPCPort, KubernetesRuntimeTargetResolver{Snapshot: bindingSnapshot, PlacementPolicy: cfg.PlacementPolicy, ProcessPolicy: cfg.ProcessPolicy, PlacementMetrics: metrics})
	store.Logger = logger
	store.PlacementMetrics = metrics
	store.MCPManifestLister = mcpmanifest.NewConnectorLister(cfg.MCPConnectorGRPCAddress, internalgrpcauth.FileTokenSource{
		Path: cfg.GatewayTokenPath,
	})
	return store
}

// errRuntimeVisibilityUnavailable is returned when a store has no target
// resolver; there is no process-unaware fallback.
var errRuntimeVisibilityUnavailable = runtimecontrol.PreparationError{Kind: "runtime_visibility_unavailable", Message: "process-aware Runtime target resolution is unavailable", Retryable: true}

const maxRuntimePreparationReentries = 2

func (s *PostgreSQLRuntimeDeliveryStore) PrepareRuntimeCommand(ctx context.Context, job RuntimeJob) (RuntimeCommandPlan, error) {
	started := time.Now()
	plan, err := s.prepareRuntimeCommand(ctx, job, 0)
	s.logRuntimePlacement(job, plan, err, started)
	return plan, err
}

// Preparation can legitimately repair one lost binding and capture one initial
// MCP manifest before retrying the same durable job. Bound those state-driven
// re-entries explicitly so a broken repair or capture cannot recurse forever.
func (s *PostgreSQLRuntimeDeliveryStore) prepareRuntimeCommand(ctx context.Context, job RuntimeJob, reentries int) (RuntimeCommandPlan, error) {
	if s == nil || s.Client == nil {
		return RuntimeCommandPlan{}, runtimecontrol.PreparationError{Kind: "runtime_reconcile_unavailable", Message: "runtime delivery store is unavailable", Retryable: true}
	}
	if job.WorkspaceID == "" || job.SessionID == "" || (job.Kind != queue.KindRuntimeRecovery && job.RuntimeInputID == "") {
		return RuntimeCommandPlan{}, runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "runtime job identity is incomplete", Retryable: false}
	}
	port := s.RuntimeGRPCPort
	if port <= 0 {
		port = defaultAgentRuntimeGRPCPort
	}
	now := storage.Now()
	if s.Clock != nil {
		now = s.Clock().UTC()
	}
	var plan RuntimeCommandPlan
	var initialMCPManifestToolsets []mcpmanifest.ToolsetConfig
	err := s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "jobrunner.prepare_runtime_command", func(tx *dbconnect.Tx) error {
		if err := storage.AcquireSessionRuntimeMutationLock(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
			return err
		}
		if job.Kind == queue.KindSessionDeleteCleanup {
			deletePlan, err := s.prepareSessionDeleteCleanupCommandTx(ctx, tx, job, port, now)
			if err != nil {
				return err
			}
			plan = deletePlan
			return nil
		}
		var terminal bool
		if err := tx.QueryRow(ctx, `SELECT lifecycle_state = 'deleted' OR status = 'terminated' FROM sessions WHERE workspace_id=$1 AND id=$2`, job.WorkspaceID, job.SessionID).Scan(&terminal); dbconnect.IsNoRows(err) {
			plan = RuntimeCommandPlan{StaleAccepted: true}
			return nil
		} else if err != nil {
			return err
		}
		if terminal {
			plan = RuntimeCommandPlan{StaleAccepted: true}
			return nil
		}
		if job.Kind == queue.KindRuntimeRecovery {
			recoveryPlan, err := s.prepareRuntimeRecoveryCommandTx(ctx, tx, job)
			if err != nil {
				return err
			}
			plan = recoveryPlan
			return nil
		}
		if job.Kind == queue.KindRuntimeInput {
			// Runtime input preparation and lifecycle admission share the Session
			// mutation lock before either path locks Inbox custody. This preserves
			// one lock order when a leased notification races child close.
			if err := runtimecontrol.LockRuntimeMutationSessionTx(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
				return err
			}
			if job.InputKind == "interrupt_control" {
				authority, err := interruptDeliveryAuthorityTx(ctx, tx, job, now)
				if err != nil {
					return err
				}
				if !authority.Active {
					plan = RuntimeCommandPlan{
						StaleAccepted:         true,
						DeliveryAuthorityLost: !authority.QueueLeaseSettled,
						QueueLeaseSettled:     authority.QueueLeaseSettled,
					}
					return nil
				}
			}
		}
		if job.Kind == queue.KindRuntimeInput && job.InputKind != "agent_mail" {
			effectiveJob, err := effectiveRuntimeInputJobTx(ctx, tx, job)
			if err != nil {
				return err
			}
			job = effectiveJob
		}
		// A reclaimed lease for an input already accepted by the exact current
		// binding is settlement work, not a new delivery attempt. Check the
		// durable identity before readiness and target-availability gates so
		// transient control-plane observations cannot exhaust accepted custody.
		if job.Kind == queue.KindRuntimeInput {
			settled, err := settleCurrentBindingAcceptedRuntimeInputTx(ctx, tx, job, now)
			if err != nil {
				return err
			}
			if settled {
				plan = RuntimeCommandPlan{SettledAccepted: true, QueueLeaseSettled: true}
				return nil
			}
		}
		if job.Kind == queue.KindCleanupSession {
			cleanupPlan, err := s.prepareCleanupSessionCommandTx(ctx, tx, job, port, now)
			if err != nil {
				return err
			}
			plan = cleanupPlan
			return nil
		}
		if job.Kind == queue.KindRuntimeInput && job.InputKind == "agent_mail" {
			mailPlan, err := s.prepareAgentMailCommandTx(ctx, tx, job, port, now)
			if err != nil {
				return err
			}
			plan = mailPlan
			return nil
		}
		if job.Kind == queue.KindRuntimeInput && job.InputKind == "task_notification" {
			taskPlan, taskCommandPlan, err := s.prepareTaskNotificationCommandTx(ctx, tx, job, port, now)
			if err != nil {
				var initialMCP runtimeInitialMCPManifestRequiredError
				if errors.As(err, &initialMCP) {
					initialMCPManifestToolsets = initialMCP.toolsets
					return nil
				}
				return err
			}
			plan = taskCommandPlan
			plan.TaskNotification = taskPlan
			return nil
		}
		if job.Kind == queue.KindRuntimeInput && job.InputKind != "task_notification" {
			stale, err := allRuntimeInputEventsProcessedTx(ctx, tx, job)
			if err != nil {
				return err
			}
			if stale {
				plan = RuntimeCommandPlan{StaleAccepted: true}
				return nil
			}
			if job.InputKind == "messages" {
				if err := requireUserInputMCPReadyTx(ctx, tx, job); err != nil {
					return err
				}
			} else if job.InputKind != "interrupt_control" {
				if err := requireInitialMCPManifestReadyTx(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
					return err
				}
			}
		}
		binding, err := s.resolveRuntimeTarget(ctx, tx, job)
		if err != nil {
			return err
		}
		sessionThreadID := job.SessionThreadID
		if sessionThreadID == "" {
			var err error
			sessionThreadID, err = readRuntimeCommandSessionThreadIDTx(ctx, tx, job.WorkspaceID, job.SessionID)
			if err != nil {
				return err
			}
		}
		if job.Kind == queue.KindRuntimeInput {
			inboxJob := job
			inboxJob.SessionThreadID = sessionThreadID
			if err := claimRuntimeInboxDeliveryTx(ctx, tx, inboxJob, binding, now); err != nil {
				return err
			}
		}
		payloadJSON, runtimeInputID, err := runtimeCommandPayloadForJobTx(ctx, tx, job)
		if err != nil {
			return err
		}
		plan, err = runtimeCommandPlanForPayload(job, sessionThreadID, runtimeInputID, payloadJSON, binding, port)
		if err != nil {
			return err
		}
		if job.Kind == queue.KindRuntimeInput && job.InputKind == "messages" {
			plan.MCPBeforeInput, err = mcpDiscoveryInstallationTx(ctx, tx, job, binding, port)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		var confirmation runtimePodConfirmationRequired
		if errors.As(err, &confirmation) {
			resolver, ok := s.TargetResolver.(KubernetesRuntimeTargetResolver)
			if !ok {
				return RuntimeCommandPlan{}, err
			}
			observation, confirmErr := resolver.confirmRuntimePod(ctx, confirmation.binding)
			if confirmErr != nil {
				return RuntimeCommandPlan{}, confirmErr
			}
			return s.prepareRuntimeCommand(context.WithValue(ctx, runtimePodObservationKey{}, observation), job, reentries)
		}
		var sampleRequired runtimePlacementRequiredError
		if errors.As(err, &sampleRequired) {
			resolver, ok := s.TargetResolver.(KubernetesRuntimeTargetResolver)
			if !ok {
				return RuntimeCommandPlan{}, err
			}
			choice, sampleErr := resolver.sampleRuntimePlacement(ctx, s.Client, job)
			if sampleErr != nil {
				return RuntimeCommandPlan{placement: &choice}, sampleErr
			}
			plan, prepareErr := s.prepareRuntimeCommand(context.WithValue(ctx, runtimePlacementContextKey{}, choice), job, reentries)
			plan.placement = &choice
			return plan, prepareErr
		}
	}
	if err != nil {
		var initialMCP runtimeInitialMCPManifestRequiredError
		var lostBinding runtimeBindingLostError
		if errors.As(err, &lostBinding) {
			if reentries >= maxRuntimePreparationReentries {
				return RuntimeCommandPlan{}, runtimecontrol.PreparationError{Kind: "runtime_reconcile_invariant", Message: "runtime preparation did not converge after durable repair", Retryable: false}
			}
			if err := s.repairLostRuntimeBinding(ctx, job.WorkspaceID, job.SessionID, lostBinding.binding, now); err != nil {
				return RuntimeCommandPlan{}, err
			}
			return s.prepareRuntimeCommand(ctx, job, reentries+1)
		} else if errors.As(err, &initialMCP) {
			initialMCPManifestToolsets = initialMCP.toolsets
		} else {
			return RuntimeCommandPlan{}, err
		}
	}
	if len(initialMCPManifestToolsets) > 0 {
		if reentries >= maxRuntimePreparationReentries {
			return RuntimeCommandPlan{}, runtimecontrol.PreparationError{Kind: "runtime_reconcile_invariant", Message: "runtime preparation did not converge after manifest capture", Retryable: false}
		}
		if err := s.captureInitialMCPManifests(ctx, job, initialMCPManifestToolsets, now); err != nil {
			return RuntimeCommandPlan{}, err
		}
		return s.prepareRuntimeCommand(ctx, job, reentries+1)
	}
	return plan, nil
}

// InterruptDeliveryAuthority is the final pre-send fence for an
// interrupt command. Preparation performs the same check before any binding or
// Inbox work; this second transaction prevents a lease lost after planning
// from reaching Runtime. Runtime's echoed capability and Bridge closeout
// validation remain the final fence for a command already in transport.
func (s *PostgreSQLRuntimeDeliveryStore) InterruptDeliveryAuthority(ctx context.Context, job RuntimeJob) (RuntimeInterruptDeliveryAuthority, error) {
	if s == nil || s.Client == nil {
		return RuntimeInterruptDeliveryAuthority{}, runtimecontrol.PreparationError{Kind: "runtime_reconcile_unavailable", Message: "runtime delivery store is unavailable", Retryable: true}
	}
	now := storage.Now()
	if s.Clock != nil {
		now = s.Clock().UTC()
	}
	var authority RuntimeInterruptDeliveryAuthority
	err := s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "jobrunner.authorize_interrupt_delivery", func(tx *dbconnect.Tx) error {
		if err := runtimecontrol.LockRuntimeMutationSessionTx(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
			return err
		}
		var err error
		authority, err = interruptDeliveryAuthorityTx(ctx, tx, job, now)
		return err
	})
	return authority, err
}

func interruptDeliveryAuthorityTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob, now time.Time) (RuntimeInterruptDeliveryAuthority, error) {
	if job.Kind != queue.KindRuntimeInput || job.InputKind != "interrupt_control" || job.WorkspaceID == "" || job.SessionID == "" ||
		job.SessionThreadID == "" || job.RuntimeInputID == "" || job.JobID == "" || job.LeaseToken == "" || job.PartitionKey == "" || job.DedupeKey == "" {
		return RuntimeInterruptDeliveryAuthority{}, runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "interrupt delivery authority is incomplete", Retryable: false}
	}
	workspaceID := workspace.ID(job.WorkspaceID)
	if job.PartitionKey != queue.FormatSessionPartitionKey(workspaceID, job.SessionID) ||
		job.DedupeKey != queue.FormatRuntimeInputDedupeKey(workspaceID, job.SessionID, job.RuntimeInputID) {
		return RuntimeInterruptDeliveryAuthority{}, runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "interrupt delivery authority binding is invalid", Retryable: false}
	}
	live, err := queue.AssertExactLeaseTx(ctx, tx, queue.ExactLeaseRequest{
		WorkspaceID: workspaceID, JobID: job.JobID, LeaseToken: job.LeaseToken, Kind: job.Kind,
		PartitionKey: job.PartitionKey, DedupeKey: job.DedupeKey,
	})
	if err != nil {
		return RuntimeInterruptDeliveryAuthority{}, err
	}
	if !live {
		return RuntimeInterruptDeliveryAuthority{}, nil
	}
	barrier, barrierActive, err := runtimecontrol.ActiveInterruptBarrierTx(ctx, tx, job.WorkspaceID, job.SessionID, job.SessionThreadID)
	if err != nil {
		return RuntimeInterruptDeliveryAuthority{}, err
	}
	if !barrierActive || barrier.RuntimeInputID != job.RuntimeInputID {
		pendingCloseout, err := committedInterruptCloseoutNeedsRuntimeTx(ctx, tx, job)
		if err != nil {
			return RuntimeInterruptDeliveryAuthority{}, err
		}
		if pendingCloseout {
			return RuntimeInterruptDeliveryAuthority{Active: true}, nil
		}
		settled, err := queue.CancelLeasedRuntimeInputCustodyTx(ctx, tx, queue.CancelLeasedRuntimeInputRequest{
			Lease: queue.ExactLeaseRequest{
				WorkspaceID: workspaceID, JobID: job.JobID, LeaseToken: job.LeaseToken, Kind: job.Kind,
				PartitionKey: job.PartitionKey, DedupeKey: job.DedupeKey,
			},
			SessionID: job.SessionID, RuntimeInputID: job.RuntimeInputID, InputKind: job.InputKind, Now: now,
		})
		if err != nil {
			return RuntimeInterruptDeliveryAuthority{}, err
		}
		return RuntimeInterruptDeliveryAuthority{QueueLeaseSettled: settled}, nil
	}
	return RuntimeInterruptDeliveryAuthority{Active: true}, nil
}

func (s *PostgreSQLRuntimeDeliveryStore) MarkRuntimeInputAccepted(ctx context.Context, job RuntimeJob, attempt RuntimeAttemptedBinding) (bool, error) {
	if job.Kind != queue.KindRuntimeInput {
		return false, nil
	}
	if s == nil || s.Client == nil {
		return false, runtimecontrol.PreparationError{Kind: "runtime_reconcile_unavailable", Message: "runtime delivery store is unavailable", Retryable: true}
	}
	if job.WorkspaceID == "" || job.SessionID == "" || job.RuntimeInputID == "" ||
		(job.InputKind == "agent_mail" && (job.JobID == "" || job.LeaseToken == "" || job.PartitionKey == "" || job.DedupeKey == "")) ||
		attempt.BindingID == "" || attempt.Generation <= 0 || attempt.TargetPodUID == "" || attempt.RuntimeProcessID == "" {
		return false, runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "runtime job identity is incomplete", Retryable: false}
	}
	now := storage.Now()
	if s.Clock != nil {
		now = s.Clock().UTC()
	}
	queueLeaseSettled := false
	err := s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "jobrunner.mark_runtime_input_accepted", func(tx *dbconnect.Tx) error {
		if err := runtimecontrol.LockRuntimeMutationSessionTx(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
			return err
		}
		if err := lockRuntimeAttemptProcessTx(ctx, tx, job, attempt); err != nil {
			return err
		}
		if job.InputKind == "agent_mail" {
			active, err := queue.AssertExactLeaseTx(ctx, tx, queue.ExactLeaseRequest{
				WorkspaceID: workspace.ID(job.WorkspaceID), JobID: job.JobID, LeaseToken: job.LeaseToken,
				Kind: job.Kind, PartitionKey: job.PartitionKey, DedupeKey: job.DedupeKey,
			})
			if err != nil {
				return err
			}
			if !active {
				return runtimecontrol.PreparationError{Kind: "runtime_queue_lease_stale", Message: "runtime input queue lease is stale", Retryable: true}
			}
		}
		if job.InputKind == "task_notification" {
			closing, err := childcontrol.ThreadOrAncestorClosingTx(ctx, tx, job.WorkspaceID, job.SessionID, job.SessionThreadID)
			if err != nil {
				return err
			}
			if closing {
				settled, err := deferLeasedTaskNotificationTx(ctx, tx, job, now)
				if err != nil {
					return err
				}
				queueLeaseSettled = settled
				return nil
			}
		}
		result, err := tx.Exec(ctx,
			`UPDATE session_runtime_inbox
			    SET status = CASE
						WHEN input_kind='agent_mail' AND $8='agent_mail' THEN 'accepted'
						WHEN status <> 'committed' THEN 'accepted'
			            ELSE status
			        END,
			        updated_at = $4
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND runtime_input_id = $3
			    AND binding_id = $5
			    AND binding_generation = $6
			    AND target_pod_uid = $7
			    AND status IN ('delivering', 'accepted', 'committed')`,
			job.WorkspaceID,
			job.SessionID,
			job.RuntimeInputID,
			now,
			attempt.BindingID,
			attempt.Generation,
			attempt.TargetPodUID,
			job.InputKind,
		)
		if err != nil {
			return err
		}
		if !runtimecontrol.RowsAffected(result) {
			if job.InputKind == "task_notification" {
				replayed, found, replayErr := replayTaskNotificationDeliveryFinalizationTx(ctx, tx, job)
				if replayErr != nil {
					return replayErr
				}
				if found && replayed.Status == RuntimeDeliveryDuplicate {
					return nil
				}
			}
			return runtimecontrol.PreparationError{Kind: "runtime_inbox_accept_missing", Message: "runtime inbox row is missing for accepted input", Retryable: true}
		}
		return nil
	})
	if err == nil && queueLeaseSettled {
		runtimecontrol.LogRuntimeInputCustodyTransition(s.Logger, ServiceNameJobRunner, runtimeScopeFromAttempt(job, attempt), "accepted_to_parked", 1)
	}
	return queueLeaseSettled, err
}

func (s *PostgreSQLRuntimeDeliveryStore) PrepareRuntimeInputRejection(ctx context.Context, job RuntimeJob, result RuntimeDeliveryResult) (bool, error) {
	if job.Kind != queue.KindRuntimeInput {
		return false, nil
	}
	reasonCode, eligible := boundedRuntimeRejectionReason(result)
	if !eligible || job.InputKind != "messages" {
		return false, nil
	}
	if s == nil || s.Client == nil {
		return false, runtimecontrol.PreparationError{Kind: "runtime_reconcile_unavailable", Message: "runtime delivery store is unavailable", Retryable: true}
	}
	if job.WorkspaceID == "" || job.SessionID == "" || job.SessionThreadID == "" || job.RuntimeInputID == "" {
		return false, runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "runtime input rejection identity is incomplete", Retryable: false}
	}
	now := storage.Now()
	if s.Clock != nil {
		now = s.Clock().UTC()
	}
	converted := false
	err := s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "jobrunner.prepare_runtime_input_rejection", func(tx *dbconnect.Tx) error {
		var inboxStatus, inboxKind string
		err := tx.QueryRow(ctx,
			`SELECT status, input_kind
			   FROM session_runtime_inbox
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND runtime_input_id = $3
			  FOR UPDATE`,
			job.WorkspaceID,
			job.SessionID,
			job.RuntimeInputID,
		).Scan(&inboxStatus, &inboxKind)
		if dbconnect.IsNoRows(err) {
			return runtimecontrol.PreparationError{Kind: "runtime_inbox_rejection_missing", Message: "runtime inbox row is missing for rejected input", Retryable: true}
		}
		if err != nil {
			return err
		}
		if inboxKind == "rejection" || inboxStatus != "delivering" {
			return nil
		}
		if inboxKind != job.InputKind {
			return runtimecontrol.PreparationError{Kind: "runtime_inbox_payload_conflict", Message: "runtime input rejection conflicts with the durable inbox row", Retryable: false}
		}
		updateResult, err := tx.Exec(ctx,
			`UPDATE session_runtime_inbox
			    SET input_kind = 'rejection',
			        rejection_reason_code = $4,
			        updated_at = $5
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND runtime_input_id = $3
			    AND status = 'delivering'
			    AND input_kind = $6`,
			job.WorkspaceID,
			job.SessionID,
			job.RuntimeInputID,
			reasonCode,
			now,
			job.InputKind,
		)
		if err != nil {
			return err
		}
		if !runtimecontrol.RowsAffected(updateResult) {
			return runtimecontrol.PreparationError{Kind: "runtime_inbox_rejection_missing", Message: "runtime inbox row is missing for rejected input", Retryable: true}
		}
		converted = true
		return nil
	})
	return converted, err
}

func boundedRuntimeRejectionReason(result RuntimeDeliveryResult) (string, bool) {
	switch result.ErrorKind {
	case "runtime_command_payload_too_large":
		return "runtime_command_payload_too_large", true
	case "runtime_contract_failure", "runtime_command_invalid_argument", "runtime_rejected_input":
		return "runtime_command_rejected", true
	default:
		return "", false
	}
}

// FinalizeRuntimeDelivery turns an exhausted runtime-input delivery into its
// durable terminal result before the Queue job is closed.
func (s *PostgreSQLRuntimeDeliveryStore) FinalizeRuntimeDelivery(ctx context.Context, job RuntimeJob, result RuntimeDeliveryResult) (RuntimeDeliveryResult, error) {
	if s == nil || s.Client == nil {
		return RuntimeDeliveryResult{}, runtimecontrol.PreparationError{Kind: "runtime_reconcile_unavailable", Message: "runtime delivery store is unavailable", Retryable: true}
	}
	if isMCPManifestRuntimeJob(job) {
		return s.finalizeMCPManifestDelivery(ctx, job, result)
	}
	if job.Kind == queue.KindRuntimeRecovery {
		return s.finalizeRuntimeRecoveryDelivery(ctx, job, result)
	}
	if job.Kind != queue.KindRuntimeInput || job.WorkspaceID == "" || job.SessionID == "" || job.SessionThreadID == "" || job.RuntimeInputID == "" ||
		((job.InputKind == "interrupt_control" || job.InputKind == "agent_mail" || result.Status == RuntimeDeliveryBarrierStale) &&
			(job.JobID == "" || job.LeaseToken == "" || job.PartitionKey == "" || job.DedupeKey == "")) {
		return RuntimeDeliveryResult{}, runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "runtime delivery finalization identity is incomplete", Retryable: false}
	}
	if result.Status != RuntimeDeliveryRejected && result.Status != RuntimeDeliveryBarrierStale {
		return RuntimeDeliveryResult{}, runtimecontrol.PreparationError{Kind: "invalid_runtime_response", Message: "runtime delivery finalization requires a rejected result", Retryable: false}
	}
	now := storage.Now()
	if s.Clock != nil {
		now = s.Clock().UTC()
	}
	finalized := RuntimeDeliveryResult{}
	err := s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "jobrunner.finalize_runtime_delivery", func(tx *dbconnect.Tx) error {
		if err := runtimecontrol.LockRuntimeMutationSessionTx(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
			return err
		}
		replayed, found, err := replayRuntimeDeliveryFinalizationTx(ctx, tx, job)
		if err != nil {
			return err
		}
		if found {
			finalized = replayed
			return nil
		}
		if result.Status == RuntimeDeliveryBarrierStale {
			active, err := queue.DeferLeasedRuntimeInputCustodyTx(ctx, tx, queue.DeferLeasedRuntimeInputRequest{
				Lease: queue.ExactLeaseRequest{
					WorkspaceID: workspace.ID(job.WorkspaceID), JobID: job.JobID, LeaseToken: job.LeaseToken,
					Kind: job.Kind, PartitionKey: job.PartitionKey, DedupeKey: job.DedupeKey,
				},
				SessionID: job.SessionID, RuntimeInputID: job.RuntimeInputID, InputKind: job.InputKind, Now: now,
			})
			if err != nil {
				return err
			}
			finalized = RuntimeDeliveryResult{Status: RuntimeDeliveryBarrierStale, QueueLeaseSettled: true}
			if !active {
				finalized = RuntimeDeliveryResult{Status: RuntimeDeliveryAuthorityLost}
			}
			return nil
		}
		if job.InputKind == "interrupt_control" {
			active, err := queue.AssertExactLeaseTx(ctx, tx, queue.ExactLeaseRequest{
				WorkspaceID: workspace.ID(job.WorkspaceID),
				JobID:       job.JobID, LeaseToken: job.LeaseToken, Kind: job.Kind,
				PartitionKey: job.PartitionKey, DedupeKey: job.DedupeKey,
			})
			if err != nil {
				return err
			}
			if !active {
				finalized = RuntimeDeliveryResult{Status: RuntimeDeliveryAuthorityLost}
				return nil
			}
			finalized, err = finalizeInterruptDeliveryExhaustionTx(runtimecontrol.WithInterruptCloseout(ctx, job.WorkspaceID, job.SessionID, job.SessionThreadID, job.RuntimeInputID), tx, job, now)
			return err
		}
		if err := validateRuntimeFinalizationBindingTx(ctx, tx, job, result); err != nil {
			return err
		}
		if job.InputKind == "agent_mail" {
			stale, err := agentMailRecipientTerminalTx(ctx, tx, job)
			if err != nil {
				return err
			}
			replayed, found, err := replayAgentMailDeliveryFinalizationTx(ctx, tx, job)
			if err != nil {
				return err
			}
			if found {
				finalized = replayed
				return nil
			}
			if stale {
				finalized = RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}
				return nil
			}
			role, err := agentMailRecipientRoleTx(ctx, tx, job)
			if err != nil {
				return err
			}
			if role == "subagent" {
				if !runtimeJobAgentMailFinalizationOnly(job) {
					finalized = result
					finalized.Retryable = true
					return nil
				}
				finalized, err = finalizeSubagentAgentMailFailureTx(ctx, tx, job, now)
				return err
			}
			active, err := queue.AssertExactLeaseTx(ctx, tx, queue.ExactLeaseRequest{
				WorkspaceID: workspace.ID(job.WorkspaceID), JobID: job.JobID, LeaseToken: job.LeaseToken,
				Kind: job.Kind, PartitionKey: job.PartitionKey, DedupeKey: job.DedupeKey,
			})
			if err != nil {
				return err
			}
			if !active {
				finalized = RuntimeDeliveryResult{Status: RuntimeDeliveryAuthorityLost}
				return nil
			}
			if err := settleAgentMailDeliveryExhaustionTx(ctx, tx, job, now); err != nil {
				return err
			}
			deadLettered, err := queue.DeadLetterTx(ctx, tx, queue.DeadLetterRequest{
				WorkspaceID: workspace.ID(job.WorkspaceID), JobID: job.JobID, LeaseToken: job.LeaseToken,
				ErrorKind: "runtime_delivery_exhausted", ErrorMessage: "runtime delivery attempts are exhausted", Now: now,
			})
			if err != nil {
				return err
			}
			if !deadLettered {
				return runtimecontrol.PreparationError{Kind: "runtime_queue_lease_stale", Message: "agent mail finalization lost Queue custody", Retryable: true}
			}
			finalized = runtimeDeliveryExhaustedResult()
			finalized.QueueLeaseSettled = true
			return nil
		}
		if job.InputKind == "task_notification" {
			finalized, err = finalizeTaskNotificationDeliveryTx(ctx, tx, job, now)
			return err
		}
		scope := runtimecontrol.SessionScope(job.WorkspaceID, job.SessionID, job.SessionThreadID)
		threadScope, err := runtimecontrol.LockThreadMutationTx(ctx, tx, scope)
		if err != nil {
			return err
		}
		replayed, found, err = replayRuntimeDeliveryFinalizationTx(ctx, tx, job)
		if err != nil {
			return err
		}
		if found {
			finalized = replayed
			return nil
		}
		if err := markRuntimeInputEventsProcessedByIDTx(ctx, tx, job.WorkspaceID, job.SessionID, job.EventIDs, now); err != nil {
			return err
		}
		if err := insertRuntimeDeliveryExhaustionEventWithThreadScopeTx(ctx, tx, job, threadScope, now); err != nil {
			return err
		}
		_, err = tx.Exec(ctx,
			`UPDATE session_runtime_inbox
			    SET status = 'dead_lettered',
			        updated_at = $4
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND runtime_input_id = $3
			    AND status IN ('queued', 'delivering', 'accepted')`,
			job.WorkspaceID,
			job.SessionID,
			job.RuntimeInputID,
			now,
		)
		if err != nil {
			return err
		}
		finalized = runtimeDeliveryExhaustedResult()
		return nil
	})
	if err != nil {
		return RuntimeDeliveryResult{}, err
	}
	if finalized.QueueLeaseSettled {
		return finalized, nil
	}
	if !result.Retryable && finalized.Status == RuntimeDeliveryRejected {
		result.Retryable = false
		return result, nil
	}
	return finalized, nil
}

func (s *PostgreSQLRuntimeDeliveryStore) finalizeRuntimeRecoveryDelivery(
	ctx context.Context,
	job RuntimeJob,
	result RuntimeDeliveryResult,
) (RuntimeDeliveryResult, error) {
	if job.WorkspaceID == "" || job.SessionID == "" || job.SessionThreadID == "" ||
		(job.RecoverySourceEventID == "") == (job.RecoveryHandoffID == "") || job.JobID == "" || job.LeaseToken == "" ||
		job.PartitionKey == "" || job.DedupeKey == "" || result.Status != RuntimeDeliveryRejected ||
		!runtimeJobFinalAttempt(job) {
		return RuntimeDeliveryResult{}, runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "runtime recovery finalization identity is incomplete", Retryable: false}
	}
	now := storage.Now()
	if s.Clock != nil {
		now = s.Clock().UTC()
	}
	finalized := RuntimeDeliveryResult{}
	err := s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "jobrunner.finalize_runtime_recovery", func(tx *dbconnect.Tx) error {
		if err := runtimecontrol.LockRuntimeMutationSessionTx(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
			return err
		}
		active, err := queue.AssertExactLeaseTx(ctx, tx, queue.ExactLeaseRequest{
			WorkspaceID: workspace.ID(job.WorkspaceID), JobID: job.JobID, LeaseToken: job.LeaseToken,
			Kind: job.Kind, PartitionKey: job.PartitionKey, DedupeKey: job.DedupeKey,
		})
		if err != nil {
			return err
		}
		if !active {
			finalized = RuntimeDeliveryResult{Status: RuntimeDeliveryAuthorityLost}
			return nil
		}
		recoveryPayload := queue.RuntimeRecoveryPayload{SessionID: job.SessionID, SessionThreadID: job.SessionThreadID, SourceEventID: job.RecoverySourceEventID, HandoffID: job.RecoveryHandoffID}
		if job.Kind != queue.KindRuntimeRecovery || job.PartitionKey != queue.FormatSessionPartitionKey(workspace.ID(job.WorkspaceID), job.SessionID) || job.DedupeKey != runtimecontrol.RecoveryDedupeKey(job.WorkspaceID, recoveryPayload) {
			return runtimecontrol.InvalidRuntimeFinalizationIdentity("runtime recovery source keys are invalid")
		}
		sourceCurrent, err := runtimecontrol.VerifyRecoverySourceTx(ctx, tx, job.WorkspaceID, job.JobID, recoveryPayload)
		if err != nil {
			return err
		}
		if !sourceCurrent {
			finalized = RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}
			return nil
		}
		var sessionStatus string
		if err := tx.QueryRow(ctx,
			`SELECT status FROM sessions WHERE workspace_id=$1 AND id=$2`,
			job.WorkspaceID, job.SessionID,
		).Scan(&sessionStatus); err != nil {
			return err
		}
		if sessionStatus == "terminated" {
			updated, err := tx.Exec(ctx,
				`UPDATE queue_jobs
				    SET status='cancelled', cancelled_at=$4,
				        lease_token=NULL, leased_by=NULL, leased_at=NULL, leased_until=NULL,
				        lease_previous_attempt_count=NULL, updated_at=$4
				  WHERE workspace_id=$1 AND id=$2 AND lease_token=$3
				    AND kind=$5 AND partition_key=$6 AND dedupe_key=$7 AND status='leased'`,
				job.WorkspaceID, job.JobID, job.LeaseToken, now, job.Kind, job.PartitionKey, job.DedupeKey,
			)
			if err != nil {
				return err
			}
			if !runtimecontrol.RowsAffected(updated) {
				finalized = RuntimeDeliveryResult{Status: RuntimeDeliveryAuthorityLost}
				return nil
			}
			finalized = RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate, QueueLeaseSettled: true}
			return nil
		}
		var bindingID, podUID, processID sql.NullString
		var bindingGeneration sql.NullInt64
		err = tx.QueryRow(ctx,
			`SELECT binding_id, binding_generation, agent_runtime_pod_uid, runtime_process_id
			   FROM session_runtime_bindings
			  WHERE workspace_id=$1 AND session_id=$2
			  FOR UPDATE`,
			job.WorkspaceID, job.SessionID,
		).Scan(&bindingID, &bindingGeneration, &podUID, &processID)
		if err != nil && !dbconnect.IsNoRows(err) {
			return err
		}
		scope := &bridgev1.RuntimeScope{
			WorkspaceId: job.WorkspaceID, SessionId: job.SessionID, SessionThreadId: job.SessionThreadID,
			Binding: &bridgev1.RuntimeBindingRef{
				BindingId: bindingID.String, BindingGeneration: bindingGeneration.Int64, RuntimeProcessId: processID.String, TargetPodUid: podUID.String,
			},
		}
		threadScope, err := runtimecontrol.LockThreadMutationRowTx(ctx, tx, scope)
		if err != nil {
			return err
		}
		failure := runtimecontrol.RuntimeTerminationFailure{
			Type: "runtime", Code: "runtime_recovery_exhausted",
			Message: "The session runtime could not complete the request.",
			Reason:  "runtime_recovery_exhausted", Retryable: false,
		}
		failure.RetryStatus.Type = "terminal"
		failureJSON, err := runtimecontrol.MarshalJSON(failure)
		if err != nil {
			return err
		}
		runtimeWriteID := runtimecontrol.StableRuntimeID("runtime_recovery_exhausted", job.WorkspaceID, job.SessionID, job.SessionThreadID, job.RecoverySourceEventID, job.RecoveryHandoffID)
		if _, _, err := runtimecontrol.SettleRuntimeTerminationTx(ctx, tx, scope, threadScope, runtimeWriteID, failure, failureJSON, now); err != nil {
			return err
		}
		if threadScope.Role != "main" {
			settled, err := settleRuntimeRecoveryExactLeaseTx(ctx, tx, job, result, now)
			if err != nil {
				return err
			}
			if !settled {
				return runtimecontrol.PreparationError{Kind: "runtime_recovery_authority_lost", Message: "runtime recovery lease changed during finalization", Retryable: true}
			}
			if err := reconcileRuntimeRecoverySessionResidencyTx(ctx, tx, job, scope, runtimeWriteID, now); err != nil {
				return err
			}
		}
		finalized = RuntimeDeliveryResult{
			Status: RuntimeDeliveryRejected, Retryable: false,
			ErrorKind: "runtime_delivery_exhausted", ErrorMessage: "runtime delivery attempts are exhausted",
			QueueLeaseSettled: true,
		}
		return nil
	})
	return finalized, err
}

func settleRuntimeRecoveryExactLeaseTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	job RuntimeJob,
	result RuntimeDeliveryResult,
	now time.Time,
) (bool, error) {
	updated, err := tx.Exec(ctx,
		`UPDATE queue_jobs
		    SET status = 'dead_lettered', dead_lettered_at = $8,
		        lease_token = NULL, leased_by = NULL, leased_at = NULL, leased_until = NULL,
		        lease_previous_attempt_count = NULL,
		        last_error_kind = $9, last_error_message = $10, updated_at = $8
		  WHERE workspace_id = $1 AND id = $2 AND lease_token = $3
		    AND kind = $4 AND partition_key = $5 AND dedupe_key = $6
		    AND status = 'leased' AND leased_until > clock_timestamp()
		    AND max_attempts > 0 AND attempt_count = $7 AND attempt_count >= max_attempts`,
		job.WorkspaceID, job.JobID, job.LeaseToken, job.Kind, job.PartitionKey, job.DedupeKey,
		job.AttemptCount, now, result.ErrorKind, result.ErrorMessage,
	)
	if err != nil {
		return false, err
	}
	return runtimecontrol.RowsAffected(updated), nil
}

// Child recovery exhaustion closes only that Thread. Session residency stays
// with the current binding while another direct durable owner still needs the
// Runtime; otherwise it re-enters the existing idle cleanup path.
func reconcileRuntimeRecoverySessionResidencyTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	job RuntimeJob,
	scope *bridgev1.RuntimeScope,
	runtimeWriteID string,
	now time.Time,
) error {
	var remainingWork bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (
		   SELECT 1 FROM session_threads
		    WHERE workspace_id = $1 AND session_id = $2
		      AND status IN ('running', 'rescheduling')
		 ) OR EXISTS (
		   SELECT 1 FROM session_runtime_inbox
		    WHERE workspace_id = $1 AND session_id = $2
		      AND status IN ('queued', 'delivering', 'accepted')
		 ) OR EXISTS (
		   SELECT 1 FROM queue_jobs
		    WHERE workspace_id = $1 AND kind = $3 AND partition_key = $4
		      AND status IN ('pending', 'leased')
		 )`,
		job.WorkspaceID, job.SessionID, queue.KindRuntimeRecovery,
		queue.FormatSessionPartitionKey(workspace.ID(job.WorkspaceID), job.SessionID),
	).Scan(&remainingWork); err != nil {
		return err
	}
	if remainingWork {
		return nil
	}
	var mainThreadID string
	if err := tx.QueryRow(ctx,
		`SELECT main_thread_id FROM sessions WHERE workspace_id = $1 AND id = $2`,
		job.WorkspaceID, job.SessionID,
	).Scan(&mainThreadID); err != nil {
		return err
	}
	mainScope := runtimecontrol.ScopeForThread(scope, mainThreadID)
	mainThreadScope, err := runtimecontrol.LockThreadMutationRowTx(ctx, tx, mainScope)
	if err != nil {
		return err
	}
	idlePayload, err := runtimecontrol.IdleStatusPayloadJSON(`{"type":"end_turn"}`)
	if err != nil {
		return err
	}
	idleWriteID := runtimeWriteID + ":session_idle"
	idleStamp, err := runtimecontrol.InsertRuntimeTerminationEventTx(
		ctx, tx, mainScope, mainThreadScope, idleWriteID, idleWriteID,
		"session.status_idle", idlePayload, now,
	)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE session_threads
		    SET status = 'idle', last_active_at = $4, updated_at = $4
		  WHERE workspace_id = $1 AND session_id = $2 AND id = $3
		    AND status IN ('idle', 'running', 'rescheduling')`,
		job.WorkspaceID, job.SessionID, mainThreadID, now,
	); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE sessions
		    SET status = CASE WHEN status = 'rescheduling' THEN 'idle' ELSE status END,
		        updated_at = $3
		  WHERE workspace_id = $1 AND id = $2 AND status <> 'terminated'`,
		job.WorkspaceID, job.SessionID, now,
	); err != nil {
		return err
	}
	updated, err := tx.Exec(ctx,
		`UPDATE session_runtime_status
		    SET status = 'idle', status_event_id = $5, idle_since = $6,
		        active_seconds_total = active_seconds_total + CASE
		          WHEN running_since IS NULL THEN 0
		          ELSE GREATEST(0, EXTRACT(EPOCH FROM ($6 - running_since)))
		        END,
		        running_since = NULL, cleanup_after = $7,
		        cleanup_enqueued_at = NULL, cleanup_claimed_at = NULL, cleanup_job_id = NULL,
		        updated_at = $6
		  WHERE workspace_id = $1 AND session_id = $2
		    AND binding_id = $3 AND binding_generation = $4`,
		job.WorkspaceID, job.SessionID, scope.GetBinding().GetBindingId(), scope.GetBinding().GetBindingGeneration(),
		idleStamp.EventID, now, now.Add(runtimecontrol.IdleCleanupDelay),
	)
	if err != nil {
		return err
	}
	if !runtimecontrol.RowsAffected(updated) {
		return status.Error(codes.FailedPrecondition, "runtime recovery residency is stale")
	}
	return nil
}

func finalizeInterruptDeliveryExhaustionTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	job RuntimeJob,
	now time.Time,
) (RuntimeDeliveryResult, error) {
	if !runtimeJobFinalAttempt(job) {
		return RuntimeDeliveryResult{}, runtimecontrol.PreparationError{
			Kind: "runtime_inbox_status_invalid", Message: "interrupt delivery attempts remain", Retryable: true,
		}
	}
	return finalizeInterruptDeliveryTerminalTx(ctx, tx, job, now)
}

func finalizeInterruptDeliveryTerminalTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	job RuntimeJob,
	now time.Time,
) (RuntimeDeliveryResult, error) {
	var inboxStatus string
	var inboxBindingID, inboxPodUID sql.NullString
	var inboxBindingGeneration sql.NullInt64
	if err := tx.QueryRow(ctx,
		`SELECT status, binding_id, binding_generation, target_pod_uid
		   FROM session_runtime_inbox
		  WHERE workspace_id=$1 AND session_id=$2 AND runtime_input_id=$3
		  FOR UPDATE`,
		job.WorkspaceID, job.SessionID, job.RuntimeInputID,
	).Scan(&inboxStatus, &inboxBindingID, &inboxBindingGeneration, &inboxPodUID); err != nil {
		if dbconnect.IsNoRows(err) {
			return RuntimeDeliveryResult{}, runtimecontrol.InvalidRuntimeFinalizationIdentity("interrupt Inbox custody is missing")
		}
		return RuntimeDeliveryResult{}, err
	}
	if inboxStatus == "queued" {
		if inboxBindingID.Valid || inboxBindingGeneration.Valid || inboxPodUID.Valid {
			return RuntimeDeliveryResult{}, runtimecontrol.InvalidRuntimeFinalizationIdentity("queued interrupt Inbox has attempted binding custody")
		}
	} else if inboxStatus != "delivering" && inboxStatus != "accepted" {
		return RuntimeDeliveryResult{}, runtimecontrol.InvalidRuntimeFinalizationIdentity("interrupt Inbox custody is not terminalizable")
	}

	var bindingID, podUID, processID sql.NullString
	var bindingGeneration sql.NullInt64
	err := tx.QueryRow(ctx,
		`SELECT binding_id, binding_generation, agent_runtime_pod_uid, runtime_process_id
		   FROM session_runtime_bindings
		  WHERE workspace_id=$1 AND session_id=$2
		  FOR UPDATE`,
		job.WorkspaceID, job.SessionID,
	).Scan(&bindingID, &bindingGeneration, &podUID, &processID)
	if err != nil && !dbconnect.IsNoRows(err) {
		return RuntimeDeliveryResult{}, err
	}
	if err == nil && inboxStatus != "queued" &&
		(!inboxBindingID.Valid || inboxBindingID.String != bindingID.String ||
			!inboxBindingGeneration.Valid || inboxBindingGeneration.Int64 != bindingGeneration.Int64 ||
			!inboxPodUID.Valid || inboxPodUID.String != podUID.String) {
		return RuntimeDeliveryResult{}, runtimecontrol.InvalidRuntimeFinalizationIdentity("interrupt Inbox binding conflicts with the Session fence")
	}

	scope := &bridgev1.RuntimeScope{
		WorkspaceId: job.WorkspaceID, SessionId: job.SessionID, SessionThreadId: job.SessionThreadID,
		Binding: &bridgev1.RuntimeBindingRef{
			BindingId: bindingID.String, BindingGeneration: bindingGeneration.Int64, RuntimeProcessId: processID.String, TargetPodUid: podUID.String,
		},
	}
	threadScope, err := runtimecontrol.LockThreadMutationTx(ctx, tx, scope)
	if err != nil {
		return RuntimeDeliveryResult{}, err
	}
	turnID, err := runtimecontrol.LoadOpenDurableTurnIDTx(ctx, tx, scope)
	if err != nil {
		return RuntimeDeliveryResult{}, err
	}
	runtimeWriteID := runtimecontrol.StableRuntimeID("interrupt_delivery_exhausted", job.WorkspaceID, job.SessionID, job.RuntimeInputID)
	if turnID != nil {
		runtimeWriteID = *turnID
	}
	failure := runtimecontrol.RuntimeTerminationFailure{
		Type: "runtime", Code: "runtime_persistence_exhausted",
		Message: "The session runtime could not complete the request.",
		Reason:  "runtime_input_commit_exhausted", Retryable: false,
	}
	failure.RetryStatus.Type = "terminal"
	failureJSON, err := runtimecontrol.MarshalJSON(failure)
	if err != nil {
		return RuntimeDeliveryResult{}, err
	}
	if _, _, err := runtimecontrol.SettleRuntimeTerminationTx(
		ctx, tx, scope, threadScope, runtimeWriteID, failure, failureJSON, now,
	); err != nil {
		return RuntimeDeliveryResult{}, err
	}
	if threadScope.Role == "main" {
		if _, err := tx.Exec(ctx,
			`DELETE FROM session_runtime_bindings WHERE workspace_id=$1 AND session_id=$2`,
			job.WorkspaceID, job.SessionID,
		); err != nil {
			return RuntimeDeliveryResult{}, err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE session_runtime_status
			    SET cleanup_after=NULL, cleanup_enqueued_at=NULL, cleanup_claimed_at=NULL,
			        cleanup_job_id=NULL, binding_id=NULL, binding_generation=NULL, updated_at=$3
			  WHERE workspace_id=$1 AND session_id=$2`,
			job.WorkspaceID, job.SessionID, now,
		); err != nil {
			return RuntimeDeliveryResult{}, err
		}
	}
	return RuntimeDeliveryResult{
		Status: RuntimeDeliveryRejected, Retryable: false,
		ErrorKind: "runtime_delivery_exhausted", ErrorMessage: "runtime delivery attempts are exhausted",
		QueueLeaseSettled: true,
	}, nil
}

func (s *PostgreSQLRuntimeDeliveryStore) finalizeMCPManifestDelivery(ctx context.Context, job RuntimeJob, result RuntimeDeliveryResult) (RuntimeDeliveryResult, error) {
	if job.WorkspaceID == "" || job.SessionID == "" || job.RuntimeInputID == "" || result.Status != RuntimeDeliveryRejected || !runtimeJobFinalAttempt(job) {
		return RuntimeDeliveryResult{}, runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "MCP manifest delivery finalization identity is incomplete", Retryable: false}
	}
	jobGeneration, err := strconv.ParseInt(job.MCPManifestGeneration, 10, 64)
	if err != nil || jobGeneration <= 0 {
		return RuntimeDeliveryResult{}, runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "MCP manifest generation is invalid", Retryable: false}
	}
	now := storage.Now()
	if s.Clock != nil {
		now = s.Clock().UTC()
	}
	var acceptance mcpmanifest.Acceptance
	err = s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "jobrunner.finalize_mcp_manifest_delivery", func(tx *dbconnect.Tx) error {
		if err := mcpmanifest.AcquireAcceptanceLockTx(ctx, tx, job.WorkspaceID, job.SessionID, job.MCPServerName); err != nil {
			return err
		}
		current, found, err := mcpmanifest.LoadRowForUpdateTx(ctx, tx, job.WorkspaceID, job.SessionID, job.MCPServerName)
		if err != nil {
			return err
		}
		if !found || current.Generation != jobGeneration {
			return nil
		}
		transitioned := current.Readiness != mcpmanifest.ReadinessUnready || !current.Diagnostic.Valid || current.Diagnostic.String != mcpmanifest.DiagnosticDeliveryExhausted
		toolset, err := mcpmanifest.ToolsetConfigTx(ctx, tx, job.WorkspaceID, job.SessionID, job.MCPServerName)
		if err != nil {
			return err
		}
		generation, err := mcpmanifest.TransitionDeliveryExhaustedTx(
			ctx, tx, job.WorkspaceID, job.SessionID, job.MCPServerName, current, toolset, now,
		)
		acceptance = mcpmanifest.Acceptance{
			PreviousGeneration: current.Generation,
			Generation:         generation,
			Readiness:          mcpmanifest.ReadinessUnready,
			Diagnostic:         mcpmanifest.DiagnosticDeliveryExhausted,
			QueueCustody:       "retained",
			Transitioned:       transitioned,
		}
		return err
	})
	if err != nil {
		return RuntimeDeliveryResult{}, err
	}
	mcpmanifest.LogTransitionCommitted(s.Logger, ServiceNameJobRunner, job.WorkspaceID, job.SessionID, job.MCPServerName, acceptance, false)
	return runtimeDeliveryExhaustedResult(), nil
}

func (s *PostgreSQLRuntimeDeliveryStore) ReplayRuntimeDeliveryFinalization(ctx context.Context, job RuntimeJob) (RuntimeDeliveryResult, bool, error) {
	if s == nil || s.Client == nil {
		return RuntimeDeliveryResult{}, false, runtimecontrol.PreparationError{Kind: "runtime_reconcile_unavailable", Message: "runtime delivery store is unavailable", Retryable: true}
	}
	if job.Kind != queue.KindRuntimeInput || job.WorkspaceID == "" || job.SessionID == "" || job.SessionThreadID == "" || job.RuntimeInputID == "" {
		return RuntimeDeliveryResult{}, false, runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "runtime delivery replay identity is incomplete", Retryable: false}
	}
	if job.InputKind == "agent_mail" && (job.JobID == "" || job.LeaseToken == "" || job.PartitionKey == "" || job.DedupeKey == "") {
		return RuntimeDeliveryResult{}, false, runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "agent mail delivery replay identity is incomplete", Retryable: false}
	}
	var result RuntimeDeliveryResult
	var found bool
	err := s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "jobrunner.replay_runtime_delivery_finalization", func(tx *dbconnect.Tx) error {
		switch job.InputKind {
		case "interrupt_control":
			if job.JobID == "" || job.LeaseToken == "" || job.PartitionKey == "" || job.DedupeKey == "" || job.SequenceTo <= 0 {
				return runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "interrupt delivery replay identity is incomplete", Retryable: false}
			}
			// Replay is the first production step for every interrupt lease. Fence
			// pending messages only after locking the Session and exact live Queue
			// identity; a reclaimed worker returns an authority-loss no-op before either
			// Queue followers or Session receipt facts can change.
			if err := runtimecontrol.LockRuntimeMutationSessionTx(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
				return err
			}
			active, _, err := queue.CancelInterruptFencedMessagesTx(ctx, tx, queue.InterruptFenceRequest{
				Lease: queue.ExactLeaseRequest{
					WorkspaceID: workspace.ID(job.WorkspaceID),
					JobID:       job.JobID, LeaseToken: job.LeaseToken, Kind: job.Kind,
					PartitionKey: job.PartitionKey, DedupeKey: job.DedupeKey,
				},
				SessionID: job.SessionID, SessionThreadID: job.SessionThreadID,
				InterruptFenceSequence: job.SequenceTo,
			})
			if err != nil {
				return err
			}
			if !active {
				result = RuntimeDeliveryResult{Status: RuntimeDeliveryAuthorityLost}
				found = true
				return nil
			}
		case "agent_mail":
			if err := runtimecontrol.LockRuntimeMutationSessionTx(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
				return err
			}
			active, err := queue.AssertExactLeaseTx(ctx, tx, queue.ExactLeaseRequest{
				WorkspaceID: workspace.ID(job.WorkspaceID), JobID: job.JobID, LeaseToken: job.LeaseToken,
				Kind: job.Kind, PartitionKey: job.PartitionKey, DedupeKey: job.DedupeKey,
			})
			if err != nil {
				return err
			}
			if !active {
				result = RuntimeDeliveryResult{Status: RuntimeDeliveryAuthorityLost}
				found = true
				return nil
			}
		}
		var err error
		result, found, err = replayRuntimeDeliveryFinalizationTx(ctx, tx, job)
		return err
	})
	return result, found, err
}

func (s *PostgreSQLRuntimeDeliveryStore) ReplaceMalformedRuntimeInputCustody(ctx context.Context, job RuntimeJob) (queue.ReplaceMalformedRuntimeInputCustodyResult, error) {
	if s == nil || s.Client == nil || job.WorkspaceID == "" || job.SessionID == "" || job.RuntimeInputID == "" {
		return queue.ReplaceMalformedRuntimeInputCustodyResult{}, nil
	}
	now := time.Now().UTC()
	if s.Clock != nil {
		now = s.Clock().UTC()
	}
	return queue.NewPostgreSQLStore(s.Client).ReplaceMalformedRuntimeInputCustody(ctx, queue.ReplaceMalformedRuntimeInputCustodyRequest{
		WorkspaceID:    workspace.ID(job.WorkspaceID),
		SessionID:      job.SessionID,
		RuntimeInputID: job.RuntimeInputID,
		JobID:          job.JobID,
		LeaseToken:     job.LeaseToken,
		Now:            now,
	})
}

// FinalizeMalformedRuntimeInputCustody derives business custody exclusively
// from the live Queue row's canonical keys and the unique Inbox relation. An
// interrupt uses the existing Session-wide terminal owner in the same
// transaction as the exact Queue lease; other input kinds retain the Queue
// store's existing kind-specific replacement/dead-letter policy.
func (s *PostgreSQLRuntimeDeliveryStore) FinalizeMalformedRuntimeInputCustody(
	ctx context.Context,
	lease MalformedRuntimeInputLease,
) (MalformedRuntimeInputCustodyResult, error) {
	if s == nil || s.Client == nil || lease.WorkspaceID == "" || lease.JobID == "" || lease.LeaseToken == "" ||
		lease.Kind != queue.KindRuntimeInput || lease.PartitionKey == "" || lease.DedupeKey == "" {
		return MalformedRuntimeInputCustodyResult{}, nil
	}
	now := storage.Now()
	if s.Clock != nil {
		now = s.Clock().UTC()
	}
	var canonical RuntimeJob
	outcome := MalformedRuntimeInputCustodyResult{}
	err := s.Client.WithWorkspaceTx(ctx, lease.WorkspaceID, "jobrunner.finalize_malformed_runtime_input", func(tx *dbconnect.Tx) error {
		var kind, partitionKey, dedupeKey, queueStatus, leaseToken string
		var leaseCurrent bool
		var attemptCount, maxAttempts int32
		if err := tx.QueryRow(ctx, `SELECT kind, partition_key, dedupe_key, status,
			COALESCE(lease_token, ''), COALESCE(leased_until > clock_timestamp(), false),
			attempt_count, max_attempts
			FROM queue_jobs WHERE workspace_id=$1 AND id=$2`,
			lease.WorkspaceID, lease.JobID,
		).Scan(&kind, &partitionKey, &dedupeKey, &queueStatus, &leaseToken, &leaseCurrent, &attemptCount, &maxAttempts); dbconnect.IsNoRows(err) {
			outcome.Handled = true
			outcome.QueueLeaseSettled = true
			return nil
		} else if err != nil {
			return err
		}
		if kind != lease.Kind || partitionKey != lease.PartitionKey || dedupeKey != lease.DedupeKey {
			outcome.Handled = true
			return nil
		}
		if queueStatus == queue.StatusAcknowledged || queueStatus == queue.StatusCancelled || queueStatus == queue.StatusDeadLettered {
			outcome.Handled = true
			outcome.QueueLeaseSettled = true
			return nil
		}
		if queueStatus != queue.StatusLeased || leaseToken != lease.LeaseToken || !leaseCurrent {
			outcome.Handled = true
			return nil
		}

		rows, err := tx.Query(ctx, `SELECT session_id, session_thread_id, runtime_input_id,
			input_kind, event_ids_json, sequence_from, sequence_to
			FROM session_runtime_inbox
			WHERE workspace_id=$1
			  AND $2='session:' || workspace_id || ':' || session_id
			  AND $3='runtime_input:' || workspace_id || ':' || session_id || ':' || runtime_input_id
			ORDER BY runtime_input_id LIMIT 2`, lease.WorkspaceID, partitionKey, dedupeKey)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		var candidates int
		var eventIDsJSON string
		var sequenceFrom, sequenceTo sql.NullInt64
		for rows.Next() {
			candidates++
			if err := rows.Scan(&canonical.SessionID, &canonical.SessionThreadID, &canonical.RuntimeInputID,
				&canonical.InputKind, &eventIDsJSON, &sequenceFrom, &sequenceTo); err != nil {
				return err
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if candidates != 1 {
			return nil
		}
		canonical.WorkspaceID = lease.WorkspaceID
		canonical.JobID = lease.JobID
		canonical.LeaseToken = lease.LeaseToken
		canonical.Kind = lease.Kind
		canonical.PartitionKey = partitionKey
		canonical.DedupeKey = dedupeKey
		canonical.AttemptCount = attemptCount
		canonical.MaxAttempts = maxAttempts
		if canonical.InputKind != "interrupt_control" {
			return nil
		}
		var canonicalEventIDs []string
		eventIDsValid := json.Unmarshal([]byte(eventIDsJSON), &canonicalEventIDs) == nil && len(canonicalEventIDs) > 0
		sequenceValid := sequenceFrom.Valid && sequenceTo.Valid && sequenceFrom.Int64 > 0 && sequenceTo.Int64 >= sequenceFrom.Int64
		effectiveMaxAttempts := maxAttempts
		if effectiveMaxAttempts <= 0 {
			effectiveMaxAttempts = queue.DefaultMaxAttempts
		}
		if attemptCount < effectiveMaxAttempts {
			payload, err := json.Marshal(runtimecontrol.InputQueuePayload{
				WorkspaceID: canonical.WorkspaceID, SessionID: canonical.SessionID, SessionThreadID: canonical.SessionThreadID,
				RuntimeInputID: canonical.RuntimeInputID, EventIDs: canonicalEventIDs,
				SequenceFrom: sequenceFrom.Int64, SequenceTo: sequenceTo.Int64, InputKind: canonical.InputKind,
			})
			if err != nil {
				return err
			}
			updated, err := tx.Exec(ctx, `UPDATE queue_jobs
				SET payload_json=$4, payload_version=1, updated_at=$5
				WHERE workspace_id=$1 AND id=$2 AND lease_token=$3
				  AND status='leased' AND leased_until > clock_timestamp()`,
				canonical.WorkspaceID, canonical.JobID, canonical.LeaseToken, string(payload), now)
			if err != nil {
				return err
			}
			if !runtimecontrol.RowsAffected(updated) {
				return runtimecontrol.InvalidRuntimeFinalizationIdentity("malformed interrupt Queue authority changed during retry preparation")
			}
			outcome.Handled = true
			outcome.Retry = true
			return nil
		}
		if err := runtimecontrol.LockRuntimeMutationSessionTx(ctx, tx, canonical.WorkspaceID, canonical.SessionID); err != nil {
			return err
		}
		active, err := queue.AssertExactLeaseTx(ctx, tx, queue.ExactLeaseRequest{
			WorkspaceID: workspace.ID(canonical.WorkspaceID), JobID: canonical.JobID, LeaseToken: canonical.LeaseToken,
			Kind: canonical.Kind, PartitionKey: canonical.PartitionKey, DedupeKey: canonical.DedupeKey,
		})
		if err != nil {
			return err
		}
		if !active {
			outcome.Handled = true
			return nil
		}
		var lockedThreadID, lockedInputKind, lockedEventIDsJSON string
		var lockedSequenceFrom, lockedSequenceTo sql.NullInt64
		if err := tx.QueryRow(ctx, `SELECT session_thread_id, input_kind, event_ids_json, sequence_from, sequence_to
			FROM session_runtime_inbox
			WHERE workspace_id=$1 AND session_id=$2 AND runtime_input_id=$3 FOR UPDATE`,
			canonical.WorkspaceID, canonical.SessionID, canonical.RuntimeInputID,
		).Scan(&lockedThreadID, &lockedInputKind, &lockedEventIDsJSON, &lockedSequenceFrom, &lockedSequenceTo); err != nil {
			return err
		}
		if lockedThreadID != canonical.SessionThreadID || lockedInputKind != canonical.InputKind || lockedEventIDsJSON != eventIDsJSON ||
			lockedSequenceFrom != sequenceFrom || lockedSequenceTo != sequenceTo {
			return runtimecontrol.InvalidRuntimeFinalizationIdentity("malformed runtime Inbox relation changed during finalization")
		}
		if eventIDsValid && sequenceValid {
			canonical.EventIDs = canonicalEventIDs
			canonical.SequenceFrom = sequenceFrom.Int64
			canonical.SequenceTo = sequenceTo.Int64
		}
		if _, err := finalizeInterruptDeliveryTerminalTx(
			runtimecontrol.WithInterruptCloseout(ctx, canonical.WorkspaceID, canonical.SessionID, canonical.SessionThreadID, canonical.RuntimeInputID), tx, canonical, now,
		); err != nil {
			return err
		}
		outcome.Handled = true
		outcome.QueueLeaseSettled = true
		outcome.InterruptTerminalized = true
		return nil
	})
	if err != nil || outcome.Handled || canonical.SessionID == "" {
		return outcome, err
	}
	replaced, err := queue.NewPostgreSQLStore(s.Client).ReplaceMalformedRuntimeInputCustody(ctx, queue.ReplaceMalformedRuntimeInputCustodyRequest{
		WorkspaceID: workspace.ID(canonical.WorkspaceID), SessionID: canonical.SessionID, RuntimeInputID: canonical.RuntimeInputID,
		JobID: canonical.JobID, LeaseToken: canonical.LeaseToken, Now: now,
	})
	outcome.Handled = true
	outcome.QueueLeaseSettled = replaced.DeadLettered
	outcome.CanonicalReplacement = replaced.Replaced
	return outcome, err
}

// The Session lock must precede this exact binding/process fence. The shared
// process row remains locked through the acknowledgement/finalization commit.
func lockRuntimeAttemptProcessTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob, attempt RuntimeAttemptedBinding) error {
	binding, found, err := runtimecontrol.ReadOptionalRuntimeBindingForDeliveryTx(ctx, tx, job.WorkspaceID, job.SessionID)
	if err != nil {
		return err
	}
	if !found || binding.BindingID != attempt.BindingID || binding.BindingGeneration != attempt.Generation || binding.PodUID != attempt.TargetPodUID || binding.RuntimeProcessID != attempt.RuntimeProcessID {
		return runtimecontrol.InvalidRuntimeFinalizationIdentity("runtime delivery binding/process is stale")
	}
	_, err = runtimecontrol.RequireCurrentProcessTx(ctx, tx, runtimecontrol.ProcessIdentity{Namespace: binding.Namespace, PodUID: binding.PodUID, ID: binding.RuntimeProcessID})
	return err
}

func validateRuntimeFinalizationBindingTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	job RuntimeJob,
	result RuntimeDeliveryResult,
) error {
	var status string
	var bindingID, targetPodUID sql.NullString
	var bindingGeneration sql.NullInt64
	if err := tx.QueryRow(ctx, `SELECT status, binding_id, binding_generation, target_pod_uid
		FROM session_runtime_inbox
		WHERE workspace_id=$1 AND session_id=$2 AND runtime_input_id=$3
		FOR UPDATE`, job.WorkspaceID, job.SessionID, job.RuntimeInputID).Scan(
		&status, &bindingID, &bindingGeneration, &targetPodUID,
	); err != nil {
		if dbconnect.IsNoRows(err) {
			return runtimecontrol.InvalidRuntimeFinalizationIdentity("runtime Inbox custody is missing")
		}
		return err
	}
	attemptedComplete := result.AttemptedBindingID != "" && result.AttemptedBindingGeneration > 0 && result.AttemptedTargetPodUID != "" && result.AttemptedRuntimeProcessID != ""
	attemptedEmpty := result.AttemptedBindingID == "" && result.AttemptedBindingGeneration == 0 && result.AttemptedTargetPodUID == "" && result.AttemptedRuntimeProcessID == ""
	switch status {
	case "queued":
		if !attemptedEmpty || bindingID.Valid || bindingGeneration.Valid || targetPodUID.Valid {
			return runtimecontrol.InvalidRuntimeFinalizationIdentity("queued runtime Inbox conflicts with attempted binding")
		}
	case "delivering", "accepted":
		if runtimeJobAgentMailFinalizationOnly(job) && attemptedEmpty {
			if !bindingID.Valid || bindingID.String == "" || !bindingGeneration.Valid || bindingGeneration.Int64 <= 0 ||
				!targetPodUID.Valid || targetPodUID.String == "" {
				return runtimecontrol.InvalidRuntimeFinalizationIdentity("finalization-only runtime Inbox binding is incomplete")
			}
			break
		}
		if !attemptedComplete || !bindingID.Valid || bindingID.String != result.AttemptedBindingID ||
			!bindingGeneration.Valid || bindingGeneration.Int64 != result.AttemptedBindingGeneration ||
			!targetPodUID.Valid || targetPodUID.String != result.AttemptedTargetPodUID {
			return runtimecontrol.InvalidRuntimeFinalizationIdentity("runtime Inbox binding conflicts with delivery attempt")
		}
	case "committed":
		if !attemptedEmpty && (!attemptedComplete || !bindingID.Valid || bindingID.String != result.AttemptedBindingID ||
			!bindingGeneration.Valid || bindingGeneration.Int64 != result.AttemptedBindingGeneration ||
			!targetPodUID.Valid || targetPodUID.String != result.AttemptedTargetPodUID) {
			return runtimecontrol.InvalidRuntimeFinalizationIdentity("committed runtime Inbox binding conflicts with delivery attempt")
		}
	}
	if attemptedComplete {
		if err := lockRuntimeAttemptProcessTx(ctx, tx, job, RuntimeAttemptedBinding{BindingID: result.AttemptedBindingID, Generation: result.AttemptedBindingGeneration, TargetPodUID: result.AttemptedTargetPodUID, RuntimeProcessID: result.AttemptedRuntimeProcessID}); err != nil {
			return err
		}
	}
	return nil
}

func replayRuntimeDeliveryFinalizationTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (RuntimeDeliveryResult, bool, error) {
	if job.InputKind == "task_notification" {
		return replayTaskNotificationDeliveryFinalizationTx(ctx, tx, job)
	}
	if job.InputKind == "agent_mail" {
		return replayAgentMailDeliveryFinalizationTx(ctx, tx, job)
	}
	inbox, err := runtimecontrol.LockRuntimeInboxFinalizationTx(ctx, tx, job.inputIdentity())
	if err != nil {
		return RuntimeDeliveryResult{}, false, err
	}
	switch inbox.Status {
	case "dead_lettered":
		return runtimeDeliveryExhaustedResult(), true, nil
	case "committed":
		if job.InputKind == "interrupt_control" {
			pendingCloseout, err := committedInterruptCloseoutNeedsRuntimeTx(ctx, tx, job)
			if err != nil {
				return RuntimeDeliveryResult{}, false, err
			}
			if pendingCloseout {
				return RuntimeDeliveryResult{}, false, nil
			}
		}
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}, true, nil
	case "cancelled":
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate, QueueLeaseSettled: true}, true, nil
	case "queued", "delivering", "accepted":
		return RuntimeDeliveryResult{}, false, nil
	default:
		return RuntimeDeliveryResult{}, false, runtimecontrol.PreparationError{Kind: "runtime_inbox_status_invalid", Message: "runtime inbox status is invalid", Retryable: false}
	}
}

func committedInterruptCloseoutNeedsRuntimeTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (bool, error) {
	if job.InputKind != "interrupt_control" {
		return false, nil
	}
	var committed bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (
		   SELECT 1
		     FROM session_runtime_inbox inbox
		     JOIN session_bridge_operations receipt
		       ON receipt.workspace_id = inbox.workspace_id
		      AND receipt.session_id = inbox.session_id
		      AND receipt.session_thread_id = inbox.session_thread_id
		      AND receipt.operation = $5
		      AND receipt.source_kind = 'interrupt_control'
		      AND receipt.idempotency_key = inbox.runtime_input_id
		      AND receipt.receipt_json <> ''
		    WHERE inbox.workspace_id = $1
		      AND inbox.session_id = $2
		      AND inbox.session_thread_id = $3
		      AND inbox.runtime_input_id = $4
		      AND inbox.input_kind = 'interrupt_control'
		      AND inbox.status = 'committed'
		 )`,
		job.WorkspaceID,
		job.SessionID,
		job.SessionThreadID,
		job.RuntimeInputID,
		runtimecontrol.OperationCommitInputs,
	).Scan(&committed); err != nil {
		return false, err
	}
	if !committed {
		return false, nil
	}
	openTurn, err := runtimecontrol.LoadOpenDurableTurnIDTx(ctx, tx, &bridgev1.RuntimeScope{
		WorkspaceId:     job.WorkspaceID,
		SessionId:       job.SessionID,
		SessionThreadId: job.SessionThreadID,
	})
	return openTurn != nil, err
}

func replayAgentMailDeliveryFinalizationTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (RuntimeDeliveryResult, bool, error) {
	inbox, err := runtimecontrol.LockRuntimeInboxFinalizationTx(ctx, tx, job.inputIdentity())
	if err != nil {
		return RuntimeDeliveryResult{}, false, err
	}
	switch inbox.Status {
	case "dead_lettered":
		result, err := settleDeadLetteredAgentMailQueueTx(ctx, tx, job, storage.Now())
		return result, true, err
	case "cancelled":
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate, QueueLeaseSettled: true}, true, nil
	case "accepted":
		settled, err := settleCurrentBindingAcceptedRuntimeInputTx(ctx, tx, job, storage.Now())
		if err != nil {
			return RuntimeDeliveryResult{}, false, err
		}
		if settled {
			return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate, QueueLeaseSettled: true}, true, nil
		}
		return RuntimeDeliveryResult{}, false, nil
	case "committed":
		return RuntimeDeliveryResult{}, false, nil
	case "queued", "delivering":
		return RuntimeDeliveryResult{}, false, nil
	default:
		return RuntimeDeliveryResult{}, false, runtimecontrol.PreparationError{Kind: "runtime_inbox_status_invalid", Message: "runtime inbox status is invalid", Retryable: false}
	}
}

// A dead-lettered Inbox is the durable agent-mail finalization receipt. Replay
// either observes the exact Queue row already terminal or settles its exact
// current lease in this same transaction. A reclaimed token has no authority
// to change either owner.
func settleDeadLetteredAgentMailQueueTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	job RuntimeJob,
	now time.Time,
) (RuntimeDeliveryResult, error) {
	active, err := queue.AssertExactLeaseTx(ctx, tx, queue.ExactLeaseRequest{
		WorkspaceID: workspace.ID(job.WorkspaceID), JobID: job.JobID, LeaseToken: job.LeaseToken,
		Kind: job.Kind, PartitionKey: job.PartitionKey, DedupeKey: job.DedupeKey,
	})
	if err != nil {
		return RuntimeDeliveryResult{}, err
	}
	if active {
		deadLettered, err := queue.DeadLetterTx(ctx, tx, queue.DeadLetterRequest{
			WorkspaceID: workspace.ID(job.WorkspaceID), JobID: job.JobID, LeaseToken: job.LeaseToken,
			ErrorKind: "runtime_delivery_exhausted", ErrorMessage: "runtime delivery attempts are exhausted", Now: now,
		})
		if err != nil {
			return RuntimeDeliveryResult{}, err
		}
		if !deadLettered {
			return RuntimeDeliveryResult{}, runtimecontrol.PreparationError{Kind: "runtime_queue_lease_stale", Message: "agent mail replay lost Queue custody", Retryable: true}
		}
		result := runtimeDeliveryExhaustedResult()
		result.QueueLeaseSettled = true
		return result, nil
	}
	var queueStatus string
	err = tx.QueryRow(ctx, `SELECT status FROM queue_jobs
		WHERE workspace_id=$1 AND id=$2 AND kind=$3 AND partition_key=$4 AND dedupe_key=$5
		FOR UPDATE`, job.WorkspaceID, job.JobID, job.Kind, job.PartitionKey, job.DedupeKey).Scan(&queueStatus)
	if dbconnect.IsNoRows(err) {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryAuthorityLost}, nil
	}
	if err != nil {
		return RuntimeDeliveryResult{}, err
	}
	if queueStatus != queue.StatusDeadLettered {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryAuthorityLost}, nil
	}
	result := runtimeDeliveryExhaustedResult()
	result.QueueLeaseSettled = true
	return result, nil
}

func finalizeSubagentAgentMailFailureTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	job RuntimeJob,
	now time.Time,
) (RuntimeDeliveryResult, error) {
	active, err := queue.AssertExactLeaseTx(ctx, tx, queue.ExactLeaseRequest{
		WorkspaceID: workspace.ID(job.WorkspaceID), JobID: job.JobID, LeaseToken: job.LeaseToken,
		Kind: job.Kind, PartitionKey: job.PartitionKey, DedupeKey: job.DedupeKey,
	})
	if err != nil {
		return RuntimeDeliveryResult{}, err
	}
	if !active {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryAuthorityLost}, nil
	}
	scope := runtimecontrol.SessionScope(job.WorkspaceID, job.SessionID, job.SessionThreadID)
	threadScope, err := runtimecontrol.LockThreadMutationTx(ctx, tx, scope)
	if err != nil {
		return RuntimeDeliveryResult{}, err
	}
	failure := runtimecontrol.RuntimeTerminationFailure{
		Type: "runtime", Code: "runtime_persistence_exhausted",
		Message: "The sub-agent input exhausted Runtime admission.",
		Reason:  "runtime_input_commit_exhausted",
	}
	failure.RetryStatus.Type = "terminal"
	failureJSON, err := runtimecontrol.MarshalJSON(failure)
	if err != nil {
		return RuntimeDeliveryResult{}, err
	}
	runtimeWriteID := runtimecontrol.StableRuntimeID("subagent_agent_mail_exhausted", job.WorkspaceID, job.SessionID, job.RuntimeInputID)
	inboxResult, err := tx.Exec(ctx, `UPDATE session_runtime_inbox SET status='dead_lettered',updated_at=$4
		WHERE workspace_id=$1 AND session_id=$2 AND runtime_input_id=$3
		  AND status IN ('queued','delivering','accepted','committed')`,
		job.WorkspaceID, job.SessionID, job.RuntimeInputID, now)
	if err != nil {
		return RuntimeDeliveryResult{}, err
	}
	if !runtimecontrol.RowsAffected(inboxResult) {
		return RuntimeDeliveryResult{}, runtimecontrol.PreparationError{Kind: "runtime_inbox_terminal_missing", Message: "sub-agent input finalization lost Inbox custody", Retryable: true}
	}
	if _, _, err := runtimecontrol.SettleRuntimeTerminationTx(ctx, tx, scope, threadScope, runtimeWriteID, failure, failureJSON, now); err != nil {
		return RuntimeDeliveryResult{}, err
	}
	deadLettered, err := tx.Exec(ctx, `UPDATE queue_jobs
		SET status='dead_lettered',dead_lettered_at=$4,last_error_kind='runtime_delivery_exhausted',
		    last_error_message='sub-agent input exhausted before Runtime admission',
		    lease_token=NULL,leased_by=NULL,leased_at=NULL,leased_until=NULL,lease_previous_attempt_count=NULL,updated_at=$4
		WHERE workspace_id=$1 AND id=$2 AND kind=$5 AND partition_key=$6 AND dedupe_key=$7
		  AND status='leased' AND lease_token=$3 AND leased_until > clock_timestamp()`,
		job.WorkspaceID, job.JobID, job.LeaseToken, now, job.Kind, job.PartitionKey, job.DedupeKey)
	if err != nil {
		return RuntimeDeliveryResult{}, err
	}
	if !runtimecontrol.RowsAffected(deadLettered) {
		return RuntimeDeliveryResult{}, runtimecontrol.PreparationError{Kind: "runtime_queue_lease_stale", Message: "sub-agent input finalization lost Queue custody", Retryable: true}
	}
	result := runtimeDeliveryExhaustedResult()
	result.QueueLeaseSettled = true
	return result, nil
}

func settleAgentMailDeliveryExhaustionTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	job RuntimeJob,
	now time.Time,
) error {
	if err := insertRuntimeDeliveryExhaustionEventTx(ctx, tx, job, now); err != nil {
		return err
	}
	finalizationOnly := runtimeJobAgentMailFinalizationOnly(job)
	deliveryID := strings.TrimPrefix(job.RuntimeInputID, "agent_mail:")
	if deliveryID == "" || deliveryID == job.RuntimeInputID {
		return runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "agent mail runtime input id is invalid", Retryable: false}
	}
	if _, err := tx.Exec(ctx,
		`UPDATE session_events
		    SET processed_at = COALESCE(processed_at, $5),
		        updated_at = $5
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND type = 'agent.thread_message_received'
		    AND payload_json::jsonb ->> 'delivery_id' = $4`,
		job.WorkspaceID,
		job.SessionID,
		job.SessionThreadID,
		deliveryID,
		now,
	); err != nil {
		return err
	}
	_, err := tx.Exec(ctx,
		`UPDATE session_runtime_inbox
		    SET status = 'dead_lettered',
		        updated_at = $4
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND runtime_input_id = $3
		    AND (status IN ('queued', 'delivering', 'accepted')
		         OR (status = 'committed' AND $5))`,
		job.WorkspaceID,
		job.SessionID,
		job.RuntimeInputID,
		now,
		finalizationOnly,
	)
	return err
}

func replayTaskNotificationDeliveryFinalizationTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (RuntimeDeliveryResult, bool, error) {
	taskID := runtimecontrol.TaskNotificationTaskID(job.RuntimeInputID)
	if taskID == "" {
		return RuntimeDeliveryResult{}, false, runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "task notification runtime input id must identify a task", Retryable: false}
	}
	inbox, err := runtimecontrol.LockRuntimeInboxFinalizationTx(ctx, tx, job.inputIdentity())
	if err != nil {
		return RuntimeDeliveryResult{}, false, err
	}
	switch inbox.Status {
	case "dead_lettered":
		var errorCode string
		err := tx.QueryRow(ctx, `SELECT error_code
				FROM session_bridge_operations
				WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
				  AND operation=$4 AND source_kind=$4 AND idempotency_key=$5
				  AND ack_status=$6 AND runtime_input_id=$7
				FOR UPDATE`,
			job.WorkspaceID, job.SessionID, job.SessionThreadID,
			runtimecontrol.OperationCommitTaskNotificationResult, taskID+":"+job.RuntimeInputID,
			runtimecontrol.AckRejected, job.RuntimeInputID,
		).Scan(&errorCode)
		if err == nil && runtimecontrol.TaskNotificationRejectionCode(errorCode) {
			return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}, true, nil
		}
		if err != nil && !dbconnect.IsNoRows(err) {
			return RuntimeDeliveryResult{}, false, err
		}
		return runtimeDeliveryExhaustedResult(), true, nil
	case "committed", "cancelled":
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}, true, nil
	case "parked":
		// Deferral owns parked custody; a stale Queue lease cannot terminalize it.
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}, true, nil
	case "queued", "delivering", "accepted":
	default:
		return RuntimeDeliveryResult{}, false, runtimecontrol.PreparationError{Kind: "runtime_inbox_status_invalid", Message: "runtime inbox status is invalid", Retryable: false}
	}
	var taskThreadID string
	var terminalResultJSON sql.NullString
	err = tx.QueryRow(ctx,
		`SELECT session_thread_id, terminal_result_json
		   FROM session_background_tasks
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND task_id = $3
		  FOR UPDATE`,
		job.WorkspaceID,
		job.SessionID,
		taskID,
	).Scan(&taskThreadID, &terminalResultJSON)
	if dbconnect.IsNoRows(err) {
		return RuntimeDeliveryResult{}, false, runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "task notification source is missing", Retryable: false}
	}
	if err != nil {
		return RuntimeDeliveryResult{}, false, err
	}
	if taskThreadID != job.SessionThreadID || !terminalResultJSON.Valid || terminalResultJSON.String == "" {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}, true, nil
	}
	return RuntimeDeliveryResult{}, false, nil
}

// agentMailRecipientTerminalTx reports the mail stale: once the recipient session or thread
// is terminal the delivery is no longer current and settles accepted without a wake.
func agentMailRecipientTerminalTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (bool, error) {
	var sessionStatus string
	var lifecycleState string
	if err := tx.QueryRow(ctx,
		`SELECT status, lifecycle_state
		   FROM sessions
		  WHERE workspace_id=$1 AND id=$2
		  FOR UPDATE`,
		job.WorkspaceID,
		job.SessionID,
	).Scan(&sessionStatus, &lifecycleState); dbconnect.IsNoRows(err) {
		return true, nil
	} else if err != nil {
		return false, err
	}
	if lifecycleState == "deleted" || sessionStatus == "terminated" {
		return true, nil
	}
	var recipientStatus string
	if err := tx.QueryRow(ctx,
		`SELECT status
		   FROM session_threads
		  WHERE workspace_id=$1 AND session_id=$2 AND id=$3
		  FOR UPDATE`,
		job.WorkspaceID,
		job.SessionID,
		job.SessionThreadID,
	).Scan(&recipientStatus); dbconnect.IsNoRows(err) {
		return true, nil
	} else if err != nil {
		return false, err
	}
	return recipientStatus == "closed_for_runtime" || recipientStatus == "terminated", nil
}

func agentMailRecipientRoleTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (string, error) {
	var role string
	err := tx.QueryRow(ctx, `SELECT role
		FROM session_threads
		WHERE workspace_id=$1 AND session_id=$2 AND id=$3
		FOR UPDATE`, job.WorkspaceID, job.SessionID, job.SessionThreadID).Scan(&role)
	if dbconnect.IsNoRows(err) {
		return "", runtimecontrol.InvalidRuntimeFinalizationIdentity("agent mail recipient is missing")
	}
	return role, err
}

func finalizeTaskNotificationDeliveryTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob, now time.Time) (RuntimeDeliveryResult, error) {
	if err := insertRuntimeDeliveryExhaustionEventTx(ctx, tx, job, now); err != nil {
		return RuntimeDeliveryResult{}, err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE session_runtime_inbox
		    SET status = 'dead_lettered',
		        updated_at = $4
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND runtime_input_id = $3
		    AND status IN ('queued', 'delivering', 'accepted')`,
		job.WorkspaceID,
		job.SessionID,
		job.RuntimeInputID,
		now,
	); err != nil {
		return RuntimeDeliveryResult{}, err
	}
	return runtimeDeliveryExhaustedResult(), nil
}

func insertRuntimeDeliveryExhaustionEventTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob, now time.Time) error {
	scope := runtimecontrol.SessionScope(job.WorkspaceID, job.SessionID, job.SessionThreadID)
	threadScope, err := runtimecontrol.LockThreadMutationTx(ctx, tx, scope)
	if err != nil {
		return err
	}
	return insertRuntimeDeliveryExhaustionEventWithThreadScopeTx(ctx, tx, job, threadScope, now)
}

func insertRuntimeDeliveryExhaustionEventWithThreadScopeTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob, threadScope runtimecontrol.ThreadMutationScope, now time.Time) error {
	scope := runtimecontrol.SessionScope(job.WorkspaceID, job.SessionID, job.SessionThreadID)
	visibility, sessionVisible := threadScope.PublicProjection("session.error")
	message := "The session runtime exhausted delivery attempts for this input."
	payloadJSON, err := runtimeDeliveryExhaustionPayloadJSON(message)
	if err != nil {
		return err
	}
	eventID := runtimeDeliveryExhaustionEventID(job)
	sequence, err := runtimecontrol.NextSessionEventSequenceTx(ctx, tx, scope)
	if err != nil {
		return err
	}
	if _, err := sessioneventwrite.InsertInitialTx(ctx, tx, sessioneventwrite.InitialEvent{
		WorkspaceID: job.WorkspaceID, SessionID: job.SessionID, SessionThreadID: job.SessionThreadID,
		EventID: eventID, Sequence: sequence, Type: "session.error",
		PayloadJSON: payloadJSON, ProjectionJSON: payloadJSON, Visibility: visibility, SessionVisible: sessionVisible,
		CreatedAt: now, ProcessedAt: &now,
	}); err != nil {
		return err
	}
	return nil
}

func runtimeDeliveryExhaustionEventID(job RuntimeJob) string {
	digest := runtimecontrol.Sha256Hex(strings.Join([]string{job.WorkspaceID, job.SessionID, job.RuntimeInputID, "runtime_delivery_exhausted"}, "\x00"))
	return "evt_runtime_exhausted_" + digest[:24]
}

func runtimeDeliveryExhaustionPayloadJSON(message string) (string, error) {
	return runtimecontrol.MarshalJSON(map[string]any{
		"type": "session.error",
		"error": map[string]any{
			"type":    "unknown_error",
			"message": message,
			"retry_status": map[string]any{
				"type": "exhausted",
			},
		},
	})
}

func runtimeDeliveryExhaustedResult() RuntimeDeliveryResult {
	return RuntimeDeliveryResult{
		Status:       RuntimeDeliveryRejected,
		Retryable:    false,
		ErrorKind:    "runtime_delivery_exhausted",
		ErrorMessage: "runtime delivery attempts are exhausted",
	}
}

type runtimeInitialMCPManifestRequiredError struct {
	toolsets []mcpmanifest.ToolsetConfig
}

func (e runtimeInitialMCPManifestRequiredError) Error() string {
	return "initial MCP manifest delivery is required"
}

func requireInitialMCPManifestReadyTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string) error {
	toolsets, err := mcpmanifest.InitialToolsetsTx(ctx, tx, workspaceID, sessionID)
	if err != nil || len(toolsets) == 0 {
		return err
	}
	return runtimeInitialMCPManifestRequiredError{toolsets: toolsets}
}

func (s *PostgreSQLRuntimeDeliveryStore) captureInitialMCPManifests(ctx context.Context, job RuntimeJob, toolsets []mcpmanifest.ToolsetConfig, now time.Time) error {
	return s.captureInitialMCPManifestsWithListTimeout(ctx, job, toolsets, now, initialMCPManifestListTimeout)
}

func (s *PostgreSQLRuntimeDeliveryStore) captureInitialMCPManifestsWithListTimeout(
	ctx context.Context,
	job RuntimeJob,
	toolsets []mcpmanifest.ToolsetConfig,
	now time.Time,
	listTimeout time.Duration,
) error {
	if job.InputKind == "messages" {
		return s.discoverUserInputMCP(ctx, job, toolsets, listTimeout)
	}
	if s.MCPManifestLister == nil {
		return runtimecontrol.PreparationError{Kind: "mcp_manifest_discovery_unavailable", Message: "mcp manifest lister is unavailable", Retryable: true}
	}
	for _, toolset := range toolsets {
		listCtx, cancelList := context.WithTimeout(ctx, listTimeout)
		manifest, err := s.MCPManifestLister.ListMCPTools(listCtx, mcpmanifest.ListRequest{
			WorkspaceID:   job.WorkspaceID,
			SessionID:     job.SessionID,
			MCPServerName: toolset.MCPServerName,
		})
		cancelList()
		if err != nil {
			var discoveryFailure mcpmanifest.DiscoveryError
			if !errors.As(err, &discoveryFailure) {
				return err
			}
			acceptance, commitErr := s.captureInitialMCPManifestFailure(ctx, job, toolset, discoveryFailure.Diagnostic, now)
			if commitErr != nil {
				return commitErr
			}
			mcpmanifest.LogTransitionCommitted(s.Logger, ServiceNameJobRunner, job.WorkspaceID, job.SessionID, toolset.MCPServerName, acceptance, true)
			continue
		}
		var acceptance mcpmanifest.Acceptance
		if err := s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "jobrunner.enqueue_initial_mcp_manifest", func(tx *dbconnect.Tx) error {
			var err error
			acceptance, err = mcpmanifest.CaptureInitialAcceptanceTx(
				ctx, tx, job.WorkspaceID, job.SessionID, toolset, manifest, now,
			)
			return err
		}); err != nil {
			return err
		}
		mcpmanifest.LogTransitionCommitted(s.Logger, ServiceNameJobRunner, job.WorkspaceID, job.SessionID, toolset.MCPServerName, acceptance, true)
		if !acceptance.Duplicate {
			mcpmanifest.LogOmissions(s.Logger, ServiceNameJobRunner, job.WorkspaceID, job.SessionID, toolset.MCPServerName, acceptance.BuiltinFamily, acceptance.Omissions)
		}
	}
	return nil
}

func (s *PostgreSQLRuntimeDeliveryStore) captureInitialMCPManifestFailure(
	ctx context.Context,
	job RuntimeJob,
	toolset mcpmanifest.ToolsetConfig,
	diagnostic string,
	now time.Time,
) (mcpmanifest.Acceptance, error) {
	var acceptance mcpmanifest.Acceptance
	err := s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "jobrunner.capture_initial_mcp_manifest_failure", func(tx *dbconnect.Tx) error {
		var err error
		acceptance, err = mcpmanifest.CaptureInitialUnreadyTx(ctx, tx, job.WorkspaceID, job.SessionID, toolset, diagnostic, now)
		return err
	})
	return acceptance, err
}

func runtimeTaskNotificationPayloadJSON(plan *RuntimeTaskNotificationPlan, terminalStatus string, resultJSON string) (string, error) {
	if plan == nil || plan.TaskID == "" || plan.SourceToolUseEventID == "" {
		return "", runtimecontrol.PreparationError{Kind: "task_notification_result_invalid", Message: "task notification source identity is incomplete", Retryable: false}
	}
	payloadJSON, err := runtimecontrol.CanonicalTaskNotificationPayloadJSON(plan.TaskID, plan.SourceToolUseEventID, terminalStatus, resultJSON)
	if err != nil {
		return "", runtimecontrol.PreparationError{Kind: "task_notification_result_invalid", Message: err.Error(), Retryable: false}
	}
	return payloadJSON, nil
}

func (s *PostgreSQLRuntimeDeliveryStore) resolveRuntimeTarget(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (runtimecontrol.Binding, error) {
	if s.TargetResolver == nil {
		return runtimecontrol.Binding{}, errRuntimeVisibilityUnavailable
	}
	return s.TargetResolver.ResolveRuntimeTarget(ctx, tx, job)
}

func runtimeCommandPayloadForJobTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (string, string, error) {
	if job.Kind == queue.KindRuntimeConfigUpdate {
		if job.MCPServerName != "" {
			return runtimeMCPManifestCommandPayloadTx(ctx, tx, job)
		}
		return runtimeSessionConfigCommandPayloadTx(ctx, tx, job)
	}
	if job.Kind != queue.KindRuntimeInput {
		return job.PayloadJSON, job.RuntimeInputID, nil
	}
	switch job.InputKind {
	case "messages":
		payload, err := acceptedMessageCommandPayloadTx(ctx, tx, job)
		return payload, job.RuntimeInputID, err
	case "rejection":
		payload, err := runtimecontrol.MarshalDataJSON(map[string]any{
			"input_kind":  "rejection",
			"reason_code": job.RejectionReasonCode,
		})
		return payload, job.RuntimeInputID, err
	case "interrupt_control":
		payload, err := interruptControlCommandPayloadTx(ctx, tx, job)
		return payload, job.RuntimeInputID, err
	case "tool_confirmation":
		payload, err := toolConfirmationCommandPayloadTx(ctx, tx, job)
		return payload, job.RuntimeInputID, err
	default:
		return job.PayloadJSON, job.RuntimeInputID, nil
	}
}

func runtimeCommandPlanForPayload(job RuntimeJob, sessionThreadID, runtimeInputID, contentJSON string, binding runtimecontrol.Binding, port int) (RuntimeCommandPlan, error) {
	target := RuntimePodTarget{Namespace: binding.Namespace, PodName: binding.PodName, PodUID: binding.PodUID, RuntimeProcessID: binding.RuntimeProcessID, PodIP: binding.PodIP, Port: port}
	attempt := RuntimeAttemptedBinding{BindingID: binding.BindingID, Generation: binding.BindingGeneration, TargetPodUID: binding.PodUID, RuntimeProcessID: binding.RuntimeProcessID}
	plan := RuntimeCommandPlan{Target: target, AttemptedBinding: attempt}
	thread := func() (string, string, string, int64, string) {
		return job.WorkspaceID, job.SessionID, sessionThreadID, binding.BindingGeneration, binding.PodUID
	}
	workspaceID, sessionID, threadID, generation, podUID := thread()
	switch job.Kind {
	case queue.KindRuntimeConfigUpdate:
		configGeneration, err := runtimeGenerationFromInputID(runtimeInputID)
		if err != nil {
			return RuntimeCommandPlan{}, err
		}
		request := &agentruntimev1.ApplyRuntimeConfigRequest{WorkspaceId: workspaceID, SessionId: sessionID, BindingId: binding.BindingID, BindingGeneration: generation, RuntimeProcessId: binding.RuntimeProcessID, TargetPodUid: podUID}
		if job.MCPServerName != "" {
			request.Config = &agentruntimev1.ApplyRuntimeConfigRequest_McpManifest{McpManifest: &agentruntimev1.RuntimeMcpManifestConfig{McpServerName: job.MCPServerName, Generation: configGeneration, ContentJson: contentJSON}}
		} else {
			request.Config = &agentruntimev1.ApplyRuntimeConfigRequest_SessionConfig{SessionConfig: &agentruntimev1.RuntimeSessionConfig{Generation: configGeneration, ContentJson: contentJSON}}
		}
		plan.RuntimeConfig = request
		return plan, nil
	case queue.KindRuntimeInput:
	default:
		return RuntimeCommandPlan{}, runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "unsupported runtime command job", Retryable: false}
	}
	switch job.InputKind {
	case "messages", "rejection":
		request := &agentruntimev1.AcceptInputRequest{WorkspaceId: workspaceID, SessionId: sessionID, SessionThreadId: threadID, BindingId: binding.BindingID, BindingGeneration: generation, RuntimeProcessId: binding.RuntimeProcessID, TargetPodUid: podUID, RuntimeInputId: runtimeInputID, InputOrder: job.SequenceTo}
		if job.InputKind == "messages" {
			request.Content = &agentruntimev1.AcceptInputRequest_MessagesJson{MessagesJson: contentJSON}
		} else {
			reason := agentruntimev1.AcceptInputRejectionReason_ACCEPT_INPUT_REJECTION_REASON_RUNTIME_REJECTED
			if job.RejectionReasonCode == "runtime_command_payload_too_large" {
				reason = agentruntimev1.AcceptInputRejectionReason_ACCEPT_INPUT_REJECTION_REASON_PAYLOAD_TOO_LARGE
			}
			request.Content = &agentruntimev1.AcceptInputRequest_Rejection{Rejection: &agentruntimev1.AcceptInputRejection{Reason: reason}}
		}
		plan.AcceptInput = request
	case "interrupt_control":
		var content struct {
			Origin string `json:"origin"`
		}
		if err := json.Unmarshal([]byte(contentJSON), &content); err != nil {
			return RuntimeCommandPlan{}, err
		}
		origin := agentruntimev1.InterruptOrigin_INTERRUPT_ORIGIN_USER
		if content.Origin == "agent" {
			origin = agentruntimev1.InterruptOrigin_INTERRUPT_ORIGIN_AGENT
		}
		plan.Interrupt = &agentruntimev1.InterruptRequest{
			WorkspaceId: workspaceID, SessionId: sessionID, SessionThreadId: threadID,
			BindingId: binding.BindingID, BindingGeneration: generation, RuntimeProcessId: binding.RuntimeProcessID, TargetPodUid: podUID,
			RuntimeInputId: runtimeInputID, Origin: origin,
			InterruptLeaseRef: &agentruntimev1.InterruptLeaseRef{
				JobId: job.JobID, LeaseToken: job.LeaseToken,
				PartitionKey: job.PartitionKey, DedupeKey: job.DedupeKey,
			},
		}
	case "tool_confirmation":
		var content struct {
			ToolUseEventID string  `json:"tool_use_event_id"`
			Decision       string  `json:"decision"`
			DenyMessage    *string `json:"deny_message"`
		}
		if err := json.Unmarshal([]byte(contentJSON), &content); err != nil || content.ToolUseEventID == "" {
			return RuntimeCommandPlan{}, runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "tool confirmation command is invalid", Retryable: false}
		}
		decision := agentruntimev1.ToolConfirmationDecision_TOOL_CONFIRMATION_DECISION_ALLOW
		if content.Decision == "deny" {
			decision = agentruntimev1.ToolConfirmationDecision_TOOL_CONFIRMATION_DECISION_DENY
		}
		request := &agentruntimev1.ResolveToolConfirmationRequest{WorkspaceId: workspaceID, SessionId: sessionID, SessionThreadId: threadID, BindingId: binding.BindingID, BindingGeneration: generation, RuntimeProcessId: binding.RuntimeProcessID, TargetPodUid: podUID, RuntimeInputId: runtimeInputID, ToolUseEventId: content.ToolUseEventID, Decision: decision}
		if content.DenyMessage != nil {
			request.DenyMessage = content.DenyMessage
		}
		plan.ToolConfirmation = request
	default:
		return RuntimeCommandPlan{}, runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "unsupported runtime input kind", Retryable: false}
	}
	return plan, nil
}

func (s *PostgreSQLRuntimeDeliveryStore) prepareRuntimeRecoveryCommandTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (RuntimeCommandPlan, error) {
	active, terminal, err := validateRuntimeRecoveryAuthorityTx(ctx, tx, job)
	if err != nil {
		return RuntimeCommandPlan{}, err
	}
	if terminal {
		return RuntimeCommandPlan{StaleAccepted: true}, nil
	}
	if !active {
		return RuntimeCommandPlan{DeliveryAuthorityLost: true}, nil
	}
	return RuntimeCommandPlan{RecoveryPrepared: true}, nil
}

func validateRuntimeRecoveryAuthorityTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (active bool, terminal bool, resultErr error) {
	if (job.RecoverySourceEventID == "") == (job.RecoveryHandoffID == "") || job.JobID == "" || job.LeaseToken == "" || job.PartitionKey == "" || job.DedupeKey == "" {
		return false, false, runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "runtime recovery identity is incomplete", Retryable: false}
	}
	if err := runtimecontrol.LockRuntimeMutationSessionTx(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
		return false, false, err
	}
	var sessionStatus string
	if err := tx.QueryRow(ctx,
		`SELECT status FROM sessions WHERE workspace_id=$1 AND id=$2`,
		job.WorkspaceID, job.SessionID,
	).Scan(&sessionStatus); err != nil {
		return false, false, err
	}
	if sessionStatus == "terminated" {
		return false, true, nil
	}
	live, err := queue.AssertExactLeaseTx(ctx, tx, queue.ExactLeaseRequest{
		WorkspaceID: workspace.ID(job.WorkspaceID), JobID: job.JobID, LeaseToken: job.LeaseToken, Kind: job.Kind,
		PartitionKey: job.PartitionKey, DedupeKey: job.DedupeKey,
	})
	if err != nil {
		return false, false, err
	}
	if !live {
		return false, false, nil
	}
	payload := queue.RuntimeRecoveryPayload{SessionID: job.SessionID, SessionThreadID: job.SessionThreadID, SourceEventID: job.RecoverySourceEventID, HandoffID: job.RecoveryHandoffID}
	if job.Kind != queue.KindRuntimeRecovery || job.PartitionKey != queue.FormatSessionPartitionKey(workspace.ID(job.WorkspaceID), job.SessionID) || job.DedupeKey != runtimecontrol.RecoveryDedupeKey(job.WorkspaceID, payload) {
		return false, false, runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "runtime recovery keys are invalid", Retryable: false}
	}
	found, err := runtimecontrol.VerifyRecoverySourceTx(ctx, tx, job.WorkspaceID, job.JobID, payload)
	if err != nil {
		return false, false, err
	}
	if !found {
		return false, true, nil
	}

	return true, false, nil
}

func (s *PostgreSQLRuntimeDeliveryStore) ActivateRuntimeRecovery(ctx context.Context, job RuntimeJob) (RuntimeCommandPlan, error) {
	started := time.Now()
	plan, err := s.activateRuntimeRecovery(ctx, job, 0)
	s.logRuntimePlacement(job, plan, err, started)
	return plan, err
}
func (s *PostgreSQLRuntimeDeliveryStore) activateRuntimeRecovery(ctx context.Context, job RuntimeJob, reentries int) (RuntimeCommandPlan, error) {
	if s == nil || s.Client == nil {
		return RuntimeCommandPlan{}, runtimecontrol.PreparationError{Kind: "runtime_reconcile_unavailable", Message: "runtime delivery store is unavailable", Retryable: true}
	}
	port := s.RuntimeGRPCPort
	if port <= 0 {
		port = defaultAgentRuntimeGRPCPort
	}
	now := storage.Now()
	if s.Clock != nil {
		now = s.Clock().UTC()
	}
	var plan RuntimeCommandPlan
	err := s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "jobrunner.activate_runtime_recovery", func(tx *dbconnect.Tx) error {
		active, terminal, err := validateRuntimeRecoveryAuthorityTx(ctx, tx, job)
		if err != nil {
			return err
		}
		if terminal {
			plan = RuntimeCommandPlan{StaleAccepted: true}
			return nil
		}
		if !active {
			plan = RuntimeCommandPlan{DeliveryAuthorityLost: true}
			return nil
		}
		binding, err := s.resolveRuntimeTarget(ctx, tx, job)
		if err != nil {
			return err
		}
		result, err := tx.Exec(ctx,
			`UPDATE session_runtime_status
		    SET status='running', binding_id=$3, binding_generation=$4,
		        running_since=COALESCE(running_since,$5), idle_since=NULL,
		        cleanup_after=NULL, cleanup_enqueued_at=NULL, cleanup_claimed_at=NULL, cleanup_job_id=NULL, updated_at=$5
		  WHERE workspace_id=$1 AND session_id=$2`,
			job.WorkspaceID, job.SessionID, binding.BindingID, binding.BindingGeneration, now,
		)
		if err != nil {
			return err
		}
		if !runtimecontrol.RowsAffected(result) {
			return runtimecontrol.PreparationError{Kind: "runtime_binding_unavailable", Message: "runtime residency is unavailable", Retryable: true}
		}
		plan = RuntimeCommandPlan{
			Target:           RuntimePodTarget{Namespace: binding.Namespace, PodName: binding.PodName, PodUID: binding.PodUID, RuntimeProcessID: binding.RuntimeProcessID, PodIP: binding.PodIP, Port: port},
			AttemptedBinding: RuntimeAttemptedBinding{BindingID: binding.BindingID, Generation: binding.BindingGeneration, TargetPodUID: binding.PodUID, RuntimeProcessID: binding.RuntimeProcessID},
			RecoverThread: &agentruntimev1.RecoverThreadRequest{
				WorkspaceId: job.WorkspaceID, SessionId: job.SessionID, SessionThreadId: job.SessionThreadID,
				BindingId: binding.BindingID, BindingGeneration: binding.BindingGeneration, RuntimeProcessId: binding.RuntimeProcessID, TargetPodUid: binding.PodUID,
				SourceEventId: job.RecoverySourceEventID,
				HandoffId:     job.RecoveryHandoffID,
				RecoveryLeaseRef: &agentruntimev1.RecoveryLeaseRef{
					JobId: job.JobID, LeaseToken: job.LeaseToken,
					PartitionKey: job.PartitionKey, DedupeKey: job.DedupeKey,
				},
			},
		}
		return nil
	})
	if err != nil {
		var confirmation runtimePodConfirmationRequired
		if errors.As(err, &confirmation) {
			resolver, ok := s.TargetResolver.(KubernetesRuntimeTargetResolver)
			if !ok {
				return RuntimeCommandPlan{}, err
			}
			observation, confirmErr := resolver.confirmRuntimePod(ctx, confirmation.binding)
			if confirmErr != nil {
				return RuntimeCommandPlan{}, confirmErr
			}
			return s.activateRuntimeRecovery(context.WithValue(ctx, runtimePodObservationKey{}, observation), job, reentries)
		}
		var sampleRequired runtimePlacementRequiredError
		if errors.As(err, &sampleRequired) {
			resolver, ok := s.TargetResolver.(KubernetesRuntimeTargetResolver)
			if !ok {
				return RuntimeCommandPlan{}, err
			}
			choice, sampleErr := resolver.sampleRuntimePlacement(ctx, s.Client, job)
			if sampleErr != nil {
				return RuntimeCommandPlan{placement: &choice}, sampleErr
			}
			plan, prepareErr := s.activateRuntimeRecovery(context.WithValue(ctx, runtimePlacementContextKey{}, choice), job, reentries)
			plan.placement = &choice
			return plan, prepareErr
		}
	}
	var lost runtimeBindingLostError
	if errors.As(err, &lost) {
		if reentries >= maxRuntimePreparationReentries {
			return RuntimeCommandPlan{}, runtimecontrol.PreparationError{Kind: "runtime_reconcile_invariant", Message: "Runtime recovery failed to converge after loss repair", Retryable: false}
		}
		if repairErr := s.repairLostRuntimeBinding(ctx, job.WorkspaceID, job.SessionID, lost.binding, now); repairErr != nil {
			return RuntimeCommandPlan{}, repairErr
		}
		return s.activateRuntimeRecovery(ctx, job, reentries+1)
	}
	return plan, err
}

func runtimeGenerationFromInputID(runtimeInputID string) (int64, error) {
	value := runtimeInputID[strings.LastIndex(runtimeInputID, ":")+1:]
	generation, err := strconv.ParseInt(value, 10, 64)
	if err != nil || generation <= 0 {
		return 0, runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "runtime config generation is invalid", Retryable: false}
	}
	return generation, nil
}

func effectiveRuntimeInputJobTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (RuntimeJob, error) {
	var inputKind string
	var rejectionReason sql.NullString
	err := tx.QueryRow(ctx,
		`SELECT input_kind, rejection_reason_code
		   FROM session_runtime_inbox
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND runtime_input_id = $3
		  FOR UPDATE`,
		job.WorkspaceID,
		job.SessionID,
		job.RuntimeInputID,
	).Scan(&inputKind, &rejectionReason)
	if dbconnect.IsNoRows(err) {
		return job, nil
	}
	if err != nil {
		return RuntimeJob{}, err
	}
	if inputKind != "rejection" {
		return job, nil
	}
	if !rejectionReason.Valid ||
		(rejectionReason.String != "runtime_command_payload_too_large" &&
			rejectionReason.String != "runtime_command_rejected") {
		return RuntimeJob{}, runtimecontrol.PreparationError{
			Kind:      "runtime_inbox_payload_conflict",
			Message:   "runtime rejection inbox row has an invalid reason code",
			Retryable: false,
		}
	}
	job.InputKind = "rejection"
	job.RejectionReasonCode = rejectionReason.String
	return job, nil
}

func runtimeMCPManifestCommandPayloadTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (string, string, error) {
	// Queue intents carry references only. The manifest row is the complete
	// delivery source; tool policy reaches the pod through its own carrier.
	row, found, err := mcpmanifest.LoadRowForUpdateTx(ctx, tx, job.WorkspaceID, job.SessionID, job.MCPServerName)
	if err != nil {
		return "", "", err
	}
	if !found {
		return "", "", runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "MCP manifest durable row is missing", Retryable: false}
	}
	payloadJSON, err := mcpmanifest.CommandPayload(job.WorkspaceID, job.SessionID, job.MCPServerName, row)
	if err != nil {
		return "", "", err
	}
	return payloadJSON, mcpmanifest.InputID(job.SessionID, job.MCPServerName, row.Generation), nil
}

func runtimeSessionConfigCommandPayloadTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (string, string, error) {
	// Queue intents carry references only. Rebuild from the locked durable
	// session/config rows with the same policy serializer used by cold bootstrap.
	var configGeneration int64
	var approvalMode string
	var installedToolsJSON string
	var agentConfigJSON string
	err := tx.QueryRow(ctx,
		`SELECT s.config_generation, s.approval_mode, s.installed_tools_json, av.config_json
		   FROM sessions s
		   JOIN agent_versions av
		     ON av.workspace_id = s.workspace_id
		    AND av.id = s.agent_version_id
		  WHERE s.workspace_id = $1
		    AND s.id = $2
		  FOR UPDATE OF s`,
		job.WorkspaceID,
		job.SessionID,
	).Scan(&configGeneration, &approvalMode, &installedToolsJSON, &agentConfigJSON)
	if dbconnect.IsNoRows(err) {
		return "", "", runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "runtime config durable row is missing", Retryable: false}
	}
	if err != nil {
		return "", "", err
	}
	memoryStores, err := runtimeconfig.ReadMemoryStoresTx(ctx, tx, string(job.WorkspaceID), job.SessionID)
	if err != nil {
		return "", "", err
	}
	settings, err := runtimeconfig.InterpretSessionSettings(approvalMode, agentConfigJSON, installedToolsJSON, memoryStores)
	if err != nil {
		return "", "", runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "runtime config durable row is invalid", Retryable: false}
	}
	payloadJSON, err := runtimecontrol.MarshalDataJSON(map[string]any{
		"workspace_id":      job.WorkspaceID,
		"session_id":        job.SessionID,
		"config_generation": configGeneration,
		"approval_mode":     approvalMode,
		"system":            settings.System,
		"memory_stores":     settings.MemoryStores,
		"tool_policy":       settings.ToolPolicy,
	})
	if err != nil {
		return "", "", err
	}
	return payloadJSON, runtimeConfigUpdateInputID(job.SessionID, strconv.FormatInt(configGeneration, 10)), nil
}

func acceptedMessageCommandPayloadTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (string, error) {
	if len(job.EventIDs) == 0 || job.SessionThreadID == "" {
		return "", runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "message runtime input is incomplete", Retryable: false}
	}
	messages := make([]json.RawMessage, 0, len(job.EventIDs))
	for _, eventID := range job.EventIDs {
		var eventType string
		var payloadJSON string
		if err := tx.QueryRow(ctx,
			`SELECT type, payload_json
			   FROM session_events
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND session_thread_id = $3
			    AND event_id = $4`,
			job.WorkspaceID, job.SessionID, job.SessionThreadID, eventID,
		).Scan(&eventType, &payloadJSON); dbconnect.IsNoRows(err) {
			return "", runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "message runtime input event is missing", Retryable: false}
		} else if err != nil {
			return "", err
		}
		if eventType != "user.message" {
			return "", runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "message runtime input event type is invalid", Retryable: false}
		}
		messageJSON, err := runtimecontrol.UserMessageContextDraftJSON(payloadJSON)
		if err != nil {
			return "", err
		}
		messages = append(messages, json.RawMessage(messageJSON))
	}
	return runtimecontrol.MarshalDataJSON(map[string]any{"messages": messages})
}

func interruptControlCommandPayloadTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (string, error) {
	eventID, eventType, _, sequence, err := loadSingleRuntimeInputEventTx(ctx, tx, job)
	if err != nil {
		return "", err
	}
	if eventType != "user.interrupt" && eventType != runtimecontrol.ChildInterruptRequestedEventType {
		return "", runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "interrupt control event type is invalid", Retryable: false}
	}
	return runtimecontrol.MarshalJSON(map[string]any{
		"source_event_id":          eventID,
		"interrupt_fence_sequence": sequence,
		"origin":                   map[bool]string{true: "agent", false: "user"}[eventType == runtimecontrol.ChildInterruptRequestedEventType],
		"reason":                   nil,
	})
}

func toolConfirmationCommandPayloadTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (string, error) {
	eventID, eventType, payloadJSON, _, err := loadSingleRuntimeInputEventTx(ctx, tx, job)
	if err != nil {
		return "", err
	}
	if eventType != "user.tool_confirmation" {
		return "", runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "tool confirmation event type is invalid", Retryable: false}
	}
	var payload struct {
		ToolUseID   string  `json:"tool_use_id"`
		Result      string  `json:"result"`
		DenyMessage *string `json:"deny_message"`
	}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil || payload.ToolUseID == "" || (payload.Result != "allow" && payload.Result != "deny") {
		return "", runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "tool confirmation event payload is invalid", Retryable: false}
	}
	command := map[string]any{
		"source_event_id":   eventID,
		"tool_use_event_id": payload.ToolUseID,
		"decision":          payload.Result,
	}
	if payload.Result == "deny" && payload.DenyMessage != nil {
		command["deny_message"] = *payload.DenyMessage
	}
	return runtimecontrol.MarshalJSON(command)
}

func loadSingleRuntimeInputEventTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (string, string, string, int64, error) {
	if len(job.EventIDs) != 1 {
		return "", "", "", 0, runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "control runtime input must reference exactly one event", Retryable: false}
	}
	eventID := job.EventIDs[0]
	var eventType string
	var payloadJSON string
	var sequence int64
	err := tx.QueryRow(ctx,
		`SELECT type, payload_json, sequence
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND event_id = $4`,
		job.WorkspaceID,
		job.SessionID,
		job.SessionThreadID,
		eventID,
	).Scan(&eventType, &payloadJSON, &sequence)
	if dbconnect.IsNoRows(err) {
		return "", "", "", 0, runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "control runtime input event is missing", Retryable: false}
	}
	if err != nil {
		return "", "", "", 0, err
	}
	return eventID, eventType, payloadJSON, sequence, nil
}

type cleanupPendingWait struct {
	ThreadID        string
	ToolUseEventID  string
	ModelRequestID  string
	ModelToolCallID string
	EventType       string
}

type pendingToolTerminal struct {
	ErrorType string
	Message   string
}

func insertPendingToolTerminalResultTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, wait cleanupPendingWait, terminal pendingToolTerminal, now time.Time) (string, error) {
	payloadJSON, eventType, err := pendingToolTerminalPayloadJSON(wait, terminal)
	if err != nil {
		return "", err
	}
	threadScope, err := runtimecontrol.LockThreadMutationTx(ctx, tx, scope)
	if err != nil {
		return "", err
	}
	visibility, sessionVisible := threadScope.PublicProjection(eventType)
	eventID := id.New("evt_")
	sequence, err := runtimecontrol.NextSessionEventSequenceTx(ctx, tx, scope)
	if err != nil {
		return "", err
	}
	toolUse := runtimecontrol.OrphanToolUse{
		SessionThreadID: wait.ThreadID, EventID: wait.ToolUseEventID,
		EventType:       wait.EventType,
		ModelRequestID:  wait.ModelRequestID,
		ModelToolCallID: wait.ModelToolCallID,
	}
	terminalResult := runtimecontrol.TerminalToolResult{
		ErrorType: terminal.ErrorType,
		Message:   terminal.Message,
		Retryable: false,
	}
	projection, err := runtimecontrol.SettleRuntimeTerminalToolPartTx(ctx, tx, scope, toolUse, terminalResult, now)
	if err != nil {
		return "", err
	}
	projectionJSON, err := runtimecontrol.MarshalJSON(projection)
	if err != nil {
		return "", err
	}
	if _, err := sessioneventwrite.InsertInitialTx(ctx, tx, sessioneventwrite.InitialEvent{
		WorkspaceID: scope.GetWorkspaceId(), SessionID: scope.GetSessionId(), SessionThreadID: scope.GetSessionThreadId(),
		EventID: eventID, Sequence: sequence, Type: eventType,
		PayloadJSON: payloadJSON, ProjectionJSON: projectionJSON, Visibility: visibility, SessionVisible: sessionVisible,
		ModelRequestID: wait.ModelRequestID, ToolUseEventID: wait.ToolUseEventID, CreatedAt: now, ProcessedAt: &now,
	}); err != nil {
		return "", runtimecontrol.ToolRelationInsertError(err)
	}
	return eventID, nil
}

func pendingToolTerminalPayloadJSON(wait cleanupPendingWait, terminal pendingToolTerminal) (string, string, error) {
	eventType := "agent.tool_result"
	toolUseField := "tool_use_id"
	if wait.EventType == "agent.mcp_tool_use" {
		eventType = "agent.mcp_tool_result"
		toolUseField = "mcp_tool_use_id"
	}
	payload, err := runtimecontrol.MarshalJSON(map[string]any{
		"type":       eventType,
		toolUseField: wait.ToolUseEventID,
		"is_error":   true,
		"content": []map[string]any{
			{
				"type": "text",
				"text": terminal.Message,
			},
		},
	})
	return payload, eventType, err
}

func (s *PostgreSQLRuntimeDeliveryStore) prepareAgentMailCommandTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	job RuntimeJob,
	port int,
	now time.Time,
) (RuntimeCommandPlan, error) {
	var sessionStatus string
	var lifecycleState string
	if err := tx.QueryRow(ctx,
		`SELECT status, lifecycle_state
		   FROM sessions
		  WHERE workspace_id=$1 AND id=$2
		  FOR UPDATE`,
		job.WorkspaceID,
		job.SessionID,
	).Scan(&sessionStatus, &lifecycleState); dbconnect.IsNoRows(err) {
		return RuntimeCommandPlan{StaleAccepted: true}, nil
	} else if err != nil {
		return RuntimeCommandPlan{}, err
	}
	if lifecycleState == "deleted" || sessionStatus == "terminated" {
		return RuntimeCommandPlan{StaleAccepted: true}, nil
	}
	var recipientStatus string
	if err := tx.QueryRow(ctx,
		`SELECT status
		   FROM session_threads
		  WHERE workspace_id=$1 AND session_id=$2 AND id=$3
		  FOR UPDATE`,
		job.WorkspaceID,
		job.SessionID,
		job.SessionThreadID,
	).Scan(&recipientStatus); dbconnect.IsNoRows(err) {
		return RuntimeCommandPlan{StaleAccepted: true}, nil
	} else if err != nil {
		return RuntimeCommandPlan{}, err
	}
	if recipientStatus == "closed_for_runtime" || recipientStatus == "terminated" {
		return RuntimeCommandPlan{StaleAccepted: true}, nil
	}
	if job.JobID == "" || job.LeaseToken == "" || job.PartitionKey == "" || job.DedupeKey == "" ||
		job.PartitionKey != queue.FormatSessionPartitionKey(workspace.ID(job.WorkspaceID), job.SessionID) ||
		job.DedupeKey != queue.FormatRuntimeInputDedupeKey(workspace.ID(job.WorkspaceID), job.SessionID, job.RuntimeInputID) {
		return RuntimeCommandPlan{}, runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "agent mail Queue identity is incomplete", Retryable: false}
	}
	active, err := queue.AssertExactLeaseTx(ctx, tx, queue.ExactLeaseRequest{
		WorkspaceID: workspace.ID(job.WorkspaceID), JobID: job.JobID, LeaseToken: job.LeaseToken,
		Kind: job.Kind, PartitionKey: job.PartitionKey, DedupeKey: job.DedupeKey,
	})
	if err != nil {
		return RuntimeCommandPlan{}, err
	}
	if !active {
		return RuntimeCommandPlan{StaleAccepted: true, DeliveryAuthorityLost: true}, nil
	}
	var inboxStatus string
	if err := tx.QueryRow(ctx, `SELECT status FROM session_runtime_inbox
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
		  AND runtime_input_id=$4 AND input_kind='agent_mail'
		FOR UPDATE`, job.WorkspaceID, job.SessionID, job.SessionThreadID, job.RuntimeInputID).Scan(&inboxStatus); dbconnect.IsNoRows(err) {
		return RuntimeCommandPlan{}, runtimecontrol.PreparationError{Kind: "runtime_inbox_custody_missing", Message: "agent mail has no producer custody", Retryable: false}
	} else if err != nil {
		return RuntimeCommandPlan{}, err
	}
	binding, err := s.resolveRuntimeTarget(ctx, tx, job)
	if err != nil {
		return RuntimeCommandPlan{}, err
	}
	deliveryID := strings.TrimPrefix(job.RuntimeInputID, "agent_mail:")
	if deliveryID == "" || deliveryID == job.RuntimeInputID {
		return RuntimeCommandPlan{}, runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "agent mail runtime input id is invalid", Retryable: false}
	}
	envelope, err := runtimecontrol.LoadStoredAgentMailEnvelopeByDeliveryTx(ctx, tx, job.WorkspaceID, job.SessionID, deliveryID)
	if err != nil {
		return RuntimeCommandPlan{}, err
	}
	if envelope.TargetThreadID != job.SessionThreadID {
		return RuntimeCommandPlan{}, runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "agent mail queue target does not match the stored envelope", Retryable: false}
	}
	_, err = runtimecontrol.AdmitAgentMailDeliveryTx(
		ctx,
		tx,
		runtimeScopeForDeliveryJob(job, binding),
		envelope,
		binding,
		now,
	)
	if err != nil {
		return RuntimeCommandPlan{}, err
	}
	return RuntimeCommandPlan{
		Target: RuntimePodTarget{
			Namespace:        binding.Namespace,
			PodName:          binding.PodName,
			PodUID:           binding.PodUID,
			RuntimeProcessID: binding.RuntimeProcessID,
			PodIP:            binding.PodIP,
			Port:             port,
		},
		AttemptedBinding: RuntimeAttemptedBinding{BindingID: binding.BindingID, Generation: binding.BindingGeneration, TargetPodUID: binding.PodUID, RuntimeProcessID: binding.RuntimeProcessID},
		AcceptAgentMail: &agentruntimev1.AcceptAgentMailRequest{
			WorkspaceId: job.WorkspaceID, SessionId: job.SessionID, SessionThreadId: job.SessionThreadID,
			BindingId: binding.BindingID, BindingGeneration: binding.BindingGeneration, RuntimeProcessId: binding.RuntimeProcessID, TargetPodUid: binding.PodUID,
			RuntimeInputId: job.RuntimeInputID, DeliveryId: envelope.DeliveryID, Content: envelope.Content,
		},
	}, nil
}

func (s *PostgreSQLRuntimeDeliveryStore) prepareTaskNotificationCommandTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob, port int, now time.Time) (*RuntimeTaskNotificationPlan, RuntimeCommandPlan, error) {
	if err := runtimecontrol.LockRuntimeMutationSessionTx(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
		return nil, RuntimeCommandPlan{}, err
	}
	taskID := runtimecontrol.TaskNotificationTaskID(job.RuntimeInputID)
	if taskID == "" {
		return nil, RuntimeCommandPlan{}, runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "task notification runtime input id must identify a task", Retryable: false}
	}
	var sessionThreadID, sourceToolUseEventID, taskStatus string
	var terminalResultJSON, terminalEventID sql.NullString
	err := tx.QueryRow(ctx, `SELECT session_thread_id, source_tool_use_event_id, status,
		terminal_result_json, terminal_event_id
		FROM session_background_tasks
		WHERE workspace_id=$1 AND session_id=$2 AND task_id=$3
		FOR UPDATE`, job.WorkspaceID, job.SessionID, taskID).Scan(
		&sessionThreadID, &sourceToolUseEventID, &taskStatus, &terminalResultJSON, &terminalEventID,
	)
	if dbconnect.IsNoRows(err) {
		return nil, RuntimeCommandPlan{StaleAccepted: true}, nil
	}
	if err != nil {
		return nil, RuntimeCommandPlan{}, err
	}
	if sessionThreadID != job.SessionThreadID || sourceToolUseEventID == "" {
		return nil, RuntimeCommandPlan{}, runtimecontrol.PreparationError{Kind: "task_notification_identity_invalid", Message: "task notification durable identity is invalid", Retryable: false}
	}
	if terminalEventID.Valid {
		return nil, RuntimeCommandPlan{StaleAccepted: true}, nil
	}
	if taskStatus == "running" {
		return nil, RuntimeCommandPlan{}, runtimecontrol.PreparationError{Kind: "task_notification_not_terminal", Message: "task notification result is not terminal", Retryable: true}
	}
	if !terminalResultJSON.Valid || terminalResultJSON.String == "" || !json.Valid([]byte(terminalResultJSON.String)) || !runtimecontrol.ValidBackgroundTaskTerminalStatus(taskStatus) {
		return nil, RuntimeCommandPlan{}, runtimecontrol.PreparationError{Kind: "task_notification_result_invalid", Message: "task notification durable result is invalid", Retryable: false}
	}
	closing, err := childcontrol.ThreadOrAncestorClosingTx(ctx, tx, job.WorkspaceID, job.SessionID, job.SessionThreadID)
	if err != nil {
		return nil, RuntimeCommandPlan{}, err
	}
	if closing {
		settled, err := deferLeasedTaskNotificationTx(ctx, tx, job, now)
		if err != nil {
			return nil, RuntimeCommandPlan{}, err
		}
		return nil, RuntimeCommandPlan{SettledAccepted: true, QueueLeaseSettled: settled}, nil
	}
	if err := requireInitialMCPManifestReadyTx(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
		return nil, RuntimeCommandPlan{}, err
	}
	binding, err := s.resolveRuntimeTarget(ctx, tx, job)
	if err != nil {
		var prepareErr runtimecontrol.PreparationError
		if errors.As(err, &prepareErr) && prepareErr.Kind == "runtime_binding_unavailable" {
			return nil, RuntimeCommandPlan{StaleAccepted: true}, nil
		}
		return nil, RuntimeCommandPlan{}, err
	}
	payloadJSON, err := runtimeTaskNotificationPayloadJSON(&RuntimeTaskNotificationPlan{
		TaskID: taskID, SourceToolUseEventID: sourceToolUseEventID,
	}, taskStatus, terminalResultJSON.String)
	if err != nil {
		return nil, RuntimeCommandPlan{}, err
	}
	if err := claimRuntimeInboxDeliveryTx(ctx, tx, job, binding, now); err != nil {
		return nil, RuntimeCommandPlan{}, err
	}
	return &RuntimeTaskNotificationPlan{
			TaskID:               taskID,
			SourceToolUseEventID: sourceToolUseEventID,
			ResultJSON:           payloadJSON,
		}, RuntimeCommandPlan{
			Target: RuntimePodTarget{
				Namespace:        binding.Namespace,
				PodName:          binding.PodName,
				PodUID:           binding.PodUID,
				RuntimeProcessID: binding.RuntimeProcessID,
				PodIP:            binding.PodIP,
				Port:             port,
			},
			AttemptedBinding: RuntimeAttemptedBinding{BindingID: binding.BindingID, Generation: binding.BindingGeneration, TargetPodUID: binding.PodUID, RuntimeProcessID: binding.RuntimeProcessID},
			AcceptTask: &agentruntimev1.AcceptTaskNotificationRequest{
				WorkspaceId: job.WorkspaceID, SessionId: job.SessionID, SessionThreadId: job.SessionThreadID,
				BindingId: binding.BindingID, BindingGeneration: binding.BindingGeneration, RuntimeProcessId: binding.RuntimeProcessID, TargetPodUid: binding.PodUID,
				RuntimeInputId: job.RuntimeInputID, InputOrder: job.SequenceTo, NotificationJson: payloadJSON,
			},
		}, nil
}

// settleCurrentBindingAcceptedRuntimeInputTx consumes a reclaimed Queue lease
// without invoking Runtime again when the durable current binding already owns
// the accepted input. It deliberately does not consult Kubernetes availability:
// only the separate proven-loss transaction may transfer this custody.
func settleCurrentBindingAcceptedRuntimeInputTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	job RuntimeJob,
	now time.Time,
) (bool, error) {
	var accepted bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (
		    SELECT 1
		      FROM session_runtime_inbox inbox
		      JOIN session_runtime_bindings binding
		        ON binding.workspace_id = inbox.workspace_id
		       AND binding.session_id = inbox.session_id
		       AND binding.binding_id = inbox.binding_id
		       AND binding.binding_generation = inbox.binding_generation
		       AND binding.agent_runtime_pod_uid = inbox.target_pod_uid
		     WHERE inbox.workspace_id = $1
		       AND inbox.session_id = $2
		       AND inbox.runtime_input_id = $3
		       AND inbox.status = 'accepted'
		)`,
		job.WorkspaceID,
		job.SessionID,
		job.RuntimeInputID,
	).Scan(&accepted); err != nil {
		return false, err
	}
	if !accepted {
		return false, nil
	}
	acked, err := queue.AckTx(ctx, tx, queue.AckRequest{
		WorkspaceID: workspace.ID(job.WorkspaceID),
		JobID:       job.JobID,
		LeaseToken:  job.LeaseToken,
		Now:         now,
	})
	if err != nil {
		return false, err
	}
	if !acked {
		return false, runtimecontrol.PreparationError{Kind: "runtime_queue_lease_stale", Message: "runtime input queue lease is stale", Retryable: true}
	}
	return true, nil
}

// deferLeasedTaskNotificationTx transfers ownership of a leased notification
// from Queue to the dormant Runtime Inbox record. The caller holds the Session
// mutation lock, so the close fence and inbox transition share one winner with
// notification commit and resume.
func deferLeasedTaskNotificationTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob, now time.Time) (bool, error) {
	binding, err := runtimecontrol.ReadRuntimeBindingForDeliveryTx(ctx, tx, job.WorkspaceID, job.SessionID)
	if err != nil {
		return false, err
	}
	parked, err := runtimecontrol.ParkTaskNotificationInboxTx(ctx, tx, job.WorkspaceID, job.SessionID, job.SessionThreadID, job.RuntimeInputID, binding, now)
	if err != nil {
		return false, err
	}
	if !parked {
		return false, runtimecontrol.PreparationError{Kind: "task_notification_defer_missing", Message: "task notification inbox is not deferrable", Retryable: true}
	}
	updated, err := queue.AckTx(ctx, tx, queue.AckRequest{
		WorkspaceID: workspace.ID(job.WorkspaceID), JobID: job.JobID, LeaseToken: job.LeaseToken, Now: now,
	})
	if err != nil {
		return false, err
	}
	if !updated {
		return false, runtimecontrol.PreparationError{Kind: "queue_authority_lost", Message: "task notification Queue lease is no longer owned", Retryable: true}
	}
	return true, nil
}

func runtimeScopeForDeliveryJob(job RuntimeJob, binding runtimecontrol.Binding) *bridgev1.RuntimeScope {
	return &bridgev1.RuntimeScope{
		WorkspaceId:     job.WorkspaceID,
		SessionId:       job.SessionID,
		SessionThreadId: job.SessionThreadID,
		Binding: &bridgev1.RuntimeBindingRef{
			BindingId:         binding.BindingID,
			BindingGeneration: binding.BindingGeneration,
			TargetPodUid:      binding.PodUID,
			RuntimeProcessId:  binding.RuntimeProcessID,
		},
	}
}

func runtimeScopeFromAttempt(job RuntimeJob, attempt RuntimeAttemptedBinding) *bridgev1.RuntimeScope {
	return &bridgev1.RuntimeScope{
		WorkspaceId:     job.WorkspaceID,
		SessionId:       job.SessionID,
		SessionThreadId: job.SessionThreadID,
		Binding: &bridgev1.RuntimeBindingRef{
			BindingId:         attempt.BindingID,
			BindingGeneration: attempt.Generation,
			TargetPodUid:      attempt.TargetPodUID,
			RuntimeProcessId:  attempt.RuntimeProcessID,
		},
	}
}

func (s *PostgreSQLRuntimeDeliveryStore) logRuntimePlacement(job RuntimeJob, plan RuntimeCommandPlan, err error, started time.Time) {
	if s == nil || s.Logger == nil {
		return
	}
	if plan.placement == nil && plan.Target.PodUID == "" {
		return
	}
	outcome := "reused"
	if err != nil {
		outcome = "failed"
	} else if plan.placement != nil {
		outcome = "committed"
		if plan.Target.PodUID != plan.placement.Candidate.PodUID || plan.Target.RuntimeProcessID != plan.placement.ProcessID {
			outcome = "concurrent_binding_reused"
		}
	}
	attrs := []any{slog.String("component", ServiceNameJobRunner), slog.String("event.kind", "runtime_placement"), slog.String("workspace.id", job.WorkspaceID), slog.String("session.id", job.SessionID), slog.String("job.id", job.JobID), slog.String("outcome", outcome), slog.String("kubernetes.uid", plan.Target.PodUID), slog.String("runtime.process.id", plan.Target.RuntimeProcessID), slog.Int64("duration.ms", time.Since(started).Milliseconds())}
	if plan.placement != nil {
		choice := plan.placement
		attrs = append(attrs, slog.String("reason", choice.Reason))
		for index, observation := range choice.Observations {
			if index >= 4 {
				break
			}
			fields := []any{slog.String("kubernetes.uid", observation.Candidate.PodUID), slog.String("runtime.process.id", observation.ProcessID), slog.String("reason", observation.Reason)}
			if observation.Report.Capacity > 0 && observation.Report.MemoryLimit > 0 {
				fields = append(fields, slog.Float64("runtime.load.active_sessions", observation.Report.ActiveSessions), slog.Float64("runtime.load.session_capacity", observation.Report.Capacity), slog.Float64("runtime.load.memory_ratio", observation.Report.MemoryUsage/observation.Report.MemoryLimit))
			}
			attrs = append(attrs, slog.Group(fmt.Sprintf("runtime.placement.candidate.%d", index+1), fields...))
		}
		attrs = append(attrs, slog.Int("runtime.placement.rounds", choice.Rounds), slog.Int("runtime.placement.probes", choice.Probes), slog.String("runtime.placement.sampled_pod_uid", choice.Candidate.PodUID))
		if choice.Report.MemoryLimit > 0 {
			attrs = append(attrs, slog.Float64("runtime.load.active_sessions", choice.Report.ActiveSessions), slog.Float64("runtime.load.session_capacity", choice.Report.Capacity), slog.Float64("runtime.load.memory_ratio", choice.Report.MemoryUsage/choice.Report.MemoryLimit))
		}
	}
	if err != nil {
		kind := "runtime_placement_unavailable"
		var preparation runtimecontrol.PreparationError
		if errors.As(err, &preparation) {
			kind = preparation.Kind
		}
		attrs = append(attrs, slog.String("error.class", "runtime_placement"), slog.String("error.code", kind), slog.Bool("retryable", true))
	}
	s.Logger.Info("runtime_placement", attrs...)
}

type KubernetesRuntimeTargetResolver struct {
	Snapshot         func() enginekubernetes.BindingVisibilitySnapshot
	Clock            func() time.Time
	PlacementMetrics *RuntimePlacementMetrics
	PlacementPolicy  RuntimePlacementPolicy
	LoadClient       *http.Client
	RandomIndex      func(int) int
	GetPod           func(context.Context, string, string) (*enginekubernetes.PodObservation, error)
	ProcessPolicy    runtimecontrol.ProcessPolicy
}

func (r KubernetesRuntimeTargetResolver) BindingVisibilitySnapshot() enginekubernetes.BindingVisibilitySnapshot {
	if r.Snapshot == nil {
		return enginekubernetes.BindingVisibilitySnapshot{}
	}
	return r.Snapshot()
}

type runtimeBindingLostError struct {
	binding runtimecontrol.Binding
}

func (e runtimeBindingLostError) Error() string { return "runtime binding target is gone" }

// ResolveRuntimeTarget shares the process-aware classifier with repair discovery and cleanup.
func (r KubernetesRuntimeTargetResolver) ResolveRuntimeTarget(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (runtimecontrol.Binding, error) {
	if r.Snapshot == nil {
		return runtimecontrol.Binding{}, runtimecontrol.PreparationError{Kind: "runtime_visibility_unavailable", Message: "runtime visibility snapshot is unavailable", Retryable: true}
	}
	snapshot := r.Snapshot()
	if !snapshot.Ready {
		return runtimecontrol.Binding{}, runtimecontrol.PreparationError{Kind: "runtime_visibility_not_ready", Message: "runtime pod visibility is not ready", Retryable: true}
	}
	current, found, err := runtimecontrol.ReadOptionalRuntimeBindingForDeliveryTx(ctx, tx, job.WorkspaceID, job.SessionID)
	if err != nil {
		return runtimecontrol.Binding{}, err
	}
	if found {
		decision, err := r.runtimeProcessDecisionTx(ctx, tx, current)
		if err != nil {
			return runtimecontrol.Binding{}, err
		}
		switch decision {
		case runtimeProcessReuse:
			return current, nil
		case runtimeProcessLoss:
			return runtimecontrol.Binding{}, runtimeBindingLostError{binding: current}
		default:
			return runtimecontrol.Binding{}, runtimecontrol.PreparationError{Kind: "runtime_binding_not_available", Message: "Runtime process does not admit delivery", Retryable: true}
		}
	}
	choice, ok := ctx.Value(runtimePlacementContextKey{}).(runtimePlacementChoice)
	if !ok || choice.WorkspaceID != job.WorkspaceID || choice.SessionID != job.SessionID {
		return runtimecontrol.Binding{}, runtimePlacementRequiredError{}
	}
	candidate := choice.Candidate
	if snapshot.VisibilityFor(enginekubernetes.BoundRuntimePod(candidate)) != enginekubernetes.BindingVisibilityReusable {
		return runtimecontrol.Binding{}, runtimecontrol.PreparationError{Kind: "runtime_binding_candidate_unavailable", Message: "sampled Runtime candidate changed before commit", Retryable: true}
	}
	processID := choice.ProcessID
	process, err := runtimecontrol.RequireCurrentProcessTx(ctx, tx, runtimecontrol.ProcessIdentity{Namespace: candidate.Namespace, PodUID: candidate.PodUID, ID: processID})
	if err != nil {
		return runtimecontrol.Binding{}, err
	}
	if process.Phase != runtimecontrol.ProcessAccepting {
		return runtimecontrol.Binding{}, runtimecontrol.PreparationError{Kind: "runtime_binding_candidate_unavailable", Message: "sampled Runtime candidate is draining", Retryable: true}
	}
	now := storage.Now()
	if r.Clock != nil {
		now = r.Clock().UTC()
	}
	var generation int64
	if err := tx.QueryRow(ctx, `SELECT nextval('session_runtime_binding_generation_seq')`).Scan(&generation); err != nil {
		return runtimecontrol.Binding{}, err
	}
	binding := runtimecontrol.Binding{
		BindingID:         id.New("bind_"),
		BindingGeneration: generation,
		Namespace:         candidate.Namespace,
		PodName:           candidate.PodName,
		PodUID:            candidate.PodUID,
		PodIP:             candidate.PodIP,
		RuntimeProcessID:  processID,
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO session_runtime_bindings (
			workspace_id, session_id, binding_id, binding_generation, agent_runtime_namespace,
			agent_runtime_pod_name, agent_runtime_pod_uid, agent_runtime_pod_ip, runtime_process_id, bound_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10)
		ON CONFLICT (workspace_id, session_id) DO UPDATE SET
			binding_id = EXCLUDED.binding_id,
			binding_generation = EXCLUDED.binding_generation,
			agent_runtime_namespace = EXCLUDED.agent_runtime_namespace,
			agent_runtime_pod_name = EXCLUDED.agent_runtime_pod_name,
			agent_runtime_pod_uid = EXCLUDED.agent_runtime_pod_uid,
			agent_runtime_pod_ip = EXCLUDED.agent_runtime_pod_ip,
 runtime_process_id=EXCLUDED.runtime_process_id,
			bound_at = EXCLUDED.bound_at,
			updated_at = EXCLUDED.updated_at`,
		job.WorkspaceID,
		job.SessionID,
		binding.BindingID,
		binding.BindingGeneration,
		binding.Namespace,
		binding.PodName,
		binding.PodUID,
		binding.PodIP,
		binding.RuntimeProcessID,
		now,
	)
	if err != nil {
		return runtimecontrol.Binding{}, err
	}
	return binding, nil
}

func markRuntimeInputEventsProcessedByIDTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, eventIDs []string, now time.Time) error {
	for _, eventID := range eventIDs {
		revision, revised, err := sessioneventwrite.RecordProcessedRevisionTx(ctx, tx, sessioneventwrite.ProcessedRevision{
			WorkspaceID: workspaceID, SessionID: sessionID, EventID: eventID, ProcessedAt: now,
		})
		if err != nil {
			return err
		}
		if !revised {
			alreadyProcessed, exists, err := sessionEventProcessedStateByIDTx(ctx, tx, workspaceID, sessionID, eventID)
			if err != nil {
				return err
			}
			if !exists {
				return runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "runtime input event is not settleable", Retryable: false}
			}
			if alreadyProcessed {
				continue
			}
			return runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "runtime input event is not settleable", Retryable: false}
		}
		if revision.SessionThreadID == "" {
			return runtimecontrol.PreparationError{Kind: "invalid_runtime_job_payload", Message: "runtime input event thread is unavailable", Retryable: false}
		}
	}
	return nil
}

func sessionEventProcessedStateByIDTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, eventID string) (bool, bool, error) {
	var processedAt sql.NullTime
	err := tx.QueryRow(ctx,
		`SELECT processed_at
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND event_id = $3`,
		workspaceID,
		sessionID,
		eventID,
	).Scan(&processedAt)
	if dbconnect.IsNoRows(err) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	return processedAt.Valid, true, nil
}

func readRuntimeCommandSessionThreadIDTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string) (string, error) {
	row := tx.QueryRow(ctx,
		`SELECT id
		   FROM session_threads
		  WHERE workspace_id = $1 AND session_id = $2
		  ORDER BY created_at ASC, id ASC
		  LIMIT 1`,
		workspaceID,
		sessionID,
	)
	var sessionThreadID string
	if err := row.Scan(&sessionThreadID); dbconnect.IsNoRows(err) {
		return "", runtimecontrol.PreparationError{Kind: "runtime_thread_unavailable", Message: "runtime command session thread is unavailable", Retryable: true}
	} else if err != nil {
		return "", err
	}
	return sessionThreadID, nil
}

func allRuntimeInputEventsProcessedTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (bool, error) {
	if len(job.EventIDs) == 0 {
		return false, nil
	}
	placeholders := make([]string, 0, len(job.EventIDs))
	args := []any{job.WorkspaceID, job.SessionID}
	for index, eventID := range job.EventIDs {
		placeholders = append(placeholders, "$"+strconv.Itoa(index+3))
		args = append(args, eventID)
	}
	row := tx.QueryRow(ctx,
		`SELECT count(*), count(processed_at)
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND event_id IN (`+strings.Join(placeholders, ", ")+`)`,
		args...,
	)
	var total int
	var processed int
	if err := row.Scan(&total, &processed); err != nil {
		return false, err
	}
	return total == len(job.EventIDs) && processed == len(job.EventIDs), nil
}

// claimRuntimeInboxDeliveryTx binds producer-created Inbox custody to the
// selected Runtime. session_runtime_inbox is never reconstructed from a Queue
// payload; LoadContext also never projects it into context.
//
//	status         meaning                                writer
//	queued         source fact and Queue custody committed  input producer
//	delivering     existing custody bound before send       this claim
//	accepted       execution custody durably witnessed       MarkRuntimeInputAccepted
//	committed      inputs durably committed                 CommitInputs, the
//	                                                         task_notification commit
//	                                                         (CommitTaskNotificationResult)
//	cancelled      superseded / retracted                   interrupt fence
//	dead_lettered  invariant/exhaustion terminal            finalization
//
// An accepted row stays owned without an active Queue job; only exact
// binding-loss reconciliation may hand it back to Queue custody.
func claimRuntimeInboxDeliveryTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob, binding runtimecontrol.Binding, now time.Time) error {
	eventIDs := job.EventIDs
	if eventIDs == nil {
		eventIDs = []string{}
	}
	events, err := json.Marshal(eventIDs)
	if err != nil {
		return err
	}
	result, err := tx.Exec(ctx,
		`UPDATE session_runtime_inbox
		    SET status='delivering',binding_id=$10,binding_generation=$11,target_pod_uid=$12,updated_at=$13
		  WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND runtime_input_id=$4
		    AND input_kind=$5 AND rejection_reason_code IS NOT DISTINCT FROM $6
		    AND event_ids_json=$7 AND sequence_from IS NOT DISTINCT FROM $8 AND sequence_to IS NOT DISTINCT FROM $9
		    AND (
		      status='queued'
		      OR (status='delivering' AND binding_id=$10 AND binding_generation=$11 AND target_pod_uid=$12)
		    )`,
		job.WorkspaceID,
		job.SessionID,
		job.SessionThreadID,
		job.RuntimeInputID,
		job.InputKind,
		sql.NullString{String: job.RejectionReasonCode, Valid: job.RejectionReasonCode != ""},
		string(events),
		sql.NullInt64{Int64: job.SequenceFrom, Valid: job.SequenceFrom > 0},
		sql.NullInt64{Int64: job.SequenceTo, Valid: job.SequenceTo > 0},
		binding.BindingID,
		binding.BindingGeneration,
		binding.PodUID,
		now,
	)
	if err != nil {
		return err
	}
	if !runtimecontrol.RowsAffected(result) {
		var statusValue string
		var payloadMatches, bindingMatches bool
		if err := tx.QueryRow(ctx, `SELECT status,
			input_kind=$3
			  AND rejection_reason_code IS NOT DISTINCT FROM $4
			  AND event_ids_json=$5
			  AND sequence_from IS NOT DISTINCT FROM $6
			  AND sequence_to IS NOT DISTINCT FROM $7,
			COALESCE(binding_id=$8 AND binding_generation=$9 AND target_pod_uid=$10, false)
			FROM session_runtime_inbox
			WHERE workspace_id=$1 AND runtime_input_id=$2`,
			job.WorkspaceID,
			job.RuntimeInputID,
			job.InputKind,
			sql.NullString{String: job.RejectionReasonCode, Valid: job.RejectionReasonCode != ""},
			string(events),
			sql.NullInt64{Int64: job.SequenceFrom, Valid: job.SequenceFrom > 0},
			sql.NullInt64{Int64: job.SequenceTo, Valid: job.SequenceTo > 0},
			binding.BindingID,
			binding.BindingGeneration,
			binding.PodUID,
		).Scan(&statusValue, &payloadMatches, &bindingMatches); dbconnect.IsNoRows(err) {
			return runtimecontrol.PreparationError{Kind: "runtime_inbox_custody_invalid", Message: "runtime input has no producer custody", Retryable: false}
		} else if err != nil {
			return err
		}
		if job.InputKind == "interrupt_control" && statusValue == "delivering" && payloadMatches && !bindingMatches {
			return runtimecontrol.PreparationError{Kind: "runtime_inbox_binding_changed", Message: "runtime input binding changed during delivery", Retryable: true}
		}
		return runtimecontrol.PreparationError{Kind: "runtime_inbox_payload_conflict", Message: "runtime input replay conflicts with producer custody", Retryable: false}
	}
	return nil
}

type runtimeCommandChannel struct {
	connection *grpc.ClientConn
	active     int
	retired    bool
}

type RuntimePodCommandClient struct {
	TokenSource internalgrpcauth.TokenSource
	Policy      RuntimeCommandPolicy
	DialOptions []grpc.DialOption
	mutex       sync.Mutex
	channels    map[string]*runtimeCommandChannel
	retired     map[*runtimeCommandChannel]bool
	closed      bool
}

func NewRuntimePodCommandClient(tokenSource internalgrpcauth.TokenSource, dialOptions ...grpc.DialOption) *RuntimePodCommandClient {
	return &RuntimePodCommandClient{TokenSource: tokenSource, DialOptions: append([]grpc.DialOption(nil), dialOptions...)}
}

// Close is called after all command users join; a closed owner cannot create
// another connection. Channels retain native TLS generation reload behavior.
func (c *RuntimePodCommandClient) Close() error {
	if c == nil {
		return nil
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	var failures []error
	for _, channel := range c.channels {
		failures = append(failures, channel.connection.Close())
	}
	for channel := range c.retired {
		failures = append(failures, channel.connection.Close())
	}
	c.retired = nil
	c.channels = nil
	return errors.Join(failures...)
}

// RetireChannels withdraws the old trust generation from future admission.
// Admitted RPCs retain their own deadlines and are never replayed or interrupted
// solely because trust changed. Their final release closes the retired channel.
func (c *RuntimePodCommandClient) RetireChannels() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for address, channel := range c.channels {
		delete(c.channels, address)
		channel.retired = true
		if channel.active == 0 {
			_ = channel.connection.Close()
		} else {
			if c.retired == nil {
				c.retired = make(map[*runtimeCommandChannel]bool)
			}
			c.retired[channel] = true
		}
	}
}
func (c *RuntimePodCommandClient) channel(target RuntimePodTarget) (*runtimeCommandChannel, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.closed {
		return nil, errors.New("runtime command client is closed")
	}
	address := "passthrough:///" + net.JoinHostPort(target.PodIP, strconv.Itoa(target.Port))
	if channel := c.channels[address]; channel != nil {
		channel.active++
		return channel, nil
	}
	options := append([]grpc.DialOption{}, internalgrpc.RuntimeCommandRPCDialOptions()...)
	options = append(options, grpc.WithDisableRetry())
	if len(c.DialOptions) == 0 {
		options = append(options, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		options = append(options, c.DialOptions...)
	}
	options = append(options, grpc.WithPerRPCCredentials(internalgrpcauth.NewServiceAccountTokenCredentials(c.TokenSource)))
	conn, err := grpc.NewClient(address, options...)
	if err != nil {
		return nil, err
	}
	channel := &runtimeCommandChannel{connection: conn, active: 1}
	if c.channels == nil {
		c.channels = make(map[string]*runtimeCommandChannel)
	}
	c.channels[address] = channel
	return channel, nil
}
func (c *RuntimePodCommandClient) releaseChannel(channel *runtimeCommandChannel) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	channel.active--
	if channel.active == 0 && channel.retired {
		_ = channel.connection.Close()
		delete(c.retired, channel)
	}
}

func runtimePodCall[Request proto.Message, Response any](ctx context.Context, c *RuntimePodCommandClient, target RuntimePodTarget, request Request, invoke func(agentruntimev1.AgentRuntimePodServiceClient, context.Context, Request) (Response, error)) (Response, error) {
	var zero Response
	if c == nil || c.TokenSource == nil {
		return zero, errors.New("runtime pod command client is required")
	}
	if proto.Size(request) > sessionrpc.MaxRuntimeCommandGRPCMessageBytes {
		return zero, &runtimeCommandPayloadTooLargeError{}
	}
	if target.PodIP == "" || target.Port <= 0 {
		return zero, errors.New("runtime pod target is required")
	}
	if _, err := netip.ParseAddr(target.PodIP); err != nil {
		return zero, errors.New("runtime pod target ip is invalid")
	}
	method := strings.TrimSuffix(string(request.ProtoReflect().Descriptor().Name()), "Request")
	timeout, err := c.Policy.timeout(method)
	if err != nil {
		return zero, err
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, err := c.channel(target)
	if err != nil {
		return zero, err
	}
	defer c.releaseChannel(conn)
	client := agentruntimev1.NewAgentRuntimePodServiceClient(conn.connection)
	return invoke(client, ctx, request)
}

func (c *RuntimePodCommandClient) AcceptInput(ctx context.Context, target RuntimePodTarget, request *agentruntimev1.AcceptInputRequest) (*agentruntimev1.AcceptInputResponse, error) {
	return runtimePodCall(ctx, c, target, request, func(client agentruntimev1.AgentRuntimePodServiceClient, ctx context.Context, request *agentruntimev1.AcceptInputRequest) (*agentruntimev1.AcceptInputResponse, error) {
		return client.AcceptInput(ctx, request)
	})
}
func (c *RuntimePodCommandClient) RecoverThread(ctx context.Context, target RuntimePodTarget, request *agentruntimev1.RecoverThreadRequest) (*agentruntimev1.RecoverThreadResponse, error) {
	return runtimePodCall(ctx, c, target, request, func(client agentruntimev1.AgentRuntimePodServiceClient, ctx context.Context, request *agentruntimev1.RecoverThreadRequest) (*agentruntimev1.RecoverThreadResponse, error) {
		return client.RecoverThread(ctx, request)
	})
}
func (c *RuntimePodCommandClient) AcceptAgentMail(ctx context.Context, target RuntimePodTarget, request *agentruntimev1.AcceptAgentMailRequest) (*agentruntimev1.AcceptAgentMailResponse, error) {
	return runtimePodCall(ctx, c, target, request, func(client agentruntimev1.AgentRuntimePodServiceClient, ctx context.Context, request *agentruntimev1.AcceptAgentMailRequest) (*agentruntimev1.AcceptAgentMailResponse, error) {
		return client.AcceptAgentMail(ctx, request)
	})
}
func (c *RuntimePodCommandClient) AcceptTaskNotification(ctx context.Context, target RuntimePodTarget, request *agentruntimev1.AcceptTaskNotificationRequest) (*agentruntimev1.AcceptTaskNotificationResponse, error) {
	return runtimePodCall(ctx, c, target, request, func(client agentruntimev1.AgentRuntimePodServiceClient, ctx context.Context, request *agentruntimev1.AcceptTaskNotificationRequest) (*agentruntimev1.AcceptTaskNotificationResponse, error) {
		return client.AcceptTaskNotification(ctx, request)
	})
}
func (c *RuntimePodCommandClient) Interrupt(ctx context.Context, target RuntimePodTarget, request *agentruntimev1.InterruptRequest) (*agentruntimev1.InterruptResponse, error) {
	return runtimePodCall(ctx, c, target, request, func(client agentruntimev1.AgentRuntimePodServiceClient, ctx context.Context, request *agentruntimev1.InterruptRequest) (*agentruntimev1.InterruptResponse, error) {
		return client.Interrupt(ctx, request)
	})
}
func (c *RuntimePodCommandClient) ResolveToolConfirmation(ctx context.Context, target RuntimePodTarget, request *agentruntimev1.ResolveToolConfirmationRequest) (*agentruntimev1.ResolveToolConfirmationResponse, error) {
	return runtimePodCall(ctx, c, target, request, func(client agentruntimev1.AgentRuntimePodServiceClient, ctx context.Context, request *agentruntimev1.ResolveToolConfirmationRequest) (*agentruntimev1.ResolveToolConfirmationResponse, error) {
		return client.ResolveToolConfirmation(ctx, request)
	})
}
func (c *RuntimePodCommandClient) ApplyRuntimeConfig(ctx context.Context, target RuntimePodTarget, request *agentruntimev1.ApplyRuntimeConfigRequest) (*agentruntimev1.ApplyRuntimeConfigResponse, error) {
	return runtimePodCall(ctx, c, target, request, func(client agentruntimev1.AgentRuntimePodServiceClient, ctx context.Context, request *agentruntimev1.ApplyRuntimeConfigRequest) (*agentruntimev1.ApplyRuntimeConfigResponse, error) {
		return client.ApplyRuntimeConfig(ctx, request)
	})
}
func (c *RuntimePodCommandClient) CleanupSession(ctx context.Context, target RuntimePodTarget, request *agentruntimev1.CleanupSessionRequest) (*agentruntimev1.CleanupSessionResponse, error) {
	return runtimePodCall(ctx, c, target, request, func(client agentruntimev1.AgentRuntimePodServiceClient, ctx context.Context, request *agentruntimev1.CleanupSessionRequest) (*agentruntimev1.CleanupSessionResponse, error) {
		return client.CleanupSession(ctx, request)
	})
}

type runtimeCommandPayloadTooLargeError struct{}

func (*runtimeCommandPayloadTooLargeError) Error() string {
	return "runtime command exceeds the transport fuse"
}

func runtimeDeliveryResultFromPrepareError(err error) RuntimeDeliveryResult {
	var authorityLost mcpDiscoveryAuthorityLostError
	if errors.As(err, &authorityLost) {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryAuthorityLost}
	}
	if runtimecontrol.IsThreadInterruptBarrierStaleError(err) {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryBarrierStale}
	}
	var prepareErr runtimecontrol.PreparationError
	if errors.As(err, &prepareErr) {
		return RuntimeDeliveryResult{
			Status:       RuntimeDeliveryRejected,
			Retryable:    prepareErr.Retryable,
			ErrorKind:    prepareErr.Kind,
			ErrorMessage: prepareErr.Error(),
		}
	}
	return RuntimeDeliveryResult{
		Status:       RuntimeDeliveryRejected,
		Retryable:    true,
		ErrorKind:    "runtime_reconcile_error",
		ErrorMessage: "runtime delivery reconciliation failed",
	}
}
