package integration

import (
	"context"

	"google.golang.org/protobuf/proto"

	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
)

type recordingRuntimeCommandSender struct {
	result   jobrunner.RuntimeDeliveryResult
	results  []jobrunner.RuntimeDeliveryResult
	err      error
	targets  []jobrunner.RuntimePodTarget
	requests []proto.Message
}

type staticRuntimeCommandTokenSource struct{}

func (staticRuntimeCommandTokenSource) Token(context.Context) (string, error) {
	return "test-token", nil
}

func (s *recordingRuntimeCommandSender) record(target jobrunner.RuntimePodTarget, request proto.Message) (jobrunner.RuntimeDeliveryResult, error) {
	s.targets = append(s.targets, target)
	s.requests = append(s.requests, request)
	if s.err != nil {
		return jobrunner.RuntimeDeliveryResult{}, s.err
	}
	if len(s.results) > 0 {
		result := s.results[0]
		s.results = s.results[1:]
		return result, nil
	}
	return s.result, nil
}

func (s *recordingRuntimeCommandSender) AcceptInput(_ context.Context, target jobrunner.RuntimePodTarget, request *agentruntimev1.AcceptInputRequest) (*agentruntimev1.AcceptInputResponse, error) {
	result, err := s.record(target, request)
	if err != nil || result.Status == "" {
		return nil, err
	}
	if result.Status == jobrunner.RuntimeDeliveryAccepted {
		return &agentruntimev1.AcceptInputResponse{Outcome: &agentruntimev1.AcceptInputResponse_Accepted{Accepted: &agentruntimev1.AcceptInputAccepted{}}}, nil
	}
	if result.Status == jobrunner.RuntimeDeliveryDuplicate {
		return &agentruntimev1.AcceptInputResponse{Outcome: &agentruntimev1.AcceptInputResponse_Duplicate{Duplicate: &agentruntimev1.AcceptInputDuplicate{}}}, nil
	}
	return &agentruntimev1.AcceptInputResponse{Outcome: &agentruntimev1.AcceptInputResponse_Rejected{Rejected: &agentruntimev1.AcceptInputRejected{Reason: agentruntimev1.AcceptInputFailure_ACCEPT_INPUT_FAILURE_IDENTITY_CONFLICT, Retryable: result.Retryable}}}, nil
}

func (s *recordingRuntimeCommandSender) RecoverThread(_ context.Context, target jobrunner.RuntimePodTarget, request *agentruntimev1.RecoverThreadRequest) (*agentruntimev1.RecoverThreadResponse, error) {
	result, err := s.record(target, request)
	if err != nil || result.Status == "" {
		return nil, err
	}
	if result.Status == jobrunner.RuntimeDeliveryDuplicate {
		return &agentruntimev1.RecoverThreadResponse{Outcome: &agentruntimev1.RecoverThreadResponse_Duplicate{Duplicate: &agentruntimev1.RecoverThreadDuplicate{}}}, nil
	}
	if result.Status == jobrunner.RuntimeDeliveryRejected {
		return &agentruntimev1.RecoverThreadResponse{Outcome: &agentruntimev1.RecoverThreadResponse_Rejected{Rejected: &agentruntimev1.RecoverThreadRejected{Reason: agentruntimev1.RecoverThreadFailure_RECOVER_THREAD_FAILURE_CONTEXT_LOAD_FAILED, Retryable: result.Retryable}}}, nil
	}
	return &agentruntimev1.RecoverThreadResponse{Outcome: &agentruntimev1.RecoverThreadResponse_Accepted{Accepted: &agentruntimev1.RecoverThreadAccepted{}}}, nil
}

func (s *recordingRuntimeCommandSender) AcceptAgentMail(_ context.Context, target jobrunner.RuntimePodTarget, request *agentruntimev1.AcceptAgentMailRequest) (*agentruntimev1.AcceptAgentMailResponse, error) {
	result, err := s.record(target, request)
	if err != nil || result.Status == "" {
		return nil, err
	}
	if result.Status == jobrunner.RuntimeDeliveryDuplicate {
		return &agentruntimev1.AcceptAgentMailResponse{Outcome: &agentruntimev1.AcceptAgentMailResponse_Duplicate{Duplicate: &agentruntimev1.AcceptAgentMailDuplicate{}}}, nil
	}
	if result.Status == jobrunner.RuntimeDeliveryRejected {
		return &agentruntimev1.AcceptAgentMailResponse{Outcome: &agentruntimev1.AcceptAgentMailResponse_Rejected{Rejected: &agentruntimev1.AcceptAgentMailRejected{Reason: agentruntimev1.AcceptAgentMailFailure_ACCEPT_AGENT_MAIL_FAILURE_IDENTITY_CONFLICT, Retryable: result.Retryable}}}, nil
	}
	return &agentruntimev1.AcceptAgentMailResponse{Outcome: &agentruntimev1.AcceptAgentMailResponse_Accepted{Accepted: &agentruntimev1.AcceptAgentMailAccepted{}}}, nil
}

func (s *recordingRuntimeCommandSender) AcceptTaskNotification(_ context.Context, target jobrunner.RuntimePodTarget, request *agentruntimev1.AcceptTaskNotificationRequest) (*agentruntimev1.AcceptTaskNotificationResponse, error) {
	result, err := s.record(target, request)
	if err != nil || result.Status == "" {
		return nil, err
	}
	if result.Status == jobrunner.RuntimeDeliveryDuplicate {
		return &agentruntimev1.AcceptTaskNotificationResponse{Outcome: &agentruntimev1.AcceptTaskNotificationResponse_Duplicate{Duplicate: &agentruntimev1.AcceptTaskNotificationDuplicate{}}}, nil
	}
	if result.Status == jobrunner.RuntimeDeliveryRejected {
		return &agentruntimev1.AcceptTaskNotificationResponse{Outcome: &agentruntimev1.AcceptTaskNotificationResponse_Rejected{Rejected: &agentruntimev1.AcceptTaskNotificationRejected{Reason: agentruntimev1.AcceptTaskNotificationFailure_ACCEPT_TASK_NOTIFICATION_FAILURE_CONTROL_BUSY, Retryable: result.Retryable}}}, nil
	}
	return &agentruntimev1.AcceptTaskNotificationResponse{Outcome: &agentruntimev1.AcceptTaskNotificationResponse_Accepted{Accepted: &agentruntimev1.AcceptTaskNotificationAccepted{}}}, nil
}

func (s *recordingRuntimeCommandSender) Interrupt(_ context.Context, target jobrunner.RuntimePodTarget, request *agentruntimev1.InterruptRequest) (*agentruntimev1.InterruptResponse, error) {
	result, err := s.record(target, request)
	if err != nil || result.Status == "" {
		return nil, err
	}
	if result.Status == jobrunner.RuntimeDeliveryDuplicate {
		return &agentruntimev1.InterruptResponse{Outcome: &agentruntimev1.InterruptResponse_Duplicate{Duplicate: &agentruntimev1.InterruptDuplicate{}}}, nil
	}
	if result.Status == jobrunner.RuntimeDeliveryRejected {
		return &agentruntimev1.InterruptResponse{Outcome: &agentruntimev1.InterruptResponse_Rejected{Rejected: &agentruntimev1.InterruptRejected{Reason: agentruntimev1.InterruptFailure_INTERRUPT_FAILURE_CONTROL_BUSY, Retryable: result.Retryable}}}, nil
	}
	return &agentruntimev1.InterruptResponse{Outcome: &agentruntimev1.InterruptResponse_Accepted{Accepted: &agentruntimev1.InterruptAccepted{}}}, nil
}

func (s *recordingRuntimeCommandSender) ResolveToolConfirmation(_ context.Context, target jobrunner.RuntimePodTarget, request *agentruntimev1.ResolveToolConfirmationRequest) (*agentruntimev1.ResolveToolConfirmationResponse, error) {
	result, err := s.record(target, request)
	if err != nil || result.Status == "" {
		return nil, err
	}
	if result.Status == jobrunner.RuntimeDeliveryDuplicate {
		return &agentruntimev1.ResolveToolConfirmationResponse{Outcome: &agentruntimev1.ResolveToolConfirmationResponse_Duplicate{Duplicate: &agentruntimev1.ResolveToolConfirmationDuplicate{}}}, nil
	}
	if result.Status == jobrunner.RuntimeDeliveryRejected {
		return &agentruntimev1.ResolveToolConfirmationResponse{Outcome: &agentruntimev1.ResolveToolConfirmationResponse_Rejected{Rejected: &agentruntimev1.ResolveToolConfirmationRejected{Reason: agentruntimev1.ResolveToolConfirmationFailure_RESOLVE_TOOL_CONFIRMATION_FAILURE_CONTROL_CONFLICT, Retryable: result.Retryable}}}, nil
	}
	return &agentruntimev1.ResolveToolConfirmationResponse{Outcome: &agentruntimev1.ResolveToolConfirmationResponse_Accepted{Accepted: &agentruntimev1.ResolveToolConfirmationAccepted{}}}, nil
}

func (s *recordingRuntimeCommandSender) ApplyRuntimeConfig(_ context.Context, target jobrunner.RuntimePodTarget, request *agentruntimev1.ApplyRuntimeConfigRequest) (*agentruntimev1.ApplyRuntimeConfigResponse, error) {
	result, err := s.record(target, request)
	if err != nil || result.Status == "" {
		return nil, err
	}
	if result.Status == jobrunner.RuntimeDeliveryDuplicate {
		return &agentruntimev1.ApplyRuntimeConfigResponse{Outcome: &agentruntimev1.ApplyRuntimeConfigResponse_Duplicate{Duplicate: &agentruntimev1.ApplyRuntimeConfigDuplicate{}}}, nil
	}
	if result.Status == jobrunner.RuntimeDeliveryRejected {
		return &agentruntimev1.ApplyRuntimeConfigResponse{Outcome: &agentruntimev1.ApplyRuntimeConfigResponse_Rejected{Rejected: &agentruntimev1.ApplyRuntimeConfigRejected{Reason: agentruntimev1.ApplyRuntimeConfigFailure_APPLY_RUNTIME_CONFIG_FAILURE_BINDING_MISMATCH, Retryable: result.Retryable}}}, nil
	}
	return &agentruntimev1.ApplyRuntimeConfigResponse{Outcome: &agentruntimev1.ApplyRuntimeConfigResponse_Applied{Applied: &agentruntimev1.ApplyRuntimeConfigApplied{}}}, nil
}

func (s *recordingRuntimeCommandSender) CleanupSession(_ context.Context, target jobrunner.RuntimePodTarget, request *agentruntimev1.CleanupSessionRequest) (*agentruntimev1.CleanupSessionResponse, error) {
	result, err := s.record(target, request)
	if err != nil || result.Status == "" {
		return nil, err
	}
	if result.Status == jobrunner.RuntimeDeliveryDuplicate {
		return &agentruntimev1.CleanupSessionResponse{Outcome: &agentruntimev1.CleanupSessionResponse_Duplicate{Duplicate: &agentruntimev1.CleanupSessionDuplicate{}}}, nil
	}
	if result.Status == jobrunner.RuntimeDeliveryRejected {
		return &agentruntimev1.CleanupSessionResponse{Outcome: &agentruntimev1.CleanupSessionResponse_Rejected{Rejected: &agentruntimev1.CleanupSessionRejected{Reason: agentruntimev1.CleanupSessionFailure_CLEANUP_SESSION_FAILURE_BINDING_MISMATCH, Retryable: result.Retryable}}}, nil
	}
	return &agentruntimev1.CleanupSessionResponse{Outcome: &agentruntimev1.CleanupSessionResponse_Completed{Completed: &agentruntimev1.CleanupSessionCompleted{}}}, nil
}
