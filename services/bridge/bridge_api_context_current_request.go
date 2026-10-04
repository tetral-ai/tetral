package agentruntimebridge

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// selectCurrentRequestMessage projects the Runtime's current-request selection
// onto a durable Assistant identity. The shared literal selection cases bind
// this transport projection to selectCurrentRequestStart in Runtime turn/load.
// Message visibility and request lifecycle remain Runtime responsibilities.
func selectCurrentRequestMessage(facts bridgeLoadContextTurnFacts, messages []bridgeLoadContextMessageDescriptor) (*bridgeRuntimeCurrentRequestMessage, error) {
	var running, idleBefore, idle, terminated *bridgeLoadContextTurnEvent
	starts := make([]*bridgeLoadContextTurnEvent, 0)
	seen := make(map[string]bool)
	for index := range facts.Events {
		event := &facts.Events[index]
		if event.Type == "session.status_running" || event.Type == "session.thread_status_running" {
			running = event
		}
		if event.Type == "span.model_request_start" {
			if event.ModelRequestID == nil || event.RequestStart == nil || seen[*event.ModelRequestID] {
				return nil, status.Error(codes.FailedPrecondition, "current request association is ambiguous")
			}
			seen[*event.ModelRequestID] = true
			starts = append(starts, event)
		}
	}
	for index := range facts.Events {
		event := &facts.Events[index]
		isIdle := event.Type == "session.status_idle" || event.Type == "session.thread_status_idle"
		if running != nil && isIdle && event.EventSequence < running.EventSequence {
			idleBefore = event
		}
		if running != nil && event.EventSequence < running.EventSequence {
			continue
		}
		if isIdle {
			idle = event
		}
		if event.Type == "session.status_terminated" || event.Type == "session.thread_status_terminated" {
			terminated = event
		}
	}
	if terminated != nil || (idle != nil && (idle.Idle == nil || idle.Idle.StopReason != "requires_action")) {
		return nil, nil
	}
	preservePrior := idleBefore != nil && idleBefore.Idle != nil && idleBefore.Idle.StopReason == "requires_action"
	var selected *bridgeLoadContextTurnEvent
	for _, start := range starts {
		if running == nil || preservePrior || start.EventSequence >= running.EventSequence {
			selected = start
		}
	}
	if selected == nil {
		return nil, nil
	}
	var result *bridgeRuntimeCurrentRequestMessage
	for _, message := range messages {
		if message.Kind != "assistant" || message.ModelRequestID == nil || *message.ModelRequestID != *selected.ModelRequestID {
			continue
		}
		if result != nil {
			return nil, status.Error(codes.FailedPrecondition, "current request association is ambiguous")
		}
		result = &bridgeRuntimeCurrentRequestMessage{ModelRequestID: *selected.ModelRequestID, AssistantMessageSequence: message.MessageSequence}
	}
	return result, nil
}
