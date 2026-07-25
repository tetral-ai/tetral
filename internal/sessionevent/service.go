package sessionevent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/storage"

	"github.com/tetral-ai/tetral/internal/workspace"
)

type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string { return e.Message }

type NotFoundError struct {
	Message string
}

func (e *NotFoundError) Error() string { return e.Message }

type ConflictError struct {
	Message string
}

func (e *ConflictError) Error() string { return e.Message }

type Store interface {
	AppendClientEvents(context.Context, workspace.ID, string, []preparedEvent, appendIdempotency, appendSettings) (*appendOutcome, error)
}

type Service struct {
	store           Store
	clock           func() time.Time
	eventIDStrategy func() string
	limits          Limits
}

type Option func(*Service)

func WithClock(clock func() time.Time) Option {
	return func(s *Service) {
		if clock != nil {
			s.clock = clock
		}
	}
}

func WithLimits(limits Limits) Option {
	return func(s *Service) {
		s.limits = limits
	}
}

func NewService(store Store, options ...Option) *Service {
	service := &Service{
		store:           store,
		clock:           func() time.Time { return storage.Now() },
		eventIDStrategy: NewEventID,
		limits:          DefaultLimits(),
	}
	for _, option := range options {
		option(service)
	}
	return service
}

func (s *Service) AppendClientEvents(ctx context.Context, workspaceID workspace.ID, sessionID string, idempotencyKey string, request AppendRequest) (*AppendResult, error) {
	if workspaceID == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	if sessionID == "" {
		return nil, &ValidationError{Message: "session_id is required"}
	}
	if s == nil || s.store == nil {
		return nil, errors.New("sessionevent: store is required")
	}
	idempotency, err := prepareAppendIdempotency(idempotencyKey, request)
	if err != nil {
		return nil, err
	}
	prepared, err := prepareIncomingEvents(request, s.limits)
	if err != nil {
		return nil, err
	}
	now := s.clock().UTC()
	for index := range prepared {
		prepared[index].id = s.eventIDStrategy()
	}
	outcome, err := s.store.AppendClientEvents(ctx, workspaceID, sessionID, prepared, idempotency, appendSettings{
		now: now,
	})
	if err != nil {
		return nil, err
	}
	if outcome == nil {
		return &AppendResult{}, nil
	}
	return &AppendResult{Data: outcome.events}, nil
}

type preparedEvent struct {
	id             string
	eventType      string
	payload        json.RawMessage
	targetThreadID string
	toolUseEventID string
	toolResult     string
	denyMessage    string
	fileReferences []fileAttachmentReference
}

type fileAttachmentReference struct {
	blockType string
	fileID    string
}

type appendSettings struct {
	now time.Time
}

type appendIdempotency struct {
	keyDigest []byte
}

type appendOutcome struct {
	events   []*Event
	replayed bool
}

type messagePayload struct {
	Content []ContentBlock `json:"content"`
}

type toolConfirmationPayload struct {
	ToolUseID   string `json:"tool_use_id"`
	Result      string `json:"result"`
	DenyMessage string `json:"deny_message,omitempty"`
}

func prepareIncomingEvents(request AppendRequest, limits Limits) ([]preparedEvent, error) {
	if err := validateLimits(limits); err != nil {
		return nil, err
	}
	if len(request.Events) == 0 {
		return nil, &ValidationError{Message: "events must be a non-empty array"}
	}
	if len(request.Events) > limits.MaxEventsPerRequest {
		return nil, &ValidationError{Message: "events exceeds the per-request limit"}
	}
	prepared := make([]preparedEvent, 0, len(request.Events))
	fileReferenceCount := 0
	for _, event := range request.Events {
		switch event.Type {
		case EventTypeUserMessage:
			if len(event.Content) == 0 {
				return nil, &ValidationError{Message: "user.message content must be a non-empty array"}
			}
			fileReferences := make([]fileAttachmentReference, 0)
			for _, block := range event.Content {
				if err := validateContentBlock(block); err != nil {
					return nil, err
				}
				if block.Source != nil {
					fileReferences = append(fileReferences, fileAttachmentReference{
						blockType: block.Type,
						fileID:    block.Source.FileID,
					})
				}
			}
			fileReferenceCount += len(fileReferences)
			if fileReferenceCount > MaxProviderRequestAttachments {
				return nil, &ValidationError{Message: "events request exceeds the file attachment limit"}
			}
			payload, err := json.Marshal(messagePayload{Content: event.Content})
			if err != nil {
				return nil, err
			}
			prepared = append(prepared, preparedEvent{eventType: EventTypeUserMessage, payload: payload, fileReferences: fileReferences})
		case EventTypeUserInterrupt:
			if len(event.Content) != 0 {
				return nil, &ValidationError{Message: "user.interrupt must not include content"}
			}
			prepared = append(prepared, preparedEvent{
				eventType:      EventTypeUserInterrupt,
				payload:        json.RawMessage(`{}`),
				targetThreadID: strings.TrimSpace(event.SessionThreadID),
			})
		case EventTypeUserToolConfirmation:
			if strings.TrimSpace(event.ToolUseID) == "" {
				return nil, &ValidationError{Message: "user.tool_confirmation tool_use_id is required"}
			}
			if event.Result != ToolConfirmationResultAllow && event.Result != ToolConfirmationResultDeny {
				return nil, &ValidationError{Message: "user.tool_confirmation result must be allow or deny"}
			}
			if event.Result == ToolConfirmationResultAllow && event.DenyMessage != "" {
				return nil, &ValidationError{Message: "deny_message is only valid with deny"}
			}
			payload, err := json.Marshal(toolConfirmationPayload{
				ToolUseID:   strings.TrimSpace(event.ToolUseID),
				Result:      event.Result,
				DenyMessage: event.DenyMessage,
			})
			if err != nil {
				return nil, err
			}
			prepared = append(prepared, preparedEvent{
				eventType:      EventTypeUserToolConfirmation,
				payload:        payload,
				toolUseEventID: strings.TrimSpace(event.ToolUseID),
				toolResult:     event.Result,
				denyMessage:    event.DenyMessage,
			})
		case "user.custom_tool_result":
			return nil, &ValidationError{Message: "custom tools are not supported in this stage"}
		default:
			return nil, &ValidationError{Message: "event type must be user.message, user.interrupt, or user.tool_confirmation"}
		}
	}
	return prepared, nil
}

func validateContentBlock(block ContentBlock) error {
	switch block.Type {
	case ContentBlockTypeText:
		if block.Source != nil {
			return &ValidationError{Message: "unknown content block field"}
		}
		return nil
	case ContentBlockTypeImage, ContentBlockTypeDocument:
		if block.Source == nil {
			return &ValidationError{Message: "content block source is required"}
		}
		if block.Text != "" {
			return &ValidationError{Message: "unknown content block field"}
		}
		if block.Source.Type != ContentSourceTypeFile {
			return &ValidationError{Message: block.Source.Type + " source is not supported; upload the bytes via /v1/files and reference the file_id"}
		}
		if strings.TrimSpace(block.Source.FileID) == "" {
			return &ValidationError{Message: "content block source file_id must be a non-empty string"}
		}
		return nil
	default:
		return &ValidationError{Message: "content block type must be text, image, or document"}
	}
}

func prepareAppendIdempotency(rawKey string, request AppendRequest) (appendIdempotency, error) {
	key := strings.TrimSpace(rawKey)
	if key == "" {
		return appendIdempotency{}, &ValidationError{Message: "Idempotency-Key header is required"}
	}
	if len([]byte(key)) > 255 {
		return appendIdempotency{}, &ValidationError{Message: "Idempotency-Key header is too long"}
	}
	keyDigest := sha256.Sum256([]byte(key))
	return appendIdempotency{
		keyDigest: keyDigest[:],
	}, nil
}

func DecodeAppendRequest(body []byte) (AppendRequest, error) {
	return DecodeAppendRequestWithLimits(body, DefaultLimits())
}

func DecodeAppendRequestWithLimits(body []byte, limits Limits) (AppendRequest, error) {
	if err := validateLimits(limits); err != nil {
		return AppendRequest{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	var envelope map[string]json.RawMessage
	if err := decoder.Decode(&envelope); err != nil {
		return AppendRequest{}, &ValidationError{Message: "invalid request body"}
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return AppendRequest{}, &ValidationError{Message: "invalid request body: unexpected trailing data"}
		}
		return AppendRequest{}, &ValidationError{Message: "invalid request body"}
	}
	if envelope == nil {
		return AppendRequest{}, &ValidationError{Message: "request body must be an object"}
	}
	for field := range envelope {
		if field != "events" {
			return AppendRequest{}, &ValidationError{Message: "unknown request field"}
		}
	}
	rawEvents, ok := envelope["events"]
	if !ok {
		return AppendRequest{}, &ValidationError{Message: "events is required"}
	}
	var eventBodies []json.RawMessage
	if err := json.Unmarshal(rawEvents, &eventBodies); err != nil {
		return AppendRequest{}, &ValidationError{Message: "events must be an array"}
	}
	if len(eventBodies) == 0 {
		return AppendRequest{}, &ValidationError{Message: "events must be a non-empty array"}
	}
	if len(eventBodies) > limits.MaxEventsPerRequest {
		return AppendRequest{}, &ValidationError{Message: "events exceeds the per-request limit"}
	}
	events := make([]IncomingEvent, 0, len(eventBodies))
	for _, raw := range eventBodies {
		event, err := decodeIncomingEvent(raw)
		if err != nil {
			return AppendRequest{}, err
		}
		events = append(events, event)
	}
	return AppendRequest{Events: events}, nil
}

func validateLimits(limits Limits) error {
	if limits.MaxEventsPerRequest <= 0 {
		return &ValidationError{Message: "max events per request must be positive"}
	}
	return nil
}

func decodeIncomingEvent(raw json.RawMessage) (IncomingEvent, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return IncomingEvent{}, &ValidationError{Message: "event must be an object"}
	}
	if fields == nil {
		return IncomingEvent{}, &ValidationError{Message: "event must be an object"}
	}
	rawType, ok := fields["type"]
	if !ok {
		return IncomingEvent{}, &ValidationError{Message: "event type is required"}
	}
	var eventType string
	if err := json.Unmarshal(rawType, &eventType); err != nil {
		return IncomingEvent{}, &ValidationError{Message: "event type must be a string"}
	}
	switch eventType {
	case EventTypeUserMessage:
		for field := range fields {
			if field != "type" && field != "content" {
				return IncomingEvent{}, &ValidationError{Message: "unknown event field"}
			}
		}
		rawContent, ok := fields["content"]
		if !ok {
			return IncomingEvent{}, &ValidationError{Message: "user.message content is required"}
		}
		content, err := decodeContentBlocks(rawContent)
		if err != nil {
			return IncomingEvent{}, err
		}
		return IncomingEvent{Type: eventType, Content: content}, nil
	case EventTypeUserInterrupt:
		for field := range fields {
			if field != "type" && field != "session_thread_id" {
				return IncomingEvent{}, &ValidationError{Message: "user.interrupt must not include payload fields"}
			}
		}
		var sessionThreadID string
		if rawThreadID, ok := fields["session_thread_id"]; ok {
			if err := json.Unmarshal(rawThreadID, &sessionThreadID); err != nil {
				return IncomingEvent{}, &ValidationError{Message: "user.interrupt session_thread_id must be a string"}
			}
			sessionThreadID = strings.TrimSpace(sessionThreadID)
			if sessionThreadID == "" {
				return IncomingEvent{}, &ValidationError{Message: "user.interrupt session_thread_id must not be blank"}
			}
		}
		return IncomingEvent{Type: eventType, SessionThreadID: sessionThreadID}, nil
	case EventTypeUserToolConfirmation:
		for field := range fields {
			if field != "type" && field != "tool_use_id" && field != "result" && field != "deny_message" {
				return IncomingEvent{}, &ValidationError{Message: "unknown event field"}
			}
		}
		rawToolUseID, ok := fields["tool_use_id"]
		if !ok {
			return IncomingEvent{}, &ValidationError{Message: "user.tool_confirmation tool_use_id is required"}
		}
		var toolUseID string
		if err := json.Unmarshal(rawToolUseID, &toolUseID); err != nil {
			return IncomingEvent{}, &ValidationError{Message: "user.tool_confirmation tool_use_id must be a string"}
		}
		rawResult, ok := fields["result"]
		if !ok {
			return IncomingEvent{}, &ValidationError{Message: "user.tool_confirmation result is required"}
		}
		var result string
		if err := json.Unmarshal(rawResult, &result); err != nil {
			return IncomingEvent{}, &ValidationError{Message: "user.tool_confirmation result must be a string"}
		}
		if result != ToolConfirmationResultAllow && result != ToolConfirmationResultDeny {
			return IncomingEvent{}, &ValidationError{Message: "user.tool_confirmation result must be allow or deny"}
		}
		var denyMessage string
		if rawDenyMessage, ok := fields["deny_message"]; ok {
			if err := json.Unmarshal(rawDenyMessage, &denyMessage); err != nil {
				return IncomingEvent{}, &ValidationError{Message: "deny_message must be a string"}
			}
		}
		if result == ToolConfirmationResultAllow && denyMessage != "" {
			return IncomingEvent{}, &ValidationError{Message: "deny_message is only valid with deny"}
		}
		return IncomingEvent{Type: eventType, ToolUseID: toolUseID, Result: result, DenyMessage: denyMessage}, nil
	case "user.custom_tool_result":
		return IncomingEvent{}, &ValidationError{Message: "custom tools are not supported in this stage"}
	default:
		return IncomingEvent{}, &ValidationError{Message: "event type must be user.message, user.interrupt, or user.tool_confirmation"}
	}
}

func decodeContentBlocks(raw json.RawMessage) ([]ContentBlock, error) {
	var bodies []json.RawMessage
	if err := json.Unmarshal(raw, &bodies); err != nil {
		return nil, &ValidationError{Message: "user.message content must be an array"}
	}
	if len(bodies) == 0 {
		return nil, &ValidationError{Message: "user.message content must be a non-empty array"}
	}
	blocks := make([]ContentBlock, 0, len(bodies))
	for _, body := range bodies {
		block, err := decodeContentBlock(body)
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, block)
	}
	return blocks, nil
}

func decodeContentBlock(raw json.RawMessage) (ContentBlock, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return ContentBlock{}, &ValidationError{Message: "content block must be an object"}
	}
	if fields == nil {
		return ContentBlock{}, &ValidationError{Message: "content block must be an object"}
	}
	rawType, ok := fields["type"]
	if !ok {
		return ContentBlock{}, &ValidationError{Message: "content block type is required"}
	}
	var blockType string
	if err := json.Unmarshal(rawType, &blockType); err != nil {
		return ContentBlock{}, &ValidationError{Message: "content block type must be a string"}
	}
	switch blockType {
	case ContentBlockTypeText:
		for field := range fields {
			if field != "type" && field != "text" {
				return ContentBlock{}, &ValidationError{Message: "unknown content block field"}
			}
		}
		rawText, ok := fields["text"]
		if !ok {
			return ContentBlock{}, &ValidationError{Message: "content block text is required"}
		}
		var text string
		if err := json.Unmarshal(rawText, &text); err != nil {
			return ContentBlock{}, &ValidationError{Message: "content block text must be a string"}
		}
		return ContentBlock{Type: blockType, Text: text}, nil
	case ContentBlockTypeImage, ContentBlockTypeDocument:
		for field := range fields {
			if field != "type" && field != "source" {
				return ContentBlock{}, &ValidationError{Message: "unknown content block field"}
			}
		}
		rawSource, ok := fields["source"]
		if !ok {
			return ContentBlock{}, &ValidationError{Message: "content block source is required"}
		}
		source, err := decodeFileContentSource(rawSource)
		if err != nil {
			return ContentBlock{}, err
		}
		return ContentBlock{Type: blockType, Source: source}, nil
	default:
		return ContentBlock{}, &ValidationError{Message: "content block type must be text, image, or document"}
	}
}

func decodeFileContentSource(raw json.RawMessage) (*ContentSource, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return nil, &ValidationError{Message: "content block source must be an object"}
	}
	rawType, ok := fields["type"]
	if !ok {
		return nil, &ValidationError{Message: "content block source type is required"}
	}
	var sourceType string
	if err := json.Unmarshal(rawType, &sourceType); err != nil {
		return nil, &ValidationError{Message: "content block source type must be a string"}
	}
	if sourceType != ContentSourceTypeFile {
		return nil, &ValidationError{Message: sourceType + " source is not supported; upload the bytes via /v1/files and reference the file_id"}
	}
	for field := range fields {
		if field != "type" && field != "file_id" {
			return nil, &ValidationError{Message: "unknown content block source field"}
		}
	}
	rawFileID, ok := fields["file_id"]
	if !ok {
		return nil, &ValidationError{Message: "content block source file_id is required"}
	}
	var fileID string
	if err := json.Unmarshal(rawFileID, &fileID); err != nil || strings.TrimSpace(fileID) == "" {
		return nil, &ValidationError{Message: "content block source file_id must be a non-empty string"}
	}
	return &ContentSource{Type: sourceType, FileID: fileID}, nil
}
