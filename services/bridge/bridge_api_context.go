package agentruntimebridge

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// This file owns the Bridge context protocol-family boundary.

func (s *PostgreSQLBridgeAPIStore) LoadContext(ctx context.Context, request *bridgev1.LoadContextRequest) (*bridgev1.LoadContextResponse, error) {
	if request.GetRuntimeInputId() == "" {
		return nil, status.Error(codes.InvalidArgument, "runtime input id is required")
	}
	var response *bridgev1.LoadContextResponse
	if err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.load_context", func(tx *dbconnect.Tx) error {
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		if err := verifyRuntimeThreadScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		contextJSON, err := loadThreadContextJSONTx(
			ctx,
			tx,
			request.GetScope(),
			s.providerRescheduleBudget(),
			s.compactionRescheduleBudget(),
			request.GetAgentMailSourceThreadId(),
		)
		if err != nil {
			return err
		}
		token, err := s.runtimeBindingToken(request.GetScope())
		if err != nil {
			return err
		}
		response = &bridgev1.LoadContextResponse{
			Ack:                 committedAck(request.GetRuntimeInputId(), ""),
			ContextJson:         contextJSON,
			RuntimeBindingToken: token,
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return response, nil
}

func (s *PostgreSQLBridgeAPIStore) RefreshRuntimeBindingToken(ctx context.Context, request *bridgev1.RefreshRuntimeBindingTokenRequest) (*bridgev1.RefreshRuntimeBindingTokenResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "runtime binding token refresh request is required")
	}
	var token string
	if err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.refresh_runtime_binding_token", func(tx *dbconnect.Tx) error {
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		if err := verifyRuntimeThreadScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		var err error
		token, err = s.runtimeBindingToken(request.GetScope())
		return err
	}); err != nil {
		return nil, err
	}
	return &bridgev1.RefreshRuntimeBindingTokenResponse{RuntimeBindingToken: token}, nil
}

type bridgeLoadContextPayload struct {
	Messages            []json.RawMessage                    `json:"messages"`
	ThreadContextPrefix *bridgeLoadContextThreadPrefix       `json:"threadContextPrefix"`
	Thread              bridgeLoadContextThread              `json:"thread"`
	RuntimeConfig       bridgeLoadContextRuntimeConfig       `json:"runtimeConfig"`
	MCPManifests        []bridgeLoadContextMCPManifest       `json:"mcpManifests"`
	PendingToolUses     []bridgeLoadContextPendingTool       `json:"pendingToolUses"`
	BackgroundTools     []bridgeLoadContextBackgroundTool    `json:"backgroundTools"`
	PendingAttachments  []bridgeLoadContextPendingAttachment `json:"pendingAttachments"`
	PendingAgentMail    []bridgeLoadContextAgentMail         `json:"pendingAgentMail"`
}

type bridgeLoadContextThreadPrefix struct {
	ChildThreadID                 string            `json:"childThreadId"`
	ParentThreadID                string            `json:"parentThreadId"`
	ParentBoundaryEventID         string            `json:"parentBoundaryEventId"`
	Entries                       []json.RawMessage `json:"entries"`
	CreatedAt                     string            `json:"createdAt"`
	ConsumedByCheckpointMessageID *string           `json:"consumedByCheckpointMessageId"`
}

type bridgeLoadContextThread struct {
	ParentThreadID *string `json:"parentThreadId"`
	Role           string  `json:"role"`
	Visibility     string  `json:"visibility"`
	TaskName       *string `json:"taskName"`
	AgentType      string  `json:"agentType"`
	Status         string  `json:"status"`
}

type bridgeLoadContextAgentMail struct {
	DeliveryID           string          `json:"deliveryId"`
	SourceThreadID       string          `json:"sourceThreadId"`
	SourceToolUseEventID string          `json:"sourceToolUseEventId"`
	Message              json.RawMessage `json:"message"`
}

type bridgeLoadContextPendingAttachment struct {
	Origin   bridgeLoadContextAttachmentOrigin `json:"origin"`
	Mime     string                            `json:"mime"`
	Filename string                            `json:"filename"`
}

type bridgeLoadContextAttachmentOrigin struct {
	Transient  *bridgeLoadContextTransientAttachment `json:"transient,omitempty"`
	FileBacked *bridgeLoadContextFileAttachment      `json:"fileBacked,omitempty"`
}

type bridgeLoadContextTransientAttachment struct {
	AttachmentRef        string `json:"attachmentRef"`
	SourceToolUseEventID string `json:"sourceToolUseEventId"`
	SourcePath           string `json:"sourcePath,omitempty"`
	PageRange            string `json:"pageRange,omitempty"`
	Detail               string `json:"detail,omitempty"`
}

type bridgeLoadContextFileAttachment struct {
	SourceEventID string `json:"sourceEventId"`
	FileID        string `json:"fileId"`
}

type bridgeLoadContextMCPManifest struct {
	MCPServerName      string                     `json:"mcpServerName"`
	ManifestETag       string                     `json:"manifestETag,omitempty"`
	ManifestGeneration int64                      `json:"manifestGeneration"`
	Readiness          string                     `json:"readiness"`
	Diagnostic         *string                    `json:"diagnostic"`
	Tools              []bridgeLoadContextMCPTool `json:"tools"`
}

type bridgeLoadContextMCPTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"inputSchema"`
}

type bridgeLoadContextRuntimeConfig struct {
	ConfigGeneration           int64                      `json:"configGeneration"`
	ApprovalMode               string                     `json:"approvalMode"`
	System                     *string                    `json:"system"`
	MemoryStores               []bridgeRuntimeMemoryStore `json:"memoryStores"`
	Agent                      bridgeLoadAgent            `json:"agent"`
	Environment                bridgeLoadEnv              `json:"environment"`
	ToolPolicy                 map[string]any             `json:"toolPolicy"`
	Skills                     json.RawMessage            `json:"skills"`
	SkillsIndex                json.RawMessage            `json:"skillsIndex"`
	InstalledTools             json.RawMessage            `json:"installedTools"`
	ProviderRescheduleBudget   int64                      `json:"providerRescheduleBudget"`
	CompactionRescheduleBudget int64                      `json:"compactionRescheduleBudget"`
}

type bridgeLoadAgent struct {
	ID         string          `json:"id"`
	Version    int64           `json:"version"`
	ConfigHash string          `json:"configHash"`
	Config     json.RawMessage `json:"config"`
}

type bridgeLoadEnv struct {
	ID                string          `json:"id"`
	CurrentGeneration int64           `json:"currentGeneration"`
	Config            json.RawMessage `json:"config"`
}

type bridgeLoadContextPendingTool struct {
	ToolUseEventID  string          `json:"toolUseEventId"`
	ModelRequestID  string          `json:"modelRequestId"`
	ModelToolCallID string          `json:"modelToolCallId"`
	ToolName        string          `json:"toolName"`
	Kind            string          `json:"kind"`
	Input           json.RawMessage `json:"input"`
	Decision        *string         `json:"decision,omitempty"`
	DenyMessage     *string         `json:"denyMessage,omitempty"`
	Status          string          `json:"status"`
	ExpiresAt       string          `json:"expiresAt"`
}

type bridgeLoadContextBackgroundTool struct {
	TaskID               string `json:"taskId"`
	SourceToolUseEventID string `json:"sourceToolUseEventId"`
}

func loadThreadContextJSONTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	providerRescheduleBudget int64,
	compactionRescheduleBudget int64,
	agentMailSourceThreadID string,
) (string, error) {
	runtimeConfig, err := loadThreadRuntimeConfigTx(ctx, tx, scope)
	if err != nil {
		return "", err
	}
	runtimeConfig.ProviderRescheduleBudget = providerRescheduleBudget
	runtimeConfig.CompactionRescheduleBudget = compactionRescheduleBudget
	mcpManifests, err := loadSessionMCPManifestsTx(ctx, tx, scope)
	if err != nil {
		return "", err
	}
	pendingToolUses, err := loadThreadPendingToolUsesTx(ctx, tx, scope)
	if err != nil {
		return "", err
	}
	backgroundTools, err := loadThreadBackgroundToolsTx(ctx, tx, scope)
	if err != nil {
		return "", err
	}
	pendingAttachments, err := loadThreadPendingAttachmentsTx(ctx, tx, scope)
	if err != nil {
		return "", err
	}
	pendingAgentMail, err := loadThreadPendingAgentMailTx(ctx, tx, scope, agentMailSourceThreadID)
	if err != nil {
		return "", err
	}
	thread, err := loadThreadMetadataForContextTx(ctx, tx, scope)
	if err != nil {
		return "", err
	}
	threadContextPrefix, err := loadThreadContextPrefixTx(ctx, tx, scope)
	if err != nil {
		return "", err
	}
	rows, err := tx.Query(ctx,
		`WITH latest_compaction AS (
			SELECT MAX(sequence) AS boundary_sequence
			  FROM session_messages
			 WHERE workspace_id = $1
			   AND session_id = $2
			   AND session_thread_id = $3
			   AND kind = 'compaction'
		)
			SELECT m.kind,
			       m.message_id,
			       m.sequence,
			       m.data_json,
			       COALESCE(m.last_event_id, m.source_event_id),
		       m.created_at,
		       m.updated_at,
		       e.sequence
		  FROM session_messages m
		  LEFT JOIN session_events e
		    ON e.workspace_id = m.workspace_id
		   AND e.session_id = m.session_id
		   AND e.session_thread_id = m.session_thread_id
		   AND e.event_id = COALESCE(m.last_event_id, m.source_event_id)
		  CROSS JOIN latest_compaction c
		 WHERE m.workspace_id = $1
		   AND m.session_id = $2
		   AND m.session_thread_id = $3
		   AND (c.boundary_sequence IS NULL OR m.sequence >= c.boundary_sequence)
		 ORDER BY m.sequence ASC`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	)
	if err != nil {
		return "", err
	}
	defer func() { _ = rows.Close() }()
	messages := make([]json.RawMessage, 0)
	for rows.Next() {
		var kind string
		var messageID string
		var sequence int64
		var raw string
		var owningEventID sql.NullString
		var createdAt time.Time
		var updatedAt time.Time
		var eventSequence sql.NullInt64
		if err := rows.Scan(
			&kind,
			&messageID,
			&sequence,
			&raw,
			&owningEventID,
			&createdAt,
			&updatedAt,
			&eventSequence,
		); err != nil {
			return "", err
		}
		if !json.Valid([]byte(raw)) {
			return "", status.Error(codes.FailedPrecondition, "session message projection is malformed")
		}
		if kind == "compaction" {
			projected, err := compactionProjectionForLoad(raw, sequence)
			if err != nil {
				return "", err
			}
			raw = projected
		}
		if !owningEventID.Valid || !eventSequence.Valid {
			return "", status.Error(codes.FailedPrecondition, "session message projection has no owning event stamp")
		}
		stamped, err := stampRuntimeMessageForLoad(
			raw,
			scope.GetSessionId(),
			messageID,
			sequence,
			owningEventID.String,
			eventSequence.Int64,
			createdAt,
			updatedAt,
		)
		if err != nil {
			return "", err
		}
		messages = append(messages, stamped)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return marshalBridgeJSON(bridgeLoadContextPayload{
		Messages:            messages,
		ThreadContextPrefix: threadContextPrefix,
		Thread:              thread,
		RuntimeConfig:       runtimeConfig,
		MCPManifests:        mcpManifests,
		PendingToolUses:     pendingToolUses,
		BackgroundTools:     backgroundTools,
		PendingAttachments:  pendingAttachments,
		PendingAgentMail:    pendingAgentMail,
	})
}

func loadThreadContextPrefixTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
) (*bridgeLoadContextThreadPrefix, error) {
	var prefix bridgeLoadContextThreadPrefix
	var entriesJSON string
	var createdAt time.Time
	var consumed sql.NullString
	err := tx.QueryRow(ctx,
		`SELECT child_thread_id,
		        parent_thread_id,
		        parent_boundary_event_id,
		        entries_json,
		        created_at,
		        consumed_by_checkpoint_message_id
		   FROM session_thread_context_prefixes
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND child_thread_id = $3
		    AND consumed_by_checkpoint_message_id IS NULL`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	).Scan(
		&prefix.ChildThreadID,
		&prefix.ParentThreadID,
		&prefix.ParentBoundaryEventID,
		&entriesJSON,
		&createdAt,
		&consumed,
	)
	if dbconnect.IsNoRows(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(entriesJSON), &prefix.Entries); err != nil || prefix.Entries == nil {
		return nil, status.Error(codes.FailedPrecondition, "thread context prefix is malformed")
	}
	prefix.CreatedAt = createdAt.UTC().Format(time.RFC3339Nano)
	if consumed.Valid {
		prefix.ConsumedByCheckpointMessageID = &consumed.String
	}
	return &prefix, nil
}

func stampRuntimeMessageForLoad(
	raw string,
	sessionID string,
	messageID string,
	messageSequence int64,
	owningEventID string,
	eventSequence int64,
	createdAt time.Time,
	updatedAt time.Time,
) (json.RawMessage, error) {
	var message map[string]any
	if err := json.Unmarshal([]byte(raw), &message); err != nil || message == nil {
		return nil, status.Error(codes.FailedPrecondition, "session message projection is malformed")
	}
	if _, ok := message["providerId"]; ok {
		return nil, status.Error(codes.FailedPrecondition, "session message projection contains routing metadata")
	}
	if _, ok := message["modelId"]; ok {
		return nil, status.Error(codes.FailedPrecondition, "session message projection contains routing metadata")
	}
	parts, ok := message["parts"].([]any)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "session message projection parts are malformed")
	}
	for index, rawPart := range parts {
		part, ok := rawPart.(map[string]any)
		if !ok {
			return nil, status.Error(codes.FailedPrecondition, "session message projection part is malformed")
		}
		partID, partIDOK := part["id"].(string)
		partMessageID, partMessageIDOK := part["messageId"].(string)
		partSessionID, partSessionIDOK := part["sessionId"].(string)
		partSequence, partSequenceOK := declarationJSONInt64(part["sequence"])
		partCreatedAt, partCreatedAtOK := part["createdAt"].(string)
		partUpdatedAt, partUpdatedAtOK := part["updatedAt"].(string)
		if !partIDOK || partID == "" ||
			!partMessageIDOK || partMessageID != messageID ||
			!partSessionIDOK || partSessionID != sessionID ||
			!partSequenceOK || partSequence != int64(index) ||
			!partCreatedAtOK || partCreatedAt == "" ||
			!partUpdatedAtOK || partUpdatedAt == "" {
			return nil, status.Error(codes.FailedPrecondition, "session message projection part stamp is invalid")
		}
	}
	message["id"] = messageID
	message["sessionId"] = sessionID
	message["sequence"] = messageSequence
	message["owningEventId"] = owningEventID
	message["eventSequence"] = eventSequence
	message["createdAt"] = createdAt.UTC().Format(time.RFC3339Nano)
	message["updatedAt"] = updatedAt.UTC().Format(time.RFC3339Nano)
	stamped, err := json.Marshal(message)
	if err != nil {
		return nil, err
	}
	return stamped, nil
}

func declarationJSONInt64(value any) (int64, bool) {
	number, ok := value.(float64)
	if !ok || number != float64(int64(number)) {
		return 0, false
	}
	return int64(number), true
}

func loadThreadMetadataForContextTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
) (bridgeLoadContextThread, error) {
	var thread bridgeLoadContextThread
	var parentThreadID sql.NullString
	var taskName sql.NullString
	if err := tx.QueryRow(ctx,
		`SELECT parent_thread_id, role, visibility, task_name, agent_type, status
		   FROM session_threads
		  WHERE workspace_id=$1 AND session_id=$2 AND id=$3`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	).Scan(&parentThreadID, &thread.Role, &thread.Visibility, &taskName, &thread.AgentType, &thread.Status); err != nil {
		return bridgeLoadContextThread{}, err
	}
	if parentThreadID.Valid {
		thread.ParentThreadID = &parentThreadID.String
	}
	if taskName.Valid {
		thread.TaskName = &taskName.String
	}
	return thread, nil
}

func loadThreadPendingAgentMailTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	sourceThreadID string,
) ([]bridgeLoadContextAgentMail, error) {
	query := `SELECT sent.payload_json::jsonb ->> 'delivery_id',
		        sent.payload_json::jsonb ->> 'source_thread_id',
		        sent.payload_json::jsonb ->> 'source_tool_use_event_id',
		        sent.payload_json::jsonb -> 'message'
		   FROM session_events sent
		   JOIN session_threads source
		     ON source.workspace_id=sent.workspace_id
		    AND source.session_id=sent.session_id
		    AND source.id=sent.payload_json::jsonb ->> 'source_thread_id'
		    AND source.parent_thread_id=$3
		    AND source.role='subagent'
			  WHERE sent.workspace_id=$1
			    AND sent.session_id=$2
			    AND sent.type='agent.thread_message_sent'
			    AND sent.payload_json::jsonb ->> 'target_thread_id'=$3
			    AND sent.payload_json::jsonb ->> 'source_thread_id'=$4
			    AND (
			      NOT EXISTS (
			        SELECT 1
			          FROM session_events received
			         WHERE received.workspace_id=sent.workspace_id
			           AND received.session_id=sent.session_id
			           AND received.session_thread_id=$3
			           AND received.type='agent.thread_message_received'
			           AND received.payload_json::jsonb ->> 'delivery_id'=sent.payload_json::jsonb ->> 'delivery_id'
			      )
			      OR NOT EXISTS (
			        SELECT 1
			          FROM session_events child_received
			         WHERE child_received.workspace_id=sent.workspace_id
			           AND child_received.session_id=sent.session_id
			           AND child_received.session_thread_id=$4
			           AND child_received.type='agent.thread_message_received'
			           AND child_received.sequence > sent.sequence
			      )
			    )
			  ORDER BY
		    CASE WHEN NOT EXISTS (
			SELECT 1
			  FROM session_events received
			 WHERE received.workspace_id=sent.workspace_id
			   AND received.session_id=sent.session_id
			   AND received.session_thread_id=$3
			   AND received.type='agent.thread_message_received'
			   AND received.payload_json::jsonb ->> 'delivery_id'=sent.payload_json::jsonb ->> 'delivery_id'
		    ) THEN 0 ELSE 1 END ASC,
		    CASE WHEN NOT EXISTS (
			SELECT 1
			  FROM session_events received
			 WHERE received.workspace_id=sent.workspace_id
			   AND received.session_id=sent.session_id
			   AND received.session_thread_id=$3
			   AND received.type='agent.thread_message_received'
			   AND received.payload_json::jsonb ->> 'delivery_id'=sent.payload_json::jsonb ->> 'delivery_id'
		    ) THEN sent.sequence END ASC,
		    CASE WHEN EXISTS (
			SELECT 1
			  FROM session_events received
			 WHERE received.workspace_id=sent.workspace_id
			   AND received.session_id=sent.session_id
			   AND received.session_thread_id=$3
			   AND received.type='agent.thread_message_received'
			   AND received.payload_json::jsonb ->> 'delivery_id'=sent.payload_json::jsonb ->> 'delivery_id'
		    ) THEN sent.sequence END DESC,
		    sent.event_id DESC
		  LIMIT 1`
	if sourceThreadID == "" {
		query = `SELECT sent.payload_json::jsonb ->> 'delivery_id',
			        sent.payload_json::jsonb ->> 'source_thread_id',
			        sent.payload_json::jsonb ->> 'source_tool_use_event_id',
			        sent.payload_json::jsonb -> 'message'
			   FROM session_events sent
			   JOIN session_threads source
			     ON source.workspace_id=sent.workspace_id
			    AND source.session_id=sent.session_id
			    AND source.id=sent.payload_json::jsonb ->> 'source_thread_id'
			    AND source.parent_thread_id=$3
			    AND source.role='subagent'
			  WHERE sent.workspace_id=$1
			    AND sent.session_id=$2
			    AND sent.type='agent.thread_message_sent'
			    AND sent.payload_json::jsonb ->> 'target_thread_id'=$3
			    AND NOT EXISTS (
				SELECT 1
				  FROM session_events received
				 WHERE received.workspace_id=sent.workspace_id
				   AND received.session_id=sent.session_id
				   AND received.session_thread_id=$3
				   AND received.type='agent.thread_message_received'
				   AND received.payload_json::jsonb ->> 'delivery_id'=sent.payload_json::jsonb ->> 'delivery_id'
			    )
			  ORDER BY sent.sequence ASC, sent.event_id ASC
			  LIMIT 4`
	}
	queryArgs := []any{
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	}
	if sourceThreadID != "" {
		queryArgs = append(queryArgs, sourceThreadID)
	}
	rows, err := tx.Query(ctx, query, queryArgs...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	pending := make([]bridgeLoadContextAgentMail, 0)
	bodyBytes := 0
	for rows.Next() {
		var mail bridgeLoadContextAgentMail
		if err := rows.Scan(&mail.DeliveryID, &mail.SourceThreadID, &mail.SourceToolUseEventID, &mail.Message); err != nil {
			return nil, err
		}
		if mail.DeliveryID == "" || mail.SourceThreadID == "" || mail.SourceToolUseEventID == "" || len(mail.Message) == 0 || !json.Valid(mail.Message) {
			return nil, status.Error(codes.FailedPrecondition, "pending agent mail is malformed")
		}
		if sourceThreadID == "" && len(pending) > 0 && bodyBytes+len(mail.Message) > MailFetchMaxBodyBytes {
			break
		}
		pending = append(pending, mail)
		bodyBytes += len(mail.Message)
	}
	return pending, rows.Err()
}

func compactionProjectionForLoad(raw string, sequence int64) (string, error) {
	var projection map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &projection); err != nil || projection == nil {
		return "", status.Error(codes.FailedPrecondition, "compaction projection is malformed")
	}
	encodedSequence, err := json.Marshal(sequence)
	if err != nil {
		return "", err
	}
	projection["sequence"] = encodedSequence
	return marshalBridgeJSON(projection)
}

const loadThreadPendingFileAttachmentsSQL = `WITH media_events AS MATERIALIZED (
		SELECT event_id, payload_json
		  FROM session_events
		 WHERE workspace_id = $1
		   AND session_id = $2
		   AND session_thread_id = $3
		   AND type = 'user.message'
		   AND payload_json::jsonb @? '$.content[*] ? (@.type == "image" || @.type == "document")'
	),
	ordered_pairs AS (
		SELECT e.event_id,
		       block.value->'source'->>'file_id' AS file_id,
		       f.mime_type,
		       f.filename,
		       projected.sequence AS message_sequence,
		       block.position,
		       ROW_NUMBER() OVER (
		           PARTITION BY e.event_id, block.value->'source'->>'file_id'
		           ORDER BY block.position ASC
		       ) AS pair_rank
		  FROM media_events e
		  JOIN LATERAL (
		      SELECT m.sequence
		        FROM session_messages m
		       WHERE m.workspace_id = $1
		         AND m.session_id = $2
		         AND m.session_thread_id = $3
		         AND m.source_event_id = e.event_id
		       LIMIT 1
		  ) projected ON TRUE
		  CROSS JOIN LATERAL jsonb_array_elements(e.payload_json::jsonb->'content')
		       WITH ORDINALITY AS block(value, position)
		   LEFT JOIN files f
		     ON f.workspace_id = $1
		    AND f.file_id = block.value->'source'->>'file_id'
		  WHERE block.value->>'type' IN ('image', 'document')
		    AND block.value->'source'->>'type' = 'file'
	)
	SELECT p.event_id, p.file_id, p.mime_type, p.filename
	  FROM ordered_pairs p
	 WHERE p.pair_rank = 1
	   AND NOT EXISTS (
	       SELECT 1
	         FROM session_file_attachment_consumptions c
	        WHERE c.workspace_id = $1
	          AND c.session_id = $2
	          AND c.session_thread_id = $3
	          AND c.source_event_id = p.event_id
	          AND c.file_id = p.file_id
	   )
	 ORDER BY p.message_sequence ASC, p.position ASC`

func loadThreadPendingAttachmentsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
) ([]bridgeLoadContextPendingAttachment, error) {
	pending := make([]bridgeLoadContextPendingAttachment, 0)
	transientRows, err := tx.Query(ctx,
		`SELECT attachment_ref, source_tool_use_event_id, mime, metadata_json
		   FROM session_transient_attachments
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND status = 'active'
		  ORDER BY storage_sequence ASC`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	)
	if err != nil {
		return nil, err
	}
	for transientRows.Next() {
		var attachmentRef, sourceToolUseEventID, mime, metadataJSON string
		if err := transientRows.Scan(&attachmentRef, &sourceToolUseEventID, &mime, &metadataJSON); err != nil {
			_ = transientRows.Close()
			return nil, err
		}
		var metadata transientAttachmentMetadata
		if err := json.Unmarshal([]byte(defaultString(metadataJSON, "{}")), &metadata); err != nil {
			_ = transientRows.Close()
			return nil, status.Error(codes.FailedPrecondition, "transient attachment metadata is malformed")
		}
		pending = append(pending, bridgeLoadContextPendingAttachment{
			Origin: bridgeLoadContextAttachmentOrigin{Transient: &bridgeLoadContextTransientAttachment{
				AttachmentRef:        attachmentRef,
				SourceToolUseEventID: sourceToolUseEventID,
				SourcePath:           metadata.SourcePath,
				PageRange:            metadata.PageRange,
				Detail:               metadata.Detail,
			}},
			Mime:     mime,
			Filename: metadata.Filename,
		})
	}
	if err := transientRows.Err(); err != nil {
		_ = transientRows.Close()
		return nil, err
	}
	if err := transientRows.Close(); err != nil {
		return nil, err
	}

	fileRows, err := tx.Query(ctx,
		loadThreadPendingFileAttachmentsSQL,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = fileRows.Close() }()
	for fileRows.Next() {
		var sourceEventID, fileID string
		var mime, filename sql.NullString
		if err := fileRows.Scan(&sourceEventID, &fileID, &mime, &filename); err != nil {
			return nil, err
		}
		pending = append(pending, bridgeLoadContextPendingAttachment{
			Origin: bridgeLoadContextAttachmentOrigin{FileBacked: &bridgeLoadContextFileAttachment{
				SourceEventID: sourceEventID,
				FileID:        fileID,
			}},
			Mime:     mime.String,
			Filename: filename.String,
		})
	}
	if err := fileRows.Err(); err != nil {
		return nil, err
	}
	return pending, nil
}

func loadSessionMCPManifestsTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) ([]bridgeLoadContextMCPManifest, error) {
	rows, err := tx.Query(ctx,
		`SELECT mcp_server_name, tools_json, manifest_etag, manifest_generation, readiness, diagnostic
		   FROM session_mcp_manifests
		  WHERE workspace_id = $1 AND session_id = $2
		  ORDER BY mcp_server_name ASC`, scope.GetWorkspaceId(), scope.GetSessionId())
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	manifests := make([]bridgeLoadContextMCPManifest, 0)
	for rows.Next() {
		var manifest bridgeLoadContextMCPManifest
		var toolsJSON sql.NullString
		var manifestETag sql.NullString
		var diagnostic sql.NullString
		if err := rows.Scan(&manifest.MCPServerName, &toolsJSON, &manifestETag, &manifest.ManifestGeneration, &manifest.Readiness, &diagnostic); err != nil {
			return nil, err
		}
		manifest.Tools = make([]bridgeLoadContextMCPTool, 0)
		if diagnostic.Valid {
			manifest.Diagnostic = &diagnostic.String
		}
		if manifest.Readiness == mcpManifestReadinessUnready {
			manifests = append(manifests, manifest)
			continue
		}
		if manifest.Readiness != mcpManifestReadinessReady || !toolsJSON.Valid || !manifestETag.Valid {
			return nil, status.Error(codes.FailedPrecondition, "stored mcp manifest readiness is malformed")
		}
		manifest.ManifestETag = manifestETag.String
		var storedTools []struct {
			Name        string          `json:"name"`
			Description string          `json:"description"`
			InputSchema json.RawMessage `json:"input_schema"`
		}
		if err := json.Unmarshal([]byte(toolsJSON.String), &storedTools); err != nil {
			return nil, status.Error(codes.FailedPrecondition, "stored mcp manifest is malformed")
		}
		manifest.Tools = make([]bridgeLoadContextMCPTool, 0, len(storedTools))
		for _, tool := range storedTools {
			if tool.Name == "" || len(tool.InputSchema) == 0 || !json.Valid(tool.InputSchema) {
				return nil, status.Error(codes.FailedPrecondition, "stored mcp manifest is malformed")
			}
			manifest.Tools = append(manifest.Tools, bridgeLoadContextMCPTool{
				Name: tool.Name, Description: tool.Description, InputSchema: tool.InputSchema,
			})
		}
		manifests = append(manifests, manifest)
	}
	return manifests, rows.Err()
}

func loadThreadBackgroundToolsTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) ([]bridgeLoadContextBackgroundTool, error) {
	rows, err := tx.Query(ctx,
		`SELECT task_id, source_tool_use_event_id
		   FROM session_background_tasks
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND binding_id = $4
		    AND status = 'running'
		  ORDER BY created_at ASC, task_id ASC`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		scope.GetBinding().GetBindingId(),
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	backgroundTools := make([]bridgeLoadContextBackgroundTool, 0)
	for rows.Next() {
		var task bridgeLoadContextBackgroundTool
		if err := rows.Scan(&task.TaskID, &task.SourceToolUseEventID); err != nil {
			return nil, err
		}
		backgroundTools = append(backgroundTools, task)
	}
	return backgroundTools, rows.Err()
}

func loadThreadRuntimeConfigTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) (bridgeLoadContextRuntimeConfig, error) {
	var (
		agentID          string
		agentVersion     int64
		agentConfigHash  string
		environmentID    string
		configGeneration int64
		approvalMode     string
		installedTools   string
		agentConfig      string
		envGeneration    int64
		envConfig        string
		skillsIndex      string
	)
	err := tx.QueryRow(ctx,
		`SELECT s.agent_id,
		        s.agent_version,
		        s.agent_config_hash,
		        s.environment_id,
		        s.config_generation,
		        s.approval_mode,
		        s.installed_tools_json,
		        av.config_json,
		        e.current_generation,
		        e.config_json,
		        COALESCE((
		            SELECT p.skills_index_json
		              FROM session_preparations p
		             WHERE p.workspace_id = s.workspace_id
		               AND p.session_id = s.id
		               AND p.status = 'ready'
		               AND p.superseded_at IS NULL
		             ORDER BY p.created_at DESC, p.preparation_attempt_id DESC
		             LIMIT 1
		        ), '[]')
		   FROM sessions s
		   JOIN agent_versions av
		     ON av.workspace_id = s.workspace_id
		    AND av.id = s.agent_version_id
		   JOIN environments e
		     ON e.workspace_id = s.workspace_id
		    AND e.id = s.environment_id
		  WHERE s.workspace_id = $1
		    AND s.id = $2`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
	).Scan(
		&agentID,
		&agentVersion,
		&agentConfigHash,
		&environmentID,
		&configGeneration,
		&approvalMode,
		&installedTools,
		&agentConfig,
		&envGeneration,
		&envConfig,
		&skillsIndex,
	)
	if err != nil {
		return bridgeLoadContextRuntimeConfig{}, err
	}
	agentConfigRaw := bridgeRawJSON(agentConfig, "{}")
	memoryStores, err := bridgeRuntimeMemoryStoresTx(ctx, tx, scope.GetWorkspaceId(), scope.GetSessionId())
	if err != nil {
		return bridgeLoadContextRuntimeConfig{}, err
	}
	settings, err := bridgeRuntimeSessionAgentSettings(approvalMode, agentConfig, installedTools, memoryStores)
	if err != nil {
		return bridgeLoadContextRuntimeConfig{}, status.Error(codes.Internal, "api_error")
	}
	if _, err := bridgeInstalledBuiltinFamily(settings.Config); err != nil {
		return bridgeLoadContextRuntimeConfig{}, status.Error(codes.Internal, "api_error")
	}
	installedToolDeclarations, err := json.Marshal(settings.Config.Tools)
	if err != nil {
		return bridgeLoadContextRuntimeConfig{}, status.Error(codes.Internal, "api_error")
	}
	return bridgeLoadContextRuntimeConfig{
		ConfigGeneration: configGeneration,
		ApprovalMode:     approvalMode,
		System:           settings.System,
		MemoryStores:     settings.MemoryStores,
		Agent: bridgeLoadAgent{
			ID:         agentID,
			Version:    agentVersion,
			ConfigHash: agentConfigHash,
			Config:     agentConfigRaw,
		},
		Environment: bridgeLoadEnv{
			ID:                environmentID,
			CurrentGeneration: envGeneration,
			Config:            bridgeRawJSON(envConfig, "{}"),
		},
		ToolPolicy:     settings.ToolPolicy,
		Skills:         bridgeJSONFieldRaw(agentConfigRaw, "skills", "[]"),
		SkillsIndex:    bridgeRawJSON(skillsIndex, "[]"),
		InstalledTools: json.RawMessage(installedToolDeclarations),
	}, nil
}

func loadThreadPendingToolUsesTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) ([]bridgeLoadContextPendingTool, error) {
	rows, err := tx.Query(ctx,
		`SELECT p.tool_use_event_id,
		        COALESCE(e.model_request_id, ''),
		        p.model_tool_call_id,
		        p.tool_name,
		        p.kind,
		        p.input_json,
		        p.decision,
		        p.deny_message,
		        p.status,
		        p.expires_at
		   FROM session_pending_tool_uses p
		   LEFT JOIN session_events e
		     ON e.workspace_id = p.workspace_id
		    AND e.session_id = p.session_id
		    AND e.session_thread_id = p.session_thread_id
		    AND e.event_id = p.tool_use_event_id
		  WHERE p.workspace_id = $1
		    AND p.session_id = $2
		    AND p.session_thread_id = $3
		    AND p.status IN ('pending', 'resolving')
		  ORDER BY e.sequence ASC, p.tool_use_event_id ASC`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	pending := make([]bridgeLoadContextPendingTool, 0)
	for rows.Next() {
		var item bridgeLoadContextPendingTool
		var inputJSON string
		var decision sql.NullString
		var denyMessage sql.NullString
		if err := rows.Scan(
			&item.ToolUseEventID,
			&item.ModelRequestID,
			&item.ModelToolCallID,
			&item.ToolName,
			&item.Kind,
			&inputJSON,
			&decision,
			&denyMessage,
			&item.Status,
			&item.ExpiresAt,
		); err != nil {
			return nil, err
		}
		if item.ModelRequestID == "" {
			return nil, status.Error(codes.FailedPrecondition, "pending tool use has no model request id")
		}
		item.Input = bridgeRawJSON(inputJSON, "{}")
		if decision.Valid {
			item.Decision = &decision.String
		}
		if denyMessage.Valid {
			item.DenyMessage = &denyMessage.String
		}
		pending = append(pending, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return pending, nil
}

type runtimeBindingTokenPayload struct {
	Version           int    `json:"v"`
	WorkspaceID       string `json:"workspace_id"`
	SessionID         string `json:"session_id"`
	SessionThreadID   string `json:"session_thread_id"`
	BindingID         string `json:"binding_id"`
	BindingGeneration int64  `json:"binding_generation"`
	RuntimePodUID     string `json:"runtime_pod_uid"`
	ExpiresAtUnix     int64  `json:"exp"`
}

func (s *PostgreSQLBridgeAPIStore) runtimeBindingToken(scope *bridgev1.RuntimeScope) (string, error) {
	if scope == nil || scope.GetBinding() == nil ||
		scope.GetWorkspaceId() == "" ||
		scope.GetSessionId() == "" ||
		scope.GetSessionThreadId() == "" ||
		scope.GetBinding().GetBindingId() == "" ||
		scope.GetBinding().GetBindingGeneration() <= 0 ||
		scope.GetBinding().GetTargetPodUid() == "" {
		return "", status.Error(codes.FailedPrecondition, "runtime binding scope is incomplete")
	}
	if len(s.RuntimeBindingTokenHMACKey) == 0 {
		return "", status.Error(codes.FailedPrecondition, "runtime binding token signer is unavailable")
	}
	ttl := s.RuntimeBindingTokenTTL
	if ttl <= 0 {
		ttl = defaultRuntimeBindingTokenTTL
	}
	payload, err := marshalBridgeJSON(runtimeBindingTokenPayload{
		Version:           1,
		WorkspaceID:       scope.GetWorkspaceId(),
		SessionID:         scope.GetSessionId(),
		SessionThreadID:   scope.GetSessionThreadId(),
		BindingID:         scope.GetBinding().GetBindingId(),
		BindingGeneration: scope.GetBinding().GetBindingGeneration(),
		RuntimePodUID:     scope.GetBinding().GetTargetPodUid(),
		ExpiresAtUnix:     s.now().Add(ttl).Unix(),
	})
	if err != nil {
		return "", err
	}
	payloadPart := base64.RawURLEncoding.EncodeToString([]byte(payload))
	mac := hmac.New(sha256.New, s.RuntimeBindingTokenHMACKey)
	_, _ = mac.Write([]byte(payloadPart))
	signaturePart := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return "rtbt_v1." + payloadPart + "." + signaturePart, nil
}

func sha256Hex(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}
