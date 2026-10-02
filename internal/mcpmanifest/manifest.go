package mcpmanifest

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"log/slog"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// UpdatePayload builds the manifest patch that rides a
// runtime_config_update job. That job kind is shared, but the MCP manifest path
// must NOT reuse config_generation: config_generation is owned exclusively by
// api session admission, and a GitHub-driven manifest change never passes
// through api. The payload therefore carries manifest_generation only, and
// Runtime applies the patch independently of config_generation, gated solely on
// manifest_generation monotonicity. Incrementing config_generation on this path
// would collapse the two-generation separation.
func UpdatePayload(workspaceID string, sessionID string, mcpServerName string, manifestGeneration int64) (string, error) {
	payload, err := json.Marshal(map[string]any{
		"workspace_id":        workspaceID,
		"session_id":          sessionID,
		"mcp_server_name":     mcpServerName,
		"manifest_generation": manifestGeneration,
	})
	if err != nil {
		return "", err
	}
	return string(payload), nil
}

func CommandPayload(workspaceID string, sessionID string, mcpServerName string, row Row) (string, error) {
	manifest := map[string]any{
		"mcp_server_name":     mcpServerName,
		"manifest_generation": row.Generation,
		"readiness":           row.Readiness,
		"diagnostic":          nil,
	}
	if row.Readiness == ReadinessReady {
		if !row.ToolsJSON.Valid || !row.ManifestETag.Valid || !json.Valid([]byte(row.ToolsJSON.String)) {
			return "", status.Error(codes.Internal, "mcp manifest ready content is invalid")
		}
		manifest["manifest_etag"] = row.ManifestETag.String
		manifest["tools"] = json.RawMessage(row.ToolsJSON.String)
	} else {
		if !row.Diagnostic.Valid || row.Diagnostic.String == "" {
			return "", status.Error(codes.Internal, "mcp manifest unready diagnostic is invalid")
		}
		manifest["diagnostic"] = row.Diagnostic.String
	}
	payload, err := runtimecontrol.MarshalDataJSON(map[string]any{
		"workspace_id": workspaceID,
		"session_id":   sessionID,
		"mcp_manifest": manifest,
	})
	if err != nil {
		return "", err
	}
	return payload, nil
}

type Acceptance struct {
	RuntimeInputID     string
	PreviousGeneration int64
	Generation         int64
	Readiness          string
	Diagnostic         string
	QueueCustody       string
	BuiltinFamily      string
	Omissions          []Omission
	Duplicate          bool
	Transitioned       bool
}

type Row struct {
	ToolsJSON    sql.NullString
	ManifestETag sql.NullString
	Generation   int64
	Readiness    string
	Diagnostic   sql.NullString
}

func LoadRowForUpdateTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, mcpServerName string) (Row, bool, error) {
	var row Row
	err := tx.QueryRow(ctx, `SELECT tools_json, manifest_etag, manifest_generation, readiness, diagnostic
		FROM session_mcp_manifests
		WHERE workspace_id = $1 AND session_id = $2 AND mcp_server_name = $3
		FOR UPDATE`, workspaceID, sessionID, mcpServerName).Scan(
		&row.ToolsJSON, &row.ManifestETag, &row.Generation, &row.Readiness, &row.Diagnostic,
	)
	if dbconnect.IsNoRows(err) {
		return Row{}, false, nil
	}
	return row, err == nil, err
}

func CaptureAcceptanceTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	mcpServerName string,
	manifestETag string,
	tools []Tool,
	now time.Time,
) (Acceptance, error) {
	if err := AcquireAcceptanceLockTx(ctx, tx, workspaceID, sessionID, mcpServerName); err != nil {
		return Acceptance{}, err
	}
	if _, err := runtimecontrol.LockMainThreadIDTx(ctx, tx, workspaceID, sessionID); err != nil {
		return Acceptance{}, err
	}
	toolsetConfig, err := ToolsetConfigTx(ctx, tx, workspaceID, sessionID, mcpServerName)
	if err != nil {
		return Acceptance{}, err
	}
	filteredTools, omissions := FilterCollisions(toolsetConfig.BuiltinFamily, tools)
	toolsJSON, err := CanonicalToolsJSON(filteredTools)
	if err != nil {
		return Acceptance{}, err
	}
	current, rowExists, err := LoadRowForUpdateTx(ctx, tx, workspaceID, sessionID, mcpServerName)
	if err != nil {
		return Acceptance{}, err
	}
	if len([]byte(toolsJSON)) > MaxBytes {
		transitioned := !rowExists || current.Readiness != ReadinessUnready || !current.Diagnostic.Valid || current.Diagnostic.String != DiagnosticTooLarge
		generation, err := TransitionUnreadyTx(ctx, tx, workspaceID, sessionID, mcpServerName, current, rowExists, DiagnosticTooLarge, toolsetConfig, now)
		return Acceptance{
			PreviousGeneration: current.Generation,
			Generation:         generation,
			Readiness:          ReadinessUnready,
			Diagnostic:         DiagnosticTooLarge,
			QueueCustody:       "created",
			BuiltinFamily:      toolsetConfig.BuiltinFamily,
			Duplicate:          !transitioned,
			Transitioned:       transitioned,
		}, err
	}
	if rowExists && current.ManifestETag.Valid && current.ManifestETag.String == manifestETag && current.Readiness == ReadinessReady {
		return Acceptance{
			RuntimeInputID: InputID(sessionID, mcpServerName, current.Generation),
			Generation:     current.Generation, BuiltinFamily: toolsetConfig.BuiltinFamily, Duplicate: true,
		}, nil
	}
	generation := int64(1)
	if rowExists {
		generation = current.Generation + 1
	}
	acceptance, err := CommitReadyTx(ctx, tx, workspaceID, sessionID, mcpServerName, manifestETag, toolsJSON, generation, toolsetConfig, now)
	acceptance.PreviousGeneration = current.Generation
	acceptance.Readiness = ReadinessReady
	acceptance.QueueCustody = "created"
	acceptance.Transitioned = true
	acceptance.BuiltinFamily = toolsetConfig.BuiltinFamily
	acceptance.Omissions = omissions
	return acceptance, err
}

func CommitReadyTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, mcpServerName string, manifestETag string, toolsJSON string, generation int64, toolsetConfig ToolsetConfig, now time.Time) (Acceptance, error) {
	sessionThreadID, err := runtimecontrol.LockMainThreadIDTx(ctx, tx, workspaceID, sessionID)
	if err != nil {
		return Acceptance{}, err
	}
	payloadJSON, err := UpdatePayload(workspaceID, sessionID, mcpServerName, generation)
	if err != nil {
		return Acceptance{}, err
	}
	formattedNow := now
	_, err = tx.Exec(ctx, `INSERT INTO session_mcp_manifests (
		workspace_id, session_id, mcp_server_name, tools_json, manifest_etag,
		manifest_generation, readiness, diagnostic, created_at, updated_at
	) VALUES ($1, $2, $3, $4, $5, $6, 'ready', NULL, $7, $7)
	ON CONFLICT (workspace_id, session_id, mcp_server_name) DO UPDATE SET
		tools_json = EXCLUDED.tools_json, manifest_etag = EXCLUDED.manifest_etag,
		manifest_generation = EXCLUDED.manifest_generation, readiness = 'ready',
		diagnostic = NULL, updated_at = EXCLUDED.updated_at`,
		workspaceID, sessionID, mcpServerName, toolsJSON, manifestETag, generation, formattedNow)
	if err != nil {
		return Acceptance{}, err
	}
	if err := enqueueUpdateTx(ctx, tx, workspaceID, sessionID, mcpServerName, generation, payloadJSON, now); err != nil {
		return Acceptance{}, err
	}
	runtimeInputID := InputID(sessionID, mcpServerName, generation)
	if err := runtimecontrol.InsertOperationTx(ctx, tx, runtimecontrol.SessionScope(workspaceID, sessionID, sessionThreadID), runtimecontrol.OperationInsert{
		Operation:      OperationChanged,
		IdempotencyKey: mcpServerName + ":" + strconv.FormatInt(generation, 10),
		RequestHash:    runtimecontrol.RequestHash(OperationChanged, workspaceID, sessionID, mcpServerName, manifestETag, strconv.FormatInt(generation, 10)),
		AckStatus:      runtimecontrol.AckCommitted,
		RuntimeInputID: sql.NullString{String: runtimeInputID, Valid: true},
		Now:            now,
	}); err != nil {
		return Acceptance{}, err
	}
	return Acceptance{
		RuntimeInputID: runtimeInputID, Generation: generation,
	}, nil
}

func TransitionUnreadyTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, mcpServerName string, current Row, rowExists bool, diagnostic string, toolsetConfig ToolsetConfig, now time.Time) (int64, error) {
	return transitionUnreadyWithDeliveryTx(
		ctx, tx, workspaceID, sessionID, mcpServerName, current, rowExists, diagnostic, toolsetConfig, now, true,
	)
}

func TransitionDeliveryExhaustedTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, mcpServerName string, current Row, toolsetConfig ToolsetConfig, now time.Time) (int64, error) {
	// The leased job that exhausted delivery remains the queue barrier and
	// carries the new unready generation. A replacement job would move the
	// barrier behind later Session input.
	return transitionUnreadyWithDeliveryTx(
		ctx, tx, workspaceID, sessionID, mcpServerName, current, true, DiagnosticDeliveryExhausted, toolsetConfig, now, false,
	)
}

func transitionUnreadyWithDeliveryTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, mcpServerName string, current Row, rowExists bool, diagnostic string, toolsetConfig ToolsetConfig, now time.Time, enqueueDelivery bool) (int64, error) {
	if rowExists && current.Readiness == ReadinessUnready && current.Diagnostic.Valid && current.Diagnostic.String == diagnostic {
		return current.Generation, nil
	}
	generation := int64(1)
	if rowExists {
		generation = current.Generation + 1
	}
	row := current
	row.Generation = generation
	row.Readiness = ReadinessUnready
	row.Diagnostic = sql.NullString{String: diagnostic, Valid: true}
	formattedNow := now
	if !rowExists {
		_, err := tx.Exec(ctx, `INSERT INTO session_mcp_manifests
			(workspace_id, session_id, mcp_server_name, tools_json, manifest_etag, manifest_generation, readiness, diagnostic, created_at, updated_at)
			VALUES ($1, $2, $3, NULL, NULL, $4, 'unready', $5, $6, $6)`, workspaceID, sessionID, mcpServerName, generation, diagnostic, formattedNow)
		if err != nil {
			return 0, err
		}
	} else {
		_, err := tx.Exec(ctx, `UPDATE session_mcp_manifests
			SET manifest_generation = $4, readiness = 'unready', diagnostic = $5, updated_at = $6
			WHERE workspace_id = $1 AND session_id = $2 AND mcp_server_name = $3`, workspaceID, sessionID, mcpServerName, generation, diagnostic, formattedNow)
		if err != nil {
			return 0, err
		}
	}
	if enqueueDelivery {
		payloadJSON, err := UpdatePayload(workspaceID, sessionID, mcpServerName, generation)
		if err != nil {
			return 0, err
		}
		if err := enqueueUpdateTx(ctx, tx, workspaceID, sessionID, mcpServerName, generation, payloadJSON, now); err != nil {
			return 0, err
		}
	}
	return generation, nil
}

func AcquireAcceptanceLockTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, mcpServerName string) error {
	if err := runtimecontrol.LockRuntimeMutationSessionTx(ctx, tx, workspaceID, sessionID); err != nil {
		return err
	}
	var sessionStatus string
	if err := tx.QueryRow(ctx,
		`SELECT status FROM sessions WHERE workspace_id=$1 AND id=$2`,
		workspaceID,
		sessionID,
	).Scan(&sessionStatus); err != nil {
		return err
	}
	if sessionStatus == "terminated" {
		return runtimecontrol.ScopeSupersededError(status.Error(codes.FailedPrecondition, "runtime session is terminal"))
	}
	sum := sha256.Sum256([]byte(workspaceID + "\x00" + sessionID + "\x00" + mcpServerName))
	resource := int32(binary.BigEndian.Uint32(sum[:4]))
	_, err := tx.Exec(ctx,
		"SELECT pg_advisory_xact_lock($1, $2)",
		acceptanceLockCategory,
		resource,
	)
	return err
}

type Omission struct {
	ToolName string
}

func FilterCollisions(family string, tools []Tool) ([]Tool, []Omission) {
	blocked := make(map[string]struct{}, 6)
	switch family {
	case "claude":
		for _, name := range []string{"Bash", "Read", "Write", "Edit", "Glob", "Grep"} {
			blocked[name] = struct{}{}
		}
	case "gpt":
		for _, name := range []string{"exec_command", "write_stdin", "view_image", "apply_patch"} {
			blocked[name] = struct{}{}
		}
	}
	filtered := make([]Tool, 0, len(tools))
	omissions := make([]Omission, 0)
	for _, tool := range tools {
		if _, collision := blocked[tool.Name]; collision {
			omissions = append(omissions, Omission{ToolName: tool.Name})
			continue
		}
		filtered = append(filtered, tool)
	}
	return filtered, omissions
}

func LogOmissions(logger *slog.Logger, component string, workspaceID string, sessionID string, mcpServerName string, family string, omissions []Omission) {
	if logger == nil {
		return
	}
	defer func() { _ = recover() }()
	for _, omission := range omissions {
		logger.Warn("bridge.mcp_manifest.tool_omitted",
			slog.String("operation", "mcp_manifest.filter"),
			slog.String("event.kind", "mcp_manifest.tool_omitted"),
			slog.String("component", component),
			slog.String("workspace.id", workspaceID),
			slog.String("session.id", sessionID),
			slog.String("mcp.server.name", mcpServerName),
			slog.String("mcp.tool.name", omission.ToolName),
			slog.String("mcp.tool.family", family),
			slog.String("mcp.omission.reason", "builtin_name_collision"),
		)
	}
}

// The transaction result is the sole source of this event. Keeping the logger
// at the post-commit boundary prevents telemetry from becoming manifest state
// or Queue custody evidence.
func LogTransitionCommitted(logger *slog.Logger, component string, workspaceID string, sessionID string, mcpServerName string, acceptance Acceptance, inputContinued bool) {
	if logger == nil || !acceptance.Transitioned {
		return
	}
	defer func() {
		_ = recover()
	}()
	logger.Info("bridge.mcp_manifest.transition_committed",
		slog.String("operation", "mcp_manifest.transition"),
		slog.String("event.kind", "mcp_manifest_transition_committed"),
		slog.String("component", component),
		slog.String("workspace.id", workspaceID),
		slog.String("session.id", sessionID),
		slog.String("mcp.server.name", mcpServerName),
		slog.Int64("mcp.manifest.previous_generation", acceptance.PreviousGeneration),
		slog.Int64("mcp.manifest.generation", acceptance.Generation),
		slog.String("mcp.manifest.readiness", acceptance.Readiness),
		slog.String("mcp.manifest.diagnostic", acceptance.Diagnostic),
		slog.String("queue.custody", acceptance.QueueCustody),
		slog.Bool("runtime.input.continued", inputContinued),
	)
}

func enqueueUpdateTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, mcpServerName string, manifestGeneration int64, payloadJSON string, now time.Time) error {
	ws := workspace.ID(workspaceID)
	_, err := queue.EnqueueTx(ctx, tx, queue.EnqueueRequest{
		ID:             id.New("qjob_"),
		WorkspaceID:    ws,
		Kind:           queue.KindRuntimeConfigUpdate,
		PartitionKey:   queue.FormatSessionPartitionKey(ws, sessionID),
		DedupeKey:      queue.FormatRuntimeMCPManifestUpdateDedupeKey(ws, sessionID, mcpServerName, strconv.FormatInt(manifestGeneration, 10)),
		PayloadVersion: 2,
		PayloadJSON:    []byte(payloadJSON),
		MaxAttempts:    DeliveryMaxAttempts,
		Now:            now,
	})
	return err
}

// session_mcp_manifests carries readiness and diagnostic ORTHOGONALLY to the
// accepted content (tools_json, manifest_etag):
//
//	readiness   meaning                                writer
//	ready       tools_json/manifest_etag are the       CommitReadyTx
//	            latest accepted, within-cap manifest
//	unready     server's toolset is closed; diagnostic TransitionUnreadyTx
//	            (manifest_too_large | delivery_exhausted)  (content columns untouched)
//
// Rules the transition helpers enforce:
//   - manifest_generation increments on EVERY accepted content change AND every
//     readiness transition; a repeated same-class failure report is an
//     increment-free no-op.
//   - Supersession keys SOLELY on generation monotonicity, never on etag (a
//     flapping A->B->A etag must not clobber newer state).
//   - An over-cap or delivery-exhausted report flips to unready WITHOUT touching
//     the accepted content columns.
//   - A re-notify matching the STORED etag while the row is unready RESTORES ready
//     (generation+1, diagnostic cleared) rather than short-circuiting as a
//     duplicate no-op — otherwise a server reverting to its last-accepted manifest
//     would stay unready forever.
const (
	ReadinessReady              = "ready"
	ReadinessUnready            = "unready"
	DiagnosticTooLarge          = "manifest_too_large"
	DiagnosticDeliveryExhausted = "delivery_exhausted"
	//nolint:gosec // This is a public readiness diagnostic token, not credential material.
	DiagnosticCredentialUnavailable = "credential_unavailable"
	DiagnosticDiscoveryUnavailable  = "discovery_unavailable"
	DiagnosticInvalid               = "manifest_invalid"
	DeliveryMaxAttempts             = 5
)

const acceptanceLockCategory = int32(0x6D63_7061) // "mcpa"
const OperationChanged = "mcp_manifest_changed"
