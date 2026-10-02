package mcpmanifest

import (
	"context"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
)

func CaptureInitialAcceptanceTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	toolset ToolsetConfig,
	manifest ListResult,
	now time.Time,
) (Acceptance, error) {
	if err := AcquireAcceptanceLockTx(ctx, tx, workspaceID, sessionID, toolset.MCPServerName); err != nil {
		return Acceptance{}, err
	}
	current, exists, err := LoadRowForUpdateTx(ctx, tx, workspaceID, sessionID, toolset.MCPServerName)
	if err != nil {
		return Acceptance{}, err
	}
	if exists && current.Readiness == ReadinessReady {
		return Acceptance{PreviousGeneration: current.Generation, Generation: current.Generation, Readiness: ReadinessReady, Duplicate: true}, nil
	}
	filtered, omissions := FilterCollisions(toolset.BuiltinFamily, manifest.Tools)
	toolsJSON, canonicalErr := CanonicalToolsJSON(filtered)
	if strings.TrimSpace(manifest.ManifestETag) == "" || canonicalErr != nil || len([]byte(toolsJSON)) > MaxBytes {
		acceptance, err := CaptureInitialUnreadyTx(ctx, tx, workspaceID, sessionID, toolset, DiagnosticInvalid, now)
		return acceptance, err
	}
	if exists {
		return CaptureAcceptanceTx(ctx, tx, workspaceID, sessionID, toolset.MCPServerName, manifest.ManifestETag, manifest.Tools, now)
	}
	acceptance, err := CommitReadyTx(ctx, tx, workspaceID, sessionID, toolset.MCPServerName, manifest.ManifestETag, toolsJSON, 1, toolset, now)
	acceptance.Readiness = ReadinessReady
	acceptance.QueueCustody = "created"
	acceptance.Transitioned = true
	acceptance.BuiltinFamily = toolset.BuiltinFamily
	acceptance.Omissions = omissions
	return acceptance, err
}

func CaptureInitialUnreadyTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	toolset ToolsetConfig,
	diagnostic string,
	now time.Time,
) (Acceptance, error) {
	if err := AcquireAcceptanceLockTx(ctx, tx, workspaceID, sessionID, toolset.MCPServerName); err != nil {
		return Acceptance{}, err
	}
	current, exists, err := LoadRowForUpdateTx(ctx, tx, workspaceID, sessionID, toolset.MCPServerName)
	if err != nil {
		return Acceptance{}, err
	}
	if exists {
		return Acceptance{PreviousGeneration: current.Generation, Generation: current.Generation, Duplicate: true}, nil
	}
	return CaptureInitialUnreadyLockedTx(ctx, tx, workspaceID, sessionID, toolset, diagnostic, now)
}

func CaptureInitialUnreadyLockedTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	toolset ToolsetConfig,
	diagnostic string,
	now time.Time,
) (Acceptance, error) {
	generation, err := TransitionUnreadyTx(ctx, tx, workspaceID, sessionID, toolset.MCPServerName, Row{}, false, diagnostic, toolset, now)
	return Acceptance{
		Generation: generation, Readiness: ReadinessUnready, Diagnostic: diagnostic,
		QueueCustody: "created", Transitioned: true, BuiltinFamily: toolset.BuiltinFamily,
	}, err
}
