package workspace_test

import (
	"context"
	"errors"
	"testing"

	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestWithContextRoundTrip(t *testing.T) {
	ws := workspace.Workspace{
		ID:        "workspace_alpha",
		Type:      "workspace",
		Name:      "Alpha",
		CreatedAt: "2026-04-28T00:00:00Z",
	}
	ctx := workspace.WithContext(context.Background(), ws)
	got, ok := workspace.FromContext(ctx)
	if !ok {
		t.Fatal("FromContext returned ok=false after WithContext attachment")
	}
	if got != ws {
		t.Errorf("FromContext = %+v; want %+v", got, ws)
	}
}

func TestFromContextEmptyContext(t *testing.T) {
	if _, ok := workspace.FromContext(context.Background()); ok {
		t.Error("FromContext on a bare context must report ok=false")
	}
	if _, ok := workspace.IDFromContext(context.Background()); ok {
		t.Error("IDFromContext on a bare context must report ok=false")
	}
}

func TestMustIDFromContextReturnsErrWhenAbsent(t *testing.T) {
	_, err := workspace.MustIDFromContext(context.Background())
	if err == nil {
		t.Fatal("expected ErrNoWorkspaceInContext for bare context, got nil")
	}
	if !errors.Is(err, workspace.ErrNoWorkspaceInContext) {
		t.Errorf("expected ErrNoWorkspaceInContext, got %v", err)
	}
}

func TestMustIDFromContextReturnsIDWhenPresent(t *testing.T) {
	ctx := workspace.WithContext(context.Background(), workspace.Workspace{ID: "workspace_beta"})
	id, err := workspace.MustIDFromContext(ctx)
	if err != nil {
		t.Fatalf("MustIDFromContext: %v", err)
	}
	if id != workspace.ID("workspace_beta") {
		t.Errorf("MustIDFromContext = %q; want %q", id, "workspace_beta")
	}
}

func TestDefaultIDLiteral(t *testing.T) {
	if workspace.DefaultID != workspace.ID("default") {
		t.Errorf("DefaultID = %q; want %q legacy test fixture literal", workspace.DefaultID, "default")
	}
}
