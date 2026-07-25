package environment

import (
	"testing"

	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestEnvironmentPageTokenRejectsWrongResourceKind(t *testing.T) {
	token, err := encodeEnvironmentPageToken(environmentPageToken{
		Version:         1,
		Resource:        "sessions",
		WorkspaceID:     string(workspace.DefaultID),
		IncludeArchived: false,
		LastSequence:    10,
	})
	if err != nil {
		t.Fatalf("encodeEnvironmentPageToken: %v", err)
	}

	_, err = decodeEnvironmentPageToken(token, workspace.DefaultID, false)
	if err == nil {
		t.Fatal("token with non-environments resource kind must reject")
	}
	if _, ok := err.(*ValidationError); !ok {
		t.Fatalf("expected *ValidationError, got %T (%v)", err, err)
	}
}
