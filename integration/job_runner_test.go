package integration

import (
	"bytes"
	"context"
	"sync"

	"github.com/tetral-ai/tetral/internal/workspace"
)

type staticWorkspaceLister []workspace.ID

func (l staticWorkspaceLister) ListIDs(context.Context) ([]workspace.ID, error) {
	return append([]workspace.ID(nil), l...), nil
}

type lockedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(data)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}
