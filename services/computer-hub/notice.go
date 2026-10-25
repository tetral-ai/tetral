package computerhub

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// Run listens on this pod's connection notice channel
// (instance.ConnectionNoticeChannel of the pod name), reconnecting after a
// failure, until ctx ends; then it closes every stream. For each notice
// naming a stream the pod holds, it reads the connection row and hands it to
// the stream. When the listening connection is in place again after a loss,
// it does so for every stream it holds, since notices sent meanwhile are
// lost.
func (s *Streams) Run(ctx context.Context) error {
	defer s.closeAll()
	listener := queue.PostgreSQLNotificationListener{Client: s.database, Operation: "computer_hub.connection_notices"}
	onReady := func() {
		first := false
		s.listeningOnce.Do(func() { first = true; close(s.listening) })
		if !first {
			for _, stream := range s.held() {
				s.refresh(ctx, stream)
			}
		}
	}
	return queue.RunListener(ctx, listener, instance.ConnectionNoticeChannel(s.podName), onReady, func(payload string) { s.notice(ctx, payload) }, func(error) {
		s.logger.WarnContext(ctx, "connection_notices.listener_disconnected",
			slog.String("operation", "computer_hub.connection_notices"),
			slog.String("error.class", "dependency_unavailable"),
			slog.String("error.code", "notification_listener_disconnected"),
			slog.String("error.message_safe", "connection notice listener disconnected"))
	})
}

// notice handles one payload; a malformed notice and one for a stream this
// pod does not hold are skipped.
func (s *Streams) notice(ctx context.Context, payload string) {
	var notice instance.ConnectionNotice
	if json.Unmarshal([]byte(payload), &notice) != nil || notice.WorkspaceID == "" {
		return
	}
	if stream := s.lookup(notice.ConnectionID); stream != nil && stream.workspace == workspace.ID(notice.WorkspaceID) {
		s.refresh(ctx, stream)
	}
}

// refresh reads stream's connection row in its Workspace and offers it to
// the stream. A failed read offers nothing; the stream keeps its state until
// the next notice.
func (s *Streams) refresh(ctx context.Context, stream *clientStream) {
	var connection instance.Connection
	err := s.database.WithWorkspaceReadOnlyTx(ctx, string(stream.workspace), "computer_hub.connection_notice_read", func(tx *dbconnect.Tx) error {
		var err error
		connection, err = instance.ReadConnection(ctx, tx, stream.workspace, stream.connection)
		return err
	})
	if err == nil {
		stream.offer(connection)
	}
}
