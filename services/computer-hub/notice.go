package computerhub

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// Run listens on this pod's connection notice channel
// (instance.ConnectionNoticeChannel of the pod name), reconnecting after a
// failure, until ctx ends; then it closes every stream. Each notice naming a
// stream the pod holds raises that stream's bound to the notice's issue_seq
// and wakes it. When the listening connection is in place again after a
// loss, every held stream is woken to write all its connection's unreported
// instructions, since notices sent meanwhile are lost.
func (s *Streams) Run(ctx context.Context) error {
	defer s.closeAll()
	listener := queue.PostgreSQLNotificationListener{Client: s.database, Operation: "computer_hub.connection_notices"}
	onReady := func() {
		first := false
		s.listeningOnce.Do(func() { first = true; close(s.listening) })
		if !first {
			for _, stream := range s.held() {
				stream.release(0, true)
			}
		}
	}
	return queue.RunListener(ctx, listener, instance.ConnectionNoticeChannel(s.podName), onReady, s.notice, func(error) {
		s.logger.WarnContext(ctx, "connection_notices.listener_disconnected",
			slog.String("operation", "computer_hub.connection_notices"),
			slog.String("error.class", "dependency_unavailable"),
			slog.String("error.code", "notification_listener_disconnected"),
			slog.String("error.message_safe", "connection notice listener disconnected"))
	})
}

// notice handles one payload; a malformed notice and one for a stream this
// pod does not hold are skipped.
func (s *Streams) notice(payload string) {
	var notice instance.ConnectionNotice
	if json.Unmarshal([]byte(payload), &notice) != nil || notice.WorkspaceID == "" {
		return
	}
	if stream := s.lookup(notice.ConnectionID); stream != nil && stream.workspace == workspace.ID(notice.WorkspaceID) {
		stream.release(notice.IssueSeq, false)
	}
}
