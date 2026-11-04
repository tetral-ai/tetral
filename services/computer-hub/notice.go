package computerhub

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/tetral-ai/tetral/internal/drive"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// Run listens on this pod's connection notice channel
// (instance.ConnectionNoticeChannel of the pod name) and on
// drive.NoticeChannel on one connection, reconnecting after a failure, until
// ctx ends; then it closes every stream. It handles the notices one at a
// time in arrival order, the commit order of their transactions. Each
// connection notice naming a stream the pod holds raises that stream's bound
// to the notice's issue_seq and wakes it; each drive notice queues
// drive.changed on the streams it selects (drive_notices.go). Each time the
// listening connection is in place, the first time included, every held
// stream is woken to resend its drives' last commits and then write all its
// connection's unreported instructions, since notices sent while none was in
// place are lost.
func (s *Streams) Run(ctx context.Context) error {
	defer s.closeAll()
	listener := queue.PostgreSQLNotificationListener{Client: s.database, Operation: "computer_hub.connection_notices"}
	onReady := func() {
		for _, stream := range s.held() {
			stream.resendDrives()
			stream.release(0, true)
		}
		s.listeningOnce.Do(func() { close(s.listening) })
	}
	// The pod's channel is listened on last: tests find the connection by
	// its last statement.
	channels := []string{drive.NoticeChannel, instance.ConnectionNoticeChannel(s.podName)}
	onNotification := func(channel, payload string) {
		if channel == drive.NoticeChannel {
			s.driveNotice(ctx, payload)
			return
		}
		s.notice(payload)
	}
	return queue.RunChannelsListener(ctx, listener, channels, onReady, onNotification, func(error) {
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
