package reference

import (
	"encoding/json"

	"github.com/tetral-ai/tetral/cli/reference/wire"
)

// sentDrives is a session.drives written through the send control: the
// applied report that answers it names its Session and binding.
type sentDrives struct {
	sessionID string
	bindingID string
}

// noteSentDrives records data, written on conn's stream, when it is a
// session.drives, so that its applied report is accepted.
func (conn *connection) noteSentDrives(data []byte) {
	var event struct {
		MessageID string `json:"message_id"`
		Type      string `json:"type"`
		Payload   struct {
			SessionID string `json:"session_id"`
			BindingID string `json:"binding_id"`
		} `json:"payload"`
	}
	if json.Unmarshal(data, &event) != nil || event.Type != "session.drives" || event.MessageID == "" {
		return
	}
	if conn.drives == nil {
		conn.drives = map[string]sentDrives{}
	}
	conn.drives[event.MessageID] = sentDrives{sessionID: event.Payload.SessionID, bindingID: event.Payload.BindingID}
}

// answersSentDrives reports whether report is the applied report of a
// session.drives sent on conn.
func (conn *connection) answersSentDrives(report *wire.SessionStatus) bool {
	sent, found := conn.drives[string(report.RequestMessageID)]
	return found && report.Status == "applied" &&
		sent.sessionID == string(report.SessionID) && sent.bindingID == string(report.BindingID)
}
