package reference

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"

	"github.com/tetral-ai/tetral/cli/reference/wire"
)

// offerRenewal sends credential.renew for a pending credential on its
// instance's live connection. The offer belongs to that connection: a
// resume repeats it while the credential is pending, a new connection never
// does.
func (server *Server) offerRenewal(pending *credential) {
	conn := server.live(pending.InstanceID)
	if conn == nil {
		return
	}
	pending.renewalConnection = conn.id
	pending.replaces = conn.credential
	server.sendRenewal(conn, pending)
}

func (server *Server) sendRenewal(conn *connection, pending *credential) {
	if conn.stream == nil {
		return
	}
	data, messageID := server.event(conn, "credential.renew", wire.CredentialRenewPayload{CredentialID: wire.ID(pending.CredentialID)})
	conn.stream.send(data, messageID, "credential.renew")
	pending.renewalSent++
}

// renewAgain repeats, on a resumed stream, the offers of the credentials
// that are still pending.
func (server *Server) renewAgain(conn *connection) {
	for _, candidate := range server.credentials {
		if candidate.State == "pending" && candidate.renewalConnection == conn.id {
			server.sendRenewal(conn, candidate)
		}
	}
}

// commitRenewal completes a renewal at the first authenticated use of a
// fetched pending credential, by a resume or by a new connection: it becomes
// the connection's credential, which caps later grants at its expiry, and
// the credential it replaces is revoked.
func (server *Server) commitRenewal(conn *connection, used *credential) {
	if used.State != "pending" {
		return
	}
	used.State = "active"
	conn.credential = used
	if used.replaces != nil {
		used.replaces.State = "revoked"
	}
}

// credential serves POST /v1/client/credential. A retry is a new fetch, so
// the replay rule does not apply: the key is returned once.
func (server *Server) credential(w http.ResponseWriter, request *http.Request) {
	server.serve(w, request, endpoint{
		name: "credential", maxBytes: wire.MessageMaxBytes,
		decode: func(body []byte) (string, string, any, error) {
			fetch, err := wire.Decode[wire.CredentialRequest](body)
			return string(fetch.ConnectionID), string(fetch.MessageID), &fetch, err
		},
		apply: func(conn *connection, value any, _ []byte) (answer, *refusal) {
			fetch := value.(*wire.CredentialRequest)
			pending := server.credentials[string(fetch.CredentialID)]
			if pending == nil || pending.InstanceID != conn.instance.id || pending.State != "pending" || pending.fetched ||
				!server.clock.Now().Before(pending.ExpiresAt) {
				return answer{}, refuse("not_found", "no pending credential of this instance to fetch")
			}
			if pending.ConnectionKey == "" {
				pending.ConnectionKey = wire.ConnectionKeyPrefix + randomHex()
			}
			pending.fetched = true
			body, _ := wire.Encode(wire.CredentialResponse{
				CredentialID:  wire.ID(pending.CredentialID),
				ConnectionKey: wire.ConnectionKey(pending.ConnectionKey),
				ExpiresAt:     wire.Time(pending.ExpiresAt.UTC().Format("2006-01-02T15:04:05.000Z")),
			})
			return answer{status: http.StatusOK, body: body, noStore: true}, nil
		},
	})
}

func randomHex() string {
	bytes := make([]byte, 16)
	_, _ = rand.Read(bytes)
	return hex.EncodeToString(bytes)
}
