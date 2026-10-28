package reference

import (
	"net/http"

	"github.com/tetral-ai/tetral/cli/reference/wire"
)

// gitTicket serves POST /v1/client/git-ticket. Each fetch, a retry
// included, issues a new ticket that replaces the computer's previous one,
// so the replay rule does not apply; the state control lists a computer's
// tickets in issue order, the last one current.
func (server *Server) gitTicket(w http.ResponseWriter, request *http.Request) {
	server.serve(w, request, endpoint{
		name: "git-ticket", maxBytes: wire.MessageMaxBytes,
		decode: func(body []byte) (string, string, any, error) {
			fetch, err := wire.Decode[wire.GitTicketRequest](body)
			return string(fetch.ConnectionID), string(fetch.MessageID), &fetch, err
		},
		apply: func(conn *connection, value any, _ []byte) (answer, *refusal) {
			fetch := value.(*wire.GitTicketRequest)
			prepared := conn.bindings[string(fetch.BindingID)]
			if prepared == nil || !prepared.cloudWithRepositories {
				return answer{}, refuse("unknown_instruction", "binding_id names no preparation of a cloud computer with repositories")
			}
			computer := computerKey{instanceID: conn.instance.id, sessionID: prepared.sessionID}
			ticket := randomHex()
			server.tickets[computer] = append(server.tickets[computer], ticket)
			body, _ := wire.Encode(wire.GitTicketResponse{
				GitProxyURL: wire.HTTPSOrigin(server.config.GitProxyURL), Ticket: wire.NonEmpty(ticket),
			})
			return answer{status: http.StatusOK, body: body, noStore: true}, nil
		},
	})
}

// computerKey names the cloud computer a ticket belongs to: one Session's
// computer on one instance.
type computerKey struct {
	instanceID string
	sessionID  string
}
