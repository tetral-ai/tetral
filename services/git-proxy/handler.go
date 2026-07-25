package gitproxy

import "net/http"

const ServiceName = "git-proxy"

// NewHTTPHandler assembles the smart-HTTP relay core around durable ticket
// validation and credential-backed repository authorization.
func NewHTTPHandler(ticketValidator TicketValidator, authorizer RepositoryAuthorizer, options HandlerOptions) http.Handler {
	options = options.withDefaults()
	return &Proxy{
		TicketValidator: ticketValidator,
		Authorizer:      authorizer,
		Options:         options,
	}
}
