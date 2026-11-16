package enginetest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/tetral-ai/tetral/internal/protocol"
	contractfiles "github.com/tetral-ai/tetral/protocol"
)

// route is one endpoint of the contract: what its request holds and what
// each answer holds.
type route struct {
	method   string
	path     string // a path, or a template with {name} segments
	endpoint protocol.DriveTransferEndpoint

	pathParams *definition
	query      *definition
	headers    *definition
	body       *definition

	// events marks the event stream, whose 200 answer is read as events.
	events  bool
	success map[int]answer
	// errorBody is the body of every answer below 500 that is not a
	// success; errorHeaders the headers that come with a status.
	errorBody    definition
	errorHeaders map[int]definition
}

// answer is one success answer. A nil body is raw content.
type answer struct {
	body    *definition
	headers *definition
	// acknowledges: the answer carries the request's connection_id and
	// message_id (README, "Endpoints", ACK).
	acknowledges bool
	// noStore: the answer comes with Cache-Control: no-store.
	noStore bool
}

func connectionRoute(path string, body definition, success answer) route {
	return route{
		method: http.MethodPost, path: path, body: &body,
		success:   map[int]answer{http.StatusOK: success},
		errorBody: definition{connectionFile, "ErrorBody"},
	}
}

func ack() answer {
	return answer{body: &definition{toolFile, "Acknowledgement"}, acknowledges: true}
}

func grant() answer {
	return answer{body: &definition{connectionFile, "GrantAcknowledgement"}, acknowledges: true}
}

func secret(name string) answer {
	return answer{body: &definition{connectionFile, name}, noStore: true}
}

// connectionRoutes is the endpoint table of README, "Endpoints".
var connectionRoutes = []route{
	{
		method: http.MethodGet, path: "/v1/client/events", query: &definition{connectionFile, "EventsQuery"},
		events: true, errorBody: definition{connectionFile, "ErrorBody"},
	},
	connectionRoute("/v1/client/ready", definition{connectionFile, "Ready"}, grant()),
	connectionRoute("/v1/client/heartbeat", definition{connectionFile, "Heartbeat"}, grant()),
	connectionRoute("/v1/client/session-status", definition{connectionFile, "SessionStatus"}, ack()),
	connectionRoute("/v1/client/tool-status", definition{toolFile, "ToolStatus"}, ack()),
	connectionRoute("/v1/client/drive-sync", definition{toolFile, "DriveSyncReport"}, ack()),
	connectionRoute("/v1/client/mcp-tools", definition{connectionFile, "McpTools"}, ack()),
	connectionRoute("/v1/client/credential", definition{connectionFile, "CredentialRequest"}, secret("CredentialResponse")),
	connectionRoute("/v1/client/git-ticket", definition{connectionFile, "GitTicketRequest"}, secret("GitTicketResponse")),
	connectionRoute("/v1/client/protocol-error", definition{toolFile, "ProtocolErrorReport"}, ack()),
	connectionRoute("/v1/client/disconnect", definition{connectionFile, "DisconnectRequest"}, ack()),
}

// routes is connectionRoutes and the drive transfer endpoints, which the
// drive transfer schema lists in DriveTransferEndpoints.
var routes = sync.OnceValues(func() ([]route, error) {
	data, err := contractfiles.Files.ReadFile(driveFile + ".schema.json")
	if err != nil {
		return nil, err
	}
	var schema struct {
		Defs struct {
			DriveTransferEndpoints struct {
				Properties map[protocol.DriveTransferEndpoint]struct {
					Const struct {
						Method         string  `json:"method"`
						Path           string  `json:"path"`
						PathParams     *string `json:"path_params"`
						Query          *string `json:"query"`
						RequestHeaders *string `json:"request_headers"`
						RequestBody    *string `json:"request_body"`
						Responses      map[string]struct {
							Body    string  `json:"body"`
							Headers *string `json:"headers"`
						} `json:"responses"`
						ErrorBody    string            `json:"error_body"`
						ErrorHeaders map[string]string `json:"error_headers"`
					} `json:"const"`
				} `json:"properties"`
			}
		} `json:"$defs"`
	}
	if err := json.Unmarshal(data, &schema); err != nil {
		return nil, err
	}
	named := func(name *string) *definition {
		if name == nil || *name == "raw" {
			return nil
		}
		return &definition{driveFile, *name}
	}
	all := append([]route(nil), connectionRoutes...)
	for endpoint, entry := range schema.Defs.DriveTransferEndpoints.Properties {
		e := entry.Const
		r := route{
			method: e.Method, path: e.Path, endpoint: endpoint,
			pathParams: named(e.PathParams), query: named(e.Query), headers: named(e.RequestHeaders), body: named(e.RequestBody),
			success: map[int]answer{}, errorBody: definition{driveFile, e.ErrorBody}, errorHeaders: map[int]definition{},
		}
		for status, response := range e.Responses {
			code, err := strconv.Atoi(status)
			if err != nil {
				return nil, fmt.Errorf("%s: response status %q", endpoint, status)
			}
			r.success[code] = answer{body: named(&response.Body), headers: named(response.Headers)}
		}
		for status, headers := range e.ErrorHeaders {
			code, err := strconv.Atoi(status)
			if err != nil {
				return nil, fmt.Errorf("%s: error status %q", endpoint, status)
			}
			r.errorHeaders[code] = definition{driveFile, headers}
		}
		all = append(all, r)
	}
	return all, nil
})

// match returns the route of a request and its path parameters.
func match(method string, requested *url.URL) (*route, map[string][]string, error) {
	all, err := routes()
	if err != nil {
		return nil, nil, err
	}
	segments := strings.Split(requested.EscapedPath(), "/")
next:
	for i := range all {
		r := &all[i]
		template := strings.Split(r.path, "/")
		if r.method != method || len(template) != len(segments) {
			continue
		}
		params := map[string][]string{}
		for j, segment := range template {
			if name, ok := strings.CutPrefix(segment, "{"); ok {
				value, err := url.PathUnescape(segments[j])
				if err != nil {
					continue next
				}
				params[strings.TrimSuffix(name, "}")] = []string{value}
			} else if segment != segments[j] {
				continue next
			}
		}
		return r, params, nil
	}
	return nil, nil, nil
}
