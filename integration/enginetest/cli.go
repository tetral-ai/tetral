package enginetest

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/tetral-ai/tetral/internal/protocol"
)

// CLI is a client in the CLI's place for one instance and credential, made
// by NewCLI. Authorize sets each request's credentials; the default is the
// connection key as a bearer token, as the CLI sends it through the public
// edge.
type CLI struct {
	BaseURL      string
	InstanceID   string
	CredentialID string
	Key          string
	HTTP         *http.Client
	Authorize    func(*http.Request) error

	checks  *checker
	invalid bool
}

// NewCLI returns a CLI that holds every request it sends and every answer
// and event it receives to the contract of protocol/ (README.md, "The
// contract check") and fails t for each one outside it. A copy of the CLI
// reports to the same test and shares what the stream rules remember.
func NewCLI(t T) *CLI {
	return &CLI{checks: &checker{t: t, connections: map[string]*connectionState{}, committed: map[string]bool{}}}
}

// t is the test c reports to.
func (c *CLI) t() T {
	if c.checks == nil {
		panic("enginetest: a CLI is made by NewCLI")
	}
	return c.checks.t
}

// Invalid returns a copy of c for a request the test puts outside the
// contract on purpose: the request is not held to the contract, and fails
// the test when it is within it; its answer is checked as any other.
func (c *CLI) Invalid() *CLI {
	marked := *c
	marked.invalid = true
	return &marked
}

// Response is one answer of a client route.
type Response struct {
	Status int
	Header http.Header
	Body   []byte
}

// Error decodes the client error body of a non-2xx answer.
func (r Response) Error() (protocol.ErrorBody, error) {
	return protocol.DecodeErrorBody(r.Status, r.Body)
}

// Grant decodes the ACK of a ready or heartbeat.
func (r Response) Grant() (protocol.GrantAcknowledgement, error) {
	if r.Status != http.StatusOK {
		return protocol.GrantAcknowledgement{}, fmt.Errorf("status %d: %s", r.Status, r.Body)
	}
	return protocol.DecodeGrantAcknowledgement(r.Body)
}

// Acknowledgement decodes a plain ACK.
func (r Response) Acknowledgement() (protocol.Acknowledgement, error) {
	if r.Status != http.StatusOK {
		return protocol.Acknowledgement{}, fmt.Errorf("status %d: %s", r.Status, r.Body)
	}
	return protocol.DecodeAcknowledgement(r.Body)
}

// EventsQuery is the query of GET /v1/client/events for this CLI's instance
// and credential, with the optional predecessor members.
func (c *CLI) EventsQuery(previousConnectionID string, resume *bool) url.Values {
	query := protocol.EventsQuery{InstanceID: c.InstanceID, CredentialID: c.CredentialID}
	if previousConnectionID != "" {
		query.PreviousConnectionID = protocol.Some(previousConnectionID)
	}
	if resume != nil {
		query.Resume = protocol.Some(fmt.Sprint(*resume))
	}
	return query.Query()
}

// OpenEvents sends GET /v1/client/events with query. A 200 answer returns
// the open stream; any other answer returns its Response and a nil stream.
func (c *CLI) OpenEvents(ctx context.Context, query url.Values) (*Stream, Response, error) {
	c.t().Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/v1/client/events?"+query.Encode(), nil)
	if err != nil {
		return nil, Response{}, err
	}
	response, err := c.send(request)
	if err != nil {
		return nil, Response{}, err
	}
	answer := Response{Status: response.StatusCode, Header: response.Header}
	if response.StatusCode != http.StatusOK {
		defer func() { _ = response.Body.Close() }()
		answer.Body, err = io.ReadAll(response.Body)
		return nil, answer, err
	}
	stream := NewStream(response.Body)
	stream.rules = c.checks.stream()
	return stream, answer, nil
}

// Ready sends POST /v1/client/ready with the encoded ready.
func (c *CLI) Ready(ctx context.Context, ready protocol.Ready) (Response, error) {
	c.t().Helper()
	return c.Post(ctx, "/v1/client/ready", protocol.Encode(ready))
}

// Heartbeat sends POST /v1/client/heartbeat with the encoded heartbeat.
func (c *CLI) Heartbeat(ctx context.Context, heartbeat protocol.Heartbeat) (Response, error) {
	c.t().Helper()
	return c.Post(ctx, "/v1/client/heartbeat", protocol.Encode(heartbeat))
}

// ProtocolError sends POST /v1/client/protocol-error with the encoded
// report.
func (c *CLI) ProtocolError(ctx context.Context, report protocol.ProtocolErrorReport) (Response, error) {
	c.t().Helper()
	return c.Post(ctx, "/v1/client/protocol-error", protocol.Encode(report))
}

// Disconnect sends POST /v1/client/disconnect with the encoded request.
func (c *CLI) Disconnect(ctx context.Context, request protocol.DisconnectRequest) (Response, error) {
	c.t().Helper()
	return c.Post(ctx, "/v1/client/disconnect", protocol.Encode(request))
}

// Post sends body unchanged to the client route path.
func (c *CLI) Post(ctx context.Context, path string, body []byte) (Response, error) {
	c.t().Helper()
	return c.PostStream(ctx, path, bytes.NewReader(body), int64(len(body)))
}

// PostStream sends to the client route path the body that body yields as
// it is read, declaring length as its Content-Length.
func (c *CLI) PostStream(ctx context.Context, path string, body io.Reader, length int64) (Response, error) {
	c.t().Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+path, body)
	if err != nil {
		return Response{}, err
	}
	request.ContentLength = length
	request.Header.Set("Content-Type", "application/json")
	response, err := c.send(request)
	if err != nil {
		return Response{}, err
	}
	defer func() { _ = response.Body.Close() }()
	data, err := io.ReadAll(response.Body)
	return Response{Status: response.StatusCode, Header: response.Header, Body: data}, err
}

// Request sends method to the client route target, a path with its query,
// with header and body, and returns the whole answer.
func (c *CLI) Request(ctx context.Context, method, target string, header http.Header, body []byte) (Response, error) {
	c.t().Helper()
	response, err := c.Open(ctx, method, target, header, bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return Response{}, err
	}
	defer func() { _ = response.Body.Close() }()
	data, err := io.ReadAll(response.Body)
	return Response{Status: response.StatusCode, Header: response.Header, Body: data}, err
}

// Open sends method to the client route target with header and the body
// that body yields as it is read, declaring length as its Content-Length,
// and returns the answer with its body unread.
func (c *CLI) Open(ctx context.Context, method, target string, header http.Header, body io.Reader, length int64) (*http.Response, error) {
	c.t().Helper()
	request, err := http.NewRequestWithContext(ctx, method, c.BaseURL+target, body)
	if err != nil {
		return nil, err
	}
	request.ContentLength = length
	for name, values := range header {
		request.Header[name] = values
	}
	return c.send(request)
}

func (c *CLI) send(request *http.Request) (*http.Response, error) {
	c.t().Helper()
	if c.Authorize != nil {
		if err := c.Authorize(request); err != nil {
			return nil, err
		}
	} else {
		request.Header.Set("Authorization", "Bearer "+c.Key)
	}
	client := c.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	sent := c.checks.sending(request, c.invalid)
	response, err := client.Do(request) //nolint:gosec // G704: BaseURL is the Engine origin the test gives the test CLI.
	if err != nil {
		return nil, err
	}
	sent.answered(response)
	return response, nil
}

// Stream is an open event stream.
type Stream struct {
	body   io.ReadCloser
	reader *bufio.Reader
	// rules is nil for a stream no CLI opened.
	rules *streamRules
}

// NewStream reads events from body, the body of an event stream response.
func NewStream(body io.ReadCloser) *Stream {
	return &Stream{body: body, reader: bufio.NewReader(body)}
}

// Event is one SSE event: its raw lines and the envelope its data holds.
type Event struct {
	Lines    []string
	Data     []byte
	Envelope protocol.Downstream
}

// ErrMalformedEvent reports an event that is not exactly one data line
// holding one envelope.
var ErrMalformedEvent = errors.New("enginetest: event is not one data line holding one envelope")

// Next reads the next event. An event must be one "data: " line holding one
// envelope that protocol.DecodeDownstream accepts; anything else, an SSE
// field or comment included, is ErrMalformedEvent with the event's lines.
// A stream a CLI opened also holds each event to the contract.
func (s *Stream) Next() (Event, error) {
	var event Event
	for {
		line, err := s.reader.ReadString('\n')
		if err != nil {
			return event, err
		}
		line = strings.TrimSuffix(line, "\n")
		if line == "" {
			if len(event.Lines) == 0 {
				continue
			}
			break
		}
		event.Lines = append(event.Lines, line)
	}
	if len(event.Lines) != 1 || !strings.HasPrefix(event.Lines[0], "data: ") {
		if s.rules != nil {
			s.rules.malformed(event)
		}
		return event, ErrMalformedEvent
	}
	event.Data = []byte(strings.TrimPrefix(event.Lines[0], "data: "))
	envelope, err := protocol.DecodeDownstream(event.Data)
	if err != nil {
		if s.rules != nil {
			s.rules.undecodable(event, err)
		}
		return event, fmt.Errorf("%w: %w", ErrMalformedEvent, err)
	}
	event.Envelope = envelope
	if s.rules != nil {
		s.rules.event(event)
	}
	return event, nil
}

// Close ends the stream from the client's side.
func (s *Stream) Close() error { return s.body.Close() }
