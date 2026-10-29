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

// CLI is a client in the CLI's place for one instance and credential.
// Authorize sets each request's credentials; the default is the connection
// key as a bearer token, as the CLI sends it through the public edge.
type CLI struct {
	BaseURL      string
	InstanceID   string
	CredentialID string
	Key          string
	HTTP         *http.Client
	Authorize    func(*http.Request) error
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
	return NewStream(response.Body), answer, nil
}

// Ready sends POST /v1/client/ready with the encoded ready.
func (c *CLI) Ready(ctx context.Context, ready protocol.Ready) (Response, error) {
	return c.Post(ctx, "/v1/client/ready", protocol.Encode(ready))
}

// Heartbeat sends POST /v1/client/heartbeat with the encoded heartbeat.
func (c *CLI) Heartbeat(ctx context.Context, heartbeat protocol.Heartbeat) (Response, error) {
	return c.Post(ctx, "/v1/client/heartbeat", protocol.Encode(heartbeat))
}

// ProtocolError sends POST /v1/client/protocol-error with the encoded
// report.
func (c *CLI) ProtocolError(ctx context.Context, report protocol.ProtocolErrorReport) (Response, error) {
	return c.Post(ctx, "/v1/client/protocol-error", protocol.Encode(report))
}

// Post sends body unchanged to the client route path.
func (c *CLI) Post(ctx context.Context, path string, body []byte) (Response, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+path, bytes.NewReader(body))
	if err != nil {
		return Response{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.send(request)
	if err != nil {
		return Response{}, err
	}
	defer func() { _ = response.Body.Close() }()
	data, err := io.ReadAll(response.Body)
	return Response{Status: response.StatusCode, Header: response.Header, Body: data}, err
}

func (c *CLI) send(request *http.Request) (*http.Response, error) {
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
	return client.Do(request) //nolint:gosec // G704: BaseURL is the Engine origin the test gives the test CLI.
}

// Stream is an open event stream.
type Stream struct {
	body   io.ReadCloser
	reader *bufio.Reader
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
		return event, ErrMalformedEvent
	}
	event.Data = []byte(strings.TrimPrefix(event.Lines[0], "data: "))
	envelope, err := protocol.DecodeDownstream(event.Data)
	if err != nil {
		return event, fmt.Errorf("%w: %w", ErrMalformedEvent, err)
	}
	event.Envelope = envelope
	return event, nil
}

// Close ends the stream from the client's side.
func (s *Stream) Close() error { return s.body.Close() }
