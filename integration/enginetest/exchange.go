package enginetest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/tetral-ai/tetral/internal/protocol"
)

// bodyLimit is the largest body the contract has (a commit and its answer,
// 16 MiB); a longer one is outside it whatever it holds.
const bodyLimit = protocol.CommitRequestMaxJSONBytes

// sentRequest is one request of a CLI on its way: checked when its body is
// known, then its answer against it.
type sentRequest struct {
	checks  *checker
	name    string
	route   *route
	params  map[string][]string
	request *http.Request
	invalid bool
	x       exchange

	// body is the request's JSON body; recorded follows a body that is
	// only known as the transport reads it.
	body     []byte
	recorded *recording
	// within is whether the request, once checked, is within the contract.
	checked, within bool
}

func (s *sentRequest) report(format string, args ...any) {
	s.checks.t.Helper()
	s.checks.report("%s: %s", s.name, fmt.Sprintf(format, args...))
}

// sending checks request before it leaves, as far as its body is known.
func (c *checker) sending(request *http.Request, invalid bool) *sentRequest {
	c.t.Helper()
	s := &sentRequest{checks: c, name: request.Method + " " + request.URL.Path, request: request, invalid: invalid}
	matched, params, err := match(request.Method, request.URL)
	if err != nil {
		s.report("the contract's endpoints: %v", err)
		return s
	}
	s.route, s.params = matched, params
	if s.route != nil {
		s.x.endpoint = s.route.endpoint
	}
	switch {
	case s.route != nil && s.route.body == nil, request.Body == nil, request.Body == http.NoBody:
		s.check()
	case request.GetBody != nil:
		if body, err := request.GetBody(); err == nil {
			s.body, _ = io.ReadAll(io.LimitReader(body, bodyLimit+1))
			_ = body.Close()
		}
		s.check()
	default:
		s.recorded = &recording{ReadCloser: request.Body}
		request.Body = s.recorded
	}
	return s
}

// check holds the request to the contract and reports it when it is
// outside it unmarked, or within it marked.
func (s *sentRequest) check() {
	s.checks.t.Helper()
	s.checked = true
	err := s.outside()
	s.within = err == nil
	switch {
	case err != nil && !s.invalid:
		s.report("request outside the contract: %v", err)
	case err == nil && s.invalid:
		s.report("request is marked Invalid and is within the contract")
	}
	if !s.within || s.route.path != "/v1/client/session-status" {
		return
	}
	if status, err := protocol.DecodeSessionStatus(s.body); err == nil {
		s.checks.reporting(status)
	}
}

func (s *sentRequest) outside() error {
	if s.route == nil {
		return errors.New("no endpoint has this method and path")
	}
	if d := s.route.pathParams; d != nil {
		if err := s.x.sentValues(*d, s.params); err != nil {
			return err
		}
	}
	if d := s.route.query; d != nil {
		if err := s.x.sentValues(*d, s.request.URL.Query()); err != nil {
			return err
		}
	}
	if d := s.route.headers; d != nil {
		if err := s.x.sentValues(*d, sentHeaders(s.request)); err != nil {
			return err
		}
	}
	if d := s.route.body; d != nil {
		if len(s.body) > bodyLimit {
			return errors.New("body over 16 MiB")
		}
		return s.x.sent(*d, s.body)
	}
	return nil
}

// sentHeaders is request's headers with the Content-Length the transport
// writes for it.
func sentHeaders(request *http.Request) map[string][]string {
	headers := request.Header.Clone()
	headers.Del("Content-Length")
	switch {
	case request.ContentLength > 0, request.ContentLength == 0 && (request.Body == nil || request.Body == http.NoBody):
		headers.Set("Content-Length", strconv.FormatInt(request.ContentLength, 10))
	}
	return headers
}

// answered checks the answer's status and headers, and its body once the
// caller has read it to its end.
func (s *sentRequest) answered(response *http.Response) {
	s.checks.t.Helper()
	if s.recorded != nil && s.recorded.complete {
		// A body the transport did not read to its end was not sent
		// whole, so nothing is known of it.
		s.body = s.recorded.data.Bytes()
		s.check()
	}
	status := response.StatusCode
	s.x.status = status
	if s.route == nil || status >= 500 {
		return
	}
	if status/100 != 2 {
		if d, found := s.route.errorHeaders[status]; found {
			if err := s.x.receivedValues(d, response.Header); err != nil {
				s.report("answer %d outside the contract: %v", status, err)
			}
		}
		s.readBody(response, func(body []byte) {
			if err := s.x.received(s.route.errorBody, body); err != nil {
				s.report("answer %d outside the contract: %v: %s", status, err, abbreviate(body))
			}
		})
		return
	}
	if s.route.events && status == http.StatusOK {
		return
	}
	success, found := s.route.success[status]
	if !found {
		s.report("answer %d is no success answer of the endpoint", status)
		return
	}
	if success.noStore && response.Header.Get("Cache-Control") != "no-store" {
		s.report("answer %d has Cache-Control %q, not no-store", status, response.Header.Get("Cache-Control"))
	}
	if d := success.headers; d != nil {
		if err := s.x.receivedValues(*d, response.Header); err != nil {
			s.report("answer %d outside the contract: %v", status, err)
		}
	}
	if success.body == nil {
		return
	}
	s.readBody(response, func(body []byte) {
		if err := s.x.received(*success.body, body); err != nil {
			s.report("answer %d outside the contract: %v: %s", status, err, abbreviate(body))
			return
		}
		if success.acknowledges && s.checked && s.within {
			var sent, got struct {
				ConnectionID string `json:"connection_id"`
				MessageID    string `json:"message_id"`
			}
			if json.Unmarshal(s.body, &sent) == nil && json.Unmarshal(body, &got) == nil && sent != got {
				s.report("answer %d acknowledges %s of %s, not the request's %s of %s", status, got.MessageID, got.ConnectionID, sent.MessageID, sent.ConnectionID)
			}
		}
		s.accepted(body)
	})
}

// accepted records what an accepted request tells the stream rules.
func (s *sentRequest) accepted(answer []byte) {
	switch {
	case s.route.path == "/v1/client/ready":
		if ready, err := protocol.DecodeReady(s.body); err == nil {
			s.checks.accepted(ready)
		}
	case s.route.path == "/v1/client/session-status":
		if status, err := protocol.DecodeSessionStatus(s.body); err == nil {
			s.checks.prepared(status)
		}
	case s.route.endpoint == protocol.EndpointCommit:
		if response, err := protocol.DecodeCommitResponse(answer, protocol.CommitResponseContext{}); err == nil && response.Seq.Present {
			s.checks.commit(s.params["drive_id"][0])
		}
	case s.route.endpoint == protocol.EndpointListFiles:
		if response, err := protocol.DecodeListFilesResponse(answer, protocol.ListFilesResponseContext{}); err == nil && response.StartSeq.Value > 0 {
			s.checks.commit(s.params["drive_id"][0])
		}
	}
}

// readBody calls check with the answer's body once the caller has read it
// to its end; a body the caller leaves unread is not checked.
func (s *sentRequest) readBody(response *http.Response, check func([]byte)) {
	response.Body = &answerBody{ReadCloser: response.Body, sent: s, check: check}
}

type answerBody struct {
	io.ReadCloser
	sent  *sentRequest
	check func([]byte)
	data  bytes.Buffer
	done  bool
}

func (b *answerBody) Read(p []byte) (int, error) {
	b.sent.checks.t.Helper()
	n, err := b.ReadCloser.Read(p)
	if b.data.Len() <= bodyLimit {
		b.data.Write(p[:n])
	}
	if err == io.EOF && !b.done {
		b.done = true
		if b.data.Len() > bodyLimit {
			b.sent.report("answer %d has a body over 16 MiB", b.sent.x.status)
		} else {
			b.check(b.data.Bytes())
		}
	}
	return n, err
}

// recording keeps a request body as the transport reads it.
type recording struct {
	io.ReadCloser
	data     bytes.Buffer
	complete bool
}

func (r *recording) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if r.data.Len() <= bodyLimit {
		r.data.Write(p[:n])
	}
	if err == io.EOF {
		r.complete = true
	}
	return n, err
}
