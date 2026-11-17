package reference

import (
	"bufio"
	"errors"
	"io"
	"net"
	"net/http"
	"slices"
	"sync"
	"time"
)

// trafficEntry is one request on Engine's side as the network delivered it:
// when it arrived and when its handler returned, on the wall clock, which a
// failing test lines up with its own records. Unlike the request log, it
// holds a request from its arrival, also one held or abandoned before it was
// served.
type trafficEntry struct {
	Seq           int    `json:"seq"`
	Method        string `json:"method"`
	Path          string `json:"path"`
	ArrivedUnixNs int64  `json:"arrived_unix_ns"`
	// EndedUnixNs and Outcome are empty while the handler runs.
	EndedUnixNs int64   `json:"ended_unix_ns,omitempty"`
	Outcome     outcome `json:"outcome,omitempty"`
	// Status is the answer's, when the handler gave one.
	Status int `json:"status,omitempty"`
}

// outcome is how a request ended, as far as this server can know it.
type outcome string

const (
	// endedAnswered: the handler answered, and the server had seen no
	// departure of its client.
	endedAnswered outcome = "answered"
	// endedClientGone: the client had gone when the handler returned.
	endedClientGone outcome = "client_gone"
	// endedClientUnknown: the handler answered without reading the request's
	// body to its end. Until then the server does not read the connection,
	// so it cannot see whether the client had gone.
	endedClientUnknown outcome = "client_unknown"
	// endedCut: the connection was closed without an answer.
	endedCut outcome = "cut"
)

// traffic is the log of every request on Engine's side, with a lock of its
// own so that it records a request that waits for mu.
type traffic struct {
	mu      sync.Mutex
	entries []trafficEntry
}

func (log *traffic) arrived(request *http.Request) int {
	log.mu.Lock()
	defer log.mu.Unlock()
	path := request.URL.Path
	if request.URL.RawQuery != "" {
		path += "?" + request.URL.RawQuery
	}
	log.entries = append(log.entries, trafficEntry{
		Seq: len(log.entries) + 1, Method: request.Method, Path: path, ArrivedUnixNs: time.Now().UnixNano(),
	})
	return len(log.entries) - 1
}

func (log *traffic) ended(index int, how outcome, status int) {
	log.mu.Lock()
	defer log.mu.Unlock()
	entry := &log.entries[index]
	entry.EndedUnixNs, entry.Outcome, entry.Status = time.Now().UnixNano(), how, status
}

func (log *traffic) snapshot() []trafficEntry {
	log.mu.Lock()
	defer log.mu.Unlock()
	return slices.Clone(log.entries)
}

// recorded enters every request that handler serves in the traffic log.
func (log *traffic) recorded(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		index := log.arrived(request)
		writer := &trafficWriter{ResponseWriter: w}
		body := &trafficBody{ReadCloser: request.Body, ended: request.Body == http.NoBody}
		request.Body = body
		returned := false
		defer func() {
			status := writer.status
			if status == 0 && !writer.hijacked {
				// The server answers 200 for a handler that wrote nothing.
				status = http.StatusOK
			}
			switch {
			case writer.hijacked || !returned:
				// A hijacked connection is the handler's to close; a
				// handler that panicked has its connection closed.
				log.ended(index, endedCut, 0)
			case request.Context().Err() != nil:
				log.ended(index, endedClientGone, writer.status)
			case !body.ended:
				log.ended(index, endedClientUnknown, status)
			default:
				log.ended(index, endedAnswered, status)
			}
		}()
		handler.ServeHTTP(writer, request)
		returned = true
	})
}

// trafficBody notes when a request's body has been read to its end, from
// which on the server watches the connection for the client's departure.
type trafficBody struct {
	io.ReadCloser
	ended bool
}

func (body *trafficBody) Read(bytes []byte) (int, error) {
	read, err := body.ReadCloser.Read(bytes)
	if err == io.EOF {
		body.ended = true
	}
	return read, err
}

// trafficWriter keeps the status a handler answered, and passes on the
// flushes, hijacks and deadlines of the writer it wraps.
type trafficWriter struct {
	http.ResponseWriter
	status   int
	hijacked bool
}

func (writer *trafficWriter) WriteHeader(status int) {
	if writer.status == 0 {
		writer.status = status
	}
	writer.ResponseWriter.WriteHeader(status)
}

func (writer *trafficWriter) Write(bytes []byte) (int, error) {
	if writer.status == 0 {
		writer.status = http.StatusOK
	}
	return writer.ResponseWriter.Write(bytes)
}

func (writer *trafficWriter) Flush() {
	if flusher, ok := writer.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (writer *trafficWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hijacker, ok := writer.ResponseWriter.(http.Hijacker); ok {
		connection, buffered, err := hijacker.Hijack()
		writer.hijacked = writer.hijacked || err == nil
		return connection, buffered, err
	}
	return nil, nil, errors.New("the response writer cannot be hijacked")
}

func (writer *trafficWriter) Unwrap() http.ResponseWriter {
	return writer.ResponseWriter
}
