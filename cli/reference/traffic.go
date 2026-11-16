package reference

import (
	"bufio"
	"errors"
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
	// EndedUnixNs and Status are zero while the handler runs.
	EndedUnixNs int64 `json:"ended_unix_ns,omitempty"`
	Status      int   `json:"status,omitempty"`
	// ClientGone is set when the client had gone by the time the handler
	// returned.
	ClientGone bool `json:"client_gone,omitempty"`
}

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

func (log *traffic) ended(index, status int, clientGone bool) {
	log.mu.Lock()
	defer log.mu.Unlock()
	entry := &log.entries[index]
	entry.EndedUnixNs, entry.Status, entry.ClientGone = time.Now().UnixNano(), status, clientGone
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
		defer func() {
			log.ended(index, writer.status, request.Context().Err() != nil)
		}()
		handler.ServeHTTP(writer, request)
	})
}

// trafficWriter keeps the status a handler answered, and passes on the
// flushes, hijacks and deadlines of the writer it wraps.
type trafficWriter struct {
	http.ResponseWriter
	status int
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
		return hijacker.Hijack()
	}
	return nil, nil, errors.New("the response writer cannot be hijacked")
}

func (writer *trafficWriter) Unwrap() http.ResponseWriter {
	return writer.ResponseWriter
}
