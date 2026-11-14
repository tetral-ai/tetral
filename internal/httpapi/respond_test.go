package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

type unencodable struct{}

func (unencodable) MarshalJSON() ([]byte, error) {
	return nil, errors.New("the agent's response cannot be built")
}

func TestWriteJSONEncodesBeforeItWritesAnything(t *testing.T) {
	for _, test := range []struct {
		name   string
		status int
		value  any
		want   int
		body   string
		cause  string
	}{
		{name: "an answer that encodes", status: http.StatusCreated, value: map[string]string{"text": "<a> & b"},
			want: http.StatusCreated, body: `{"text":"\u003ca\u003e \u0026 b"}` + "\n"},
		{name: "an answer that cannot be encoded", status: http.StatusOK, value: struct {
			Agent unencodable `json:"agent"`
		}{}, want: http.StatusInternalServerError,
			cause: "the response cannot be encoded: json: error calling MarshalJSON for type httpapi.unencodable: the agent's response cannot be built"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var buffer bytes.Buffer
			handler := RequestIDMiddleware(RequestLogMiddleware(newTestLogger(t, &buffer))(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				WriteJSON(w, r, test.status, test.value)
			})))
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/agents/agent_1", nil))

			requestID := recorder.Header().Get("request-id")
			if recorder.Code != test.want || recorder.Header().Get("Content-Type") != "application/json" || requestID == "" {
				t.Fatalf("answer = %d %v; want %d, application/json and a request-id", recorder.Code, recorder.Header(), test.want)
			}
			records := decodeHTTPLogLines(t, buffer.String())
			if test.cause == "" {
				if recorder.Body.String() != test.body || len(records) != 1 || records[0]["msg"] != "http.request.completed" {
					t.Fatalf("answer %q with records %v; want %q and only the completed record", recorder.Body.String(), records, test.body)
				}
				return
			}
			var response errorResponse
			if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
				t.Fatalf("answer %q is not an ErrorResponse: %v", recorder.Body.String(), err)
			}
			if response.Type != "error" || response.Error.Type != "api_error" || response.Error.Message != "internal error" || response.RequestID != requestID {
				t.Fatalf("answer = %+v; want an api_error ErrorResponse with the request's ID %s", response, requestID)
			}
			if len(records) != 2 || records[1]["msg"] != "http.request.failed" || records[1]["error.code"] != "server_error" ||
				records[1]["error.message"] != test.cause || records[1]["request.id"] != requestID {
				t.Fatalf("records = %v; want the completed record, then one failure record with the encoder's error as its cause", records)
			}
		})
	}
}
