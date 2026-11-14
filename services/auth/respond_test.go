package tetralauth

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/workload/workloadtest"
)

type unencodable struct{}

func (unencodable) MarshalJSON() ([]byte, error) { return nil, errors.New("the key cannot be shown") }

func TestAuthAnswersEncodeBeforeAnythingIsWritten(t *testing.T) {
	for _, test := range []struct {
		name  string
		value any
		want  int
		body  string
		cause string
	}{
		{name: "an answer that encodes", value: map[string]string{"name": "<ci> & deploy"}, want: http.StatusOK,
			body: `{"name":"\u003cci\u003e \u0026 deploy"}` + "\n"},
		{name: "an answer that cannot be encoded", value: struct {
			Key unencodable `json:"key"`
		}{}, want: http.StatusInternalServerError,
			cause: "the response cannot be encoded: json: error calling MarshalJSON for type tetralauth.unencodable: the key cannot be shown"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var buffer bytes.Buffer
			logger := workloadtest.Logger(t, &buffer, "auth", "observability.json")
			handler := httpapi.RequestIDMiddleware(httpapi.RequestLogMiddleware(logger)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				writeAuthJSON(w, r, http.StatusOK, test.value)
			})))
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/api_keys", nil))

			requestID := recorder.Header().Get("request-id")
			if recorder.Code != test.want || recorder.Header().Get("Content-Type") != "application/json" || requestID == "" {
				t.Fatalf("answer = %d %v; want %d, application/json and a request-id", recorder.Code, recorder.Header(), test.want)
			}
			var failed []map[string]any
			for _, line := range strings.Split(strings.TrimSpace(buffer.String()), "\n") {
				var record map[string]any
				if err := json.Unmarshal([]byte(line), &record); err != nil {
					t.Fatalf("record %q: %v", line, err)
				}
				if record["msg"] == "http.request.failed" {
					failed = append(failed, record)
				}
			}
			if test.cause == "" {
				if recorder.Body.String() != test.body || len(failed) != 0 {
					t.Fatalf("answer %q with failure records %v; want %q and none", recorder.Body.String(), failed, test.body)
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
			if len(failed) != 1 || failed[0]["error.code"] != "server_error" || failed[0]["error.message"] != test.cause {
				t.Fatalf("failure records = %v; want one with the encoder's error as its cause", failed)
			}
		})
	}
}
