package computerhub

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/iotest"

	"github.com/tetral-ai/tetral/internal/workload/workloadtest"
)

// A commit body that cannot be read is refused with Engine's sentence, and
// its refusal record carries the read's own error after it.
func TestCommitBodyReadFailureRecordsTheReadError(t *testing.T) {
	var logs bytes.Buffer
	handler := &clientHandler{logger: workloadtest.Logger(t, &logs, ServiceName, "")}
	request := httptest.NewRequest(http.MethodPost, "/v1/client/drives/drv_x/commits", iotest.ErrReader(errors.New("connection reset by peer")))
	request.ContentLength = 64
	answer := httptest.NewRecorder()
	handler.commitDriveChanges(answer, request)
	if answer.Code != http.StatusBadRequest {
		t.Fatalf("status = %d %s; want 400", answer.Code, answer.Body)
	}
	records := workloadtest.Find(workloadtest.Records(t, logs.Bytes()), "drive.transfer_refused")
	if len(records) != 1 || records[0]["error.message"] != "the request body could not be read: connection reset by peer" {
		t.Fatalf("drive.transfer_refused = %v; want one record whose cause names the read error", records)
	}
}
