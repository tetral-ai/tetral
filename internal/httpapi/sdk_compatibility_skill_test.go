package httpapi_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestSDKCompatibilitySkillVersionContentRouteStreamsAndClosesReader(t *testing.T) {
	store := newRouterTestSkillStore()
	router := newAuthenticatedRouter(t, newTestHandler(t), httpapi.WithSkillHandler(httpapi.NewSkillHandler(store, t.TempDir())))
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/skills/skill_x/versions/1/content?beta=true", nil)
	setAuthHeader(request)
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 body=%q", recorder.Code, recorder.Body.String())
	}
	if recorder.Header().Get("Content-Type") != "application/zip" {
		t.Fatalf("Content-Type = %q; want application/zip", recorder.Header().Get("Content-Type"))
	}
	if !bytes.Equal(recorder.Body.Bytes(), store.content) {
		t.Fatalf("body = %q; want exact durable bytes %q", recorder.Body.Bytes(), store.content)
	}
	if !store.wasCalled("open_version_content") {
		t.Fatalf("content receiver not called; calls=%v", store.calls)
	}
	if !store.closed {
		t.Fatal("content reader was not closed")
	}
}

func TestSDKCompatibilitySkillVersionContentRouteIsRegisteredForStubConfiguration(t *testing.T) {
	router := newAuthenticatedRouter(t, newTestHandler(t))
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/skills/skill_x/versions/1/content?beta=true", nil)
	setAuthHeader(request)
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d; want 501 stub envelope body=%q", recorder.Code, recorder.Body.String())
	}
	assertErrorType(t, recorder, "not_implemented")
}

func TestSDKCompatibilitySkillVersionContentMatchesDurableZip(t *testing.T) {
	env := newSkillHandlerEnv(t)
	router := env.router(t)
	parent := createSkillViaHTTP(t, router, "downloadable-skill")
	version := createVersionViaHTTP(t, router, parent.ID, "downloadable-skill", "Download exact durable bytes.")

	key := "skills/" + string(workspace.DefaultID) + "/" + parent.ID + "/versions/" + version.Version + "/package.zip"
	durableReader, err := env.blob.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get durable package: %v", err)
	}
	durable, err := io.ReadAll(durableReader)
	if err != nil {
		t.Fatalf("Read durable package: %v", err)
	}
	if err := durableReader.Close(); err != nil {
		t.Fatalf("Close durable package: %v", err)
	}

	recorder := performRequest(t, router, http.MethodGet, "/v1/skills/"+parent.ID+"/versions/"+version.Version+"/content?beta=true")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 body=%q", recorder.Code, recorder.Body.String())
	}
	if recorder.Header().Get("Content-Type") != "application/zip" {
		t.Fatalf("Content-Type = %q; want application/zip", recorder.Header().Get("Content-Type"))
	}
	if !bytes.Equal(recorder.Body.Bytes(), durable) {
		t.Fatalf("downloaded %d bytes; want exact %d durable bytes", recorder.Body.Len(), len(durable))
	}
}
