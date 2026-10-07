package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/auth/authtest"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/environment"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestPostgreSQLPublicStreamingListCursors(t *testing.T) {
	f := newPublicProjectionFixture(t, publicProjectionOptions{})
	setup := f.start(t, "", "")
	child := f.child(t, setup)
	f.end(t, setup)
	f.open(t, "uninterrupted", []string{"agent.message"}, "")
	rp := f.start(t, "", "")
	rc := f.start(t, child, "")
	type issuedCursor struct {
		thread, order string
		raw           bool
		first         string
		page          string
	}
	var cursors []issuedCursor
	for _, thread := range []string{"", child} {
		for _, order := range []string{"asc", "desc"} {
			page := f.rawList(t, thread, order, "")
			if len(page.Data) != 1 || page.Next == nil {
				t.Fatal("fixture lacks pre-commit continuation cursor")
			}
			cursors = append(cursors, issuedCursor{thread: thread, order: order, raw: thread != "" && order == "desc", first: publicEventID(page.Data[0]), page: *page.Next})
		}
	}
	assertStage := func(t *testing.T) {
		for _, thread := range []string{"", child} {
			for _, order := range []string{"asc", "desc"} {
				raw := thread != "" && order == "desc"
				actual := publicProjectionAllPages(t, f, thread, order, raw)
				want := publicProjectionSQLIDs(t, f.db, f.session, thread, order)
				if !reflect.DeepEqual(actual, want) {
					t.Fatalf("limit-one %s thread=%q ids=%v want=%v", order, thread, actual, want)
				}
			}
		}
		for _, cursor := range cursors {
			want := publicProjectionSQLIDs(t, f.db, f.session, cursor.thread, cursor.order)
			index := -1
			for i, id := range want {
				if id == cursor.first {
					index = i
					break
				}
			}
			if index < 0 {
				t.Fatal("original cursor event disappeared")
			}
			actual := publicProjectionPageTail(t, f, cursor.thread, cursor.order, cursor.page, cursor.raw)
			if !reflect.DeepEqual(actual, want[index+1:]) {
				t.Fatalf("issued %s cursor changed ordering/filter contract: got=%v want=%v", cursor.order, actual, want[index+1:])
			}
		}
	}
	t.Run("before-M1-commit", assertStage)
	m1 := f.text(t, rp, "alpha βeta omega\n", true)
	cm := f.text(t, rc, "public-child-text", false)
	f.tool(t, rp, "read", `{"path":"private-tool-input-marker"}`)
	f.text(t, rp, "second\n", false)
	positionsBeforePreview := publicProjectionPositions(t, f)
	var highwaterBeforePreview int64
	if err := f.db.QueryRow(`SELECT COALESCE(max(stream_position),0) FROM session_event_stream_changes WHERE session_id=$1`, f.session).Scan(&highwaterBeforePreview); err != nil {
		t.Fatal(err)
	}
	f.publish(t, f.frame(rp, "request_open", "", "", 0, ""), f.frame(rp, "event_start", m1, "agent.message", 0, ""), f.frame(rp, "event_delta", m1, "agent.message", 1, "alpha "))
	f.waitEvent(t, "uninterrupted", "event_delta", 1)
	var highwaterAfterPreview int64
	if err := f.db.QueryRow(`SELECT COALESCE(max(stream_position),0) FROM session_event_stream_changes WHERE session_id=$1`, f.session).Scan(&highwaterAfterPreview); err != nil || highwaterAfterPreview != highwaterBeforePreview || !reflect.DeepEqual(positionsBeforePreview, publicProjectionPositions(t, f)) {
		t.Fatal("native NATS preview advanced durable highwater or original list positions")
	}
	t.Run("after-M1-before-End", assertStage)
	if !publicProjectionContains(publicProjectionAllPages(t, f, "", "asc", false), m1) || !publicProjectionContains(publicProjectionAllPages(t, f, child, "asc", false), cm) {
		t.Fatal("committed message missing from pre-End history")
	}
	positions := publicProjectionPositions(t, f)
	f.end(t, rc)
	end := f.endRequest(rp)
	f.lostEnd.Store(true)
	response, err := f.bridge.WriteRequestEnd(f.ctx, end)
	if status.Code(err) != codes.Unavailable || response != nil {
		t.Fatalf("End response-loss boundary: %v", err)
	}
	var endID string
	if err = f.db.QueryRow(`SELECT event_id FROM session_events WHERE session_id=$1 AND model_request_id=$2 AND type='span.model_request_end'`, f.session, rp.id).Scan(&endID); err != nil {
		t.Fatal("lost response lacked durable End proof")
	}
	f.waitEvent(t, "uninterrupted", "span.model_request_end", 1)
	replayed, err := f.bridge.WriteRequestEnd(f.ctx, end)
	if err != nil || replayed.GetDuplicate().GetRequestEndEventId() != endID {
		t.Fatalf("actual End receipt retry: %v", err)
	}
	for id, before := range positions {
		if after := publicProjectionPositions(t, f)[id]; after != before {
			t.Fatalf("End mutated original list positions for %s: %v -> %v", id, before, after)
		}
	}
	var changes int
	if err = f.db.QueryRow(`SELECT count(*) FROM session_event_stream_changes WHERE session_id=$1 AND event_id=$2`, f.session, endID).Scan(&changes); err != nil || changes != 1 {
		t.Fatal("End retry appended another durable change")
	}
	next := f.start(t, "", "")
	result := f.fence(t, "uninterrupted", next)
	if countPublicEvents(result, "span.model_request_end") != 1 || countPublicEvents(result, "agent.message") != 2 {
		t.Fatal("End retry duplicated uninterrupted publication group")
	}
	f.end(t, next)
	t.Run("after-End-and-response-loss-replay", assertStage)
	t.Run("token-tampering-and-scope-rejection", func(t *testing.T) {
		otherSession := f.newSession(t)
		foreignKey := publicProjectionForeignKey(t, f)
		for _, cursor := range cursors {
			path := "/v1/sessions/" + f.session
			if cursor.thread != "" {
				path += "/threads/" + cursor.thread
			}
			path += "/events"
			query := url.Values{"beta": {"true"}, "limit": {"1"}, "order": {cursor.order}, "page": {cursor.page}}
			wrong := cursor.page
			at := len(wrong) / 2
			replacement := "A"
			if wrong[at] == 'A' {
				replacement = "B"
			}
			wrong = wrong[:at] + replacement + wrong[at+1:]
			for name, target := range map[string]string{"tampered": path, "wrong-session": "/v1/sessions/" + otherSession + "/events", "wrong-thread": "/v1/sessions/" + f.session + "/threads/" + f.thread(t) + "/events"} {
				q := url.Values{"beta": {"true"}, "limit": {"1"}, "order": {cursor.order}, "page": {cursor.page}}
				if name == "tampered" {
					q.Set("page", wrong)
				}
				if name == "wrong-thread" && cursor.thread == "" {
					continue
				}
				code, _ := f.raw(t, http.MethodGet, target+"?"+q.Encode(), "")
				if code != 400 {
					t.Fatalf("%s cursor status=%d", name, code)
				}
			}
			code, _ := f.raw(t, http.MethodGet, path+"?"+query.Encode(), foreignKey)
			if code != 400 && code != 404 {
				t.Fatalf("wrong-workspace cursor status=%d", code)
			}
			// The unmodified exact-scope token remains a positive control.
			code, _ = f.raw(t, http.MethodGet, path+"?"+query.Encode(), "")
			if code != 200 {
				t.Fatal("negative cursor controls broke valid token")
			}
		}
	})
	publicLogAssertion(t, "immutable-list-keys-limit-one-both-orders-scopes-End-replay")
}

func publicProjectionAllPages(t *testing.T, f *publicProjectionFixture, thread, order string, raw bool) []string {
	t.Helper()
	return publicProjectionPageTail(t, f, thread, order, "", raw)
}
func publicProjectionPageTail(t *testing.T, f *publicProjectionFixture, thread, order, page string, raw bool) []string {
	t.Helper()
	result := []string{}
	seen := map[string]bool{}
	for count := 0; count < 128; count++ {
		var value publicProjectionPage
		if raw {
			value = f.rawList(t, thread, order, page)
		} else {
			value = f.list(t, thread, order, page)
		}
		if len(value.Data) > 1 {
			t.Fatal("list ignored limit one")
		}
		for _, event := range value.Data {
			id := publicEventID(event)
			if id == "" || seen[id] || publicEventType(event) == "event_start" || publicEventType(event) == "event_delta" {
				t.Fatal("list returned preview or duplicate formal identity")
			}
			seen[id] = true
			result = append(result, id)
		}
		if value.Next == nil {
			return result
		}
		if *value.Next == page || *value.Next == "" {
			t.Fatal("non-progressing cursor")
		}
		page = *value.Next
	}
	t.Fatal("fixture paging budget exceeded")
	return nil
}
func publicProjectionPositions(t *testing.T, f *publicProjectionFixture) map[string][2]int64 {
	t.Helper()
	rows, err := f.db.Query(`SELECT event_id,insert_stream_position,sequence FROM session_events WHERE session_id=$1`, f.session)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	result := map[string][2]int64{}
	for rows.Next() {
		var id string
		var value [2]int64
		if err = rows.Scan(&id, &value[0], &value[1]); err != nil {
			t.Fatal(err)
		}
		result[id] = value
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	return result
}
func publicProjectionForeignKey(t *testing.T, f *publicProjectionFixture) string {
	t.Helper()
	ws := workspace.ID(id.New("workspace_"))
	if _, err := workspace.NewSeeder(f.db).Seed(f.ctx, ws, "foreign projection control"); err != nil {
		t.Fatal(err)
	}
	key, err := authtest.SeedIndependentKey(f.ctx, f.pools.OpenWorkload(t, "auth", nil), ws, fmt.Sprintf("foreign-%s", f.session))
	if err != nil {
		t.Fatal(err)
	}
	env, err := environment.NewPostgreSQLEnvironmentStore(dbconnect.NewClientForTesting(f.pools.OpenWorkload(t, "api", nil)), environment.WithDefaultArtifactRef("artifact_foreign_projection")).Create(f.ctx, ws, environment.CreateEnvironmentRequest{Name: "foreign projection"})
	if err != nil {
		t.Fatal(err)
	}
	client := startContentSDKChildContext(f.ctx, t, f.baseURL, key.APIKey)
	created := client.control(t, "provision", map[string]any{"environmentId": env.ID, "agent": map[string]any{"name": "foreign projection", "model": "anthropic/claude-opus-4-8", "approval_mode": "full_access", "tools": []any{map[string]any{"type": "tetral_agent_toolset", "family": "claude"}}, "skills": []any{}, "metadata": map[string]any{}}})
	var provisioned struct {
		Session struct {
			ID string `json:"id"`
		} `json:"session"`
	}
	if json.Unmarshal(created, &provisioned) != nil || provisioned.Session.ID == "" {
		t.Fatal("workspace-B actual SDK provision failed")
	}
	return key.APIKey
}
