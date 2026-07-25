package session_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/session"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestPostgreSQLSessionStoreListCreatedAtFiltersAndPageContinuity(t *testing.T) {
	runtime, admin := newControlPlaneSessionStoreTestDB(t)
	ctx := context.Background()
	store := newControlPlaneSessionStore(t, runtime)
	seedSessionStoreReferences(t, admin, workspace.DefaultID, "agent_page_filter", 1, "env_page_filter", "file_source_page_filter", "file_session_page_filter", "memstore_page_filter")

	base := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	fixtures := []struct {
		id        string
		createdAt time.Time
	}{
		{id: "sesn_page_filter_before_two", createdAt: base.Add(-2 * time.Minute)},
		{id: "sesn_page_filter_before_one", createdAt: base.Add(-1 * time.Minute)},
		{id: "sesn_page_filter_equal", createdAt: base},
		{id: "sesn_page_filter_after_one", createdAt: base.Add(time.Minute)},
		{id: "sesn_page_filter_after_two", createdAt: base.Add(2 * time.Minute)},
	}
	for _, fixture := range fixtures {
		sess := minimalStoreSession(fixture.id, "agent_page_filter", 1, "env_page_filter", fixture.createdAt)
		sess.Resources = []*session.Resource{memoryStoreResourceForList("sesrsc_"+fixture.id, sess.ID, "memstore_page_filter", fixture.createdAt)}
		if err := store.WithWorkspaceTx(ctx, workspace.DefaultID, func(tx session.Transaction) error {
			return tx.CreateSession(ctx, sess)
		}); err != nil {
			t.Fatalf("CreateSession %s: %v", sess.ID, err)
		}
	}

	cases := []struct {
		name string
		opts session.ListOptions
		want []string
	}{
		{
			name: "created_at_gt_excludes_equal",
			opts: session.ListOptions{CreatedAtGT: sessionPageFilterTimePointer(base), Order: session.ListOrderAscending},
			want: []string{"sesn_page_filter_after_one", "sesn_page_filter_after_two"},
		},
		{
			name: "created_at_gte_includes_equal",
			opts: session.ListOptions{CreatedAtGTE: sessionPageFilterTimePointer(base), Order: session.ListOrderAscending},
			want: []string{"sesn_page_filter_equal", "sesn_page_filter_after_one", "sesn_page_filter_after_two"},
		},
		{
			name: "created_at_lt_excludes_equal",
			opts: session.ListOptions{CreatedAtLT: sessionPageFilterTimePointer(base), Order: session.ListOrderAscending},
			want: []string{"sesn_page_filter_before_two", "sesn_page_filter_before_one"},
		},
		{
			name: "created_at_lte_includes_equal",
			opts: session.ListOptions{CreatedAtLTE: sessionPageFilterTimePointer(base), Order: session.ListOrderAscending},
			want: []string{"sesn_page_filter_before_two", "sesn_page_filter_before_one", "sesn_page_filter_equal"},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			testCase.opts.AgentID = "agent_page_filter"
			testCase.opts.AgentVersion = 1
			testCase.opts.MemoryStoreID = "memstore_page_filter"
			got, err := store.List(ctx, workspace.DefaultID, testCase.opts)
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			if ids := sessionIDs(got.Data); !slices.Equal(ids, testCase.want) {
				t.Fatalf("ids = %v; want %v", ids, testCase.want)
			}
			if got.NextPage != nil {
				t.Fatalf("next_page = %v; want nil for terminal filtered page", got.NextPage)
			}
		})
	}

	ascOptions := session.ListOptions{
		Limit:         2,
		AgentID:       "agent_page_filter",
		AgentVersion:  1,
		MemoryStoreID: "memstore_page_filter",
		CreatedAtGTE:  sessionPageFilterTimePointer(base.Add(-2 * time.Minute)),
		CreatedAtLTE:  sessionPageFilterTimePointer(base.Add(2 * time.Minute)),
		Order:         session.ListOrderAscending,
	}
	assertSessionPageSequence(ctx, t, store, ascOptions, []string{
		"sesn_page_filter_before_two",
		"sesn_page_filter_before_one",
		"sesn_page_filter_equal",
		"sesn_page_filter_after_one",
		"sesn_page_filter_after_two",
	})

	descOptions := ascOptions
	descOptions.Order = session.ListOrderDescending
	assertSessionPageSequence(ctx, t, store, descOptions, []string{
		"sesn_page_filter_after_two",
		"sesn_page_filter_after_one",
		"sesn_page_filter_equal",
		"sesn_page_filter_before_one",
		"sesn_page_filter_before_two",
	})
}

func TestPostgreSQLSessionStoreListSDKStatusAndDeploymentFilters(t *testing.T) {
	runtime, admin := newControlPlaneSessionStoreTestDB(t)
	ctx := context.Background()
	store := newControlPlaneSessionStore(t, runtime)
	seedSessionStoreReferences(t, admin, workspace.DefaultID, "agent_status_filter", 1, "env_status_filter")
	now := time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC)
	for index, id := range []string{"sesn_status_idle", "sesn_status_running", "sesn_status_terminated"} {
		sess := minimalStoreSession(id, "agent_status_filter", 1, "env_status_filter", now.Add(time.Duration(index)*time.Minute))
		if err := store.WithWorkspaceTx(ctx, workspace.DefaultID, func(tx session.Transaction) error {
			return tx.CreateSession(ctx, sess)
		}); err != nil {
			t.Fatalf("CreateSession %s: %v", id, err)
		}
	}
	if _, err := admin.ExecContext(ctx,
		`UPDATE session_runtime_status SET status = 'running' WHERE workspace_id = $1 AND session_id = 'sesn_status_running'`,
		string(workspace.DefaultID)); err != nil {
		t.Fatalf("set running status: %v", err)
	}
	if _, err := admin.ExecContext(ctx,
		`UPDATE sessions SET status = 'terminated' WHERE workspace_id = $1 AND id = 'sesn_status_terminated'`,
		string(workspace.DefaultID)); err != nil {
		t.Fatalf("set terminated status: %v", err)
	}

	listed, err := store.List(ctx, workspace.DefaultID, session.ListOptions{
		AgentID:  "agent_status_filter",
		Statuses: []session.Status{session.StatusRunning, session.StatusTerminated},
		Order:    session.ListOrderAscending,
	})
	if err != nil {
		t.Fatalf("List status filters: %v", err)
	}
	if ids := sessionIDs(listed.Data); !slices.Equal(ids, []string{"sesn_status_running", "sesn_status_terminated"}) {
		t.Fatalf("status-filter ids = %v; want running and terminated", ids)
	}

	empty, err := store.List(ctx, workspace.DefaultID, session.ListOptions{
		AgentID:      "agent_status_filter",
		DeploymentID: "deployment_unsupported",
	})
	if err != nil {
		t.Fatalf("List deployment filter: %v", err)
	}
	if len(empty.Data) != 0 || empty.NextPage != nil {
		t.Fatalf("deployment-filter response = %+v; want empty page", empty)
	}
}

func TestPostgreSQLSessionStoreRejectsPageTokenReplayWhenSignedFiltersChange(t *testing.T) {
	runtime, admin := newControlPlaneSessionStoreTestDB(t)
	ctx := context.Background()
	store := newControlPlaneSessionStore(t, runtime)
	seedSessionStoreReferences(t, admin, workspace.DefaultID, "agent_page_replay", 1, "env_page_replay", "file_source_page_replay", "file_session_page_replay", "memstore_page_replay")
	seedSessionStoreReferences(t, admin, workspace.DefaultID, "agent_page_replay", 1, "env_page_replay", "file_source_page_replay_other", "file_session_page_replay_other", "memstore_page_replay_other")

	base := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	for index, id := range []string{"sesn_page_replay_a", "sesn_page_replay_b", "sesn_page_replay_c"} {
		createdAt := base.Add(time.Duration(index) * time.Minute)
		sess := minimalStoreSession(id, "agent_page_replay", 1, "env_page_replay", createdAt)
		sess.Resources = []*session.Resource{memoryStoreResourceForList("sesrsc_"+id, sess.ID, "memstore_page_replay", createdAt)}
		if err := store.WithWorkspaceTx(ctx, workspace.DefaultID, func(tx session.Transaction) error {
			return tx.CreateSession(ctx, sess)
		}); err != nil {
			t.Fatalf("CreateSession %s: %v", sess.ID, err)
		}
	}

	baseOptions := session.ListOptions{
		Limit:           1,
		IncludeArchived: true,
		AgentID:         "agent_page_replay",
		AgentVersion:    1,
		MemoryStoreID:   "memstore_page_replay",
		CreatedAtGT:     sessionPageFilterTimePointer(base.Add(-10 * time.Minute)),
		CreatedAtGTE:    sessionPageFilterTimePointer(base.Add(-9 * time.Minute)),
		CreatedAtLT:     sessionPageFilterTimePointer(base.Add(10 * time.Minute)),
		CreatedAtLTE:    sessionPageFilterTimePointer(base.Add(9 * time.Minute)),
		Order:           session.ListOrderAscending,
	}
	first, err := store.List(ctx, workspace.DefaultID, baseOptions)
	if err != nil {
		t.Fatalf("List first page: %v", err)
	}
	if len(first.Data) != 1 || first.Data[0].ID != "sesn_page_replay_a" || first.NextPage == nil {
		t.Fatalf("first page = %#v next=%v; want first item and next page", sessionIDs(first.Data), first.NextPage)
	}

	validReplay := baseOptions
	validReplay.Page = *first.NextPage
	second, err := store.List(ctx, workspace.DefaultID, validReplay)
	if err != nil {
		t.Fatalf("valid replay: %v", err)
	}
	if ids := sessionIDs(second.Data); !slices.Equal(ids, []string{"sesn_page_replay_b"}) {
		t.Fatalf("valid replay ids = %v; want second page", ids)
	}

	cases := []struct {
		name   string
		mutate func(*session.ListOptions)
	}{
		{name: "include_archived", mutate: func(options *session.ListOptions) { options.IncludeArchived = false }},
		{name: "memory_store_id", mutate: func(options *session.ListOptions) { options.MemoryStoreID = "memstore_page_replay_other" }},
		{name: "created_at_gt", mutate: func(options *session.ListOptions) {
			options.CreatedAtGT = sessionPageFilterTimePointer(base.Add(-11 * time.Minute))
		}},
		{name: "created_at_gte", mutate: func(options *session.ListOptions) {
			options.CreatedAtGTE = sessionPageFilterTimePointer(base.Add(-8 * time.Minute))
		}},
		{name: "created_at_lt", mutate: func(options *session.ListOptions) {
			options.CreatedAtLT = sessionPageFilterTimePointer(base.Add(11 * time.Minute))
		}},
		{name: "created_at_lte", mutate: func(options *session.ListOptions) {
			options.CreatedAtLTE = sessionPageFilterTimePointer(base.Add(8 * time.Minute))
		}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			options := baseOptions
			options.Page = *first.NextPage
			testCase.mutate(&options)
			_, err := store.List(ctx, workspace.DefaultID, options)
			var validation *session.ValidationError
			if !errors.As(err, &validation) {
				t.Fatalf("List with changed %s err = %T %v; want ValidationError", testCase.name, err, err)
			}
		})
	}
}

func assertSessionPageSequence(ctx context.Context, t *testing.T, store *session.PostgreSQLSessionStore, options session.ListOptions, want []string) {
	t.Helper()
	var got []string
	var sawNextPage bool
	for {
		result, err := store.List(ctx, workspace.DefaultID, options)
		if err != nil {
			t.Fatalf("List page after %q: %v", options.Page, err)
		}
		got = append(got, sessionIDs(result.Data)...)
		if result.NextPage == nil {
			if !sawNextPage {
				t.Fatalf("sequence %v ended without pagination; want at least one next_page", got)
			}
			break
		}
		sawNextPage = true
		options.Page = *result.NextPage
	}
	if !slices.Equal(got, want) {
		t.Fatalf("paged ids = %v; want %v", got, want)
	}
}

func sessionPageFilterTimePointer(value time.Time) *time.Time { return &value }
