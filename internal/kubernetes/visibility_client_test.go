package kubernetes

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
)

func TestSyncAndWatchUsesConfiguredNamespaceAndSelectors(t *testing.T) {
	cfg, err := LoadConfig(envMap{
		"TETRAL_KUBERNETES_NAMESPACE":         "tetral-runtime",
		"TETRAL_AGENT_RUNTIME_LABEL_SELECTOR": "app=agent-runtime",
		"TETRAL_AGENT_RUNTIME_SERVICE_NAME":   "agent-runtime-svc",
	})
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	client := newRecordingVisibilityClient()
	cache := NewWatcherCache(cfg.Namespace)
	handles, err := SyncAndWatch(context.Background(), client, cfg, cache)
	if err != nil {
		t.Fatalf("SyncAndWatch: %v", err)
	}
	defer handles.Stop()

	want := []string{
		"list-pods:tetral-runtime:app=agent-runtime",
		"list-endpointslices:tetral-runtime:kubernetes.io/service-name=agent-runtime-svc",
		"watch-pods:tetral-runtime:app=agent-runtime:rv=",
		"watch-endpointslices:tetral-runtime:kubernetes.io/service-name=agent-runtime-svc:rv=",
	}
	if len(client.calls) != len(want) {
		t.Fatalf("calls = %v; want %v", client.calls, want)
	}
	for index := range want {
		if client.calls[index] != want[index] {
			t.Fatalf("calls = %v; want %v", client.calls, want)
		}
	}
}

func TestSyncAndWatchMarksCacheUnreadyOnAPIOutage(t *testing.T) {
	cfg, err := LoadConfig(envMap{
		"TETRAL_KUBERNETES_NAMESPACE": "tetral-runtime",
	})
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	cache := NewWatcherCache(cfg.Namespace)
	_, err = SyncAndWatch(context.Background(), &recordingVisibilityClient{listPodErr: errors.New("api outage")}, cfg, cache)
	if err == nil {
		t.Fatal("SyncAndWatch returned nil for API outage")
	}
	if cache.Snapshot().Ready {
		t.Fatal("cache ready after API outage")
	}
}

func TestSyncAndWatchUsesListResourceVersionForInitialWatches(t *testing.T) {
	cfg := newVisibilityTestConfig(t)
	client := newRecordingVisibilityClient()
	client.podResourceVersions = []string{"pod-rv-1"}
	client.endpointSliceResourceVersions = []string{"endpoint-rv-1"}
	cache := NewWatcherCache(cfg.Namespace)
	handles, err := SyncAndWatch(context.Background(), client, cfg, cache)
	if err != nil {
		t.Fatalf("SyncAndWatch: %v", err)
	}
	defer handles.Stop()

	client.assertCall(t, "watch-pods:tetral-runtime:app.kubernetes.io/name=agent-runtime:rv=pod-rv-1")
	client.assertCall(t, "watch-endpointslices:tetral-runtime:kubernetes.io/service-name=agent-runtime:rv=endpoint-rv-1")
}

func TestSyncAndWatchRequestsBookmarksForInitialAndReplacementWatches(t *testing.T) {
	cfg := newVisibilityTestConfig(t)
	pod := readyPod("runtime-a", "pod-uid-a")
	client := newRecordingVisibilityClient()
	client.pods = []corev1.Pod{pod}
	client.endpointSlices = []discoveryv1.EndpointSlice{endpointSlice(endpointForPod(pod, []string{"10.0.0.1"}, nil, nil, nil))}
	client.podResourceVersions = []string{"pod-rv-1", "pod-rv-2"}
	client.endpointSliceResourceVersions = []string{"endpoint-rv-1", "endpoint-rv-2"}
	cache := NewWatcherCache(cfg.Namespace)
	handles, err := SyncAndWatchWithOptions(context.Background(), client, cfg, cache, SyncOptions{
		RestartInitialBackoff: time.Millisecond,
		RestartMaximumBackoff: time.Millisecond,
		FreshnessTimeout:      time.Second,
	})
	if err != nil {
		t.Fatalf("SyncAndWatchWithOptions: %v", err)
	}
	defer handles.Stop()

	assertWatchOptions(t, client.podWatchOption(t, 0), "app.kubernetes.io/name=agent-runtime", "pod-rv-1")
	assertWatchOptions(t, client.endpointSliceWatchOption(t, 0), "kubernetes.io/service-name=agent-runtime", "endpoint-rv-1")

	client.currentPodWatch().Stop()
	client.currentEndpointSliceWatch().Stop()
	waitForCondition(t, "replacement watches", func() bool {
		return client.podWatchCount() >= 2 && client.endpointSliceWatchCount() >= 2
	})

	assertWatchOptions(t, client.podWatchOption(t, 1), "app.kubernetes.io/name=agent-runtime", "pod-rv-2")
	assertWatchOptions(t, client.endpointSliceWatchOption(t, 1), "kubernetes.io/service-name=agent-runtime", "endpoint-rv-2")
}

func TestSyncAndWatchBookmarkProgressKeepsWatchesFresh(t *testing.T) {
	cfg := newVisibilityTestConfig(t)
	pod := readyPod("runtime-a", "pod-uid-a")
	client := newRecordingVisibilityClient()
	client.pods = []corev1.Pod{pod}
	client.endpointSlices = []discoveryv1.EndpointSlice{endpointSlice(endpointForPod(pod, []string{"10.0.0.1"}, nil, nil, nil))}
	cache := NewWatcherCache(cfg.Namespace)
	freshnessTimeout := 300 * time.Millisecond
	handles, err := SyncAndWatchWithOptions(context.Background(), client, cfg, cache, SyncOptions{
		RestartInitialBackoff: time.Millisecond,
		RestartMaximumBackoff: time.Millisecond,
		FreshnessTimeout:      freshnessTimeout,
	})
	if err != nil {
		t.Fatalf("SyncAndWatchWithOptions: %v", err)
	}
	defer handles.Stop()
	waitForCache(t, cache, func(snapshot Snapshot) bool {
		return snapshot.Ready && len(snapshot.Candidates) == 1
	})

	client.currentPodWatch().Action(watch.Bookmark, &corev1.Pod{})
	client.currentEndpointSliceWatch().Action(watch.Bookmark, &discoveryv1.EndpointSlice{})
	time.Sleep(freshnessTimeout / 2)
	client.currentPodWatch().Action(watch.Bookmark, &corev1.Pod{})
	client.currentEndpointSliceWatch().Action(watch.Bookmark, &discoveryv1.EndpointSlice{})
	time.Sleep(freshnessTimeout/2 + 25*time.Millisecond)

	snapshot := cache.Snapshot()
	if !snapshot.Ready || len(snapshot.Candidates) != 1 {
		t.Fatalf("snapshot after bookmark progress = %+v; want ready with existing candidate", snapshot)
	}
	bindingSnapshot := cache.BindingVisibilitySnapshot()
	if !bindingSnapshot.Ready || len(bindingSnapshot.Candidates) != 1 {
		t.Fatalf("binding snapshot after bookmark progress = %+v; want ready with existing candidate", bindingSnapshot)
	}
}

func TestSyncAndWatchConsumesPodAndEndpointSliceEvents(t *testing.T) {
	cfg := newVisibilityTestConfig(t)
	client := newRecordingVisibilityClient()
	cache := NewWatcherCache(cfg.Namespace)
	handles, err := SyncAndWatchWithOptions(context.Background(), client, cfg, cache, SyncOptions{
		RestartInitialBackoff: time.Second,
		RestartMaximumBackoff: time.Second,
	})
	if err != nil {
		t.Fatalf("SyncAndWatchWithOptions: %v", err)
	}
	defer handles.Stop()

	pod := readyPod("runtime-a", "pod-uid-a")
	client.currentPodWatch().Add(&pod)
	slice := endpointSlice(endpointForPod(pod, []string{"10.0.0.1"}, nil, nil, nil))
	client.currentEndpointSliceWatch().Add(&slice)
	waitForCache(t, cache, func(snapshot Snapshot) bool {
		return snapshot.Ready && len(snapshot.Candidates) == 1 && snapshot.Candidates[0].PodName == "runtime-a"
	})

	notReady := notReadyPod("runtime-a", "pod-uid-a")
	client.currentPodWatch().Modify(&notReady)
	waitForCache(t, cache, func(snapshot Snapshot) bool {
		return snapshot.Ready && len(snapshot.Candidates) == 0
	})

	client.currentPodWatch().Modify(&pod)
	waitForCache(t, cache, func(snapshot Snapshot) bool {
		return snapshot.Ready && len(snapshot.Candidates) == 1
	})

	client.currentPodWatch().Delete(&pod)
	waitForCache(t, cache, func(snapshot Snapshot) bool {
		return snapshot.Ready && len(snapshot.Candidates) == 0
	})

	client.currentPodWatch().Add(&pod)
	waitForCache(t, cache, func(snapshot Snapshot) bool {
		return snapshot.Ready && len(snapshot.Candidates) == 1
	})

	modifiedSlice := endpointSlice(endpointForPod(pod, []string{"10.0.0.2"}, nil, nil, nil))
	client.currentEndpointSliceWatch().Modify(&modifiedSlice)
	waitForCache(t, cache, func(snapshot Snapshot) bool {
		return snapshot.Ready &&
			len(snapshot.Candidates) == 1 &&
			len(snapshot.Candidates[0].Addresses) == 1 &&
			snapshot.Candidates[0].Addresses[0] == "10.0.0.2"
	})

	client.currentEndpointSliceWatch().Delete(&slice)
	waitForCache(t, cache, func(snapshot Snapshot) bool {
		return snapshot.Ready && len(snapshot.Candidates) == 0
	})
}

func TestSyncAndWatchMarksStaleAndRestartsAfterWatchClosure(t *testing.T) {
	cfg := newVisibilityTestConfig(t)
	pod := readyPod("runtime-a", "pod-uid-a")
	client := newRecordingVisibilityClient()
	client.pods = []corev1.Pod{pod}
	client.endpointSlices = []discoveryv1.EndpointSlice{endpointSlice(endpointForPod(pod, []string{"10.0.0.1"}, nil, nil, nil))}
	client.blockSecondPodList = make(chan struct{})
	cache := NewWatcherCache(cfg.Namespace)
	handles, err := SyncAndWatchWithOptions(context.Background(), client, cfg, cache, SyncOptions{
		RestartInitialBackoff: time.Millisecond,
		RestartMaximumBackoff: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("SyncAndWatchWithOptions: %v", err)
	}
	defer handles.Stop()
	waitForCache(t, cache, func(snapshot Snapshot) bool {
		return snapshot.Ready && len(snapshot.Candidates) == 1
	})

	client.currentPodWatch().Stop()
	waitForCache(t, cache, func(snapshot Snapshot) bool {
		return !snapshot.Ready
	})
	close(client.blockSecondPodList)
	waitForCondition(t, "pod watch restarted", func() bool {
		return client.podWatchCount() >= 2
	})
	waitForCache(t, cache, func(snapshot Snapshot) bool {
		return snapshot.Ready && len(snapshot.Candidates) == 1
	})
}

func TestSyncAndWatchKeepsCacheUnreadyUntilReplacementWatchStarts(t *testing.T) {
	cfg := newVisibilityTestConfig(t)
	pod := readyPod("runtime-a", "pod-uid-a")
	client := newRecordingVisibilityClient()
	client.pods = []corev1.Pod{pod}
	client.endpointSlices = []discoveryv1.EndpointSlice{endpointSlice(endpointForPod(pod, []string{"10.0.0.1"}, nil, nil, nil))}
	client.podResourceVersions = []string{"pod-rv-1", "pod-rv-2", "pod-rv-3"}
	client.endpointSliceResourceVersions = []string{"endpoint-rv-1", "endpoint-rv-2", "endpoint-rv-3"}
	client.failPodWatchNumbers = map[int]error{2: errors.New("watch unavailable")}
	client.failEndpointSliceWatchNumbers = map[int]error{2: errors.New("watch unavailable")}
	client.blockThirdPodList = make(chan struct{})
	client.blockThirdEndpointSliceList = make(chan struct{})
	cache := NewWatcherCache(cfg.Namespace)
	handles, err := SyncAndWatchWithOptions(context.Background(), client, cfg, cache, SyncOptions{
		RestartInitialBackoff: time.Millisecond,
		RestartMaximumBackoff: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("SyncAndWatchWithOptions: %v", err)
	}
	defer handles.Stop()
	waitForCache(t, cache, func(snapshot Snapshot) bool {
		return snapshot.Ready && len(snapshot.Candidates) == 1
	})

	client.currentPodWatch().Stop()
	client.currentEndpointSliceWatch().Stop()
	waitForCondition(t, "failed replacement watch call", func() bool {
		return client.hasCall("watch-pods:tetral-runtime:app.kubernetes.io/name=agent-runtime:rv=pod-rv-2") &&
			client.hasCall("watch-endpointslices:tetral-runtime:kubernetes.io/service-name=agent-runtime:rv=endpoint-rv-2")
	})
	if cache.Snapshot().Ready {
		t.Fatal("cache became ready after relist while replacement watch was unavailable")
	}
	close(client.blockThirdPodList)
	close(client.blockThirdEndpointSliceList)
	waitForCondition(t, "successful replacement watch call", func() bool {
		return client.hasCall("watch-pods:tetral-runtime:app.kubernetes.io/name=agent-runtime:rv=pod-rv-3") &&
			client.hasCall("watch-endpointslices:tetral-runtime:kubernetes.io/service-name=agent-runtime:rv=endpoint-rv-3")
	})
	waitForCache(t, cache, func(snapshot Snapshot) bool {
		return snapshot.Ready && len(snapshot.Candidates) == 1
	})
}

func TestSyncAndWatchKeepsCacheUnreadyForEndpointSliceReplacementWatchFailure(t *testing.T) {
	cfg := newVisibilityTestConfig(t)
	pod := readyPod("runtime-a", "pod-uid-a")
	client := newRecordingVisibilityClient()
	client.pods = []corev1.Pod{pod}
	client.endpointSlices = []discoveryv1.EndpointSlice{endpointSlice(endpointForPod(pod, []string{"10.0.0.1"}, nil, nil, nil))}
	client.endpointSliceResourceVersions = []string{"endpoint-rv-1", "endpoint-rv-2", "endpoint-rv-3"}
	client.failEndpointSliceWatchNumbers = map[int]error{2: errors.New("watch unavailable")}
	client.blockThirdEndpointSliceList = make(chan struct{})
	cache := NewWatcherCache(cfg.Namespace)
	handles, err := SyncAndWatchWithOptions(context.Background(), client, cfg, cache, SyncOptions{
		RestartInitialBackoff: time.Millisecond,
		RestartMaximumBackoff: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("SyncAndWatchWithOptions: %v", err)
	}
	defer handles.Stop()
	waitForCache(t, cache, func(snapshot Snapshot) bool {
		return snapshot.Ready && len(snapshot.Candidates) == 1
	})

	client.currentEndpointSliceWatch().Stop()
	waitForCondition(t, "failed endpointslice replacement watch call", func() bool {
		return client.hasCall("watch-endpointslices:tetral-runtime:kubernetes.io/service-name=agent-runtime:rv=endpoint-rv-2")
	})
	if cache.Snapshot().Ready {
		t.Fatal("cache became ready after endpointslice relist while replacement watch was unavailable")
	}
	if client.podWatchCount() != 1 {
		t.Fatalf("pod watch count = %d; want unchanged healthy pod watch", client.podWatchCount())
	}
	close(client.blockThirdEndpointSliceList)
	waitForCondition(t, "successful endpointslice replacement watch call", func() bool {
		return client.hasCall("watch-endpointslices:tetral-runtime:kubernetes.io/service-name=agent-runtime:rv=endpoint-rv-3")
	})
	waitForCache(t, cache, func(snapshot Snapshot) bool {
		return snapshot.Ready && len(snapshot.Candidates) == 1
	})
}

func TestSyncAndWatchPodFreshnessTimeoutStalesPodsAndRestartsAfterReplacementWatch(t *testing.T) {
	cfg := newVisibilityTestConfig(t)
	pod := readyPod("runtime-a", "pod-uid-a")
	client := newRecordingVisibilityClient()
	client.pods = []corev1.Pod{pod}
	client.endpointSlices = []discoveryv1.EndpointSlice{endpointSlice(endpointForPod(pod, []string{"10.0.0.1"}, nil, nil, nil))}
	client.podResourceVersions = []string{"pod-rv-1", "pod-rv-2", "pod-rv-3"}
	client.failPodWatchNumbers = map[int]error{2: errors.New("watch unavailable")}
	client.blockSecondPodList = make(chan struct{})
	client.blockThirdPodList = make(chan struct{})
	cache := NewWatcherCache(cfg.Namespace)
	freshnessTimeout := 500 * time.Millisecond
	handles, err := SyncAndWatchWithOptions(context.Background(), client, cfg, cache, SyncOptions{
		RestartInitialBackoff: time.Millisecond,
		RestartMaximumBackoff: time.Millisecond,
		FreshnessTimeout:      freshnessTimeout,
	})
	if err != nil {
		t.Fatalf("SyncAndWatchWithOptions: %v", err)
	}
	defer handles.Stop()
	waitForCache(t, cache, func(snapshot Snapshot) bool {
		return snapshot.Ready && len(snapshot.Candidates) == 1
	})

	client.currentPodWatch().Action(watch.Bookmark, &corev1.Pod{})
	client.currentEndpointSliceWatch().Action(watch.Bookmark, &discoveryv1.EndpointSlice{})
	time.Sleep(freshnessTimeout / 2)
	client.currentEndpointSliceWatch().Action(watch.Bookmark, &discoveryv1.EndpointSlice{})
	waitForCondition(t, "pod freshness timeout stale state", func() bool {
		state := cacheStateForTest(cache)
		return state.podsStale && !state.podsFailed && !state.endpointSlicesStale && !state.endpointSlicesFailed
	})

	state := cacheStateForTest(cache)
	if state.ready || state.snapshotCandidates != 0 || state.bindingReady || state.bindingCandidates != 0 {
		t.Fatalf("cache state after pod freshness timeout = %+v; want not ready with no visible candidates", state)
	}
	if client.endpointSliceWatchCount() != 1 {
		t.Fatalf("endpoint slice watch count = %d; want unchanged healthy watch", client.endpointSliceWatchCount())
	}
	if client.hasCall("watch-pods:tetral-runtime:app.kubernetes.io/name=agent-runtime:rv=pod-rv-2") {
		t.Fatal("replacement pod watch started before blocked relist was released")
	}

	close(client.blockSecondPodList)
	waitForCondition(t, "failed replacement pod watch", func() bool {
		return client.hasCall("watch-pods:tetral-runtime:app.kubernetes.io/name=agent-runtime:rv=pod-rv-2")
	})
	state = cacheStateForTest(cache)
	if state.ready || state.snapshotCandidates != 0 || state.bindingReady || state.bindingCandidates != 0 {
		t.Fatalf("cache state after failed replacement pod watch = %+v; want not ready with no visible candidates", state)
	}

	close(client.blockThirdPodList)
	waitForCondition(t, "successful replacement pod watch", func() bool {
		return client.hasCall("watch-pods:tetral-runtime:app.kubernetes.io/name=agent-runtime:rv=pod-rv-3")
	})
	waitForCache(t, cache, func(snapshot Snapshot) bool {
		return snapshot.Ready && len(snapshot.Candidates) == 1
	})
}

func TestSyncAndWatchEndpointSliceFreshnessTimeoutStalesEndpointSlicesAndRestartsAfterReplacementWatch(t *testing.T) {
	cfg := newVisibilityTestConfig(t)
	pod := readyPod("runtime-a", "pod-uid-a")
	client := newRecordingVisibilityClient()
	client.pods = []corev1.Pod{pod}
	client.endpointSlices = []discoveryv1.EndpointSlice{endpointSlice(endpointForPod(pod, []string{"10.0.0.1"}, nil, nil, nil))}
	client.endpointSliceResourceVersions = []string{"endpoint-rv-1", "endpoint-rv-2", "endpoint-rv-3"}
	client.failEndpointSliceWatchNumbers = map[int]error{2: errors.New("watch unavailable")}
	client.blockSecondEndpointSliceList = make(chan struct{})
	client.blockThirdEndpointSliceList = make(chan struct{})
	cache := NewWatcherCache(cfg.Namespace)
	freshnessTimeout := 500 * time.Millisecond
	handles, err := SyncAndWatchWithOptions(context.Background(), client, cfg, cache, SyncOptions{
		RestartInitialBackoff: time.Millisecond,
		RestartMaximumBackoff: time.Millisecond,
		FreshnessTimeout:      freshnessTimeout,
	})
	if err != nil {
		t.Fatalf("SyncAndWatchWithOptions: %v", err)
	}
	defer handles.Stop()
	waitForCache(t, cache, func(snapshot Snapshot) bool {
		return snapshot.Ready && len(snapshot.Candidates) == 1
	})

	client.currentPodWatch().Action(watch.Bookmark, &corev1.Pod{})
	client.currentEndpointSliceWatch().Action(watch.Bookmark, &discoveryv1.EndpointSlice{})
	time.Sleep(freshnessTimeout / 2)
	client.currentPodWatch().Action(watch.Bookmark, &corev1.Pod{})
	waitForCondition(t, "endpointslice freshness timeout stale state", func() bool {
		state := cacheStateForTest(cache)
		return !state.podsStale && !state.podsFailed && state.endpointSlicesStale && !state.endpointSlicesFailed
	})

	state := cacheStateForTest(cache)
	if state.ready || state.snapshotCandidates != 0 || state.bindingReady || state.bindingCandidates != 0 {
		t.Fatalf("cache state after endpointslice freshness timeout = %+v; want not ready with no visible candidates", state)
	}
	if client.podWatchCount() != 1 {
		t.Fatalf("pod watch count = %d; want unchanged healthy watch", client.podWatchCount())
	}
	if client.hasCall("watch-endpointslices:tetral-runtime:kubernetes.io/service-name=agent-runtime:rv=endpoint-rv-2") {
		t.Fatal("replacement endpointslice watch started before blocked relist was released")
	}

	close(client.blockSecondEndpointSliceList)
	waitForCondition(t, "failed replacement endpointslice watch", func() bool {
		return client.hasCall("watch-endpointslices:tetral-runtime:kubernetes.io/service-name=agent-runtime:rv=endpoint-rv-2")
	})
	state = cacheStateForTest(cache)
	if state.ready || state.snapshotCandidates != 0 || state.bindingReady || state.bindingCandidates != 0 {
		t.Fatalf("cache state after failed replacement endpointslice watch = %+v; want not ready with no visible candidates", state)
	}

	close(client.blockThirdEndpointSliceList)
	waitForCondition(t, "successful replacement endpointslice watch", func() bool {
		return client.hasCall("watch-endpointslices:tetral-runtime:kubernetes.io/service-name=agent-runtime:rv=endpoint-rv-3")
	})
	waitForCache(t, cache, func(snapshot Snapshot) bool {
		return snapshot.Ready && len(snapshot.Candidates) == 1
	})
}

func TestSyncAndWatchMarksWatchErrorsUnreadyLogsSafelyAndRestarts(t *testing.T) {
	cfg := newVisibilityTestConfig(t)
	var buffer lockedBuffer
	logger := newTestLogger(&buffer)
	pod := readyPod("runtime-a", "pod-uid-a")
	client := newRecordingVisibilityClient()
	client.pods = []corev1.Pod{pod}
	client.endpointSlices = []discoveryv1.EndpointSlice{endpointSlice(endpointForPod(pod, []string{"10.0.0.1"}, nil, nil, nil))}
	client.blockSecondPodList = make(chan struct{})
	client.blockSecondEndpointSliceList = make(chan struct{})
	cache := NewWatcherCache(cfg.Namespace, WithLogger(logger))
	handles, err := SyncAndWatchWithOptions(context.Background(), client, cfg, cache, SyncOptions{
		RestartInitialBackoff: time.Millisecond,
		RestartMaximumBackoff: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("SyncAndWatchWithOptions: %v", err)
	}
	defer handles.Stop()
	waitForCache(t, cache, func(snapshot Snapshot) bool {
		return snapshot.Ready && len(snapshot.Candidates) == 1
	})

	client.currentPodWatch().Error(&metav1.Status{Reason: metav1.StatusReasonForbidden, Message: "Secret bearer-token raw payload"})
	client.currentEndpointSliceWatch().Error(&metav1.Status{Reason: metav1.StatusReasonForbidden, Message: "Secret bearer-token raw payload"})
	waitForCondition(t, "pod and endpointslice watch errors logged", func() bool {
		output := buffer.String()
		return strings.Contains(output, `"kubernetes.resource":"pods"`) &&
			strings.Contains(output, `"kubernetes.resource":"endpointslices"`)
	})
	if cache.Snapshot().Ready {
		t.Fatal("cache ready after watch errors")
	}
	for _, forbidden := range []string{"bearer-token", "raw payload"} {
		if strings.Contains(buffer.String(), forbidden) {
			t.Fatalf("watch error log leaked %q: %s", forbidden, buffer.String())
		}
	}
	for _, want := range []string{`"kubernetes.resource":"pods"`, `"kubernetes.resource":"endpointslices"`, `"kubernetes.status":"watch_error"`} {
		if !strings.Contains(buffer.String(), want) {
			t.Fatalf("watch error log missing %s: %s", want, buffer.String())
		}
	}
	close(client.blockSecondPodList)
	close(client.blockSecondEndpointSliceList)
	waitForCondition(t, "pod and endpointslice watches restarted", func() bool {
		return client.podWatchCount() >= 2 && client.endpointSliceWatchCount() >= 2
	})
	waitForCache(t, cache, func(snapshot Snapshot) bool {
		return snapshot.Ready && len(snapshot.Candidates) == 1
	})
}

func TestSyncAndWatchMarksMalformedWatchObjectsUnready(t *testing.T) {
	cfg := newVisibilityTestConfig(t)
	client := newRecordingVisibilityClient()
	cache := NewWatcherCache(cfg.Namespace)
	handles, err := SyncAndWatchWithOptions(context.Background(), client, cfg, cache, SyncOptions{
		RestartInitialBackoff: time.Second,
		RestartMaximumBackoff: time.Second,
	})
	if err != nil {
		t.Fatalf("SyncAndWatchWithOptions: %v", err)
	}
	defer handles.Stop()

	client.currentPodWatch().Add(&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "not-a-pod"}})
	waitForCache(t, cache, func(snapshot Snapshot) bool {
		return !snapshot.Ready
	})
}

type recordingVisibilityClient struct {
	mutex                         sync.Mutex
	calls                         []string
	pods                          []corev1.Pod
	endpointSlices                []discoveryv1.EndpointSlice
	podResourceVersions           []string
	endpointSliceResourceVersions []string
	podWatches                    []*watch.FakeWatcher
	endpointWatches               []*watch.FakeWatcher
	podWatchOptions               []metav1.ListOptions
	endpointSliceWatchOptions     []metav1.ListOptions
	podWatchCalls                 int
	endpointSliceWatchCalls       int
	listPodErr                    error
	failPodWatchNumbers           map[int]error
	failEndpointSliceWatchNumbers map[int]error
	blockSecondPodList            chan struct{}
	blockThirdPodList             chan struct{}
	blockSecondEndpointSliceList  chan struct{}
	blockThirdEndpointSliceList   chan struct{}
}

func newRecordingVisibilityClient() *recordingVisibilityClient {
	return &recordingVisibilityClient{}
}

func (c *recordingVisibilityClient) ListPods(_ context.Context, namespace string, options metav1.ListOptions) (*corev1.PodList, error) {
	c.mutex.Lock()
	c.calls = append(c.calls, "list-pods:"+namespace+":"+options.LabelSelector)
	callCount := c.listPodCallCountLocked()
	blockSecondPodList := c.blockSecondPodList
	blockThirdPodList := c.blockThirdPodList
	pods := append([]corev1.Pod(nil), c.pods...)
	listPodErr := c.listPodErr
	resourceVersion := resourceVersionForCall(c.podResourceVersions, callCount)
	c.mutex.Unlock()
	if callCount > 1 && blockSecondPodList != nil {
		<-blockSecondPodList
	}
	if callCount == 3 && blockThirdPodList != nil {
		<-blockThirdPodList
	}
	if listPodErr != nil {
		return nil, listPodErr
	}
	return &corev1.PodList{ListMeta: metav1.ListMeta{ResourceVersion: resourceVersion}, Items: pods}, nil
}

func (c *recordingVisibilityClient) WatchPods(_ context.Context, namespace string, options metav1.ListOptions) (watch.Interface, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.calls = append(c.calls, "watch-pods:"+namespace+":"+options.LabelSelector+":rv="+options.ResourceVersion)
	c.podWatchCalls++
	watchNumber := c.podWatchCalls
	if c.failPodWatchNumbers != nil {
		if err := c.failPodWatchNumbers[watchNumber]; err != nil {
			return nil, err
		}
	}
	watcher := watch.NewFake()
	c.podWatches = append(c.podWatches, watcher)
	c.podWatchOptions = append(c.podWatchOptions, options)
	return watcher, nil
}

func (c *recordingVisibilityClient) ListEndpointSlices(_ context.Context, namespace string, options metav1.ListOptions) (*discoveryv1.EndpointSliceList, error) {
	c.mutex.Lock()
	c.calls = append(c.calls, "list-endpointslices:"+namespace+":"+options.LabelSelector)
	callCount := c.listEndpointSliceCallCountLocked()
	blockSecondEndpointSliceList := c.blockSecondEndpointSliceList
	blockThirdEndpointSliceList := c.blockThirdEndpointSliceList
	slices := append([]discoveryv1.EndpointSlice(nil), c.endpointSlices...)
	resourceVersion := resourceVersionForCall(c.endpointSliceResourceVersions, callCount)
	c.mutex.Unlock()
	if callCount > 1 && blockSecondEndpointSliceList != nil {
		<-blockSecondEndpointSliceList
	}
	if callCount == 3 && blockThirdEndpointSliceList != nil {
		<-blockThirdEndpointSliceList
	}
	return &discoveryv1.EndpointSliceList{ListMeta: metav1.ListMeta{ResourceVersion: resourceVersion}, Items: slices}, nil
}

func (c *recordingVisibilityClient) WatchEndpointSlices(_ context.Context, namespace string, options metav1.ListOptions) (watch.Interface, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.calls = append(c.calls, "watch-endpointslices:"+namespace+":"+options.LabelSelector+":rv="+options.ResourceVersion)
	c.endpointSliceWatchCalls++
	watchNumber := c.endpointSliceWatchCalls
	if c.failEndpointSliceWatchNumbers != nil {
		if err := c.failEndpointSliceWatchNumbers[watchNumber]; err != nil {
			return nil, err
		}
	}
	watcher := watch.NewFake()
	c.endpointWatches = append(c.endpointWatches, watcher)
	c.endpointSliceWatchOptions = append(c.endpointSliceWatchOptions, options)
	return watcher, nil
}

func (c *recordingVisibilityClient) currentPodWatch() *watch.FakeWatcher {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.podWatches[len(c.podWatches)-1]
}

func (c *recordingVisibilityClient) currentEndpointSliceWatch() *watch.FakeWatcher {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.endpointWatches[len(c.endpointWatches)-1]
}

func (c *recordingVisibilityClient) podWatchCount() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return len(c.podWatches)
}

func (c *recordingVisibilityClient) endpointSliceWatchCount() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return len(c.endpointWatches)
}

func (c *recordingVisibilityClient) podWatchOption(t *testing.T, index int) metav1.ListOptions {
	t.Helper()
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if index < 0 || index >= len(c.podWatchOptions) {
		t.Fatalf("pod watch option index %d out of range for %d options", index, len(c.podWatchOptions))
	}
	return c.podWatchOptions[index]
}

func (c *recordingVisibilityClient) endpointSliceWatchOption(t *testing.T, index int) metav1.ListOptions {
	t.Helper()
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if index < 0 || index >= len(c.endpointSliceWatchOptions) {
		t.Fatalf("endpointslice watch option index %d out of range for %d options", index, len(c.endpointSliceWatchOptions))
	}
	return c.endpointSliceWatchOptions[index]
}

func (c *recordingVisibilityClient) hasCall(want string) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for _, call := range c.calls {
		if call == want {
			return true
		}
	}
	return false
}

func (c *recordingVisibilityClient) assertCall(t *testing.T, want string) {
	t.Helper()
	if !c.hasCall(want) {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		t.Fatalf("calls = %v; missing %q", c.calls, want)
	}
}

func (c *recordingVisibilityClient) listPodCallCountLocked() int {
	count := 0
	for _, call := range c.calls {
		if len(call) >= len("list-pods:") && call[:len("list-pods:")] == "list-pods:" {
			count++
		}
	}
	return count
}

func (c *recordingVisibilityClient) listEndpointSliceCallCountLocked() int {
	count := 0
	for _, call := range c.calls {
		if len(call) >= len("list-endpointslices:") && call[:len("list-endpointslices:")] == "list-endpointslices:" {
			count++
		}
	}
	return count
}

func resourceVersionForCall(resourceVersions []string, callCount int) string {
	if callCount <= 0 || len(resourceVersions) == 0 {
		return ""
	}
	index := callCount - 1
	if index >= len(resourceVersions) {
		index = len(resourceVersions) - 1
	}
	return resourceVersions[index]
}

func assertWatchOptions(t *testing.T, options metav1.ListOptions, selector string, resourceVersion string) {
	t.Helper()
	if options.LabelSelector != selector || options.ResourceVersion != resourceVersion || !options.AllowWatchBookmarks {
		t.Fatalf("watch options = %+v; want selector=%q resourceVersion=%q bookmarks=true", options, selector, resourceVersion)
	}
}

type cacheState struct {
	ready                bool
	bindingReady         bool
	snapshotCandidates   int
	bindingCandidates    int
	podsStale            bool
	endpointSlicesStale  bool
	podsFailed           bool
	endpointSlicesFailed bool
}

func cacheStateForTest(cache *WatcherCache) cacheState {
	snapshot := cache.Snapshot()
	bindingSnapshot := cache.BindingVisibilitySnapshot()
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	return cacheState{
		ready:                snapshot.Ready,
		bindingReady:         bindingSnapshot.Ready,
		snapshotCandidates:   len(snapshot.Candidates),
		bindingCandidates:    len(bindingSnapshot.Candidates),
		podsStale:            cache.podsStale,
		endpointSlicesStale:  cache.endpointSlicesStale,
		podsFailed:           cache.podsFailed,
		endpointSlicesFailed: cache.endpointSlicesFailed,
	}
}

type lockedBuffer struct {
	mutex  sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.String()
}

func newVisibilityTestConfig(t *testing.T) Config {
	t.Helper()
	cfg, err := LoadConfig(envMap{
		"TETRAL_KUBERNETES_NAMESPACE": "tetral-runtime",
	})
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	return cfg
}

func waitForCache(t *testing.T, cache *WatcherCache, condition func(Snapshot) bool) {
	t.Helper()
	waitForCondition(t, "cache condition", func() bool {
		return condition(cache.Snapshot())
	})
}

func waitForCondition(t *testing.T, name string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", name)
}
