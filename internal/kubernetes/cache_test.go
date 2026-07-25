package kubernetes

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// newTestLogger captures JSON log lines into writer through the production handler,
// carrying the Bridge service identity tests assert against.
func newTestLogger(writer io.Writer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(writer, nil)).With(slog.String("service.name", "bridge"))
}

func TestWatcherCacheCandidateRules(t *testing.T) {
	trueValue := true
	falseValue := false
	pod := readyPod("runtime-a", "pod-uid-a")
	for _, test := range []struct {
		name      string
		pod       corev1.Pod
		endpoint  discoveryv1.Endpoint
		wantCount int
	}{
		{name: "ready", pod: pod, endpoint: endpointForPod(pod, []string{"10.0.0.1"}, nil, nil, nil), wantCount: 1},
		{name: "pod not ready", pod: notReadyPod("runtime-a", "pod-uid-a"), endpoint: endpointForPod(pod, []string{"10.0.0.1"}, nil, nil, nil)},
		{name: "pod deleting", pod: deletingPod("runtime-a", "pod-uid-a"), endpoint: endpointForPod(pod, []string{"10.0.0.1"}, nil, nil, nil)},
		{name: "target kind mismatch", pod: pod, endpoint: endpointWithTarget("Service", "runtime-a", "pod-uid-a", []string{"10.0.0.1"}, nil, nil, nil)},
		{name: "uid mismatch", pod: pod, endpoint: endpointWithTarget("Pod", "runtime-a", "other-uid", []string{"10.0.0.1"}, nil, nil, nil)},
		{name: "absent pod", pod: pod, endpoint: endpointWithTarget("Pod", "runtime-b", "pod-uid-b", []string{"10.0.0.1"}, nil, nil, nil)},
		{name: "missing address", pod: pod, endpoint: endpointForPod(pod, nil, nil, nil, nil)},
		{name: "serving false", pod: pod, endpoint: endpointForPod(pod, []string{"10.0.0.1"}, nil, &falseValue, nil)},
		{name: "terminating true", pod: pod, endpoint: endpointForPod(pod, []string{"10.0.0.1"}, nil, nil, &trueValue)},
		{name: "ready false alone", pod: pod, endpoint: endpointForPod(pod, []string{"10.0.0.1"}, &falseValue, nil, nil)},
		{name: "ready true explicit", pod: pod, endpoint: endpointForPod(pod, []string{"10.0.0.1"}, &trueValue, nil, nil), wantCount: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			cache := NewWatcherCache("tetral-runtime")
			cache.ReplacePods([]corev1.Pod{test.pod})
			cache.ReplaceEndpointSlices([]discoveryv1.EndpointSlice{endpointSlice(test.endpoint)})
			snapshot := cache.Snapshot()
			if !snapshot.Ready {
				t.Fatalf("snapshot.Ready = false; want synced empty/filtered cache ready")
			}
			if len(snapshot.Candidates) != test.wantCount {
				t.Fatalf("candidates = %+v; want %d", snapshot.Candidates, test.wantCount)
			}
		})
	}
}

func TestWatcherCacheBindingVisibilityDistinguishesBoundTargetStates(t *testing.T) {
	trueValue := true
	falseValue := false
	ready := readyPod("runtime-a", "pod-uid-a")
	for _, testCase := range []struct {
		name      string
		pods      []corev1.Pod
		endpoints []discoveryv1.Endpoint
		bound     BoundRuntimePod
		want      BindingVisibilityState
	}{
		{
			name:      "reusable",
			pods:      []corev1.Pod{ready},
			endpoints: []discoveryv1.Endpoint{endpointForPod(ready, []string{"10.0.0.1"}, nil, nil, nil)},
			bound:     BoundRuntimePod{Namespace: "tetral-runtime", PodName: "runtime-a", PodUID: "pod-uid-a", PodIP: "10.0.0.1"},
			want:      BindingVisibilityReusable,
		},
		{
			name:      "absent",
			pods:      []corev1.Pod{readyPod("runtime-b", "pod-uid-b")},
			endpoints: []discoveryv1.Endpoint{},
			bound:     BoundRuntimePod{Namespace: "tetral-runtime", PodName: "runtime-a", PodUID: "pod-uid-a", PodIP: "10.0.0.1"},
			want:      BindingVisibilityAbsent,
		},
		{
			name:      "deleted",
			pods:      []corev1.Pod{deletingPod("runtime-a", "pod-uid-a")},
			endpoints: []discoveryv1.Endpoint{endpointForPod(ready, []string{"10.0.0.1"}, nil, nil, nil)},
			bound:     BoundRuntimePod{Namespace: "tetral-runtime", PodName: "runtime-a", PodUID: "pod-uid-a", PodIP: "10.0.0.1"},
			want:      BindingVisibilityDeleted,
		},
		{
			name:      "uid changed",
			pods:      []corev1.Pod{readyPod("runtime-a", "pod-uid-b")},
			endpoints: []discoveryv1.Endpoint{endpointWithTarget("Pod", "runtime-a", "pod-uid-b", []string{"10.0.0.1"}, nil, nil, nil)},
			bound:     BoundRuntimePod{Namespace: "tetral-runtime", PodName: "runtime-a", PodUID: "pod-uid-a", PodIP: "10.0.0.1"},
			want:      BindingVisibilityUIDChanged,
		},
		{
			name:      "ip changed",
			pods:      []corev1.Pod{ready},
			endpoints: []discoveryv1.Endpoint{endpointForPod(ready, []string{"10.0.0.2"}, nil, nil, nil)},
			bound:     BoundRuntimePod{Namespace: "tetral-runtime", PodName: "runtime-a", PodUID: "pod-uid-a", PodIP: "10.0.0.1"},
			want:      BindingVisibilityIPChanged,
		},
		{
			name:      "pod not ready",
			pods:      []corev1.Pod{notReadyPod("runtime-a", "pod-uid-a")},
			endpoints: []discoveryv1.Endpoint{endpointForPod(ready, []string{"10.0.0.1"}, nil, nil, nil)},
			bound:     BoundRuntimePod{Namespace: "tetral-runtime", PodName: "runtime-a", PodUID: "pod-uid-a", PodIP: "10.0.0.1"},
			want:      BindingVisibilityNotReady,
		},
		{
			name:      "endpoint not ready",
			pods:      []corev1.Pod{ready},
			endpoints: []discoveryv1.Endpoint{endpointForPod(ready, []string{"10.0.0.1"}, &falseValue, nil, nil)},
			bound:     BoundRuntimePod{Namespace: "tetral-runtime", PodName: "runtime-a", PodUID: "pod-uid-a", PodIP: "10.0.0.1"},
			want:      BindingVisibilityNotReady,
		},
		{
			name:      "not serving",
			pods:      []corev1.Pod{ready},
			endpoints: []discoveryv1.Endpoint{endpointForPod(ready, []string{"10.0.0.1"}, nil, &falseValue, nil)},
			bound:     BoundRuntimePod{Namespace: "tetral-runtime", PodName: "runtime-a", PodUID: "pod-uid-a", PodIP: "10.0.0.1"},
			want:      BindingVisibilityNotServing,
		},
		{
			name:      "terminating",
			pods:      []corev1.Pod{ready},
			endpoints: []discoveryv1.Endpoint{endpointForPod(ready, []string{"10.0.0.1"}, nil, nil, &trueValue)},
			bound:     BoundRuntimePod{Namespace: "tetral-runtime", PodName: "runtime-a", PodUID: "pod-uid-a", PodIP: "10.0.0.1"},
			want:      BindingVisibilityTerminating,
		},
		{
			name:      "live pod without endpoint evidence",
			pods:      []corev1.Pod{ready},
			endpoints: nil,
			bound:     BoundRuntimePod{Namespace: "tetral-runtime", PodName: "runtime-a", PodUID: "pod-uid-a", PodIP: "10.0.0.1"},
			want:      BindingVisibilityNotServing,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			cache := NewWatcherCache("tetral-runtime")
			cache.ReplacePods(testCase.pods)
			cache.ReplaceEndpointSlices([]discoveryv1.EndpointSlice{endpointSliceWithEndpoints(testCase.endpoints...)})
			snapshot := cache.BindingVisibilitySnapshot()
			if got := snapshot.VisibilityFor(testCase.bound); got != testCase.want {
				t.Fatalf("VisibilityFor(%+v) = %s; want %s", testCase.bound, got, testCase.want)
			}
		})
	}
}

func TestWatcherCacheBindingCandidatesRequireIPLiteralEndpointAddresses(t *testing.T) {
	ready := readyPod("runtime-a", "pod-uid-a")
	for _, test := range []struct {
		name        string
		addressType discoveryv1.AddressType
		addresses   []string
		wantCount   int
		wantVisible BindingVisibilityState
	}{
		{name: "ipv4 literal", addressType: discoveryv1.AddressTypeIPv4, addresses: []string{"10.0.0.1"}, wantCount: 1, wantVisible: BindingVisibilityReusable},
		{name: "ipv6 literal", addressType: discoveryv1.AddressTypeIPv6, addresses: []string{"2001:db8::1"}, wantCount: 1, wantVisible: BindingVisibilityReusable},
		{name: "fqdn address type", addressType: discoveryv1.AddressTypeFQDN, addresses: []string{"runtime-a.example.internal"}, wantVisible: BindingVisibilityNotServing},
		{name: "hostname string", addressType: discoveryv1.AddressTypeIPv4, addresses: []string{"runtime-a"}, wantVisible: BindingVisibilityNotServing},
		{name: "malformed string", addressType: discoveryv1.AddressTypeIPv4, addresses: []string{"10.0.0.1:9090"}, wantVisible: BindingVisibilityNotServing},
		{name: "empty string", addressType: discoveryv1.AddressTypeIPv4, addresses: []string{""}, wantVisible: BindingVisibilityNotServing},
		{name: "ipv6 under ipv4 type", addressType: discoveryv1.AddressTypeIPv4, addresses: []string{"2001:db8::1"}, wantVisible: BindingVisibilityNotServing},
		{name: "ipv4 under ipv6 type", addressType: discoveryv1.AddressTypeIPv6, addresses: []string{"10.0.0.1"}, wantVisible: BindingVisibilityNotServing},
	} {
		t.Run(test.name, func(t *testing.T) {
			cache := NewWatcherCache("tetral-runtime")
			cache.ReplacePods([]corev1.Pod{ready})
			cache.ReplaceEndpointSlices([]discoveryv1.EndpointSlice{
				endpointSliceWithAddressType(test.addressType, endpointForPod(ready, test.addresses, nil, nil, nil)),
			})
			snapshot := cache.BindingVisibilitySnapshot()
			if len(snapshot.Candidates) != test.wantCount {
				t.Fatalf("binding candidates = %+v; want %d", snapshot.Candidates, test.wantCount)
			}
			boundIP := "10.0.0.1"
			if test.addressType == discoveryv1.AddressTypeIPv6 {
				boundIP = "2001:db8::1"
			}
			got := snapshot.VisibilityFor(BoundRuntimePod{Namespace: "tetral-runtime", PodName: "runtime-a", PodUID: "pod-uid-a", PodIP: boundIP})
			if got != test.wantVisible {
				t.Fatalf("VisibilityFor = %s; want %s", got, test.wantVisible)
			}
		})
	}
}

func TestWatcherCacheBindingVisibilityReusesBoundIPAcrossSplitEndpointSlices(t *testing.T) {
	ready := readyPod("runtime-a", "pod-uid-a")
	ipv4Slice := endpointSliceWithAddressType(discoveryv1.AddressTypeIPv4, endpointForPod(ready, []string{"10.0.0.1"}, nil, nil, nil))
	ipv4Slice.Name = "agent-runtime-ipv4"
	ipv6Slice := endpointSliceWithAddressType(discoveryv1.AddressTypeIPv6, endpointForPod(ready, []string{"2001:db8::1"}, nil, nil, nil))
	ipv6Slice.Name = "agent-runtime-ipv6"
	for _, test := range []struct {
		name  string
		slice []discoveryv1.EndpointSlice
		bound BoundRuntimePod
	}{
		{
			name:  "ipv4 bound with ipv6 first",
			slice: []discoveryv1.EndpointSlice{ipv6Slice, ipv4Slice},
			bound: BoundRuntimePod{Namespace: "tetral-runtime", PodName: "runtime-a", PodUID: "pod-uid-a", PodIP: "10.0.0.1"},
		},
		{
			name:  "ipv6 bound with ipv4 first",
			slice: []discoveryv1.EndpointSlice{ipv4Slice, ipv6Slice},
			bound: BoundRuntimePod{Namespace: "tetral-runtime", PodName: "runtime-a", PodUID: "pod-uid-a", PodIP: "2001:db8::1"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			cache := NewWatcherCache("tetral-runtime")
			cache.ReplacePods([]corev1.Pod{ready})
			cache.ReplaceEndpointSlices(test.slice)
			snapshot := cache.BindingVisibilitySnapshot()
			if got := snapshot.VisibilityFor(test.bound); got != BindingVisibilityReusable {
				t.Fatalf("VisibilityFor(%+v) = %s; want %s", test.bound, got, BindingVisibilityReusable)
			}
		})
	}
}

func TestWatcherCacheBindingVisibilitySnapshotNotReady(t *testing.T) {
	cache := NewWatcherCache("tetral-runtime")
	snapshot := cache.BindingVisibilitySnapshot()
	if snapshot.Ready {
		t.Fatal("unsynced binding visibility snapshot is ready")
	}
	if got := snapshot.VisibilityFor(BoundRuntimePod{Namespace: "tetral-runtime", PodName: "runtime-a", PodUID: "pod-uid-a", PodIP: "10.0.0.1"}); got != BindingVisibilitySnapshotNotReady {
		t.Fatalf("unsynced VisibilityFor = %s; want %s", got, BindingVisibilitySnapshotNotReady)
	}
}

func TestWatcherCacheRejectsEmptyUIDCorrelation(t *testing.T) {
	for _, test := range []struct {
		name      string
		pod       corev1.Pod
		endpoint  discoveryv1.Endpoint
		wantCount int
	}{
		{
			name:     "empty targetRef UID and empty pod UID",
			pod:      readyPod("runtime-a", ""),
			endpoint: endpointWithTarget("Pod", "runtime-a", "", []string{"10.0.0.1"}, nil, nil, nil),
		},
		{
			name:     "empty targetRef UID against real pod UID",
			pod:      readyPod("runtime-a", "pod-uid-a"),
			endpoint: endpointWithTarget("Pod", "runtime-a", "", []string{"10.0.0.1"}, nil, nil, nil),
		},
		{
			name:     "real targetRef UID against empty pod UID",
			pod:      readyPod("runtime-a", ""),
			endpoint: endpointWithTarget("Pod", "runtime-a", "pod-uid-a", []string{"10.0.0.1"}, nil, nil, nil),
		},
		{
			name:      "real equal UIDs",
			pod:       readyPod("runtime-a", "pod-uid-a"),
			endpoint:  endpointWithTarget("Pod", "runtime-a", "pod-uid-a", []string{"10.0.0.1"}, nil, nil, nil),
			wantCount: 1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			cache := NewWatcherCache("tetral-runtime")
			cache.ReplacePods([]corev1.Pod{test.pod})
			cache.ReplaceEndpointSlices([]discoveryv1.EndpointSlice{endpointSlice(test.endpoint)})
			snapshot := cache.Snapshot()
			if !snapshot.Ready {
				t.Fatalf("snapshot.Ready = false; want synced cache ready")
			}
			if len(snapshot.Candidates) != test.wantCount {
				t.Fatalf("candidates = %+v; want %d", snapshot.Candidates, test.wantCount)
			}
		})
	}
}

func TestWatcherCacheReadyMatchesSnapshotReadyBitForBit(t *testing.T) {
	cache := NewWatcherCache("tetral-runtime")
	if cache.Ready() != cache.Snapshot().Ready {
		t.Fatal("unsynced: Ready() disagrees with Snapshot().Ready")
	}
	cache.ReplacePods(nil)
	cache.ReplaceEndpointSlices(nil)
	if !cache.Ready() || cache.Ready() != cache.Snapshot().Ready {
		t.Fatal("empty synced: Ready() must be true and agree with Snapshot().Ready")
	}
	cache.MarkFailure(WatchFailure{Resource: "pods"})
	if cache.Ready() || cache.Ready() != cache.Snapshot().Ready {
		t.Fatal("failed: Ready() must be false and agree with Snapshot().Ready")
	}
	cache.ReplacePods(nil)
	if !cache.Ready() || cache.Ready() != cache.Snapshot().Ready {
		t.Fatal("recovered: Ready() must be true and agree with Snapshot().Ready")
	}
	cache.MarkStale()
	if cache.Ready() || cache.Ready() != cache.Snapshot().Ready {
		t.Fatal("stale: Ready() must be false and agree with Snapshot().Ready")
	}
}

func TestWatcherCacheReadyDoesNotCallSnapshot(t *testing.T) {
	source, err := os.ReadFile("watcher_cache.go")
	if err != nil {
		t.Fatalf("read watcher_cache.go: %v", err)
	}
	readyBody := readyMethodBody(t, string(source))
	if strings.Contains(readyBody, "Snapshot()") {
		t.Fatalf("Ready() must not construct the candidate snapshot:\n%s", readyBody)
	}
}

func readyMethodBody(t *testing.T, source string) string {
	t.Helper()
	const marker = "func (c *WatcherCache) Ready() bool {"
	start := strings.Index(source, marker)
	if start < 0 {
		t.Fatal("Ready() method not found in watcher_cache.go")
	}
	rest := source[start+len(marker):]
	end := strings.Index(rest, "\n}")
	if end < 0 {
		t.Fatal("Ready() method body not terminated")
	}
	return rest[:end]
}

func TestWatcherCacheReadinessErrorStaleAndBackoff(t *testing.T) {
	cache := NewWatcherCache("tetral-runtime")
	if cache.Snapshot().Ready {
		t.Fatal("unsynced cache must not be ready")
	}
	cache.ReplacePods(nil)
	cache.ReplaceEndpointSlices(nil)
	if !cache.Snapshot().Ready {
		t.Fatal("empty synced cache must be ready with zero candidates")
	}
	cache.MarkStale()
	if cache.Snapshot().Ready {
		t.Fatal("stale cache must be unready")
	}

	backoff := NewRestartBackoff(100*time.Millisecond, time.Second)
	if first, second, third := backoff.Next(), backoff.Next(), backoff.Next(); first != 100*time.Millisecond || second != 200*time.Millisecond || third != 400*time.Millisecond {
		t.Fatalf("backoff sequence = %v,%v,%v", first, second, third)
	}
	backoff.Reset()
	if got := backoff.Next(); got != 100*time.Millisecond {
		t.Fatalf("backoff after reset = %v", got)
	}
}

func TestWatcherCacheLogsRedactedFailure(t *testing.T) {
	var buffer bytes.Buffer
	logger := newTestLogger(&buffer)
	cache := NewWatcherCache("tetral-runtime", WithLogger(logger))
	cache.MarkFailure(WatchFailure{
		Resource:  "pods",
		Namespace: "tetral-runtime",
		Name:      "runtime-a",
		UID:       "pod-uid-a",
		Status:    "forbidden",
		Err:       errors.New("Secret/k8s-secret-sentinel bearer-token raw object payload"),
	})

	logOutput := buffer.String()
	for _, forbidden := range []string{"k8s-secret-sentinel", "bearer-token", "raw object payload"} {
		if strings.Contains(logOutput, forbidden) {
			t.Fatalf("watch failure log leaked %q: %s", forbidden, logOutput)
		}
	}
	for _, want := range []string{`"msg":"kubernetes.watch.failed"`, `"operation":"kubernetes_watch"`, `"event.kind":"kubernetes.watch.failed"`, `"component":"bridge"`, `"kubernetes.resource":"pods"`, `"kubernetes.namespace":"tetral-runtime"`, `"kubernetes.name":"runtime-a"`, `"kubernetes.uid":"pod-uid-a"`, `"kubernetes.status":"forbidden"`, `"error.class":"kubernetes_error"`, `"error.code":"forbidden"`, `"error.message_safe":"kubernetes watch failed"`} {
		if !strings.Contains(logOutput, want) {
			t.Fatalf("watch failure log missing %s: %s", want, logOutput)
		}
	}
}

func TestSafeKubernetesIdentifierRedactsOnlyShapeViolations(t *testing.T) {
	for _, test := range []struct {
		name  string
		value string
		want  string
	}{
		// Empty stays empty.
		{name: "empty", value: "", want: ""},
		// Legitimate DNS-1123/UID-shaped names that happen to embed a former keyword
		// as a substring must survive verbatim — they are exactly the diagnostic
		// fields the old keyword filter destroyed.
		{name: "tokenizer name", value: "tokenizer-7d9", want: "tokenizer-7d9"},
		{name: "raw events name", value: "raw-events-abc", want: "raw-events-abc"},
		{name: "secret santa name", value: "secret-santa", want: "secret-santa"},
		{name: "uid shape", value: "a1b2c3d4-1234-5678-9abc-def012345678", want: "a1b2c3d4-1234-5678-9abc-def012345678"},
		// Keyword-bearing values that also violate the legal identifier shape are redacted.
		{name: "bearer token", value: "Bearer abc123", want: "[redacted]"},
		{name: "secret path", value: "Secret/k8s-secret-sentinel", want: "[redacted]"},
		// Keyword-free shape violations: these prove the predicate is shape-based,
		// not a pruned keyword list. A pruned-keyword implementation passes them all.
		{name: "equals no keyword", value: "alpha=beta", want: "[redacted]"},
		{name: "plus no keyword", value: "alpha+beta", want: "[redacted]"},
		{name: "whitespace no keyword", value: "alpha beta", want: "[redacted]"},
		{name: "too long no keyword", value: strings.Repeat("a", 300), want: "[redacted]"},
		{name: "uppercase only", value: "RuntimeA", want: "[redacted]"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := safeKubernetesIdentifier(test.value); got != test.want {
				t.Fatalf("safeKubernetesIdentifier(%q) = %q; want %q", test.value, got, test.want)
			}
		})
	}
}

func readyPod(name string, uid string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(uid)},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{
			Type:   corev1.PodReady,
			Status: corev1.ConditionTrue,
		}}},
	}
}

func notReadyPod(name string, uid string) corev1.Pod {
	pod := readyPod(name, uid)
	pod.Status.Conditions[0].Status = corev1.ConditionFalse
	return pod
}

func deletingPod(name string, uid string) corev1.Pod {
	pod := readyPod(name, uid)
	deletion := metav1.Now()
	pod.DeletionTimestamp = &deletion
	return pod
}

func endpointForPod(pod corev1.Pod, addresses []string, ready *bool, serving *bool, terminating *bool) discoveryv1.Endpoint {
	return endpointWithTarget("Pod", pod.Name, string(pod.UID), addresses, ready, serving, terminating)
}

func endpointWithTarget(kind string, name string, uid string, addresses []string, ready *bool, serving *bool, terminating *bool) discoveryv1.Endpoint {
	return discoveryv1.Endpoint{
		Addresses: addresses,
		Conditions: discoveryv1.EndpointConditions{
			Ready:       ready,
			Serving:     serving,
			Terminating: terminating,
		},
		TargetRef: &corev1.ObjectReference{
			Kind: kind,
			Name: name,
			UID:  types.UID(uid),
		},
	}
}

func endpointSlice(endpoint discoveryv1.Endpoint) discoveryv1.EndpointSlice {
	return endpointSliceWithEndpoints(endpoint)
}

func endpointSliceWithEndpoints(endpoints ...discoveryv1.Endpoint) discoveryv1.EndpointSlice {
	return endpointSliceWithAddressType(discoveryv1.AddressTypeIPv4, endpoints...)
}

func endpointSliceWithAddressType(addressType discoveryv1.AddressType, endpoints ...discoveryv1.Endpoint) discoveryv1.EndpointSlice {
	return discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "agent-runtime-abc",
			Namespace: "tetral-runtime",
			Labels:    map[string]string{discoveryv1.LabelServiceName: "agent-runtime"},
		},
		Endpoints:   endpoints,
		AddressType: addressType,
	}
}
