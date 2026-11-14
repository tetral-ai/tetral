package kubernetes

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func TestClientsetFreshPodObservation(t *testing.T) {
	for _, scenario := range []string{"ready", "not ready", "deleting", "not running", "absent", "forbidden", "unavailable", "caller timeout"} {
		t.Run(scenario, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/api/v1/namespaces/runtime/pods/pod-a" {
					t.Errorf("fresh Pod GET=%s %s", r.Method, r.URL.Path)
				}
				w.Header().Set("Content-Type", "application/json")
				switch scenario {
				case "absent":
					w.WriteHeader(http.StatusNotFound)
					_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`))
					return
				case "forbidden":
					w.WriteHeader(http.StatusForbidden)
					_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403}`))
					return
				case "unavailable":
					w.WriteHeader(http.StatusServiceUnavailable)
					_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"ServiceUnavailable","code":503}`))
					return
				case "caller timeout":
					<-r.Context().Done()
					return
				}
				pod := corev1.Pod{TypeMeta: metav1.TypeMeta{Kind: "Pod", APIVersion: "v1"}, ObjectMeta: metav1.ObjectMeta{Namespace: "runtime", Name: "pod-a", UID: types.UID("uid-a")}, Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.1", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
				if scenario == "not ready" {
					pod.Status.Conditions[0].Status = corev1.ConditionFalse
				}
				if scenario == "not running" {
					pod.Status.Phase = corev1.PodPending
				}
				if scenario == "deleting" {
					timestamp := metav1.Now()
					pod.DeletionTimestamp = &timestamp
				}
				if err := json.NewEncoder(w).Encode(pod); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()
			sdk, err := clientset.NewForConfig(&rest.Config{Host: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			c := &ClientsetVisibilityClient{client: sdk}
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			observation, err := c.GetPod(ctx, "runtime", "pod-a")
			if scenario == "forbidden" || scenario == "unavailable" || scenario == "caller timeout" {
				if err == nil || observation != nil {
					t.Fatalf("uncertain GET became proof=%v/%v", observation, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "absent" {
				if !observation.Absent || observation.UID != "" {
					t.Fatalf("NotFound proof=%v", observation)
				}
				return
			}
			if observation.Absent || observation.Namespace != "runtime" || observation.Name != "pod-a" || observation.UID != "uid-a" || observation.IP != "10.0.0.1" || observation.Ready != (scenario != "not ready") || observation.Running != (scenario != "not running") || observation.Deleting != (scenario == "deleting") {
				t.Fatalf("fresh Pod facts=%v", observation)
			}
		})
	}
}

// Every request the visibility client sends is a read of Pods or of
// EndpointSlices in the namespace it was asked for, the reads its Role grants.
// A method added to the client is driven here before it can pass.
func TestVisibilityClientSendsOnlyReadsOfPodsAndEndpointSlices(t *testing.T) {
	var mu sync.Mutex
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := r.Method + " " + r.URL.Path
		if r.URL.Query().Get("watch") == "true" {
			request += " watch"
		}
		mu.Lock()
		requests = append(requests, request)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	sdk, err := clientset.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	client := &ClientsetVisibilityClient{client: sdk}
	ctx := t.Context()
	watching := func(watcher watch.Interface, err error) error {
		if err == nil {
			watcher.Stop()
		}
		return err
	}
	calls := []struct {
		method string
		call   func() error
		sends  string
	}{
		{"ListPods", func() error { _, err := client.ListPods(ctx, "runtime", metav1.ListOptions{}); return err }, "GET /api/v1/namespaces/runtime/pods"},
		{"GetPod", func() error { _, err := client.GetPod(ctx, "runtime", "pod-a"); return err }, "GET /api/v1/namespaces/runtime/pods/pod-a"},
		{"WatchPods", func() error { return watching(client.WatchPods(ctx, "runtime", metav1.ListOptions{})) }, "GET /api/v1/namespaces/runtime/pods watch"},
		{"ListEndpointSlices", func() error { _, err := client.ListEndpointSlices(ctx, "runtime", metav1.ListOptions{}); return err },
			"GET /apis/discovery.k8s.io/v1/namespaces/runtime/endpointslices"},
		{"WatchEndpointSlices", func() error { return watching(client.WatchEndpointSlices(ctx, "runtime", metav1.ListOptions{})) },
			"GET /apis/discovery.k8s.io/v1/namespaces/runtime/endpointslices watch"},
	}
	var driven, want []string
	for _, one := range calls {
		if err := one.call(); err != nil {
			t.Fatalf("%s: %v", one.method, err)
		}
		driven, want = append(driven, one.method), append(want, one.sends)
	}
	var methods []string
	for index := range reflect.TypeOf(client).NumMethod() {
		methods = append(methods, reflect.TypeOf(client).Method(index).Name)
	}
	slices.Sort(driven)
	if !slices.Equal(methods, driven) {
		t.Fatalf("the client has the methods %v; this test drives %v", methods, driven)
	}
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(requests, want) {
		t.Fatalf("requests:\n%s\nwant:\n%s", strings.Join(requests, "\n"), strings.Join(want, "\n"))
	}
}
