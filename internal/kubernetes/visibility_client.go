package kubernetes

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type VisibilityClient interface {
	ListPods(context.Context, string, metav1.ListOptions) (*corev1.PodList, error)
	WatchPods(context.Context, string, metav1.ListOptions) (watch.Interface, error)
	ListEndpointSlices(context.Context, string, metav1.ListOptions) (*discoveryv1.EndpointSliceList, error)
	WatchEndpointSlices(context.Context, string, metav1.ListOptions) (watch.Interface, error)
}

type ClientsetVisibilityClient struct {
	client clientset.Interface
}

func NewInClusterVisibilityClient() (*ClientsetVisibilityClient, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	client, err := clientset.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return &ClientsetVisibilityClient{client: client}, nil
}

func (c *ClientsetVisibilityClient) ListPods(ctx context.Context, namespace string, options metav1.ListOptions) (*corev1.PodList, error) {
	return c.client.CoreV1().Pods(namespace).List(ctx, options)
}

func (c *ClientsetVisibilityClient) WatchPods(ctx context.Context, namespace string, options metav1.ListOptions) (watch.Interface, error) {
	return c.client.CoreV1().Pods(namespace).Watch(ctx, options)
}

func (c *ClientsetVisibilityClient) ListEndpointSlices(ctx context.Context, namespace string, options metav1.ListOptions) (*discoveryv1.EndpointSliceList, error) {
	return c.client.DiscoveryV1().EndpointSlices(namespace).List(ctx, options)
}

func (c *ClientsetVisibilityClient) WatchEndpointSlices(ctx context.Context, namespace string, options metav1.ListOptions) (watch.Interface, error) {
	return c.client.DiscoveryV1().EndpointSlices(namespace).Watch(ctx, options)
}

type WatchHandles struct {
	Pods           watch.Interface
	EndpointSlices watch.Interface
	cancel         context.CancelFunc
}

func (h WatchHandles) Stop() {
	if h.cancel != nil {
		h.cancel()
	}
	if h.Pods != nil {
		h.Pods.Stop()
	}
	if h.EndpointSlices != nil {
		h.EndpointSlices.Stop()
	}
}

type SyncOptions struct {
	RestartInitialBackoff time.Duration
	RestartMaximumBackoff time.Duration
	FreshnessTimeout      time.Duration
}

const defaultFreshnessTimeout = time.Minute

func SyncAndWatch(ctx context.Context, client VisibilityClient, cfg Config, cache *WatcherCache) (WatchHandles, error) {
	return SyncAndWatchWithOptions(ctx, client, cfg, cache, SyncOptions{})
}

func SyncAndWatchWithOptions(ctx context.Context, client VisibilityClient, cfg Config, cache *WatcherCache, syncOptions SyncOptions) (WatchHandles, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if syncOptions.RestartInitialBackoff <= 0 {
		syncOptions.RestartInitialBackoff = time.Second
	}
	if syncOptions.RestartMaximumBackoff <= 0 {
		syncOptions.RestartMaximumBackoff = 30 * time.Second
	}
	if syncOptions.FreshnessTimeout <= 0 {
		syncOptions.FreshnessTimeout = defaultFreshnessTimeout
	}
	podOptions := metav1.ListOptions{LabelSelector: cfg.AgentRuntimeLabelSelector.String()}
	pods, err := client.ListPods(ctx, cfg.Namespace, podOptions)
	if err != nil {
		cache.MarkFailure(WatchFailure{Resource: "pods", Namespace: cfg.Namespace, Status: "list_failed", Err: err})
		return WatchHandles{}, err
	}
	endpointOptions := metav1.ListOptions{LabelSelector: discoveryv1.LabelServiceName + "=" + cfg.AgentRuntimeServiceName}
	endpointSlices, err := client.ListEndpointSlices(ctx, cfg.Namespace, endpointOptions)
	if err != nil {
		cache.MarkFailure(WatchFailure{Resource: "endpointslices", Namespace: cfg.Namespace, Status: "list_failed", Err: err})
		return WatchHandles{}, err
	}
	watchCtx, cancel := context.WithCancel(ctx)
	podWatch, err := client.WatchPods(watchCtx, cfg.Namespace, withWatchResourceVersion(podOptions, pods.ResourceVersion))
	if err != nil {
		cancel()
		cache.MarkFailure(WatchFailure{Resource: "pods", Namespace: cfg.Namespace, Status: "watch_failed", Err: err})
		return WatchHandles{}, err
	}
	endpointWatch, err := client.WatchEndpointSlices(watchCtx, cfg.Namespace, withWatchResourceVersion(endpointOptions, endpointSlices.ResourceVersion))
	if err != nil {
		cancel()
		podWatch.Stop()
		cache.MarkFailure(WatchFailure{Resource: "endpointslices", Namespace: cfg.Namespace, Status: "watch_failed", Err: err})
		return WatchHandles{}, err
	}
	cache.ReplacePods(pods.Items)
	cache.ReplaceEndpointSlices(endpointSlices.Items)
	go runPodWatch(watchCtx, client, cfg, podOptions, cache, podWatch, syncOptions)
	go runEndpointSliceWatch(watchCtx, client, cfg, endpointOptions, cache, endpointWatch, syncOptions)
	return WatchHandles{Pods: podWatch, EndpointSlices: endpointWatch, cancel: cancel}, nil
}

func runPodWatch(ctx context.Context, client VisibilityClient, cfg Config, options metav1.ListOptions, cache *WatcherCache, current watch.Interface, syncOptions SyncOptions) {
	backoff := NewRestartBackoff(syncOptions.RestartInitialBackoff, syncOptions.RestartMaximumBackoff)
	for {
		if !consumePodWatch(ctx, cache, cfg.Namespace, current, syncOptions.FreshnessTimeout) {
			current.Stop()
			return
		}
		current.Stop()
		if !sleepBeforeRestart(ctx, backoff.Next()) {
			return
		}
		for {
			pods, err := client.ListPods(ctx, cfg.Namespace, options)
			if err != nil {
				cache.MarkFailure(WatchFailure{Resource: "pods", Namespace: cfg.Namespace, Status: "list_failed", Err: err})
				if !sleepBeforeRestart(ctx, backoff.Next()) {
					return
				}
				continue
			}
			next, err := client.WatchPods(ctx, cfg.Namespace, withWatchResourceVersion(options, pods.ResourceVersion))
			if err != nil {
				cache.MarkFailure(WatchFailure{Resource: "pods", Namespace: cfg.Namespace, Status: "watch_failed", Err: err})
				if !sleepBeforeRestart(ctx, backoff.Next()) {
					return
				}
				continue
			}
			cache.ReplacePods(pods.Items)
			current = next
			backoff.Reset()
			break
		}
	}
}

func consumePodWatch(ctx context.Context, cache *WatcherCache, namespace string, watcher watch.Interface, freshnessTimeout time.Duration) bool {
	freshnessTimer := time.NewTimer(freshnessTimeout)
	defer freshnessTimer.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-freshnessTimer.C:
			cache.MarkPodsStale()
			return true
		case event, ok := <-watcher.ResultChan():
			if !ok {
				cache.MarkFailure(WatchFailure{Resource: "pods", Namespace: namespace, Status: "watch_closed"})
				return true
			}
			switch event.Type {
			case watch.Added, watch.Modified:
				pod, ok := event.Object.(*corev1.Pod)
				if !ok {
					cache.MarkFailure(WatchFailure{Resource: "pods", Namespace: namespace, Status: "malformed_event", Err: malformedWatchObjectError(event.Object)})
					return true
				}
				cache.UpsertPod(*pod)
				resetFreshnessTimer(freshnessTimer, freshnessTimeout)
			case watch.Deleted:
				pod, ok := event.Object.(*corev1.Pod)
				if !ok {
					cache.MarkFailure(WatchFailure{Resource: "pods", Namespace: namespace, Status: "malformed_event", Err: malformedWatchObjectError(event.Object)})
					return true
				}
				cache.DeletePod(pod.Name)
				resetFreshnessTimer(freshnessTimer, freshnessTimeout)
			case watch.Error:
				cache.MarkFailure(WatchFailure{Resource: "pods", Namespace: namespace, Status: "watch_error", Err: watchObjectError(event.Object)})
				return true
			case watch.Bookmark:
				resetFreshnessTimer(freshnessTimer, freshnessTimeout)
			default:
				cache.MarkFailure(WatchFailure{Resource: "pods", Namespace: namespace, Status: "unknown_event"})
				return true
			}
		}
	}
}

func runEndpointSliceWatch(ctx context.Context, client VisibilityClient, cfg Config, options metav1.ListOptions, cache *WatcherCache, current watch.Interface, syncOptions SyncOptions) {
	backoff := NewRestartBackoff(syncOptions.RestartInitialBackoff, syncOptions.RestartMaximumBackoff)
	for {
		if !consumeEndpointSliceWatch(ctx, cache, cfg.Namespace, current, syncOptions.FreshnessTimeout) {
			current.Stop()
			return
		}
		current.Stop()
		if !sleepBeforeRestart(ctx, backoff.Next()) {
			return
		}
		for {
			slices, err := client.ListEndpointSlices(ctx, cfg.Namespace, options)
			if err != nil {
				cache.MarkFailure(WatchFailure{Resource: "endpointslices", Namespace: cfg.Namespace, Status: "list_failed", Err: err})
				if !sleepBeforeRestart(ctx, backoff.Next()) {
					return
				}
				continue
			}
			next, err := client.WatchEndpointSlices(ctx, cfg.Namespace, withWatchResourceVersion(options, slices.ResourceVersion))
			if err != nil {
				cache.MarkFailure(WatchFailure{Resource: "endpointslices", Namespace: cfg.Namespace, Status: "watch_failed", Err: err})
				if !sleepBeforeRestart(ctx, backoff.Next()) {
					return
				}
				continue
			}
			cache.ReplaceEndpointSlices(slices.Items)
			current = next
			backoff.Reset()
			break
		}
	}
}

func consumeEndpointSliceWatch(ctx context.Context, cache *WatcherCache, namespace string, watcher watch.Interface, freshnessTimeout time.Duration) bool {
	freshnessTimer := time.NewTimer(freshnessTimeout)
	defer freshnessTimer.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-freshnessTimer.C:
			cache.MarkEndpointSlicesStale()
			return true
		case event, ok := <-watcher.ResultChan():
			if !ok {
				cache.MarkFailure(WatchFailure{Resource: "endpointslices", Namespace: namespace, Status: "watch_closed"})
				return true
			}
			switch event.Type {
			case watch.Added, watch.Modified:
				slice, ok := event.Object.(*discoveryv1.EndpointSlice)
				if !ok {
					cache.MarkFailure(WatchFailure{Resource: "endpointslices", Namespace: namespace, Status: "malformed_event", Err: malformedWatchObjectError(event.Object)})
					return true
				}
				cache.UpsertEndpointSlice(*slice)
				resetFreshnessTimer(freshnessTimer, freshnessTimeout)
			case watch.Deleted:
				slice, ok := event.Object.(*discoveryv1.EndpointSlice)
				if !ok {
					cache.MarkFailure(WatchFailure{Resource: "endpointslices", Namespace: namespace, Status: "malformed_event", Err: malformedWatchObjectError(event.Object)})
					return true
				}
				cache.DeleteEndpointSlice(slice.Name)
				resetFreshnessTimer(freshnessTimer, freshnessTimeout)
			case watch.Error:
				cache.MarkFailure(WatchFailure{Resource: "endpointslices", Namespace: namespace, Status: "watch_error", Err: watchObjectError(event.Object)})
				return true
			case watch.Bookmark:
				resetFreshnessTimer(freshnessTimer, freshnessTimeout)
			default:
				cache.MarkFailure(WatchFailure{Resource: "endpointslices", Namespace: namespace, Status: "unknown_event"})
				return true
			}
		}
	}
}

func resetFreshnessTimer(timer *time.Timer, freshnessTimeout time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(freshnessTimeout)
}

func sleepBeforeRestart(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func withWatchResourceVersion(options metav1.ListOptions, resourceVersion string) metav1.ListOptions {
	options.ResourceVersion = resourceVersion
	options.AllowWatchBookmarks = true
	return options
}

func watchObjectError(object runtime.Object) error {
	if object == nil {
		return nil
	}
	if status, ok := object.(*metav1.Status); ok {
		return fmt.Errorf("kubernetes watch status %s", status.Reason)
	}
	return malformedWatchObjectError(object)
}

func malformedWatchObjectError(object runtime.Object) error {
	if object == nil {
		return fmt.Errorf("malformed kubernetes watch object")
	}
	return fmt.Errorf("malformed kubernetes watch object %T", object)
}
