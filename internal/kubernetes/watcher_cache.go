package kubernetes

import (
	"log/slog"
	"net/netip"
	"regexp"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type WatcherCache struct {
	mutex                sync.Mutex
	namespace            string
	logger               *slog.Logger
	pods                 map[string]corev1.Pod
	endpointSlices       map[string]discoveryv1.EndpointSlice
	podsSynced           bool
	endpointsSynced      bool
	podsStale            bool
	endpointSlicesStale  bool
	podsFailed           bool
	endpointSlicesFailed bool
}

type CacheOption func(*WatcherCache)

func WithLogger(logger *slog.Logger) CacheOption {
	return func(cache *WatcherCache) {
		cache.logger = logger
	}
}

func NewWatcherCache(namespace string, options ...CacheOption) *WatcherCache {
	cache := &WatcherCache{
		namespace:      namespace,
		pods:           map[string]corev1.Pod{},
		endpointSlices: map[string]discoveryv1.EndpointSlice{},
	}
	for _, option := range options {
		option(cache)
	}
	return cache
}

func (c *WatcherCache) ReplacePods(pods []corev1.Pod) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.pods = map[string]corev1.Pod{}
	for _, pod := range pods {
		c.pods[pod.Name] = pod
	}
	c.podsSynced = true
	c.podsFailed = false
	c.podsStale = false
}

func (c *WatcherCache) ReplaceEndpointSlices(slices []discoveryv1.EndpointSlice) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.endpointSlices = map[string]discoveryv1.EndpointSlice{}
	for _, slice := range slices {
		c.endpointSlices[slice.Name] = slice
	}
	c.endpointsSynced = true
	c.endpointSlicesFailed = false
	c.endpointSlicesStale = false
}

func (c *WatcherCache) UpsertPod(pod corev1.Pod) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.pods[pod.Name] = pod
	c.podsSynced = true
	c.podsFailed = false
	c.podsStale = false
}

func (c *WatcherCache) DeletePod(name string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	delete(c.pods, name)
	c.podsSynced = true
	c.podsFailed = false
	c.podsStale = false
}

func (c *WatcherCache) UpsertEndpointSlice(slice discoveryv1.EndpointSlice) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.endpointSlices[slice.Name] = slice
	c.endpointsSynced = true
	c.endpointSlicesFailed = false
	c.endpointSlicesStale = false
}

func (c *WatcherCache) DeleteEndpointSlice(name string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	delete(c.endpointSlices, name)
	c.endpointsSynced = true
	c.endpointSlicesFailed = false
	c.endpointSlicesStale = false
}

func (c *WatcherCache) MarkStale() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.podsStale = true
	c.endpointSlicesStale = true
}

func (c *WatcherCache) MarkPodsStale() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.podsStale = true
}

func (c *WatcherCache) MarkEndpointSlicesStale() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.endpointSlicesStale = true
}

type WatchFailure struct {
	Resource  string
	Namespace string
	Name      string
	UID       string
	Status    string
	Err       error
}

func (c *WatcherCache) MarkFailure(failure WatchFailure) {
	c.mutex.Lock()
	switch failure.Resource {
	case "pods":
		c.podsFailed = true
	case "endpointslices":
		c.endpointSlicesFailed = true
	default:
		c.podsFailed = true
		c.endpointSlicesFailed = true
	}
	c.mutex.Unlock()
	if c.logger != nil {
		c.logger.Warn("kubernetes.watch.failed",
			slog.String("operation", "kubernetes_watch"),
			slog.String("event.kind", "kubernetes.watch.failed"),
			slog.String("component", "bridge"),
			slog.String("kubernetes.resource", failure.Resource),
			slog.String("kubernetes.namespace", failure.Namespace),
			slog.String("kubernetes.name", safeKubernetesIdentifier(failure.Name)),
			slog.String("kubernetes.uid", safeKubernetesIdentifier(failure.UID)),
			slog.String("kubernetes.status", failure.Status),
			slog.String("error.class", classifyKubernetesError(failure.Err)),
			slog.String("error.code", failure.Status),
			slog.String("error.message_safe", "kubernetes watch failed"),
		)
	}
}

// Ready answers from the synced/stale/failed flags alone, without building the candidate
// snapshot, so readiness probes stay cheap. The truth table is identical to Snapshot().Ready.
func (c *WatcherCache) Ready() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.readyLocked()
}

// readyLocked computes readiness from cache state flags. Callers must hold c.mutex.
func (c *WatcherCache) readyLocked() bool {
	return c.podsSynced &&
		c.endpointsSynced &&
		!c.podsStale &&
		!c.endpointSlicesStale &&
		!c.podsFailed &&
		!c.endpointSlicesFailed
}

type Snapshot struct {
	Ready      bool
	Candidates []Candidate
}

type Candidate struct {
	PodName   string
	PodUID    string
	Addresses []string
}

type BindingVisibilityState string

const (
	BindingVisibilityReusable         BindingVisibilityState = "reusable"
	BindingVisibilityAbsent           BindingVisibilityState = "absent"
	BindingVisibilityDeleted          BindingVisibilityState = "deleted"
	BindingVisibilityUIDChanged       BindingVisibilityState = "uid_changed"
	BindingVisibilityIPChanged        BindingVisibilityState = "ip_changed"
	BindingVisibilityNotReady         BindingVisibilityState = "not_ready"
	BindingVisibilityNotServing       BindingVisibilityState = "not_serving"
	BindingVisibilityTerminating      BindingVisibilityState = "terminating"
	BindingVisibilitySnapshotNotReady BindingVisibilityState = "snapshot_not_ready"
)

type BoundRuntimePod struct {
	Namespace string
	PodName   string
	PodUID    string
	PodIP     string
}

type BindingCandidate struct {
	Namespace string
	PodName   string
	PodUID    string
	PodIP     string
}

type BindingVisibilitySnapshot struct {
	Ready      bool
	Candidates []BindingCandidate
	targets    map[string]bindingTarget
	overrides  map[string]BindingVisibilityState
}

type bindingTarget struct {
	pod       corev1.Pod
	endpoints []bindingEndpoint
}

type bindingEndpoint struct {
	conditions discoveryv1.EndpointConditions
	targetUID  string
	addresses  []string
}

func (c *WatcherCache) Snapshot() Snapshot {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	ready := c.readyLocked()
	snapshot := Snapshot{Ready: ready, Candidates: []Candidate{}}
	if !ready {
		return snapshot
	}
	for _, slice := range c.endpointSlices {
		for _, endpoint := range slice.Endpoints {
			candidate, ok := c.candidateForEndpoint(slice.AddressType, endpoint)
			if ok {
				snapshot.Candidates = append(snapshot.Candidates, candidate)
			}
		}
	}
	return snapshot
}

func (c *WatcherCache) BindingVisibilitySnapshot() BindingVisibilitySnapshot {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	ready := c.readyLocked()
	snapshot := BindingVisibilitySnapshot{
		Ready:      ready,
		Candidates: []BindingCandidate{},
		targets:    map[string]bindingTarget{},
	}
	if !ready {
		return snapshot
	}
	for _, pod := range c.pods {
		snapshot.targets[pod.Name] = bindingTarget{pod: pod}
	}
	for _, slice := range c.endpointSlices {
		for _, endpoint := range slice.Endpoints {
			if endpoint.TargetRef == nil || endpoint.TargetRef.Kind != "Pod" {
				continue
			}
			target := snapshot.targets[endpoint.TargetRef.Name]
			target.endpoints = append(target.endpoints, bindingEndpoint{
				conditions: endpoint.Conditions,
				targetUID:  string(endpoint.TargetRef.UID),
				addresses:  validEndpointAddresses(slice.AddressType, endpoint.Addresses),
			})
			snapshot.targets[endpoint.TargetRef.Name] = target
			if candidate, ok := c.candidateForEndpoint(slice.AddressType, endpoint); ok {
				for _, address := range candidate.Addresses {
					snapshot.Candidates = append(snapshot.Candidates, BindingCandidate{
						Namespace: c.namespace,
						PodName:   candidate.PodName,
						PodUID:    candidate.PodUID,
						PodIP:     address,
					})
				}
			}
		}
	}
	return snapshot
}

func (s BindingVisibilitySnapshot) VisibilityFor(bound BoundRuntimePod) BindingVisibilityState {
	if !s.Ready {
		return BindingVisibilitySnapshotNotReady
	}
	if s.overrides != nil {
		if state, ok := s.overrides[bound.PodName]; ok {
			return state
		}
	}
	target, ok := s.targets[bound.PodName]
	if !ok || target.pod.Name == "" {
		return BindingVisibilityAbsent
	}
	if target.pod.DeletionTimestamp != nil {
		return BindingVisibilityDeleted
	}
	if string(target.pod.UID) != bound.PodUID {
		return BindingVisibilityUIDChanged
	}
	if !podReady(target.pod) {
		return BindingVisibilityNotReady
	}
	if len(target.endpoints) == 0 {
		return BindingVisibilityNotServing
	}
	matchedEndpoint := false
	sawValidAddress := false
	for _, endpoint := range target.endpoints {
		if endpoint.targetUID != bound.PodUID {
			continue
		}
		matchedEndpoint = true
		if endpoint.conditions.Ready != nil && !*endpoint.conditions.Ready {
			return BindingVisibilityNotReady
		}
		if endpoint.conditions.Serving != nil && !*endpoint.conditions.Serving {
			return BindingVisibilityNotServing
		}
		if endpoint.conditions.Terminating != nil && *endpoint.conditions.Terminating {
			return BindingVisibilityTerminating
		}
		if len(endpoint.addresses) == 0 {
			return BindingVisibilityNotServing
		}
		sawValidAddress = true
		for _, address := range endpoint.addresses {
			if address == bound.PodIP {
				return BindingVisibilityReusable
			}
		}
	}
	if matchedEndpoint && sawValidAddress {
		return BindingVisibilityIPChanged
	}
	return BindingVisibilityNotServing
}

func NewBindingVisibilitySnapshotForTest(ready bool, candidates []BindingCandidate) BindingVisibilitySnapshot {
	snapshot := BindingVisibilitySnapshot{
		Ready:      ready,
		Candidates: append([]BindingCandidate(nil), candidates...),
		targets:    map[string]bindingTarget{},
	}
	for _, candidate := range candidates {
		target := snapshot.targets[candidate.PodName]
		if target.pod.Name == "" {
			target.pod = corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      candidate.PodName,
					Namespace: candidate.Namespace,
					UID:       types.UID(candidate.PodUID),
				},
				Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				}}},
			}
		}
		target.endpoints = append(target.endpoints, bindingEndpoint{
			targetUID: candidate.PodUID,
			addresses: []string{candidate.PodIP},
		})
		snapshot.targets[candidate.PodName] = target
	}
	return snapshot
}

func NewBindingVisibilitySnapshotStateForTest(ready bool, bound BoundRuntimePod, state BindingVisibilityState) BindingVisibilitySnapshot {
	return NewBindingVisibilitySnapshotStateWithCandidatesForTest(ready, bound, state, nil)
}

func NewBindingVisibilitySnapshotStateWithCandidatesForTest(ready bool, bound BoundRuntimePod, state BindingVisibilityState, candidates []BindingCandidate) BindingVisibilitySnapshot {
	snapshot := NewBindingVisibilitySnapshotForTest(ready, candidates)
	if snapshot.overrides == nil {
		snapshot.overrides = map[string]BindingVisibilityState{}
	}
	snapshot.overrides[bound.PodName] = state
	return snapshot
}

func (c *WatcherCache) candidateForEndpoint(addressType discoveryv1.AddressType, endpoint discoveryv1.Endpoint) (Candidate, bool) {
	if endpoint.TargetRef == nil || endpoint.TargetRef.Kind != "Pod" || len(endpoint.Addresses) == 0 {
		return Candidate{}, false
	}
	addresses := validEndpointAddresses(addressType, endpoint.Addresses)
	if len(addresses) == 0 {
		return Candidate{}, false
	}
	// Ready is the primary "can receive new traffic" signal; an explicit false excludes the
	// endpoint. A nil Ready keeps Kubernetes' default-true semantics. The Serving/Terminating
	// checks below stay as defense-in-depth because each condition catches partial-writer
	// states the others can miss.
	if endpoint.Conditions.Ready != nil && !*endpoint.Conditions.Ready {
		return Candidate{}, false
	}
	if endpoint.Conditions.Serving != nil && !*endpoint.Conditions.Serving {
		return Candidate{}, false
	}
	if endpoint.Conditions.Terminating != nil && *endpoint.Conditions.Terminating {
		return Candidate{}, false
	}
	// UID is the identity authority for correlation. An empty UID on either side is never a
	// match: empty==empty must not pass, or a stale/malformed object could impersonate a Pod.
	endpointUID := string(endpoint.TargetRef.UID)
	if endpointUID == "" {
		return Candidate{}, false
	}
	pod, ok := c.pods[endpoint.TargetRef.Name]
	if !ok || string(pod.UID) == "" || string(pod.UID) != endpointUID || pod.DeletionTimestamp != nil || !podReady(pod) {
		return Candidate{}, false
	}
	return Candidate{PodName: pod.Name, PodUID: string(pod.UID), Addresses: addresses}, true
}

func validEndpointAddresses(addressType discoveryv1.AddressType, addresses []string) []string {
	if addressType != discoveryv1.AddressTypeIPv4 && addressType != discoveryv1.AddressTypeIPv6 {
		return nil
	}
	valid := make([]string, 0, len(addresses))
	for _, address := range addresses {
		parsed, err := netip.ParseAddr(address)
		if err != nil {
			continue
		}
		if addressType == discoveryv1.AddressTypeIPv4 && !parsed.Is4() {
			continue
		}
		if addressType == discoveryv1.AddressTypeIPv6 && !parsed.Is6() {
			continue
		}
		valid = append(valid, parsed.String())
	}
	return valid
}

func podReady(pod corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

type RestartBackoff struct {
	initial time.Duration
	maximum time.Duration
	next    time.Duration
}

func NewRestartBackoff(initial time.Duration, maximum time.Duration) *RestartBackoff {
	return &RestartBackoff{initial: initial, maximum: maximum, next: initial}
}

func (b *RestartBackoff) Next() time.Duration {
	current := b.next
	b.next *= 2
	if b.next > b.maximum {
		b.next = b.maximum
	}
	return current
}

func (b *RestartBackoff) Reset() {
	b.next = b.initial
}

func classifyKubernetesError(err error) string {
	if err == nil {
		return ""
	}
	return "kubernetes_error"
}

func safeKubernetesIdentifier(value string) string {
	if value == "" {
		return ""
	}
	// The DNS-1123-subdomain/UID charset is the allowlist itself: a value matching it cannot
	// carry a bearer token, DSN, or Secret payload (no uppercase, '+', '/', '=', ':', '@', or
	// whitespace), so redaction never depends on keyword matching. The pattern is compiled per
	// call because this package forbids package-level vars, and the only caller is the rare
	// watch-failure log path where the cost is negligible.
	if len(value) <= 253 && regexp.MustCompile(`^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$`).MatchString(value) {
		return value
	}
	return "[redacted]"
}
