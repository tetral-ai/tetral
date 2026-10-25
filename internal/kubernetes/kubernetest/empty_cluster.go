package kubernetest

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/tetral-ai/tetral/internal/kubernetes"
)

// EmptyCluster is a cluster with no Runtime pods or endpoint slices whose
// watches report nothing, for a test that starts a process against a cluster
// and needs its initial sync and watch to succeed. Every pod it is asked for
// is absent.
type EmptyCluster struct{}

func (EmptyCluster) ListPods(context.Context, string, metav1.ListOptions) (*corev1.PodList, error) {
	return &corev1.PodList{}, nil
}

func (EmptyCluster) WatchPods(context.Context, string, metav1.ListOptions) (watch.Interface, error) {
	return watch.NewFake(), nil
}

func (EmptyCluster) ListEndpointSlices(context.Context, string, metav1.ListOptions) (*discoveryv1.EndpointSliceList, error) {
	return &discoveryv1.EndpointSliceList{}, nil
}

func (EmptyCluster) WatchEndpointSlices(context.Context, string, metav1.ListOptions) (watch.Interface, error) {
	return watch.NewFake(), nil
}

func (EmptyCluster) GetPod(context.Context, string, string) (*kubernetes.PodObservation, error) {
	return &kubernetes.PodObservation{Absent: true}, nil
}
