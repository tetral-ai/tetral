// Package kubernetest provides Kubernetes wire adapters for composition tests.
// It does not expose Kubernetes SDK types or install production authentication hooks.
package kubernetest

import (
	"errors"

	authenticationv1 "k8s.io/api/authentication/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
)

// TokenReviewRequest is the neutral request material needed by a controlled
// TokenReview authority. Workload verification remains owned by the real receiver.
type TokenReviewRequest struct {
	Token     string
	Audiences []string
}

// NewTokenReviewDecoder creates an official Kubernetes universal decoder for
// both the Go client's protobuf requests and Bun's JSON requests. Its scheme and
// decoder are scoped to the calling test, with no mutable package state.
func NewTokenReviewDecoder() (func([]byte) (TokenReviewRequest, error), error) {
	scheme := runtime.NewScheme()
	if err := authenticationv1.AddToScheme(scheme); err != nil {
		return nil, err
	}
	decoder := serializer.NewCodecFactory(scheme).UniversalDeserializer()
	return func(raw []byte) (TokenReviewRequest, error) {
		object, _, err := decoder.Decode(raw, nil, nil)
		if err != nil {
			return TokenReviewRequest{}, err
		}
		review, ok := object.(*authenticationv1.TokenReview)
		if !ok {
			return TokenReviewRequest{}, errors.New("invalid TokenReview kind")
		}
		return TokenReviewRequest{Token: review.Spec.Token, Audiences: append([]string(nil), review.Spec.Audiences...)}, nil
	}, nil
}
