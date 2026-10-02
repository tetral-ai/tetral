package kubernetest_test

import (
	"reflect"
	"strings"
	"testing"

	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer/protobuf"

	"github.com/tetral-ai/tetral/internal/kubernetes/kubernetest"
)

func TestTokenReviewDecoderAcceptsJSONAndOfficialProtobuf(t *testing.T) {
	decode, err := kubernetest.NewTokenReviewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	scheme := runtime.NewScheme()
	if err := authenticationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	wire, err := runtime.Encode(protobuf.NewSerializer(scheme, scheme), &authenticationv1.TokenReview{
		TypeMeta: metav1.TypeMeta{APIVersion: "authentication.k8s.io/v1", Kind: "TokenReview"},
		Spec:     authenticationv1.TokenReviewSpec{Token: "runtime", Audiences: []string{"tetral-internal-grpc"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for name, raw := range map[string][]byte{
		"json":     []byte(`{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","spec":{"token":"runtime","audiences":["tetral-internal-grpc"]}}`),
		"protobuf": wire,
	} {
		t.Run(name, func(t *testing.T) {
			got, err := decode(raw)
			want := kubernetest.TokenReviewRequest{Token: "runtime", Audiences: []string{"tetral-internal-grpc"}}
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("decoded request=%+v error=%v; want %+v", got, err, want)
			}
		})
	}
}

func TestTokenReviewDecoderRejectsMalformedAndWrongKind(t *testing.T) {
	decode, err := kubernetest.NewTokenReviewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range [][]byte{nil, []byte(`{"kind":`), []byte("invalid protobuf")} {
		if _, err := decode(raw); err == nil {
			t.Fatalf("malformed request accepted: %q", raw)
		}
	}
	_, err = decode([]byte(`{"apiVersion":"authentication.k8s.io/v1","kind":"SelfSubjectReview"}`))
	if err == nil || !strings.Contains(err.Error(), "invalid TokenReview kind") {
		t.Fatalf("wrong registered kind error=%v; want TokenReview kind rejection", err)
	}
}
