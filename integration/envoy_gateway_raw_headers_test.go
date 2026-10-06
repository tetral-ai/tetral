package integration

import (
	"bytes"
	"context"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	extauthzv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/ext_authz/v3"
	hcmv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	cachev3 "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	resourcev3 "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
	"gopkg.in/yaml.v3"

	"github.com/tetral-ai/tetral/integration/transporttest"
)

// The negative control is another complete official translation of the same
// fixture. Only the production patch enabling raw Check headers is omitted;
// neither the served xDS nor the Auth credential selector is edited by hand.
func translateEdgeWithoutRawHeaders(t *testing.T, fixture *envoyGatewayTranslation) *translatedEnvoySnapshot {
	t.Helper()
	decoder := yaml.NewDecoder(bytes.NewReader(fixture.Snapshot.FixtureYAML))
	var objects []map[string]any
	removed := 0
	for {
		var object map[string]any
		if err := decoder.Decode(&object); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal("decode bound raw-header control fixture")
		}
		if object == nil {
			continue
		}
		metadata := object["metadata"].(map[string]any)
		if object["kind"] == "EnvoyPatchPolicy" && metadata["name"] == "tetral-auth-raw-headers" {
			spec := object["spec"].(map[string]any)
			var kept []any
			for _, raw := range spec["jsonPatches"].([]any) {
				patch := raw.(map[string]any)
				operation := patch["operation"].(map[string]any)
				if operation["path"] == "/encode_raw_headers" {
					if operation["op"] != "add" || operation["value"] != true || patch["name"] != "tetral-system/tetral-public-edge/api-https" {
						t.Fatal("raw-header control target differs from production patch")
					}
					removed++
					continue
				}
				kept = append(kept, raw)
			}
			spec["jsonPatches"] = kept
		}
		objects = append(objects, object)
	}
	if removed != 1 {
		t.Fatal("raw-header control must omit exactly one production patch")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	negative := runOfficialEnvoyTranslation(ctx, t, fixture.Directory, "without-raw-headers", objects)
	completeEnvoyGatewaySecrets(ctx, t, negative)
	requireOnlyRawHeaderEncodingDelta(t, fixture.Snapshot, negative)
	return negative
}

func requireOnlyRawHeaderEncodingDelta(t *testing.T, original, negative *translatedEnvoySnapshot) {
	t.Helper()
	if !reflect.DeepEqual(original.Bootstrap, negative.Bootstrap) || len(original.Resources) != len(negative.Resources) {
		t.Fatal("negative raw-header translation changed Bootstrap or resource kinds")
	}
	changed := 0
	for kind, expected := range original.Resources {
		actual := negative.Resources[kind]
		if len(expected) != len(actual) {
			t.Fatal("negative raw-header translation changed resource count")
		}
		for index, resource := range expected {
			if cachev3.GetResourceName(resource) != cachev3.GetResourceName(actual[index]) {
				t.Fatal("negative raw-header translation changed resource identity/order")
			}
			wanted := proto.Clone(resource)
			if kind == resourcev3.ListenerType {
				listener := wanted.(*listenerv3.Listener)
				for _, chain := range listener.FilterChains {
					for _, filter := range chain.Filters {
						if filter.Name != "envoy.filters.network.http_connection_manager" {
							continue
						}
						hcm := &hcmv3.HttpConnectionManager{}
						if err := filter.GetTypedConfig().UnmarshalTo(hcm); err != nil {
							t.Fatal("decode official HCM for complete raw-header delta")
						}
						for _, httpFilter := range hcm.HttpFilters {
							if !strings.HasPrefix(httpFilter.Name, "envoy.filters.http.ext_authz/") {
								continue
							}
							check := &extauthzv3.ExtAuthz{}
							if err := httpFilter.GetTypedConfig().UnmarshalTo(check); err != nil || !check.EncodeRawHeaders || !check.ValidateMutations {
								t.Fatal("production Check raw-header/mutation guards absent")
							}
							check.EncodeRawHeaders = false
							httpFilter.ConfigType = &hcmv3.HttpFilter_TypedConfig{TypedConfig: transporttest.Must(anypb.New(check))}
							changed++
						}
						filter.ConfigType = &listenerv3.Filter_TypedConfig{TypedConfig: transporttest.Must(anypb.New(hcm))}
					}
				}
			}
			// Any stores protobuf wire bytes. Repacking both sides deterministically
			// permits map wire-order differences while retaining every known and
			// unknown field; no unmatched fields or unknown extensions are ignored.
			observed := proto.Clone(actual[index])
			canonicalizeEdgeAny(t, wanted.ProtoReflect())
			canonicalizeEdgeAny(t, observed.ProtoReflect())
			if !proto.Equal(wanted, observed) {
				t.Fatalf("negative raw-header translation changed another policy field kind=%s", kind)
			}
		}
	}
	if changed != 1 {
		t.Fatal("raw-header control must change exactly one generated Check field")
	}
	t.Log("envoy_gateway_assertion=negative_raw_header_complete_graph_delta changed_fields=1 validate_mutations=true")
}

func canonicalizeEdgeAny(t *testing.T, message protoreflect.Message) {
	t.Helper()
	if packed, ok := message.Interface().(*anypb.Any); ok {
		unpacked, err := packed.UnmarshalNew()
		if err != nil {
			t.Fatal("unknown translated Any type in complete policy comparison")
		}
		canonicalizeEdgeAny(t, unpacked.ProtoReflect())
		packed.Value = transporttest.Must((proto.MarshalOptions{Deterministic: true}).Marshal(unpacked))
		return
	}
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		switch {
		case field.IsMap() && field.MapValue().Message() != nil:
			value.Map().Range(func(_ protoreflect.MapKey, element protoreflect.Value) bool {
				canonicalizeEdgeAny(t, element.Message())
				return true
			})
		case field.IsList() && field.Message() != nil:
			for index := 0; index < value.List().Len(); index++ {
				canonicalizeEdgeAny(t, value.List().Get(index).Message())
			}
		case !field.IsMap() && !field.IsList() && field.Message() != nil:
			canonicalizeEdgeAny(t, value.Message())
		}
		return true
	})
}
