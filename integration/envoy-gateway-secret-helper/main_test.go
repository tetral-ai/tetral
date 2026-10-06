package main

import (
	"errors"
	eg "github.com/envoyproxy/gateway/api/v1alpha1"
	"reflect"
	gw "sigs.k8s.io/gateway-api/apis/v1"
	"testing"
	"time"

	"github.com/envoyproxy/gateway/internal/gatewayapi/resource"
	"github.com/envoyproxy/gateway/internal/ir"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestUnusedControlPlaneTLSFailure(t *testing.T) {
	missing := errors.New("envoy TLS secret /envoy not found")
	makeIR := func(route *ir.HTTPRoute) resource.XdsIRMap {
		return resource.XdsIRMap{"fixture": {HTTP: []*ir.HTTPListener{{Routes: []*ir.HTTPRoute{route}}}}}
	}
	emptyRoute := &ir.HTTPRoute{}
	if !unusedControlPlaneTLSFailure(missing, &resource.Resources{}, makeIR(emptyRoute)) {
		t.Fatal("exact unused controller lookup was rejected")
	}
	cases := []struct {
		name      string
		err       error
		resources *resource.Resources
		xds       resource.XdsIRMap
	}{
		{"no_error", nil, &resource.Resources{}, makeIR(emptyRoute)},
		{"unexpected", errors.New("other translator error"), &resource.Resources{}, makeIR(emptyRoute)},
		{"joined", errors.Join(missing, errors.New("another translator error")), &resource.Resources{}, makeIR(emptyRoute)},
		{"different_namespace", errors.New("envoy TLS secret controller/envoy not found"), &resource.Resources{}, makeIR(emptyRoute)},
		{"extension_server_input", missing, &resource.Resources{ExtensionServerPolicies: []unstructured.Unstructured{{}}}, makeIR(emptyRoute)},
		{"global_rate_limit", missing, &resource.Resources{}, makeIR(&ir.HTTPRoute{Traffic: &ir.TrafficFeatures{RateLimit: &ir.RateLimit{Global: &ir.GlobalRateLimit{}}}})},
		{"wasm", missing, &resource.Resources{}, makeIR(&ir.HTTPRoute{EnvoyExtensions: &ir.EnvoyExtensionFeatures{Wasms: []ir.Wasm{{}}}})},
		{"missing_gateway", missing, &resource.Resources{}, resource.XdsIRMap{}},
		{"nil_gateway", missing, &resource.Resources{}, resource.XdsIRMap{"fixture": nil}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if unusedControlPlaneTLSFailure(tc.err, tc.resources, tc.xds) {
				t.Fatal("material or unexpected failure was ignored")
			}
		})
	}
}

func TestUpstreamProxyDrainArguments(t *testing.T) {
	for _, tc := range []struct {
		name     string
		shutdown *eg.ShutdownConfig
		seconds  string
	}{
		{"provider_default", nil, "60"},
		{"declared_override", &eg.ShutdownConfig{DrainTimeout: new(gw.Duration((5 * time.Second).String()))}, "5"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			infra := &ir.ProxyInfra{Name: "fixture", Config: &eg.EnvoyProxy{Spec: eg.EnvoyProxySpec{Shutdown: tc.shutdown}}}
			actual, err := upstreamProxyDrainArgs(infra)
			if err != nil || !reflect.DeepEqual(actual, []string{"--drain-strategy", "immediate", "--drain-time-s", tc.seconds}) {
				t.Fatal("pinned provider drain arguments differ")
			}
		})
	}
	if _, err := upstreamProxyDrainArgs(nil); err == nil {
		t.Fatal("missing proxy accepted")
	}
}
