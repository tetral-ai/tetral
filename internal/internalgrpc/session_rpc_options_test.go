package internalgrpc

import (
	"reflect"
	"testing"
	"time"

	"google.golang.org/grpc/keepalive"
)

func TestInternalGRPCKeepaliveParametersArePinnedOnEveryOptionFamily(t *testing.T) {
	wantServer := keepalive.ServerParameters{
		MaxConnectionAge:      5 * time.Minute,
		MaxConnectionAgeGrace: 30 * time.Minute,
	}
	wantClient := keepalive.ClientParameters{
		Time:                30 * time.Second,
		Timeout:             10 * time.Second,
		PermitWithoutStream: false,
	}
	wantEnforcement := keepalive.EnforcementPolicy{
		MinTime:             30 * time.Second,
		PermitWithoutStream: false,
	}
	if !reflect.DeepEqual(internalServerKeepaliveParameters(), wantServer) {
		t.Fatalf("server keepalive = %#v; want %#v", internalServerKeepaliveParameters(), wantServer)
	}
	if !reflect.DeepEqual(internalClientKeepaliveParameters(), wantClient) {
		t.Fatalf("client keepalive = %#v; want %#v", internalClientKeepaliveParameters(), wantClient)
	}
	if !reflect.DeepEqual(internalServerKeepaliveEnforcementPolicy(), wantEnforcement) {
		t.Fatalf("server keepalive enforcement = %#v; want %#v", internalServerKeepaliveEnforcementPolicy(), wantEnforcement)
	}
	if internalServerKeepaliveEnforcementPolicy().MinTime > internalClientKeepaliveParameters().Time {
		t.Fatalf("server keepalive minimum = %s; client interval = %s", internalServerKeepaliveEnforcementPolicy().MinTime, internalClientKeepaliveParameters().Time)
	}
	if len(SessionRPCServerOptions()) < 4 {
		t.Fatalf("session server options = %d; want bounds plus compatible keepalive", len(SessionRPCServerOptions()))
	}
	for name, options := range map[string][]any{
		"session": optionSlice(SessionRPCDialOptions()),
		"command": optionSlice(RuntimeCommandRPCDialOptions()),
		"queue":   optionSlice(QueueRPCDialOptions()),
	} {
		if len(options) < 2 {
			t.Fatalf("%s dial options = %d; want bounds plus keepalive", name, len(options))
		}
	}
	if len(QueueRPCServerOptions()) < 4 {
		t.Fatalf("queue server options = %d; want scoped bounds plus compatible keepalive", len(QueueRPCServerOptions()))
	}
}

func optionSlice[T any](values []T) []any {
	result := make([]any, len(values))
	for index := range values {
		result[index] = values[index]
	}
	return result
}
