package static

import (
	"encoding/json"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/tetral-ai/tetral/internal/sessionrpc"
	agentruntimebridge "github.com/tetral-ai/tetral/services/bridge"
)

// Each side of a channel bounds what it sends and what it accepts. The Go
// services, the Runtime Pod and the Gateway declare those bounds apart, and a
// message one side sends must fit what the other accepts. All are compared as
// their code evaluates them.
func TestTransportBoundsMatchAcrossServices(t *testing.T) {
	pod := evaluatedBounds(t, "agent-runtime", `
import * as pod from "./packages/runtime-pod/src/bounds.ts";
import { MailFetchMaxEnvelopes } from "./packages/protocol/src/bounds.ts";
console.log(JSON.stringify({
  commandInbound: pod.MaxGrpcInboundMessageBytes, commandOutbound: pod.MaxGrpcOutboundMessageBytes,
  attachment: pod.MaxAttachmentGrpcMessageBytes, bridge: pod.MaxBridgeDurableContextGrpcMessageBytes,
  mailEnvelopes: MailFetchMaxEnvelopes,
  gatewayRequest: pod.gatewayGrpcChannelOptions()["grpc.max_send_message_length"],
  gatewayEvent: pod.gatewayGrpcChannelOptions()["grpc.max_receive_message_length"],
}));`)
	gateway := evaluatedBounds(t, "gateway", `
import { grpcServerOptions } from "./packages/provider-gateway/src/bounds.ts";
console.log(JSON.stringify({
  gatewayRequest: grpcServerOptions()["grpc.max_receive_message_length"],
  gatewayEvent: grpcServerOptions()["grpc.max_send_message_length"],
}));`)
	for name, bound := range map[string]int{
		"commandInbound":  sessionrpc.MaxRuntimeCommandGRPCMessageBytes,
		"commandOutbound": sessionrpc.MaxRuntimeCommandGRPCMessageBytes,
		"attachment":      sessionrpc.MaxAttachmentGRPCMessageBytes,
		"bridge":          sessionrpc.MaxBridgeAPIGRPCMessageBytes,
		"mailEnvelopes":   agentruntimebridge.MailFetchMaxEnvelopes,
		"gatewayRequest":  gateway["gatewayRequest"],
		"gatewayEvent":    gateway["gatewayEvent"],
	} {
		if bound == 0 || pod[name] != bound {
			t.Errorf("%s: the Runtime Pod uses %d and its peer %d", name, pod[name], bound)
		}
	}
}

// evaluatedBounds runs a script in a TypeScript service and decodes the
// object of bounds it prints.
func evaluatedBounds(t *testing.T, service string, script string) map[string]int {
	t.Helper()
	command := exec.Command("bun", "-e", script) //nolint:gosec // A fixed script of this test.
	command.Dir = filepath.Join(finalArchitectureEngineRoot(t), "services", service)
	output, err := command.Output()
	if err != nil {
		t.Fatalf("evaluate the bounds of %s: %v", service, err)
	}
	var bounds map[string]int
	if err := json.Unmarshal(output, &bounds); err != nil {
		t.Fatalf("decode the bounds of %s %q: %v", service, output, err)
	}
	return bounds
}
