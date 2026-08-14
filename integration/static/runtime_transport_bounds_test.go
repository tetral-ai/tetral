package static

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

func TestRuntimeCommandAndBridgeFusesStayAlignedAcrossGoAndTypeScript(t *testing.T) {
	root := finalArchitectureEngineRoot(t)
	goBody, err := os.ReadFile(filepath.Join(root, "internal", "sessionrpc", "bounds.go")) //nolint:gosec // repository-local source path.
	if err != nil {
		t.Fatalf("read Go transport bounds: %v", err)
	}
	tsBody, err := os.ReadFile(filepath.Join(root, "services", "agent-runtime", "packages", "runtime-pod", "src", "bounds.ts")) //nolint:gosec // repository-local source path.
	if err != nil {
		t.Fatalf("read TypeScript transport bounds: %v", err)
	}
	protocolBody, err := os.ReadFile(filepath.Join(root, "services", "agent-runtime", "packages", "protocol", "src", "bounds.ts")) //nolint:gosec // repository-local source path.
	if err != nil {
		t.Fatalf("read TypeScript protocol bounds: %v", err)
	}
	completionMailBody, err := os.ReadFile(filepath.Join(root, "services", "bridge", "completion_mail.go")) //nolint:gosec // repository-local source path.
	if err != nil {
		t.Fatalf("read Go completion-mail bounds: %v", err)
	}
	bridgeMainBody, err := os.ReadFile(filepath.Join(root, "services", "bridge", "cmd", "bridge-api", "main.go")) //nolint:gosec // repository-local source path.
	if err != nil {
		t.Fatalf("read Bridge API composition: %v", err)
	}
	gatewayBody, err := os.ReadFile(filepath.Join(root, "services", "gateway", "packages", "provider-gateway", "src", "bounds.ts")) //nolint:gosec // repository-local source path.
	if err != nil {
		t.Fatalf("read Gateway transport bounds: %v", err)
	}
	gatewayProtocolBody, err := os.ReadFile(filepath.Join(root, "services", "gateway", "packages", "protocol", "src", "bounds.ts")) //nolint:gosec // repository-local source path.
	if err != nil {
		t.Fatalf("read Gateway protocol bounds: %v", err)
	}
	webConnectorBody, err := os.ReadFile(filepath.Join(root, "services", "web-connector", "types.go")) //nolint:gosec // repository-local source path.
	if err != nil {
		t.Fatalf("read Web connector bounds: %v", err)
	}
	toolRunnerBody, err := os.ReadFile(filepath.Join(root, "services", "agent-runtime", "packages", "runtime-pod", "src", "tool-runner.ts")) //nolint:gosec // repository-local source path.
	if err != nil {
		t.Fatalf("read Runtime Web bounds: %v", err)
	}

	for _, required := range []string{
		`MaxInboundGRPCMessageBytes\s*=\s*64\s*\*\s*1024`,
		`MaxOutboundGRPCMessageBytes\s*=\s*64\s*\*\s*1024`,
		`MaxRuntimeCommandGRPCMessageBytes\s*=\s*4\s*\*\s*1024\s*\*\s*1024`,
		`MaxAttachmentGRPCMessageBytes\s*=\s*32\s*\*\s*1024\s*\*\s*1024`,
		`MaxBridgeAPIGRPCMessageBytes\s*=\s*64\s*\*\s*1024\s*\*\s*1024`,
	} {
		if !regexp.MustCompile(required).Match(goBody) {
			t.Fatalf("Go transport bounds missing %q", required)
		}
	}
	for _, required := range []string{
		`grpc\.MaxRecvMsgSize\(sessionrpc\.MaxBridgeAPIGRPCMessageBytes\)`,
		`grpc\.MaxSendMsgSize\(sessionrpc\.MaxBridgeAPIGRPCMessageBytes\)`,
	} {
		if !regexp.MustCompile(required).Match(bridgeMainBody) {
			t.Fatalf("Bridge API transport composition missing %q", required)
		}
	}
	for _, required := range []string{
		`export const MaxGrpcInboundMessageBytes\s*=\s*4\s*\*\s*1024\s*\*\s*1024;`,
		`export const MaxGrpcOutboundMessageBytes\s*=\s*4\s*\*\s*1024\s*\*\s*1024;`,
		`export const MaxAttachmentGrpcMessageBytes\s*=\s*32\s*\*\s*1024\s*\*\s*1024;`,
		`export const MaxBridgeDurableContextGrpcMessageBytes\s*=\s*64\s*\*\s*1024\s*\*\s*1024;`,
		`export const MaxGatewayRequestGrpcMessageBytes\s*=\s*64\s*\*\s*1024\s*\*\s*1024;`,
		`export const MaxGatewayStreamEventGrpcMessageBytes\s*=\s*8\s*\*\s*1024\s*\*\s*1024;`,
	} {
		if !regexp.MustCompile(required).Match(tsBody) {
			t.Fatalf("TypeScript transport bounds missing %q", required)
		}
	}
	if required := `export const MaxPayloadJsonBytes\s*=\s*2\s*\*\s*1024\s*\*\s*1024;`; !regexp.MustCompile(required).Match(protocolBody) {
		t.Fatalf("TypeScript protocol bounds missing %q", required)
	}
	for _, required := range []string{
		`export const MailFetchMaxEnvelopes\s*=\s*4;`,
	} {
		if !regexp.MustCompile(required).Match(protocolBody) {
			t.Fatalf("TypeScript completion-mail bounds missing %q", required)
		}
	}
	for _, required := range []string{
		`MailFetchMaxEnvelopes\s*=\s*4`,
	} {
		if !regexp.MustCompile(required).Match(completionMailBody) {
			t.Fatalf("Go completion-mail bounds missing %q", required)
		}
	}
	if required := `MaxGrpcInboundMessageBytes\s*=\s*64\s*\*\s*1024\s*\*\s*1024`; !regexp.MustCompile(required).Match(gatewayBody) {
		t.Fatalf("Gateway transport bounds missing %q", required)
	}
	if required := `export const MaxProviderContextTextJsonBytes\s*=\s*16\s*\*\s*1024\s*\*\s*1024;`; !regexp.MustCompile(required).Match(gatewayProtocolBody) {
		t.Fatalf("Gateway protocol bounds missing %q", required)
	}
	if required := `export const MaxProviderRequestToolOutputJsonBytes\s*=\s*512\s*\*\s*1024;`; !regexp.MustCompile(required).Match(gatewayProtocolBody) {
		t.Fatalf("Gateway protocol bounds missing %q", required)
	}
	for _, pair := range []struct {
		goPattern string
		tsPattern string
	}{
		{`maxRunWebRequestGRPCMessageBytes\s*=\s*1024\s*\*\s*1024`, `export const MaxWebRequestGrpcMessageBytes\s*=\s*1024\s*\*\s*1024;`},
		{`maxRunWebResponseGRPCMessageBytes\s*=\s*512\s*\*\s*1024`, `export const MaxWebResponseGrpcMessageBytes\s*=\s*512\s*\*\s*1024;`},
		{`maxOperations\s*=\s*8`, `const WEB_OPERATIONS_MAX\s*=\s*8;`},
		{`maxSearchDomains\s*=\s*4`, `const WEB_SEARCH_DOMAINS_MAX\s*=\s*4;`},
		{`maxRequestTextBytes\s*=\s*64\s*\*\s*1024`, `MaxTextBytes`},
		{`maxDomainBytes\s*=\s*253`, `const WEB_DOMAIN_MAX_BYTES\s*=\s*253;`},
	} {
		if !regexp.MustCompile(pair.goPattern).Match(webConnectorBody) {
			t.Fatalf("Go Web bound missing %q", pair.goPattern)
		}
		if !regexp.MustCompile(pair.tsPattern).Match(append(tsBody, toolRunnerBody...)) {
			t.Fatalf("TypeScript Web bound missing %q", pair.tsPattern)
		}
	}
}
