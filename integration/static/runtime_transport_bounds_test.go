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
	gatewayBody, err := os.ReadFile(filepath.Join(root, "services", "gateway", "packages", "provider-gateway", "src", "bounds.ts")) //nolint:gosec // repository-local source path.
	if err != nil {
		t.Fatalf("read Gateway transport bounds: %v", err)
	}
	gatewayProtocolBody, err := os.ReadFile(filepath.Join(root, "services", "gateway", "packages", "protocol", "src", "bounds.ts")) //nolint:gosec // repository-local source path.
	if err != nil {
		t.Fatalf("read Gateway protocol bounds: %v", err)
	}

	for _, required := range []string{
		`MaxInboundGRPCMessageBytes\s*=\s*64\s*\*\s*1024`,
		`MaxOutboundGRPCMessageBytes\s*=\s*64\s*\*\s*1024`,
		`MaxRuntimeCommandGRPCMessageBytes\s*=\s*4\s*\*\s*1024\s*\*\s*1024`,
		`MaxAttachmentGRPCMessageBytes\s*=\s*32\s*\*\s*1024\s*\*\s*1024`,
	} {
		if !regexp.MustCompile(required).Match(goBody) {
			t.Fatalf("Go transport bounds missing %q", required)
		}
	}
	for _, required := range []string{
		`export const MaxGrpcInboundMessageBytes\s*=\s*4\s*\*\s*1024\s*\*\s*1024;`,
		`export const MaxGrpcOutboundMessageBytes\s*=\s*4\s*\*\s*1024\s*\*\s*1024;`,
		`export const MaxAttachmentGrpcMessageBytes\s*=\s*32\s*\*\s*1024\s*\*\s*1024;`,
		`export const MaxGatewayRequestGrpcMessageBytes\s*=\s*32\s*\*\s*1024\s*\*\s*1024;`,
		`export const MaxGatewayStreamEventGrpcMessageBytes\s*=\s*512\s*\*\s*1024;`,
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
		`export const MailFetchMaxBodyBytes\s*=\s*4\s*\*\s*1024\s*\*\s*1024;`,
	} {
		if !regexp.MustCompile(required).Match(protocolBody) {
			t.Fatalf("TypeScript completion-mail bounds missing %q", required)
		}
	}
	for _, required := range []string{
		`MailFetchMaxEnvelopes\s*=\s*4`,
		`MailFetchMaxBodyBytes\s*=\s*4\s*\*\s*1024\s*\*\s*1024`,
	} {
		if !regexp.MustCompile(required).Match(completionMailBody) {
			t.Fatalf("Go completion-mail bounds missing %q", required)
		}
	}
	if required := `MaxGrpcInboundMessageBytes\s*=\s*32\s*\*\s*1024\s*\*\s*1024`; !regexp.MustCompile(required).Match(gatewayBody) {
		t.Fatalf("Gateway transport bounds missing %q", required)
	}
	if required := `export const MaxProviderRequestMessagePartBytes\s*=\s*32\s*\*\s*1024\s*\*\s*1024;`; !regexp.MustCompile(required).Match(gatewayProtocolBody) {
		t.Fatalf("Gateway protocol bounds missing %q", required)
	}
}
