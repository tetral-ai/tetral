package static

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestGitHubIntegrationRuntimeEventsTrackForkSDKShapes(t *testing.T) {
	engineRoot := finalArchitectureEngineRoot(t)
	sdkRoot := forkSDKRootForStaticTest(t, engineRoot)
	eventsPath := filepath.Join(sdkRoot, "src", "resources", "beta", "sessions", "events.ts")
	body, err := os.ReadFile(eventsPath) //nolint:gosec // local fork SDK source drift gate.
	if err != nil {
		if os.IsNotExist(err) {
			t.Fatalf("fork SDK source not available at %s; set TETRAL_ENGINE_SDK_ROOT or place the fork next to the engine checkout to run the required SDK drift gate", eventsPath)
		}
		t.Fatalf("read fork SDK events source: %v", err)
	}
	sdkEvents := string(body)
	runMCPRuntimeEventForkSDKFixtureTypecheck(t, engineRoot, eventsPath)

	assertInterfaceContains(t, sdkEvents, "BetaManagedAgentsAgentMCPToolUseEvent", []string{
		"id: string;",
		"processed_at: string;",
		"type: 'agent.mcp_tool_use';",
		"name: string;",
		"input: { [key: string]: unknown };",
		"mcp_server_name: string;",
		"evaluated_permission?: 'allow' | 'ask' | 'deny';",
	})
	assertInterfaceContains(t, sdkEvents, "BetaManagedAgentsAgentMCPToolResultEvent", []string{
		"id: string;",
		"processed_at: string;",
		"type: 'agent.mcp_tool_result';",
		"mcp_tool_use_id: string;",
		"content?: Array<",
		"is_error?: boolean | null;",
	})
	assertInterfaceContains(t, sdkEvents, "BetaManagedAgentsMCPAuthenticationFailedError", []string{
		"mcp_server_name: string;",
		"message: string;",
		"type: 'mcp_authentication_failed_error';",
		"BetaManagedAgentsRetryStatusRetrying",
		"BetaManagedAgentsRetryStatusExhausted",
		"BetaManagedAgentsRetryStatusTerminal",
	})
	assertInterfaceContains(t, sdkEvents, "BetaManagedAgentsMCPConnectionFailedError", []string{
		"mcp_server_name: string;",
		"message: string;",
		"type: 'mcp_connection_failed_error';",
		"BetaManagedAgentsRetryStatusRetrying",
		"BetaManagedAgentsRetryStatusExhausted",
		"BetaManagedAgentsRetryStatusTerminal",
	})
	assertInterfaceContains(t, sdkEvents, "BetaManagedAgentsSessionErrorEvent", []string{
		"id: string;",
		"processed_at: string;",
		"type: 'session.error';",
		"BetaManagedAgentsMCPConnectionFailedError",
		"BetaManagedAgentsMCPAuthenticationFailedError",
	})

	runtimeContractTest := finalArchitectureReadText(t, filepath.Join(
		engineRoot,
		"services",
		"agent-runtime",
		"packages",
		"core",
		"test",
		"unit",
		"runtime-contract.test.ts",
	))
	for _, required := range []string{
		`test("emits MCP runtime events compatible with fork SDK event types"`,
		"satisfies ForkSDKAgentMCPToolUseEvent",
		"satisfies ForkSDKAgentMCPToolResultEvent",
		"satisfies ForkSDKSessionMCPErrorEvent",
		"type RuntimeMCPToolUseSDKCompatibility",
		"type RuntimeMCPToolResultSDKCompatibility",
		"type RuntimeMCPAuthErrorSDKCompatibility",
		"type RuntimeMCPConnectionErrorSDKCompatibility",
	} {
		if !strings.Contains(runtimeContractTest, required) {
			t.Fatalf("runtime SDK compatibility test missing %q", required)
		}
	}
}

func runMCPRuntimeEventForkSDKFixtureTypecheck(t *testing.T, engineRoot string, sdkEventsPath string) {
	t.Helper()
	absoluteSDKEventsPath, err := filepath.Abs(sdkEventsPath)
	if err != nil {
		t.Fatalf("resolve fork SDK events path %s: %v", sdkEventsPath, err)
	}
	sdkImportPath := filepath.ToSlash(absoluteSDKEventsPath)
	fixture := fmt.Sprintf(`import type {
  BetaManagedAgentsAgentMCPToolResultEvent,
  BetaManagedAgentsAgentMCPToolUseEvent,
  BetaManagedAgentsSessionErrorEvent,
  BetaManagedAgentsSessionEvent,
} from %q;

type SDKEventEnvelope = {
  readonly id: string;
  readonly processed_at: string;
};

function withEnvelope<T extends object>(event: T): T & SDKEventEnvelope {
  return {
    id: "sevt_mcp_fixture",
    processed_at: "2026-01-01T00:00:00Z",
    ...event,
  };
}

const mcpToolUse = withEnvelope({
  type: "agent.mcp_tool_use",
  name: "create_issue",
  input: { title: "Bug" },
  mcp_server_name: "github",
  evaluated_permission: "ask",
}) satisfies BetaManagedAgentsAgentMCPToolUseEvent;

const mcpToolResult = withEnvelope({
  type: "agent.mcp_tool_result",
  mcp_tool_use_id: "sevt_mcp_tool_use_1",
  content: [{ type: "text", text: "created" }],
  is_error: false,
}) satisfies BetaManagedAgentsAgentMCPToolResultEvent;

const authError = withEnvelope({
  type: "session.error",
  error: {
    type: "mcp_authentication_failed_error",
    mcp_server_name: "github",
    message: "MCP authentication failed after refresh.",
    retry_status: { type: "terminal" },
  },
}) satisfies BetaManagedAgentsSessionErrorEvent;

const connectionError = withEnvelope({
  type: "session.error",
  error: {
    type: "mcp_connection_failed_error",
    mcp_server_name: "github",
    message: "MCP connection failed.",
    retry_status: { type: "exhausted" },
  },
}) satisfies BetaManagedAgentsSessionErrorEvent;

const emittedMCPFixtures = [
  mcpToolUse,
  mcpToolResult,
  authError,
  connectionError,
] satisfies readonly BetaManagedAgentsSessionEvent[];

void emittedMCPFixtures;
`, sdkImportPath)
	runForkSDKFixtureTypecheck(t, engineRoot, "mcp-runtime-events-fork-sdk-compat.ts", fixture)
}

func runForkSDKFixtureTypecheck(t *testing.T, engineRoot string, fixtureName string, fixture string) {
	t.Helper()
	tscPath := filepath.Join(engineRoot, "services", "agent-runtime", "node_modules", ".bin", "tsc")
	absoluteTSCPath, err := filepath.Abs(tscPath)
	if err != nil {
		t.Fatalf("resolve TypeScript compiler path %s: %v", tscPath, err)
	}
	if _, err := os.Stat(absoluteTSCPath); err != nil {
		t.Fatalf("fork SDK fixture typecheck requires local TypeScript compiler at %s: %v", absoluteTSCPath, err)
	}

	tempDir := t.TempDir()
	fixturePath := filepath.Join(tempDir, fixtureName)
	tsconfigPath := filepath.Join(tempDir, "tsconfig.json")
	if err := os.WriteFile(fixturePath, []byte(fixture), 0o600); err != nil {
		t.Fatalf("write fork SDK typecheck fixture: %v", err)
	}
	tsconfig := fmt.Sprintf(`{
  "compilerOptions": {
    "target": "ES2023",
    "module": "ESNext",
    "moduleResolution": "Bundler",
    "strict": true,
    "noUncheckedIndexedAccess": true,
    "exactOptionalPropertyTypes": true,
    "isolatedModules": true,
    "skipLibCheck": true,
    "noEmit": true,
    "allowImportingTsExtensions": true
  },
  "files": [%q]
}
`, filepath.ToSlash(fixturePath))
	if err := os.WriteFile(tsconfigPath, []byte(tsconfig), 0o600); err != nil {
		t.Fatalf("write fork SDK typecheck tsconfig: %v", err)
	}

	cmd := exec.Command(absoluteTSCPath, "-p", tsconfigPath) //nolint:gosec // repository-local compiler path; no user-controlled args.
	cmd.Dir = filepath.Join(engineRoot, "services", "agent-runtime")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("fork SDK fixture typecheck failed: %v\n%s", err, string(output))
	}
}

func TestForkSDKAgentModelUnionTracksDraftPinnedCatalog(t *testing.T) {
	engineRoot := finalArchitectureEngineRoot(t)
	sdkRoot := forkSDKRootForStaticTest(t, engineRoot)
	agentsPath := filepath.Join(sdkRoot, "src", "resources", "beta", "agents", "agents.ts")
	body, err := os.ReadFile(agentsPath) //nolint:gosec // local fork SDK source drift gate for draft 5.1.
	if err != nil {
		if os.IsNotExist(err) {
			t.Fatalf("fork SDK agent source not available at %s; set TETRAL_ENGINE_SDK_ROOT or place the fork next to the engine checkout to run the required model catalog drift gate", agentsPath)
		}
		t.Fatalf("read fork SDK agent source: %v", err)
	}
	source := string(body)
	modelUnion, ok := typescriptTypeAliasBody(source, "BetaManagedAgentsModel")
	if !ok {
		t.Fatal("fork SDK source missing type alias BetaManagedAgentsModel")
	}
	for _, required := range []string{
		"'openai/gpt-5.5'",
		"'openai/gpt-5.6-sol'",
		"'anthropic/claude-opus-4-8'",
		"'anthropic/claude-fable-5'",
		"'deepseek/deepseek-v4-pro'",
		"'moonshotai/kimi-k3'",
		"'zai/glm-5.2'",
	} {
		if !strings.Contains(modelUnion, required) {
			t.Fatalf("fork SDK BetaManagedAgentsModel missing %s in:\n%s", required, modelUnion)
		}
	}
	if count := strings.Count(modelUnion, "| '"); count != 7 {
		t.Fatalf("fork SDK BetaManagedAgentsModel known-literal count = %d; want exactly 7 in:\n%s", count, modelUnion)
	}
	if count := strings.Count(modelUnion, "(string & {})"); count != 1 {
		t.Fatalf("fork SDK BetaManagedAgentsModel widening member count = %d; want exactly 1 in:\n%s", count, modelUnion)
	}
	for _, forbidden := range []string{
		"| string",
		"'claude-opus-4-8'",
		"'gpt-5.5'",
		"'gpt-5.6-sol'",
		"'claude-fable-5'",
		"'kimi-k3'",
	} {
		if strings.Contains(modelUnion, forbidden) {
			t.Fatalf("fork SDK BetaManagedAgentsModel contains forbidden broad/providerless member %q in:\n%s", forbidden, modelUnion)
		}
	}
	for _, required := range []string{
		"id: BetaManagedAgentsModel;",
		"speed?: 'standard' | 'fast';",
		"speed?: 'standard' | 'fast' | null;",
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("fork SDK agent model config missing %q", required)
		}
	}
}

func forkSDKRootForStaticTest(t *testing.T, engineRoot string) string {
	t.Helper()
	sdkRoot := strings.TrimSpace(os.Getenv("TETRAL_ENGINE_SDK_ROOT"))
	if sdkRoot != "" {
		return sdkRoot
	}
	sdkRoot = filepath.Clean(filepath.Join(engineRoot, "..", "tetral-sdk-typescript"))
	if _, err := os.Stat(sdkRoot); os.IsNotExist(err) {
		t.Skip("set TETRAL_ENGINE_SDK_ROOT or check out tetral-ai/tetral-sdk-typescript next to the engine repository to run fork-SDK drift gates")
	}
	return sdkRoot
}

func assertInterfaceContains(t *testing.T, source string, name string, required []string) {
	t.Helper()
	body, ok := typescriptInterfaceBody(source, name)
	if !ok {
		t.Fatalf("fork SDK source missing interface %s", name)
	}
	for _, token := range required {
		if !strings.Contains(body, token) {
			t.Fatalf("fork SDK interface %s missing %q in:\n%s", name, token, body)
		}
	}
}

func typescriptInterfaceBody(source string, name string) (string, bool) {
	header := "export interface " + name
	start := strings.Index(source, header)
	if start < 0 {
		return "", false
	}
	open := strings.Index(source[start:], "{")
	if open < 0 {
		return "", false
	}
	open += start
	depth := 0
	for i := open; i < len(source); i++ {
		switch source[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return source[open+1 : i], true
			}
		}
	}
	return "", false
}

func typescriptTypeAliasBody(source string, name string) (string, bool) {
	header := "export type " + name + " ="
	start := strings.Index(source, header)
	if start < 0 {
		return "", false
	}
	bodyStart := start + len(header)
	end := strings.Index(source[bodyStart:], ";")
	if end < 0 {
		return "", false
	}
	return strings.TrimSpace(source[bodyStart : bodyStart+end]), true
}
