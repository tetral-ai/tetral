import { expect, test } from "bun:test";
import { longOperationDurationBuckets, OperationMetricsRegistry, requestDurationBuckets } from "@tetral/ts-observability";
import { providerGatewayDeclarations } from "../../src/logger.js";
import { ProviderGatewayMetricsRegistry } from "../../src/metrics.js";
import { McpConnectorMetricsRegistry } from "../../../mcp-connector/src/metrics.js";

test("declared long operations use the long buckets, success records as ok, an undeclared code is an error and other operations share unknown_method", () => {
  const metrics = new OperationMetricsRegistry("provider-gateway", ["complete_frame_write", "shutdown_drain"], providerGatewayDeclarations);
  metrics.observe("complete_frame_write", "success", 0.1);
  metrics.observe("complete_frame_write", "ok", 0.100000001);
  metrics.observe("complete_frame_write", "cancelled", 0.1);
  metrics.observe("shutdown_drain", "ok", 0.25);
  metrics.observe("shutdown_drain", "timeout", 0.25);
  metrics.observe("request-id-one", "cancelled", 11);
  metrics.observe("request-id-two", "cancelled", 1);
  metrics.observe("complete_frame_write", "error", NaN);
  const text = metrics.render();
  expect(text).toContain('tetral_operation_duration_seconds_bucket{service="provider-gateway",operation="complete_frame_write",outcome="ok",le="0.1"} 1');
  expect(text).toContain('tetral_operation_duration_seconds_bucket{service="provider-gateway",operation="complete_frame_write",outcome="ok",le="0.25"} 2');
  expect(text).toContain('tetral_operation_duration_seconds_count{service="provider-gateway",operation="complete_frame_write",outcome="cancelled"} 1');
  expect(text).toContain('tetral_long_operation_duration_seconds_bucket{service="provider-gateway",operation="shutdown_drain",outcome="ok",le="0.5"} 1');
  expect(text).toContain('tetral_long_operation_duration_seconds_count{service="provider-gateway",operation="shutdown_drain",outcome="timeout"} 1');
  // unknown_method declares no code, so its cancellations are errors.
  expect(text).toContain('operation="unknown_method",outcome="error",le="10"} 1');
  expect(text).toContain('operation="unknown_method",outcome="error",le="+Inf"} 2');
  expect(text).not.toContain('outcome="success"');
  expect(text).not.toContain("request-id");
  expect(text).not.toContain('operation="complete_frame_write",outcome="error"');
  expect(requestDurationBuckets).toEqual([0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]);
  expect(longOperationDurationBuckets).toEqual([0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800]);
});

test("Gateway stages render only seconds histograms and MCP terminal calls keep their call series", () => {
  const gateway = new ProviderGatewayMetricsRegistry();
  gateway.observeProviderStage({stage:"provider_first_fragment",outcome:"cancelled",durationMs:250});
  const gatewayText = gateway.render({ready:true});
  expect(gatewayText).toContain('tetral_long_operation_duration_seconds_bucket{service="provider-gateway",operation="provider_first_fragment",outcome="cancelled",le="0.5"} 1');
  expect(gatewayText).not.toContain("providergateway_provider_first_fragment_");
  expect(gatewayText).not.toContain("providergateway_provider_stage_");
  const mcp = new McpConnectorMetricsRegistry();
  for (const tool of ["foreign-tool-one", "foreign-tool-two"]) { mcp.recordRunTool({tool,status:"tool_error",errorKind:"mcp_timeout",durationSeconds:5}); mcp.operations.observe("RunMcpTool","timeout",5); }
  const mcpText = mcp.render();
  expect(mcpText).toContain('tetral_long_operation_duration_seconds_count{service="mcp-connector",operation="RunMcpTool",outcome="timeout"} 2');
  expect(mcpText.split("\n").filter(line=>/^tetral_(long_)?operation_/.test(line)).join("\n")).not.toContain("foreign-tool");
  expect(mcpText).toContain('mcpconnector_call_latency_seconds_sum{tool="foreign-tool-one",status="tool_error",error_kind="mcp_timeout"} 5');
});
