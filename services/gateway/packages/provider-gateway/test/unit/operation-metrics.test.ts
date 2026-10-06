import { expect, test } from "bun:test";
import { OperationMetricsRegistry, operationDurationBuckets } from "@tetral/ts-observability";
import { ProviderGatewayMetricsRegistry } from "../../src/metrics.js";
import { McpConnectorMetricsRegistry } from "../../../mcp-connector/src/metrics.js";

test("fixed duration buckets keep exact boundaries, all outcomes and one unknown-method population", () => {
  const metrics = new OperationMetricsRegistry("provider-gateway", ["complete_frame_write"]);
  metrics.observe("complete_frame_write", "success", 0.1);
  metrics.observe("complete_frame_write", "success", 0.100000001);
  metrics.observe("request-id-one", "cancelled", 1801);
  metrics.observe("request-id-two", "cancelled", 1);
  metrics.observe("complete_frame_write", "error", NaN);
  const text = metrics.render();
  expect(text).toContain('operation="complete_frame_write",outcome="success",le="0.1"} 1');
  expect(text).toContain('operation="complete_frame_write",outcome="success",le="0.25"} 2');
  expect(text).toContain('operation="unknown_method",outcome="cancelled",le="+Inf"} 2');
  expect(text).toContain('operation="unknown_method",outcome="cancelled",le="1800"} 1');
  expect(text).not.toContain("request-id");
  expect(text).not.toContain('outcome="error"');
  expect(operationDurationBuckets).toEqual([.001,.005,.01,.025,.05,.1,.25,.5,1,2.5,5,10,30,60,120,300,900,1800]);
});

test("Gateway stages and MCP terminal calls render seconds histograms while preserving old series", () => {
  const gateway = new ProviderGatewayMetricsRegistry();
  gateway.observeProviderStage({stage:"provider_first_fragment",outcome:"cancelled",kind:"none",durationMs:250,canonicalBytes:0,encodedBytes:0});
  const gatewayText = gateway.render({ready:true});
  expect(gatewayText).toContain('operation="provider_first_fragment",outcome="cancelled",le="0.25"} 1');
  expect(gatewayText).toContain("providergateway_provider_first_fragment_ms_sum 250");
  const mcp = new McpConnectorMetricsRegistry();
  for (const tool of ["foreign-tool-one", "foreign-tool-two"]) { mcp.recordRunTool({tool,status:"tool_error",errorKind:"mcp_timeout",durationSeconds:5}); mcp.operations.observe("RunMcpTool","timeout",5); }
  const mcpText = mcp.render();
  expect(mcpText).toContain('tetral_operation_duration_seconds_count{service="mcp-connector",operation="RunMcpTool",outcome="timeout"} 2');
  expect(mcpText.split("\n").filter(line=>line.startsWith("tetral_operation_")).join("\n")).not.toContain("foreign-tool");
  expect(mcpText).toContain('mcpconnector_call_latency_seconds_sum{tool="foreign-tool-one",status="tool_error",error_kind="mcp_timeout"} 5');
});
