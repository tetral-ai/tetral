import { expect, test } from "bun:test";
import { RuntimePodMetricsRegistry, runtimePodMetricsText } from "../../src/metrics.js";
import type { RuntimePodLifecycle } from "../../src/lifecycle.js";

test("Runtime owning durations expose seconds and keep unavailable approvals outside the completed population", () => {
  const metrics = new RuntimePodMetricsRegistry();
  metrics.observeContentCommitLatency("text","content_commit",100,"duplicate");
  metrics.observeContinuationLatency("approval_wait",1000,"cancelled","agent_provider_request","user");
  metrics.recordApprovalWaitUnavailable("agent_provider_request","user");
  metrics.observeProviderStreamDuration("agent_provider_request",250,"error");
  metrics.observeContextLoadLatency("build_context",500,"success");
  metrics.observeShutdownPhase("shutdown_local_join",5000,"timeout");
  const lifecycle = {metricsSnapshot:()=>({ready:true,accepting:true,inFlightCommands:0}),sessionCapacity:()=>256} as RuntimePodLifecycle;
  const text = runtimePodMetricsText(lifecycle,metrics,()=>undefined);
  expect(text).toContain('operation="content_commit",outcome="duplicate",le="0.1"} 1');
  expect(text).toContain('operation="approval_wait",outcome="cancelled"} 1');
  expect(text).not.toContain('operation="approval_wait",outcome="success"');
  expect(text).toContain('operation="shutdown_local_join",outcome="timeout"} 5');
  expect(text).toContain('runtimepod_approval_wait_unavailable_total{approval_source="user",request_kind="agent_provider_request"} 1');
  expect(text).toContain("runtimepod_session_capacity 256");
});
