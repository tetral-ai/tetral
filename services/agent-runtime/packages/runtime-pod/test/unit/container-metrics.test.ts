import { expect, test } from "bun:test";
import { containerMemoryObservation, runtimePodMetricsText } from "../../src/metrics.js";
import { RuntimePodLifecycle } from "../../src/lifecycle.js";

test("container memory uses a complete same-scope cgroup pair and refuses unlimited or malformed limits", () => {
  const read = (values: Record<string, string>) => (path: string) => {
    const value = values[path];
    if (value === undefined) throw new Error("unavailable");
    return value;
  };
  expect(
    containerMemoryObservation(
      read({ "/sys/fs/cgroup/memory.current": "81\n", "/sys/fs/cgroup/memory.max": "100\n" }),
    ),
  ).toEqual({ usageBytes: 81, limitBytes: 100 });
  expect(
    containerMemoryObservation(
      read({
        "/sys/fs/cgroup/memory/memory.usage_in_bytes": "7",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes": "10",
      }),
    ),
  ).toEqual({ usageBytes: 7, limitBytes: 10 });
  for (const limit of ["max", "0", "Infinity", "NaN", "9223372036854771712"]) {
    expect(
      containerMemoryObservation(
        read({
          "/sys/fs/cgroup/memory.current": "1",
          "/sys/fs/cgroup/memory.max": limit,
          "/sys/fs/cgroup/memory/memory.usage_in_bytes": "1",
          "/sys/fs/cgroup/memory/memory.limit_in_bytes": "10",
        }),
      ),
    ).toBeUndefined();
  }
  expect(
    containerMemoryObservation(
      read({
        "/sys/fs/cgroup/memory.current": "1",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes": "10",
      }),
    ),
  ).toBeUndefined();
});

test("unknown container memory omits admission samples instead of inventing headroom", () => {
  const lifecycle = {
    metricsSnapshot: () => ({ ready: true, accepting: true, inFlightCommands: 0 }),
    sessionCapacity: () => 256,
  } as RuntimePodLifecycle;
  const unknown = runtimePodMetricsText(lifecycle, undefined, () => undefined);
  expect(unknown).toContain("runtimepod_session_capacity 256\n");
  expect(unknown).toContain("# runtimepod_container_memory_state unknown_or_unlimited");
  expect(unknown).not.toContain("runtimepod_container_memory_usage_bytes ");
  expect(unknown).not.toContain("runtimepod_container_memory_limit_bytes ");
  const finite = runtimePodMetricsText(lifecycle, undefined, () => ({
    usageBytes: 80,
    limitBytes: 100,
  }));
  expect(finite).toContain("runtimepod_container_memory_usage_bytes 80\n");
  expect(finite).toContain("runtimepod_container_memory_limit_bytes 100\n");
});
