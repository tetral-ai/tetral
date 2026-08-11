import { describe, expect, test } from "bun:test";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Metadata } from "@grpc/grpc-js";
import {
  RuntimeCommandKind,
  RuntimeCommandStatus,
} from "@tetral/agent-runtime-protocol/src/gen/tetral/agent_runtime/v1/agent_runtime.js";
import type { RuntimeInputCommandRequest } from "@tetral/agent-runtime-protocol/src/gen/tetral/agent_runtime/v1/agent_runtime.js";
import { effectivePermissionPolicy, lookupToolEntry } from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import { createToolCatalog } from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import { loadRuntimePodConfig, loadRuntimePodConfigFromEnv } from "../../src/config.js";
import { buildRuntimePodCommandDependencies, runRuntimePodCommand, runtimeModelForThread, runtimeToolPolicyForThread, runtimeToolPolicyFromPatchPayload, runtimeToolPolicyFromPatchPayloads } from "../../src/command.js";
import type { RuntimePodCommandDependencies } from "../../src/command.js";
import { RuntimePodMetricsRegistry } from "../../src/metrics.js";
import type { RuntimePodConfig } from "../../src/config.js";
import type { RuntimePodLogger } from "../../src/logger.js";
import { buildRuntimeCoreHosts } from "../../src/core-hosts.js";
import type { RuntimeCoreHostsOptions } from "../../src/core-hosts.js";

describe("Runtime Pod command entrypoint", () => {
  test("process env projection ignores unrelated inherited variables", () => {
    const config = loadRuntimePodConfigFromEnv({
      ...validEnv(),
      PATH: "/usr/bin",
      HOME: "/home/runtime",
      KUBERNETES_SERVICE_HOST: "10.96.0.1",
      DATABASE_URL: "postgres://must-not-be-read",
    });

    expect(config).toEqual(loadRuntimePodConfig(validEnv()));
    expect(config.ok).toBe(true);
    if (config.ok) {
      expect(JSON.stringify(config.config)).not.toContain("DATABASE_URL");
      expect(JSON.stringify(config.config)).not.toContain("postgres://must-not-be-read");
      expect(JSON.stringify(config.config)).not.toContain("/usr/bin");
    }
  });

  test("production dependency assembly feeds one stream timeout to ordinary and compaction requests", async () => {
    let capturedOptions: RuntimeCoreHostsOptions | undefined;
    const dependencies = await buildRuntimePodCommandDependencies({
      config: {
        ...validConfig(),
        providerStreamTimeoutMs: 765_432,
      },
      logger: { info: () => undefined, error: () => undefined },
      builderOptions: {
        coreHostsFactory: async (options) => {
          capturedOptions = options;
          return fakeDependencies([]).coreHosts;
        },
      },
    });
    try {
      expect(capturedOptions?.threadLoop.providerCallRuntime?.timeoutMs).toBe(765_432);
      expect(capturedOptions?.threadLoop.compaction?.timeoutMs).toBe(765_432);
    } finally {
      await dependencies.coreHosts.close();
    }
  });

  test("production dependency assembly routes closeout records to metrics and severity-aware logging", async () => {
    let capturedOptions: RuntimeCoreHostsOptions | undefined;
    const logged: string[] = [];
    const dependencies = await buildRuntimePodCommandDependencies({
      config: validConfig(),
      logger: {
        info: (record) => logged.push(`info:${record.event}`),
        error: (record) => logged.push(`error:${record.event}`),
      },
      builderOptions: {
        coreHostsFactory: async (options) => {
          capturedOptions = options;
          return fakeDependencies([]).coreHosts;
        },
      },
    });
    try {
      capturedOptions?.recordCloseoutEvent?.({
        event: "runtime_closeout_stalled",
        activeCloseouts: 2,
      });
      capturedOptions?.recordCloseoutEvent?.({
        event: "runtime_closeout_recovered",
        activeCloseouts: 0,
      });
      capturedOptions?.recordCloseoutEvent?.({
        event: "runtime_closeout_unrepairable",
        activeCloseouts: 1,
        errorCode: "ack_mismatch",
      });

      expect(logged).toEqual([
        "error:runtime_closeout_stalled",
        "info:runtime_closeout_recovered",
        "error:runtime_closeout_unrepairable",
      ]);
      expect(dependencies.metrics.snapshot().closeoutEvents).toEqual(new Map([
        ["runtime_closeout_stalled", 2],
        ["runtime_closeout_recovered", 1],
        ["runtime_closeout_unrepairable", 1],
      ]));
    } finally {
      await dependencies.coreHosts.close();
    }
  });

  test("production closeout observability contains a throwing logger", async () => {
    let capturedOptions: RuntimeCoreHostsOptions | undefined;
    const dependencies = await buildRuntimePodCommandDependencies({
      config: validConfig(),
      logger: {
        info: () => {
          throw new Error("info sink failed");
        },
        error: () => {
          throw new Error("error sink failed");
        },
      },
      builderOptions: {
        coreHostsFactory: async (options) => {
          capturedOptions = options;
          return fakeDependencies([]).coreHosts;
        },
      },
    });
    try {
      expect(() => capturedOptions?.recordCloseoutEvent?.({
        event: "runtime_closeout_stalled",
        activeCloseouts: 1,
      })).not.toThrow();
      expect(() => capturedOptions?.recordCloseoutEvent?.({
        event: "runtime_closeout_recovered",
        activeCloseouts: 0,
      })).not.toThrow();
      expect(dependencies.metrics.snapshot().closeoutEvents).toEqual(new Map([
        ["runtime_closeout_stalled", 1],
        ["runtime_closeout_recovered", 1],
      ]));
    } finally {
      await dependencies.coreHosts.close();
    }
  });

  test("production manifest observability reports effective eligibility for applied and stale generations", async () => {
    let capturedOptions: RuntimeCoreHostsOptions | undefined;
    const logged: Array<Record<string, unknown>> = [];
    const dependencies = await buildRuntimePodCommandDependencies({
      config: validConfig(),
      logger: { info: (record) => logged.push(record), error: () => undefined },
      builderOptions: { coreHostsFactory: async (options) => {
        capturedOptions = options;
        return fakeDependencies([]).coreHosts;
      } },
    });
    const effectivePatches = [
      { generation: 4, payloadJson: JSON.stringify({
        runtime_config: { installedTools: [{ type: "tetral_agent_toolset", family: "claude" }] },
        tool_policy: { mcpToolsets: [{ mcpServerName: "github" }] },
      }) },
      { generation: 7, mcpServerName: "github", manifestETag: "etag_7", payloadJson: JSON.stringify({
        mcp_manifest: { mcp_server_name: "github", manifest_etag: "etag_7", manifest_generation: 7,
          tools: [{ name: "search", description: "CANARY_MANIFEST_SECRET", input_schema: { type: "object" } }] },
      }) },
    ] as const;
    try {
      const eligible = capturedOptions?.resolveMCPManifestEligibility?.(effectivePatches, "github") ?? false;
      const activeServerWithoutToolset = capturedOptions?.resolveMCPManifestEligibility?.([
        { generation: 4, payloadJson: JSON.stringify({
          runtime_config: {
            installedTools: [{ type: "tetral_agent_toolset", family: "claude" }],
            mcpServers: [{ type: "url", name: "github", url: "https://api.githubcopilot.com/mcp/" }],
          },
          tool_policy: { mcpToolsets: [] },
        }) },
        { generation: 7, mcpServerName: "github", manifestETag: "etag_7", payloadJson: JSON.stringify({
          mcp_manifest: { mcp_server_name: "github", manifest_etag: "etag_7", manifest_generation: 7,
            tools: [{ name: "search", description: "Search", input_schema: { type: "object" } }] },
        }) },
      ], "github") ?? true;
      capturedOptions?.recordMCPManifestUpdate?.({
        workspaceId: "default", sessionId: "sesn_manifest_log", mcpServerName: "github",
        disposition: "applied", source: "cold_load", receivedGeneration: 7,
        currentGeneration: 7, toolCatalogEligible: eligible,
      });
      capturedOptions?.recordMCPManifestUpdate?.({
        workspaceId: "default", sessionId: "sesn_manifest_log", mcpServerName: "github",
        disposition: "stale", source: "runtime_config_update", receivedGeneration: 6,
        currentGeneration: 7, toolCatalogEligible: eligible,
      });
      expect(logged).toEqual([
        expect.objectContaining({ event: "runtime_mcp_manifest_update", "mcp.manifest.disposition": "applied",
          "mcp.manifest.source": "cold_load", "mcp.manifest.received_generation": 7,
          "mcp.manifest.current_generation": 7, "mcp.tool_catalog.eligible": true }),
        expect.objectContaining({ event: "runtime_mcp_manifest_update", "mcp.manifest.disposition": "stale",
          "mcp.manifest.source": "runtime_config_update", "mcp.manifest.received_generation": 6,
          "mcp.manifest.current_generation": 7, "mcp.tool_catalog.eligible": true }),
      ]);
      expect(activeServerWithoutToolset).toBe(false);
      expect(JSON.stringify(logged)).not.toContain("CANARY_MANIFEST_SECRET");
    } finally {
      await dependencies.coreHosts.close();
    }
  });

  test("production manifest observation is fail-open when the log sink throws", async () => {
    let capturedOptions: RuntimeCoreHostsOptions | undefined;
    const dependencies = await buildRuntimePodCommandDependencies({
      config: validConfig(),
      logger: { info: () => { throw new Error("log sink unavailable"); }, error: () => undefined },
      builderOptions: { coreHostsFactory: async (options) => {
        capturedOptions = options;
        return fakeDependencies([]).coreHosts;
      } },
    });
    try {
      expect(() => capturedOptions?.recordMCPManifestUpdate?.({
        workspaceId: "default", sessionId: "sesn_manifest_log", mcpServerName: "github",
        disposition: "applied", source: "runtime_config_update", receivedGeneration: 1,
        currentGeneration: 1, toolCatalogEligible: false,
      })).not.toThrow();
    } finally {
      await dependencies.coreHosts.close();
    }
  });

  test("command runner starts production dependency graph and closes resources on shutdown", async () => {
    const records: string[] = [];
    let capturedConfig: RuntimePodConfig | undefined;
    let capturedLogger: RuntimePodLogger | undefined;
    const dependencies = fakeDependencies(records);

    await withEnv(validEnv(), async () => {
      await runRuntimePodCommand({
        logger: {
          info: (record) => {
            records.push(`info:${record.event}`);
          },
          error: (record) => {
            records.push(`error:${record.kind}`);
          },
        },
        dependencyBuilder: async ({ config, logger }) => {
          capturedConfig = config;
          capturedLogger = logger;
          records.push("dependencyBuilder");
          return dependencies;
        },
        waitForever: async () => {
          records.push("waitForever");
          return undefined as never;
        },
      });
    });

    expect(capturedConfig?.ownPod.name).toBe("runtime-pod-a");
    expect(capturedConfig?.gatewayGrpcAddress).toBe("gateway.engine.svc:9090");
    expect(capturedConfig?.mcpConnectorGrpcAddress).toBe("gateway.engine.svc:9091");
    expect(capturedConfig?.platformModels).toEqual({
      approvalReviewer: { providerId: "anthropic", modelId: "claude-opus-4-8" },
    });
    expect(capturedLogger).toBeDefined();
    expect(records).toEqual(["dependencyBuilder", "app.start", "info:workload.started", "waitForever"]);
  });

  test("started log sink failure does not replace successful runtime startup", async () => {
    const records: string[] = [];
    await withEnv(validEnv(), async () => {
      await runRuntimePodCommand({
        logger: {
          info: () => { throw new Error("sink unavailable"); },
          error: () => undefined,
        },
        dependencyBuilder: async () => fakeDependencies(records),
        waitForever: async () => {
          records.push("waitForever");
          return undefined as never;
        },
      });
    });

    expect(records).toEqual(["app.start", "waitForever"]);
  });

  test("command runner exposes shutdown path that closes app and core resources", async () => {
    const records: string[] = [];
    let shutdown: (() => Promise<void>) | undefined;

    await withEnv(validEnv(), async () => {
      await runRuntimePodCommand({
        logger: { info: () => undefined, error: () => undefined },
        dependencyBuilder: async () => fakeDependencies(records),
        registerSignalHandlers: (handler) => {
          shutdown = handler;
        },
        waitForever: async () => {
          await shutdown?.();
          return undefined as never;
        },
      });
    });

    expect(records).toEqual(["app.start", "app.shutdown", "core.close"]);
  });

  test("runtime config patch payload installs approval mode and provider-visible tool configs", () => {
    const policy = runtimeToolPolicyFromPatchPayload(JSON.stringify({
      config_generation: 7,
      runtime_config: { installedTools: [{ type: "tetral_agent_toolset", family: "claude" }] },
      approval_mode: "full_access",
      tools: {
        configs: [
          { name: "Write", enabled: false },
          { name: "Read", enabled: true, permission_policy: "always_allow" },
        ],
      },
    }));

    expect(policy.approvalMode).toBe("full_access");
    expect(lookupToolEntry(policy.toolCatalog, "Write")).toBeUndefined();
    expect(lookupToolEntry(policy.toolCatalog, "Read")).toBeDefined();
    expect(lookupToolEntry(policy.toolCatalog, "spawn_agent")).toBeDefined();
  });

  test("single-family cold policy and same-family patches stay generation-fenced", () => {
    const coldPayload = JSON.stringify({
      config_generation: 7,
      runtime_config: {
        installedTools: [{ type: "tetral_agent_toolset", family: "claude" }],
        system: "Operate as the session specialist.",
      },
    });
    const cold = runtimeToolPolicyFromPatchPayloads([coldPayload]);

    expect(cold.system).toBe("Operate as the session specialist.");
    expect(cold.toolCatalog.entries.map((entry) => entry.name).filter((name) =>
      ["Bash", "Read", "Write", "Edit", "Glob", "Grep", "exec_command", "write_stdin", "view_image", "apply_patch"].includes(name),
    )).toEqual(["Bash", "Read", "Write", "Edit", "Glob", "Grep"]);

    const future = runtimeToolPolicyFromPatchPayloads([
      coldPayload,
      JSON.stringify({
        config_generation: 8,
        tool_policy: { configs: [{ name: "Write", enabled: false }] },
        system: "Use the updated session instructions.",
      }),
    ]);
    expect(lookupToolEntry(cold.toolCatalog, "Write")).toBeDefined();
    expect(lookupToolEntry(future.toolCatalog, "Write")).toBeUndefined();
    expect(lookupToolEntry(future.toolCatalog, "Bash")).toBeDefined();
    expect(lookupToolEntry(future.toolCatalog, "exec_command")).toBeUndefined();
    expect(future.system).toBe("Use the updated session instructions.");

    const cleared = runtimeToolPolicyFromPatchPayloads([
      coldPayload,
      JSON.stringify({ config_generation: 8, system: null }),
    ]);
    expect(cleared.system).toBeUndefined();
  });

  test("cold runtime policy rejects absent, invalid, and ambiguous builtin family declarations", () => {
    for (const installedTools of [
      [],
      [{ type: "tetral_agent_toolset", family: "future" }],
      [
        { type: "tetral_agent_toolset", family: "claude" },
        { type: "tetral_agent_toolset", family: "claude" },
      ],
    ]) {
      expect(() => runtimeToolPolicyFromPatchPayload(JSON.stringify({
        config_generation: 1,
        runtime_config: { installedTools },
      }))).toThrow("runtime installed builtin family is malformed");
    }
    expect(() => runtimeToolPolicyFromPatchPayload(undefined)).toThrow("runtime installed builtin family is malformed");
  });

  test("runtime config patch payload carries the durable resolved skill index", () => {
    const policy = runtimeToolPolicyFromPatchPayload(JSON.stringify({
      runtime_config: {
        installedTools: [{ type: "tetral_agent_toolset", family: "claude" }],
        skills: [{ skill_id: "sk_docs", version: "latest" }],
        skillsIndex: [{
          skill_id: "sk_docs",
          skill_version_id: "skv_docs_3",
          version: "3.0.0",
          name: "Docs",
          description: "Read project documentation.",
          directory: "docs",
        }],
      },
    }));

    expect(policy.skillsIndex).toEqual([{
      skillId: "sk_docs",
      skillVersionId: "skv_docs_3",
      version: "3.0.0",
      name: "Docs",
      description: "Read project documentation.",
      directory: "docs",
    }]);
  });

  test("runtime config patch payload carries bounded surface-2 retry budgets", () => {
    const policy = runtimeToolPolicyFromPatchPayload(JSON.stringify({
      runtime_config: {
        installedTools: [{ type: "tetral_agent_toolset", family: "claude" }],
        providerRescheduleBudget: 4,
        compaction_reschedule_budget: 1,
      },
    }));

    expect(policy.providerRescheduleBudget).toBe(4);
    expect(policy.compactionRescheduleBudget).toBe(1);
    expect(() => runtimeToolPolicyFromPatchPayload(JSON.stringify({
      runtime_config: {
        installedTools: [{ type: "tetral_agent_toolset", family: "claude" }],
        providerRescheduleBudget: 11,
      },
    }))).toThrow("runtime reschedule budget is malformed");
  });

  test("runtime config patch payload rejects disabling required memory", () => {
    expect(() =>
      runtimeToolPolicyFromPatchPayload(JSON.stringify({
        config_generation: 8,
        runtime_config: { installedTools: [{ type: "tetral_agent_toolset", family: "claude" }] },
        tools: {
          configs: [{ name: "memory", enabled: false }],
        },
      })),
    ).toThrow("required tool memory cannot be disabled");
  });

  test("runtime policy aggregates admission config and MCP manifest tool projection", () => {
    const policy = runtimeToolPolicyFromPatchPayloads([
      JSON.stringify({
        config_generation: 7,
        runtime_config: { installedTools: [{ type: "tetral_agent_toolset", family: "claude" }] },
        approval_mode: "full_access",
        tools: {
          configs: [{ name: "Write", enabled: false }],
        },
        tool_policy: {
          mcpToolsets: [{
            mcpServerName: "github",
            defaultConfig: { enabled: true, permission_policy: { type: "always_ask" } },
            configs: [
              { name: "github_search", permission_policy: { type: "always_allow" } },
              { name: "github_disabled", enabled: false },
            ],
          }],
        },
      }),
      JSON.stringify({
        mcp_manifest: {
          mcp_server_name: "github",
          manifest_etag: "etag_1",
          manifest_generation: 1,
          tools: [
            { name: "github_search", description: "Search GitHub", input_schema: { type: "object" } },
            { name: "github_disabled", description: "Disabled", input_schema: { type: "object" } },
            { name: "Read", description: "builtin collision", input_schema: { type: "object" } },
          ],
        },
      }),
    ]);

    const githubSearch = lookupToolEntry(policy.toolCatalog, "github_search");
    const read = lookupToolEntry(policy.toolCatalog, "Read");

    expect(policy.approvalMode).toBe("full_access");
    expect(lookupToolEntry(policy.toolCatalog, "Write")).toBeUndefined();
    expect(githubSearch?.route).toEqual({ kind: "gateway", operation: "RunMcpTool", mcpServerName: "github" });
    expect(githubSearch !== undefined ? effectivePermissionPolicy(githubSearch, policy.toolCatalog.configs) : undefined).toBe("always_allow");
    expect(lookupToolEntry(policy.toolCatalog, "github_disabled")).toBeUndefined();
    expect(read?.route.kind).toBe("sandbox");
  });

  test("runtime policy applies session MCP toolset config over delivered manifests", () => {
    const policy = runtimeToolPolicyFromPatchPayloads([
      JSON.stringify({
        config_generation: 8,
        runtime_config: { installedTools: [{ type: "tetral_agent_toolset", family: "claude" }] },
        tool_policy: {
          mcpToolsets: [{
            mcpServerName: "github",
            defaultConfig: { enabled: false, permission_policy: { type: "always_ask" } },
            configs: [
              { name: "github_search", enabled: true, permission_policy: { type: "always_allow" } },
              { name: "github_disabled", enabled: false },
            ],
          }],
        },
      }),
      JSON.stringify({
        mcp_manifest: {
          mcp_server_name: "github",
          manifest_etag: "etag_2",
          manifest_generation: 2,
          tools: [
            { name: "github_search", description: "Search GitHub", input_schema: { type: "object" } },
            { name: "github_disabled", description: "Disabled", input_schema: { type: "object" } },
          ],
        },
      }),
    ]);

    const githubSearch = lookupToolEntry(policy.toolCatalog, "github_search");

    expect(githubSearch?.route).toEqual({ kind: "gateway", operation: "RunMcpTool", mcpServerName: "github" });
    expect(githubSearch !== undefined ? effectivePermissionPolicy(githubSearch, policy.toolCatalog.configs) : undefined).toBe("always_allow");
    expect(lookupToolEntry(policy.toolCatalog, "github_disabled")).toBeUndefined();
  });

  test("manifest intake ignores inline policy and cannot bypass the held MCP toolset carrier", () => {
    const policy = runtimeToolPolicyFromPatchPayloads([
      JSON.stringify({
        config_generation: 1,
        runtime_config: { installedTools: [{ type: "tetral_agent_toolset", family: "claude" }] },
      }),
      JSON.stringify({
        mcp_manifest: {
          mcp_server_name: "github",
          manifest_etag: "etag_inline_policy",
          manifest_generation: 1,
          default_config: { enabled: true, permission_policy: { type: "always_allow" }, ignored_marker: "inline" },
          configs: [{ name: "github_search", permission_policy: { type: "always_allow" }, ignored_marker: "inline" }],
          tools: [{ name: "github_search", description: "Search GitHub", input_schema: { type: "object" } }],
        },
      }),
    ]);

    expect(lookupToolEntry(policy.toolCatalog, "github_search")).toBeUndefined();
  });

  test("unrelated config rebuild preserves the create-time memory-store prompt metadata", () => {
    const memoryStores = [{
      memoryStoreId: "memstore_notes",
      name: "Project notes",
      access: "read_write",
      instructions: "Preserve this guidance.",
    }] as const;
    const policy = runtimeToolPolicyFromPatchPayloads([
      JSON.stringify({
        config_generation: 1,
        runtime_config: {
          installedTools: [{ type: "tetral_agent_toolset", family: "claude" }],
          memoryStores,
        },
      }),
      JSON.stringify({
        config_generation: 2,
        approval_mode: "approve_for_me",
        memory_stores: memoryStores,
        tool_policy: { approvalMode: "approve_for_me" },
      }),
    ]);

    expect(policy.memoryStores).toEqual(memoryStores);
    expect(policy.approvalMode).toBe("approve_for_me");
  });

  test("unready MCP manifest supersedes the catalog with zero tools and hides its diagnostic", () => {
    const payloads = [
      JSON.stringify({ config_generation: 1, runtime_config: { installedTools: [{ type: "tetral_agent_toolset", family: "claude" }] } }),
      JSON.stringify({ mcp_manifest: {
        mcp_server_name: "github", manifest_generation: 2,
        readiness: "unready", diagnostic: "delivery_exhausted",
      } }),
    ];
    const policy = runtimeToolPolicyFromPatchPayloads(payloads);
    expect(lookupToolEntry(policy.toolCatalog, "github_search")).toBeUndefined();
    expect(lookupToolEntry(policy.toolCatalog, "Read")?.route.kind).toBe("sandbox");
    expect(JSON.stringify(policy.toolCatalog)).not.toContain("delivery_exhausted");
  });

  test("rejects an unready MCP manifest whose tools field is not an empty array", () => {
    const payloads = [
      JSON.stringify({ config_generation: 1, runtime_config: { installedTools: [{ type: "tetral_agent_toolset", family: "claude" }] } }),
      JSON.stringify({ mcp_manifest: {
        mcp_server_name: "github", manifest_generation: 2,
        readiness: "unready", diagnostic: "delivery_exhausted", tools: "not-an-array",
      } }),
    ];
    expect(() => runtimeToolPolicyFromPatchPayloads(payloads)).toThrow("invalid MCP manifest runtime config payload");
  });

  test("runtime policy gives approval reviewer threads only read-only approval-free tools", () => {
    for (const family of ["claude", "gpt"] as const) {
      const policy = runtimeToolPolicyForThread(
        "approval_reviewer",
        [
          JSON.stringify({
            config_generation: 1,
            runtime_config: {
              installedTools: [{ type: "tetral_agent_toolset", family }],
              system: "Never expose this agent instruction to the reviewer.",
              memoryStores: [{
                memoryStoreId: "memstore_private",
                name: "Private memory",
                access: "read_only",
                instructions: "Never expose this memory guidance to the reviewer.",
              }],
            },
            approval_mode: "approve_for_me",
            tools: {
              configs: [
                { name: "Write", enabled: true, permission_policy: "always_ask" },
                { name: "web", enabled: true, permission_policy: "always_ask" },
              ],
            },
          }),
        ],
        family,
      );

      expect(policy.approvalMode).toBe("full_access");
      expect(policy.system).toBeUndefined();
      expect(policy.memoryStores).toBeUndefined();
      expect(policy.toolCatalog.entries.map((entry) => entry.name)).toEqual(["Read", "Grep", "Glob"]);
      expect(lookupToolEntry(policy.toolCatalog, "Write")).toBeUndefined();
      expect(lookupToolEntry(policy.toolCatalog, "web")).toBeUndefined();
      const read = lookupToolEntry(policy.toolCatalog, "Read");
      expect(read !== undefined ? effectivePermissionPolicy(read, policy.toolCatalog.configs) : undefined).toBe("always_allow");
    }
  });

  test("runtime model resolution gives approval reviewers their configured model regardless of session patches", () => {
    const reviewerModel = { providerId: "anthropic", modelId: "claude-opus-4-8" };

    expect(runtimeModelForThread(
      "approval_reviewer",
      [
        JSON.stringify({
          runtime_config: {
            agent: { config: { model: "openai/gpt-5.5" } },
          },
        }),
      ],
      reviewerModel,
    )).toEqual(reviewerModel);
  });

  test("runtime model resolution accepts both payload key spellings like its sibling parsers", () => {
    const reviewerModel = { providerId: "anthropic", modelId: "claude-opus-4-8" };

    expect(runtimeModelForThread(
      undefined,
      [JSON.stringify({ runtimeConfig: { agent: { config: { model: "openai/gpt-5.5" } } } })],
      reviewerModel,
    )).toEqual({ providerId: "openai", modelId: "gpt-5.5" });
    expect(runtimeModelForThread(
      undefined,
      [JSON.stringify({ runtime_config: { agent: { config: { model: "openai/gpt-5.5" } } } })],
      reviewerModel,
    )).toEqual({ providerId: "openai", modelId: "gpt-5.5" });
  });

  test("runtime policy parser rejects malformed MCP manifest tools instead of silently omitting them", () => {
    expect(() =>
      runtimeToolPolicyFromPatchPayloads([
        JSON.stringify({
          config_generation: 1,
          runtime_config: { installedTools: [{ type: "tetral_agent_toolset", family: "claude" }] },
        }),
        JSON.stringify({
          mcp_manifest: {
            mcp_server_name: "github",
            manifest_etag: "etag_bad",
            manifest_generation: 3,
            tools: [{ name: "github_search", description: "Search GitHub" }],
          },
        }),
      ]),
    ).toThrow("invalid MCP manifest runtime config payload");
  });

  test("production dependency builder wires core hosts, TokenReview auth, and app command path", async () => {
    const dir = await mkdtemp(join(tmpdir(), "runtime-command-"));
    const outboundInternalGrpcTokenPath = join(dir, "outbound-internal-grpc-token");
    const tokenReviewReviewerTokenPath = join(dir, "token-review-reviewer-token");
    const kubernetesApiCaCertPath = join(dir, "kubernetes-ca.crt");
    await writeFile(outboundInternalGrpcTokenPath, "outbound-token\n", { mode: 0o600 });
    await writeFile(tokenReviewReviewerTokenPath, "reviewer-token\n", { mode: 0o600 });
    await writeFile(kubernetesApiCaCertPath, "-----BEGIN CERTIFICATE-----\nca\n-----END CERTIFICATE-----\n", { mode: 0o600 });
    const tokenReviewRequests: unknown[] = [];
    const previousFetch = globalThis.fetch;
    globalThis.fetch = (async (_url, init) => {
      tokenReviewRequests.push(init);
      return new Response(JSON.stringify({
        status: {
          authenticated: true,
          audiences: ["tetral-internal-grpc"],
          user: { username: "system:serviceaccount:engine:bridge" },
        },
      }), { status: 201 });
    }) as typeof fetch;
    const controlCommits: unknown[] = [];
    const config: RuntimePodConfig = {
      ...validConfig(),
      grpcBindAddress: "127.0.0.1:0",
      httpBindAddress: "127.0.0.1:0",
      kubernetesApiCaCertPath,
      tokenReviewReviewerTokenPath,
      outboundInternalGrpcTokenPath,
    };
    const dependencies = await buildRuntimePodCommandDependencies({
      config,
      logger: { info: () => undefined, error: () => undefined },
      builderOptions: {
        coreHostsFactory: async (options) => await buildRuntimeCoreHosts({
          ...options,
          contextLoader: {
            loadThreadContext: async () => ({
              messages: [],
              turnFacts: { events: [], messageLineage: [] },
              runtimeBindingToken: "runtime-binding-token-command-test",
              coldCoverage: {
                pendingToolIds: [],
                pendingSandboxExecutionIds: [],
                pendingAttachmentIdentities: [],
                undeliveredMailDeliveryIds: [],
              },
            }),
          },
        }),
        controlInputCommitterFactory: () => ({
          commitControlInput: async (input) => {
            controlCommits.push(input);
            return { ok: true as const, stale: true as const };
          },
        }),
      },
    });
    try {
      await dependencies.app.start();
      const accepted = await dependencies.app.service.interrupt(
        validCommand({
          commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_INTERRUPT_CONTROL,
          runtimeInputId: "rin_interrupt",
          payloadJson: JSON.stringify({ origin: "user" }),
        }),
        authMetadata(),
      );
      const tokenReviewRequest = requireTokenReviewRequest(tokenReviewRequests[0]);

      expect(accepted).toMatchObject({
        status: RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_ACCEPTED,
        sessionId: "sesn_1",
        runtimeInputId: "rin_interrupt",
        bindingId: "bind_1",
        bindingGeneration: 42,
      });
      expect(controlCommits).toEqual([
        expect.objectContaining({
          inputKind: "interrupt_control",
          scope: expect.objectContaining({ runtimeInputId: "rin_interrupt" }),
        }),
      ]);
      expect(tokenReviewRequests).toHaveLength(1);
      expect((tokenReviewRequest.headers as Record<string, string>).authorization).toBe("bearer reviewer-token");
      const ca = tokenReviewRequest.tls?.ca?.[0];
      expect(ca !== undefined && ca !== null && typeof ca === "object" && "name" in ca ? ca.name : undefined).toBe(kubernetesApiCaCertPath);
      expect(tokenReviewReviewerTokenPath).not.toBe(outboundInternalGrpcTokenPath);
    } finally {
      globalThis.fetch = previousFetch;
      await dependencies.app.shutdown().catch(() => undefined);
      await dependencies.coreHosts.close();
    }
  });

  test("production startup validates inbound TokenReview reviewer token and CA material before readiness", async () => {
    for (const scenario of [
      { name: "missing reviewer token", token: "missing", ca: "valid" },
      { name: "unreadable reviewer token", token: "directory", ca: "valid" },
      { name: "empty reviewer token", token: "empty", ca: "valid" },
      { name: "missing ca", token: "valid", ca: "missing" },
      { name: "unreadable ca", token: "valid", ca: "directory" },
      { name: "empty ca", token: "valid", ca: "empty" },
    ] as const) {
      const fixture = await tokenReviewMaterialFixture(scenario);
      const records: unknown[] = [];
      const dependencies = await buildRuntimePodCommandDependencies({
        config: fixture.config,
        logger: {
          info: () => undefined,
          error: (record) => records.push(record),
        },
      });
      try {
        await expect(dependencies.app.start(), scenario.name).rejects.toThrow("runtime pod startup failed");

        const serialized = JSON.stringify(records);
        expect(dependencies.app.lifecycle.ready(), scenario.name).toEqual({ ready: false });
        expect(serialized, scenario.name).toContain("startup_error");
        for (const forbidden of [
          fixture.tokenCanary,
          fixture.caCanary,
          fixture.config.tokenReviewReviewerTokenPath,
          fixture.config.kubernetesApiCaCertPath,
          "kubernetes.default.svc",
          "TokenReview",
          "raw request body",
          "bearer",
        ]) {
          expect(serialized, scenario.name).not.toContain(forbidden);
        }
      } finally {
        await dependencies.coreHosts.close();
      }
    }
  });

  test("command runner classifies dependency construction failures as startup_error", async () => {
    const records: string[] = [];

    await withEnv(validEnv(), async () => {
      await expect(
        runRuntimePodCommand({
          logger: {
            info: () => undefined,
            error: (record) => {
              records.push(JSON.stringify(record));
              records.push(`${record.kind}:${record.message}:${record["error.class"]}:${record["error.code"]}:${record["error.message_safe"]}`);
            },
          },
          dependencyBuilder: async () => {
            throw new Error(`bearer secret-token https://kubernetes.default.svc {"kind":"TokenReview"} raw request body sk-provider-key kube object dump`);
          },
          waitForever: async () => undefined as never,
        }),
      ).rejects.toThrow("runtime pod startup error");
    });

    expect(records).toHaveLength(2);
    expect(JSON.parse(records[0] ?? "{}")).toMatchObject({
      kind: "startup_error",
      message: "runtime pod startup failed",
      "error.class": "startup_error",
      "error.code": "startup_error",
      "error.message_safe": "runtime pod startup failed",
      "startup.cause_class": "Error",
      "startup.cause_category": "dependency_readiness",
    });
    expect(records[1]).toBe("startup_error:runtime pod startup failed:startup_error:startup_error:runtime pod startup failed");
    for (const forbidden of ["secret-token", "kubernetes.default.svc", "TokenReview", "raw request body", "sk-provider-key", "kube object dump"]) {
      expect(records.join("\n")).not.toContain(forbidden);
    }
  });
});

function validEnv(): Record<string, string> {
  return {
    TETRAL_RUNTIME_POD_NAMESPACE: "engine",
    TETRAL_RUNTIME_POD_NAME: "runtime-pod-a",
    TETRAL_RUNTIME_POD_UID: "uid-a",
    TETRAL_RUNTIME_POD_IP: "10.0.0.1",
    TETRAL_RUNTIME_POD_GRPC_PORT: "9090",
    TETRAL_RUNTIME_POD_HTTP_ADDR: "127.0.0.1:0",
    TETRAL_DEPLOYMENT_ENVIRONMENT: "test",
    TETRAL_SERVICE_VERSION: "test",
    TETRAL_RUNTIME_POD_GRPC_AUDIENCE: "tetral-internal-grpc",
    TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS: "engine/bridge",
    KUBERNETES_API_SERVER_URL: "https://kubernetes.default.svc",
    KUBERNETES_API_CA_CERT_PATH: "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
    KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH: "/var/run/secrets/kubernetes.io/serviceaccount/token",
    TETRAL_RUNTIME_POD_OUTBOUND_GRPC_TOKEN_PATH: "/var/run/secrets/tetral-internal-grpc/runtime-pod/token",
    TETRAL_BRIDGE_API_GRPC_ADDR: "bridge.engine.svc:9090",
    TETRAL_GATEWAY_GRPC_ADDR: "gateway.engine.svc:9090",
    TETRAL_MCP_CONNECTOR_GRPC_ADDR: "gateway.engine.svc:9091",
    TETRAL_WEB_CONNECTOR_GRPC_ADDR: "gateway.engine.svc:9092",
    TETRAL_RUNTIME_APPROVAL_REVIEWER_MODEL: "anthropic/claude-opus-4-8",
    TETRAL_RUNTIME_SKILL_GUIDANCE_DESCRIPTION_BUDGET_BYTES: "32768",
  };
}

function validConfig(): RuntimePodConfig {
  const config = loadRuntimePodConfig(validEnv());
  if (!config.ok) {
    throw new Error("valid config rejected");
  }
  return config.config;
}

async function tokenReviewMaterialFixture(scenario: {
  readonly token: "valid" | "missing" | "directory" | "empty";
  readonly ca: "valid" | "missing" | "directory" | "empty";
}): Promise<{
  readonly config: RuntimePodConfig;
  readonly tokenCanary: string;
  readonly caCanary: string;
}> {
  const dir = await mkdtemp(join(tmpdir(), "runtime-tokenreview-"));
  const tokenCanary = "REVIEWER_TOKEN_CANARY";
  const caCanary = "SENSITIVE_CA_CERT_CANARY";
  const outboundInternalGrpcTokenPath = join(dir, "outbound-token");
  await writeFile(outboundInternalGrpcTokenPath, "outbound-token\n", { mode: 0o600 });
  const tokenPath = await materialPath(dir, "reviewer-token", scenario.token, `${tokenCanary}\n`);
  const caPath = await materialPath(dir, "ca.crt", scenario.ca, `-----BEGIN CERTIFICATE-----\n${caCanary}\n-----END CERTIFICATE-----\n`);
  return {
    config: {
      ...validConfig(),
      outboundInternalGrpcTokenPath,
      tokenReviewReviewerTokenPath: tokenPath,
      kubernetesApiCaCertPath: caPath,
    },
    tokenCanary,
    caCanary,
  };
}

async function materialPath(
  dir: string,
  name: string,
  kind: "valid" | "missing" | "directory" | "empty",
  content: string,
): Promise<string> {
  const path = join(dir, `${kind}-${name}`);
  if (kind === "missing") {
    return path;
  }
  if (kind === "directory") {
    return await mkdtemp(`${path}-`);
  }
  await writeFile(path, kind === "empty" ? "" : content, { mode: 0o600 });
  return path;
}

function validCommand(overrides: Partial<RuntimeInputCommandRequest> = {}): RuntimeInputCommandRequest {
  return {
    requestId: "req_1",
    workspaceId: "wksp_1",
    sessionId: "sesn_1",
    sessionThreadId: "thrd_1",
    bindingId: "bind_1",
    bindingGeneration: 42,
    targetPodNamespace: "engine",
    targetPodName: "runtime-pod-a",
    targetPodUid: "uid-a",
    targetPodIp: "10.0.0.1",
    runtimeInputId: "rin_1",
    eventIds: ["sevt_1"],
    sequenceFrom: 1,
    sequenceTo: 1,
    commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_MESSAGES,
    payloadJson: "",
    ...overrides,
  };
}

function authMetadata(): Metadata {
  const metadata = new Metadata();
  metadata.set("authorization", "bearer caller-token");
  return metadata;
}

function fakeDependencies(records: string[]): RuntimePodCommandDependencies {
  return {
    tokenReviewClient: { createTokenReview: async () => ({ authenticated: true, audiences: [], username: "" }) },
    coreHosts: {
      commandRunHost: {
        handleAcceptInput: async (command) => ({ ok: true, sessionId: command.sessionId, created: false, started: false }),
        handleAgentMail: async (command) => ({ ok: true, sessionId: command.sessionId, applied: true }),
        handleInterruptControl: async (sessionId) => ({ ok: true, sessionId, created: false, interrupted: true, idleInterrupt: false }),
        handleToolConfirmation: async (sessionId) => ({ ok: true, sessionId, created: false, applied: true }),
        handleTaskNotification: async (sessionId) => ({ ok: true, sessionId, created: false, applied: true }),
        handleRuntimeConfigPatch: async (sessionId) => ({ ok: true, sessionId, created: false, applied: true }),
      },
      subAgentRunHost: {
        enqueueThreadInput: async (input) => ({ ok: true, sessionId: input.sessionId, created: false, started: false }),
        preloadThread: async (input) => ({ ok: true, sessionId: input.sessionId, sessionThreadId: input.sessionThreadId, applied: true }),
        interruptReviewerExecution: async (command) => ({ ok: true, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, applied: true, terminal: true }),
        markThreadClosed: async (command) => ({ ok: true, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, applied: true }),
        markThreadActive: async (command) => ({ ok: true, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, applied: true }),
        waitThread: async (command) => ({ ok: true, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, observed: false, timedOut: false }),
        waitReviewerExecution: async (command) => ({ ok: true, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, status: "idle", terminal: true, timedOut: false }),
        inspectThread: async (command) => ({ ok: true, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, observed: false, messages: [] }),
        inspectReviewerExecution: async (command) => ({ ok: false, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, reason: "reviewer_execution_mismatch" }),
        commitApprovalReviewDecision: async (command) => ({ ok: true, writeId: `rwrite_${command.reviewId}_decision`, eventId: "evt_decision", processedAt: "2026-07-06T00:00:00.000Z" }),
        commitApprovalReviewFailure: async (command) => ({ ok: true, writeId: `rwrite_${command.reviewId}_failure`, eventId: "evt_failure", processedAt: "2026-07-06T00:00:00.000Z" }),
      },
      cleanupRunHost: { handleCleanupSession: async (scope) => ({ ok: true, sessionId: scope.sessionId, cleaned: false }) },
      shutdownActiveRuns: async () => undefined,
      close: async () => {
        records.push("core.close");
      },
    },
    app: {
      service: {} as RuntimePodCommandDependencies["app"]["service"],
      lifecycle: {} as RuntimePodCommandDependencies["app"]["lifecycle"],
      start: async () => {
        records.push("app.start");
        return { grpcPort: 1, httpUrl: new URL("http://127.0.0.1:1") };
      },
      shutdown: async () => {
        records.push("app.shutdown");
      },
    },
    metrics: new RuntimePodMetricsRegistry(),
  };
}

async function withEnv<T>(env: Record<string, string>, run: () => Promise<T>): Promise<T> {
  const previous = new Map<string, string | undefined>();
  for (const [key, value] of Object.entries(env)) {
    previous.set(key, process.env[key]);
    process.env[key] = value;
  }
  try {
    return await run();
  } finally {
    for (const [key, value] of previous) {
      if (value === undefined) {
        delete process.env[key];
      } else {
        process.env[key] = value;
      }
    }
  }
}

function requireTokenReviewRequest(value: unknown): {
  readonly headers?: HeadersInit;
  readonly body?: BodyInit | null;
  readonly tls?: { readonly ca?: readonly unknown[] };
} {
  if (typeof value !== "object" || value === null) {
    throw new Error("TokenReview request init was not captured");
  }
  return value as {
    readonly headers?: HeadersInit;
    readonly body?: BodyInit | null;
    readonly tls?: { readonly ca?: readonly unknown[] };
  };
}
