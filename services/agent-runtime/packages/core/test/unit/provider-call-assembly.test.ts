import { describe, expect, test } from "bun:test";
import {
  ProviderRequestKind,
  RuntimeMessageRole,
  SystemCacheHint,
  SystemSegmentKind,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { MaxTextBytes } from "@tetral/gateway-protocol/src/bounds.js";
import {
  ApplyPatchInstructionsText,
  assembleProviderCallRequest,
  PlatformBaseSystemPrompt,
  renderSkillGuidanceSegment,
} from "../../src/agent-loop/provider-call-assembly.js";
import type {
  MemoryStorePromptEntry,
  ProviderCallAssemblyInput,
  SkillGuidanceIndexEntry,
} from "../../src/agent-loop/provider-call-assembly.js";

describe("provider call skill guidance", () => {
  test("adds a stable deterministic SKILL segment only to agent requests", () => {
    const skillsIndex = [
      skillEntry("sk_zeta", "2.0.0", "Zeta", "zeta", "Zeta guidance."),
      skillEntry("sk_alpha", "1.0.0", "Alpha", "alpha", "Alpha guidance."),
    ];
    const agent = assembleProviderCallRequest(providerInput(skillsIndex));
    expect(agent.ok).toBe(true);
    if (!agent.ok) {
      return;
    }
    expect(agent.system[1]).toMatchObject({
      kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_SKILL,
      cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE,
    });
    expect(agent.system[1]?.text.indexOf('"name":"Alpha"')).toBeLessThan(
      agent.system[1]?.text.indexOf('"name":"Zeta"') ?? -1,
    );
    expect(agent.system[1]?.text).toContain('"skill_md_path":"/skills/alpha/SKILL.md"');
    expect(agent.system[1]?.text).not.toContain("skill_version_id");
    expect(agent.system[1]?.text).not.toContain("skill_id");
    expect(agent.system[1]?.text).not.toContain("skill body contents");

    for (const requestKind of [
      ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
      ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY,
      ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER_COMPACTION,
    ]) {
      const nonAgent = assembleProviderCallRequest(providerInput(skillsIndex, requestKind));
      expect(nonAgent.ok).toBe(true);
      if (nonAgent.ok) {
        expect(nonAgent.system.map((segment) => segment.kind)).not.toContain(SystemSegmentKind.SYSTEM_SEGMENT_KIND_SKILL);
      }
    }
  });

  test("orders agent and per-store memory segments between the platform base and skill guidance", () => {
    const agentSystem = "Operate as the session specialist.";
    const memoryStores: readonly MemoryStorePromptEntry[] = [
      {
        memoryStoreId: "memstore_notes",
        name: "Project notes",
        access: "read_write",
        instructions: "Keep decisions and durable context here.\nPreserve this line verbatim.",
      },
      {
        memoryStoreId: "memstore_reference",
        name: "Reference",
        access: "read_only",
      },
    ];
    const input = providerInput([
      skillEntry("sk_alpha", "1.0.0", "Alpha", "alpha", "Alpha guidance."),
    ]);
    const agent = assembleProviderCallRequest({
      ...input,
      runtime: { ...input.runtime, agentSystem, memoryStores },
    });
    expect(agent.ok).toBe(true);
    if (!agent.ok) {
      return;
    }
    expect(agent.system.map((segment) => segment.kind)).toEqual([
      SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE,
      SystemSegmentKind.SYSTEM_SEGMENT_KIND_AGENT,
      SystemSegmentKind.SYSTEM_SEGMENT_KIND_MEMORY,
      SystemSegmentKind.SYSTEM_SEGMENT_KIND_MEMORY,
      SystemSegmentKind.SYSTEM_SEGMENT_KIND_SKILL,
    ]);
    expect(agent.system[1]).toEqual({
      kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_AGENT,
      text: agentSystem,
      cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_SESSION,
    });
    expect(agent.system[2]).toEqual({
      kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_MEMORY,
      text: "Memory store: Project notes\nAccess: read_write\nInstructions:\nKeep decisions and durable context here.\nPreserve this line verbatim.",
      cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_SESSION,
    });
    expect(agent.system[3]).toEqual({
      kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_MEMORY,
      text: "Memory store: Reference\nAccess: read_only",
      cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_SESSION,
    });

    for (const requestKind of [
      ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
      ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY,
      ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER_COMPACTION,
    ]) {
      const nonAgentInput = providerInput([], requestKind);
      const nonAgent = assembleProviderCallRequest({
        ...nonAgentInput,
        runtime: { ...nonAgentInput.runtime, agentSystem, memoryStores },
      });
      expect(nonAgent.ok).toBe(true);
      if (nonAgent.ok) {
        expect(nonAgent.system.map((segment) => segment.kind)).not.toContain(
          SystemSegmentKind.SYSTEM_SEGMENT_KIND_AGENT,
        );
        expect(nonAgent.system.map((segment) => segment.text)).not.toContain(agentSystem);
        expect(nonAgent.system.map((segment) => segment.kind)).not.toContain(
          SystemSegmentKind.SYSTEM_SEGMENT_KIND_MEMORY,
        );
      }
    }
  });

  test("keeps the platform base system prompt byte-for-byte stable", () => {
    expect(PlatformBaseSystemPrompt).toBe(
      "You are Tetral Agent, working in a sandboxed Linux environment.\n\nFiles in your sandbox persist for the life of this session,\nincluding across session sleep and wake, and are gone when the\nsession ends. To keep something across sessions, use the memory\ntool. To deliver a file to the user, save it under\n/mnt/session/outputs — files there are collected and delivered\nautomatically.",
    );
  });

  test("injects bounded apply-patch instructions only for GPT-family agent requests", () => {
    expect(new TextEncoder().encode(ApplyPatchInstructionsText).byteLength).toBeLessThan(MaxTextBytes);
    expect(ApplyPatchInstructionsText).toContain(
      "Absolute paths are accepted under the declared writable roots — the workspace, /mnt/session/uploads, and /mnt/session/outputs — and rejected outside them.",
    );
    for (const [family, requestKind, shouldInject] of [
      ["gpt", ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST, true],
      ["claude", ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST, false],
      [undefined, ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST, false],
      ["gpt", ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER, false],
      ["gpt", ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY, false],
    ] as const) {
      const input = providerInput([], requestKind);
      const result = assembleProviderCallRequest({
        ...input,
        runtime: { ...input.runtime, ...(family === undefined ? {} : { toolsetFamily: family }) },
      });
      expect(result.ok).toBe(true);
      if (!result.ok) {
        continue;
      }
      const base = result.system.find((segment) => segment.kind === SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE)?.text ?? "";
      expect(base.includes(ApplyPatchInstructionsText)).toBe(shouldInject);
    }
  });

  test("rejects a GPT base prompt whose apply-patch injection exceeds the segment cap", () => {
    const input = providerInput([]);
    expect(assembleProviderCallRequest({
      ...input,
      runtime: {
        ...input.runtime,
        toolsetFamily: "gpt",
        systemInstructions: "x".repeat(MaxTextBytes),
      },
    })).toMatchObject({ ok: false, error: { reason: "bounded" } });
  });

  test("adds the approval policy as a dedicated stable segment only to reviewer requests", () => {
    const policy = "Review solely under this fixed approval policy.";
    const reviewer = assembleProviderCallRequest(providerInput(
      [],
      ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
      policy,
    ));
    expect(reviewer.ok).toBe(true);
    if (!reviewer.ok) {
      return;
    }
    expect(reviewer.system.filter(
      (segment) => segment.kind === SystemSegmentKind.SYSTEM_SEGMENT_KIND_APPROVAL_REVIEWER_POLICY,
    )).toEqual([{
      kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_APPROVAL_REVIEWER_POLICY,
      text: policy,
      cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE,
    }]);
    expect(reviewer.system.find(
      (segment) => segment.kind === SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE,
    )?.text).not.toContain(policy);

    for (const requestKind of [
      ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
      ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY,
      ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER_COMPACTION,
    ]) {
      const nonReviewer = assembleProviderCallRequest(providerInput([], requestKind, policy));
      expect(nonReviewer.ok).toBe(true);
      if (nonReviewer.ok) {
        expect(nonReviewer.system.map((segment) => segment.kind)).not.toContain(
          SystemSegmentKind.SYSTEM_SEGMENT_KIND_APPROVAL_REVIEWER_POLICY,
        );
        expect(nonReviewer.system.map((segment) => segment.text)).not.toContain(policy);
      }
    }
  });

  test("rejects reviewer requests without a non-empty dedicated approval policy", () => {
    const withPolicy = providerInput([], ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER);
    const { approvalReviewerPolicy: _removedPolicy, ...runtimeWithoutPolicy } = withPolicy.runtime;
    for (const input of [
      { ...withPolicy, runtime: runtimeWithoutPolicy },
      { ...withPolicy, runtime: { ...withPolicy.runtime, approvalReviewerPolicy: "   " } },
    ]) {
      const result = assembleProviderCallRequest(input);
      expect(result).toMatchObject({
        ok: false,
        error: { reason: "runtime_contract_validation" },
      });
    }
  });

  test("assembles reviewer compaction without system segments, tools, or output schema", () => {
    const input = providerInput([], ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER_COMPACTION);
    const result = assembleProviderCallRequest(input);
    expect(result.ok).toBe(true);
    if (!result.ok) {
      return;
    }
    expect(result.request).toMatchObject({
      requestKind: ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER_COMPACTION,
      system: [],
      tools: [],
    });
    expect(result.request.outputSchemaJson).toBeUndefined();
    expect(assembleProviderCallRequest({
      ...input,
      runtime: { ...input.runtime, outputSchemaJson: '{"type":"object"}' },
    })).toMatchObject({ ok: false, error: { reason: "runtime_contract_validation" } });
  });

  test("requires the Runtime-configured provider stream timeout", () => {
    const input = providerInput([]);
    const { timeoutMs: _timeoutMs, ...runtimeWithoutTimeout } = input.runtime;
    expect(assembleProviderCallRequest({ ...input, runtime: runtimeWithoutTimeout })).toMatchObject({
      ok: false,
      error: { reason: "bounded" },
    });
  });

  test("applies and notes per-entry and uniform description truncation", () => {
    const text = renderSkillGuidanceSegment([
      skillEntry("sk_alpha", "1.0.0", "Alpha", "alpha", "a".repeat(5_000)),
      skillEntry("sk_beta", "1.0.0", "Beta", "beta", "b".repeat(5_000)),
      skillEntry("sk_gamma", "1.0.0", "Gamma", "gamma", "界".repeat(5_000)),
    ], 1_024);

    expect(text).toContain("per-entry description cap applied");
    expect(text).toContain("uniform description shortening applied");
    const descriptionBytes = text
      .split("\n")
      .filter((line) => line.startsWith("{"))
      .map((line) => JSON.parse(line) as { readonly description: string })
      .reduce((total, entry) => total + new TextEncoder().encode(entry.description).byteLength, 0);
    expect(descriptionBytes).toBeLessThanOrEqual(1_024);
    expect(new TextEncoder().encode(text).byteLength).toBeLessThan(MaxTextBytes);
  });

  test("omits entries from the deterministic tail and notes the omission", () => {
    const text = renderSkillGuidanceSegment([
      skillEntry("sk_alpha", "1.0.0", "a".repeat(30_000), "alpha", ""),
      skillEntry("sk_beta", "1.0.0", "b".repeat(30_000), "beta", ""),
      skillEntry("sk_zeta", "1.0.0", "z".repeat(30_000), "zeta", ""),
    ], 1_024);

    expect(text).toContain('"skill_md_path":"/skills/alpha/SKILL.md"');
    expect(text).not.toContain('"skill_md_path":"/skills/zeta/SKILL.md"');
    expect(text).toContain("end-of-order skill omission applied");
    expect(new TextEncoder().encode(text).byteLength).toBeLessThan(MaxTextBytes);
  });
});

function providerInput(
  skillsIndex: readonly SkillGuidanceIndexEntry[],
  requestKind = ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
  approvalReviewerPolicy = requestKind === ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER
    ? "Fixed approval reviewer policy."
    : undefined,
): ProviderCallAssemblyInput {
  return {
    identity: {
      workspaceId: "workspace_1",
      sessionId: "sesn_1",
      sessionThreadId: "thread_1",
      parentThreadId: "",
      bindingId: "binding_1",
      bindingGeneration: 1,
      targetPodUid: "pod_1",
      runtimeBindingToken: "runtime-token",
    },
    requestId: "provider_request_1",
    modelRequestId: "model_request_1",
    currentModel: { providerId: "openai", modelId: "gpt-5.5" },
    runtimeMessages: [{
      id: "message_1",
      role: RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_USER,
      status: "completed",
      origin: "user",
      parts: [{ id: "part_1", text: { text: "hello" } }],
    }],
    runtime: {
      systemInstructions: "You are Tetral Agent.",
      timeoutMs: 1_800_000,
      requestKind,
      skillsIndex,
      skillGuidanceDescriptionBudgetBytes: 32 * 1_024,
      ...(approvalReviewerPolicy !== undefined ? { approvalReviewerPolicy } : {}),
      ...(requestKind === ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER
        ? { outputSchemaJson: '{"type":"object"}' }
        : {}),
    },
  };
}

function skillEntry(
  skillId: string,
  version: string,
  name: string,
  directory: string,
  description: string,
): SkillGuidanceIndexEntry {
  return {
    skillId,
    skillVersionId: `skv_${skillId}_${version}`,
    version,
    name,
    description,
    directory,
  };
}
