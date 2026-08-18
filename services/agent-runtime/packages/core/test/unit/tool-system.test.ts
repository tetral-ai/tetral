import { describe, expect, test } from "bun:test";
import { Deferred, Effect, Fiber, Scope } from "effect";
import { BuiltinToolNames, createApprovalReviewerToolCatalog, createToolCatalog, effectivePermissionPolicy, lookupToolEntry, providerToolDefinitions } from "../../src/tools/tool-catalog.js";
import type { ToolCatalogOptions, ToolRoute } from "../../src/tools/tool-catalog.js";
import { BuiltinToolCopy } from "../../src/tools/tool-copy.js";
import { evaluateToolGate } from "../../src/tools/tool-gate.js";
import { SessionToolCoordinator, ToolScheduler, inferBuiltinRunPolicy, inferToolRunPolicy } from "../../src/tools/tool-scheduler.js";
import type { ToolJob } from "../../src/tools/tool-scheduler.js";
import { registerRuntimeToolCall } from "../../src/thread-loop/tool-execution.js";
import { ApplyPatchLarkGrammar } from "../../src/tools/apply-patch-grammar.js";

const route: ToolRoute = { kind: "sandbox", operation: "RunTool", helperSubcommand: "write" };

function withoutDescriptions(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(withoutDescriptions);
  }
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(Object.entries(value)
      .filter(([key]) => key !== "description")
      .map(([key, item]) => [key, withoutDescriptions(item)]));
  }
  return value;
}

function job(input: {
  readonly id: string;
  readonly modelOrder: number;
  readonly name: string;
  readonly value: unknown;
}): ToolJob {
  return {
    id: input.id,
    modelOrder: input.modelOrder,
    modelToolCallId: `model-${input.id}`,
    kind: "builtin",
    name: input.name,
    route,
    input: input.value as ToolJob["input"],
    runPolicy: inferBuiltinRunPolicy(input.name, input.value as ToolJob["input"]),
    gateState: "runnable",
  };
}

describe("Tool system contracts", () => {
  test("explicit single-family catalogs keep every family-derived surface closed", () => {
    const families = [
      {
        family: "claude" as const,
        names: ["Bash", "Read", "Write", "Edit", "Glob", "Grep"],
        forbidden: ["exec_command", "write_stdin", "view_image", "apply_patch"],
      },
      {
        family: "gpt" as const,
        names: ["exec_command", "write_stdin", "view_image", "apply_patch"],
        forbidden: ["Bash", "Read", "Write", "Edit", "Glob", "Grep"],
      },
    ];

    for (const expected of families) {
      const catalog = createToolCatalog({ family: expected.family, includeSubAgentTools: false });
      const allFamilyNames = [...expected.names, ...expected.forbidden];
      const familyEntries = catalog.entries.filter((entry) => allFamilyNames.includes(entry.name));
      expect(familyEntries.map((entry) => entry.name)).toEqual(expected.names);
      expect(providerToolDefinitions(catalog).filter((definition) =>
        allFamilyNames.includes(definition.name),
      ).map((definition) => definition.name)).toEqual(expected.names);
      for (const entry of familyEntries) {
        expect(entry.definition.name).toBe(entry.name);
        expect(entry.route).toBeDefined();
        expect(entry.formatter).toBeDefined();
        expect(effectivePermissionPolicy(entry, catalog.configs)).toBeDefined();
      }
      for (const name of expected.forbidden) {
        expect(lookupToolEntry(catalog, name)).toBeUndefined();
      }
      expect(catalog.entries.map((entry) => entry.name)).toContain("memory");
    }
  });

  test("family-less catalog construction fails closed", () => {
    expect(() => createToolCatalog({} as ToolCatalogOptions)).toThrow("installed builtin family is required");
  });

  test("Claude Bash and GPT exec_command schemas stay exact and disjoint", () => {
    const claude = createToolCatalog({ family: "claude", includeSubAgentTools: false });
    const bash = providerToolDefinitions(claude).find((definition) => definition.name === "Bash");
    expect(withoutDescriptions(bash?.inputSchema)).toEqual({
      type: "object",
      additionalProperties: false,
      properties: {
        command: { type: "string" },
        cwd: { type: "string" },
        timeout: { type: "integer", maximum: 600000 },
        run_in_background: { type: "boolean" },
      },
      required: ["command"],
    });
    const serializedBash = JSON.stringify(bash?.inputSchema);
    for (const alias of ["timeout_ms", "cmd", "workdir", "yield_time_ms"]) {
      expect(serializedBash).not.toContain(`\"${alias}\"`);
    }

    const gpt = createToolCatalog({ family: "gpt", includeSubAgentTools: false });
    const execCommand = providerToolDefinitions(gpt).find((definition) => definition.name === "exec_command");
    expect(withoutDescriptions(execCommand?.inputSchema)).toEqual({
      type: "object",
      additionalProperties: false,
      properties: {
        cmd: { type: "string" },
        workdir: { type: "string" },
        yield_time_ms: { type: "integer" },
        max_output_tokens: { type: "integer" },
      },
      required: ["cmd"],
    });
    const serializedExec = JSON.stringify(execCommand?.inputSchema);
    for (const claudeOnly of ["command", "timeout", "run_in_background"]) {
      expect(serializedExec).not.toContain(`\"${claudeOnly}\"`);
    }
  });

  test("view_image and web expose only their executable input fields", () => {
    const catalog = createToolCatalog({ family: "gpt", includeSubAgentTools: false });
    const definitions = new Map(providerToolDefinitions(catalog).map((definition) => [definition.name, definition]));

    expect(withoutDescriptions(definitions.get("view_image")?.inputSchema)).toEqual({
      type: "object",
      additionalProperties: false,
      properties: { path: { type: "string" } },
      required: ["path"],
    });
    expect(withoutDescriptions(definitions.get("web")?.inputSchema)).toEqual({
      type: "object",
      additionalProperties: false,
      properties: {
        search_query: {
          type: "array",
          items: {
            type: "object",
            additionalProperties: false,
            properties: {
              q: { type: "string" },
              domains: { type: "array", items: { type: "string" }, maxItems: 4 },
            },
            required: ["q"],
          },
        },
        open: {
          type: "array",
          items: {
            type: "object",
            additionalProperties: false,
            properties: {
              url: { type: "string" },
              ref_id: { type: "string" },
              lineno: { type: "integer", minimum: 1 },
            },
            oneOf: [{ required: ["url"] }, { required: ["ref_id"] }],
          },
        },
        find: {
          type: "array",
          items: {
            type: "object",
            additionalProperties: false,
            properties: {
              ref_id: { type: "string" },
              pattern: { type: "string" },
            },
            required: ["ref_id", "pattern"],
          },
        },
      },
    });
  });

  test("builtin tool copy covers exactly the documented 19-entry census", () => {
    const expectedNames = [
      "Bash", "Read", "Write", "Edit", "Grep", "Glob",
      "exec_command", "write_stdin", "view_image", "apply_patch",
      "web", "memory", "spawn_agent", "send_message", "wait_agent",
      "interrupt_agent", "close_agent", "resume_agent", "list_agents",
    ];
    expect(BuiltinToolNames).toHaveLength(19);
    expect([...BuiltinToolNames].toSorted()).toEqual(expectedNames.toSorted());
    expect(Object.keys(BuiltinToolCopy).toSorted()).toEqual(expectedNames.toSorted());

    const definitions = new Map([
      ...providerToolDefinitions(createToolCatalog({ family: "claude" })),
      ...providerToolDefinitions(createToolCatalog({ family: "gpt" })),
    ].map((definition) => [definition.name, definition]));
    for (const [name, copy] of Object.entries(BuiltinToolCopy)) {
      const definition = definitions.get(name);
      expect(definition?.description).toBe(copy.description);
      if (name === "apply_patch") {
        expect(definition).toMatchObject({ kind: "freeform", name: "apply_patch" });
        expect(definition?.larkGrammar).toContain("start: begin_patch hunk+ end_patch");
      }
      const schema = definition?.inputSchema as {
        readonly description?: string;
        readonly properties?: Readonly<Record<string, { readonly description?: string }>>;
      };
      if ("parameters" in copy) {
        for (const [label, description] of Object.entries(copy.parameters)) {
          for (const parameter of label.replace(/ \(required\)$/, "").split(" / ")) {
            expect(schema.properties?.[parameter]?.description).toBe(description);
          }
        }
      }
    }
    expect((definitions.get("web")?.inputSchema as { readonly description?: string }).description)
      .toBe("At least one item across the three arrays is required; the three combined are capped at 8 items per call.");
    expect((definitions.get("list_agents")?.inputSchema as { readonly description?: string }).description)
      .toBe("None. This tool takes no input.");
  });

  test("MCP collision filtering uses only the explicit family plus platform tools", () => {
    const manifests = [{
      mcpServerName: "server",
      manifestETag: "etag",
      manifestGeneration: 1,
      tools: [
        { name: "Read", description: "GPT may install this MCP name.", inputSchema: { type: "object" } },
        { name: "exec_command", description: "Claude may install this MCP name.", inputSchema: { type: "object" } },
        { name: "memory", description: "Platform collision.", inputSchema: { type: "object" } },
      ],
    }];
    const claude = createToolCatalog({ family: "claude", includeSubAgentTools: false, mcpManifests: manifests });
    const gpt = createToolCatalog({ family: "gpt", includeSubAgentTools: false, mcpManifests: manifests });

    expect(lookupToolEntry(claude, "Read")?.route).toMatchObject({ kind: "sandbox" });
    expect(lookupToolEntry(claude, "exec_command")?.route).toMatchObject({ kind: "gateway", operation: "RunMcpTool" });
    expect(lookupToolEntry(gpt, "Read")?.route).toMatchObject({ kind: "gateway", operation: "RunMcpTool" });
    expect(lookupToolEntry(gpt, "exec_command")?.route).toMatchObject({ kind: "sandbox" });
    expect(claude.entries.filter((entry) => entry.name === "memory")).toHaveLength(1);
    expect(gpt.entries.filter((entry) => entry.name === "memory")).toHaveLength(1);

    const gptWithDisabledExec = createToolCatalog({
      family: "gpt",
      includeSubAgentTools: false,
      configs: [{ name: "exec_command", enabled: false }],
      mcpManifests: manifests,
    });
    expect(lookupToolEntry(gptWithDisabledExec, "exec_command")).toBeUndefined();
  });

  test("provider projection preserves third-party schema content while excluding execution fields structurally", () => {
    const catalog = createToolCatalog({
      family: "claude",
      configs: [
        { name: "Read", enabled: false },
        { name: "web", enabled: true, permissionPolicy: "always_allow" },
      ],
      includeSubAgentTools: false,
      mcpManifests: [{
        mcpServerName: "content-canary",
        manifestETag: "etag-1",
        manifestGeneration: 1,
        tools: [{
          name: "content_canary",
          description: "Describe operation and credentials without becoming execution metadata.",
          inputSchema: {
            type: "object",
            properties: {
              route: { const: "binding" },
              binding: { const: "credentials" },
            },
          },
        }],
      }],
    });

    expect(lookupToolEntry(catalog, "Read")).toBeUndefined();
    expect(lookupToolEntry(catalog, "web")).toMatchObject({
      route: { kind: "gateway", operation: "RunWeb" },
      defaultPermissionPolicy: "always_ask",
    });
    expect(lookupToolEntry(catalog, "memory")).toMatchObject({
      route: { kind: "bridge", operation: "RunMemory" },
      required: true,
    });

    const definitions = providerToolDefinitions(catalog);
    expect(definitions.some((definition) => definition.name === "Read")).toBe(false);
    expect(definitions.some((definition) => definition.name === "memory")).toBe(true);
    const projectedMCP = definitions.find((definition) => definition.name === "content_canary");
    expect(projectedMCP).toEqual({
      kind: "function",
      name: "content_canary",
      description: "Describe operation and credentials without becoming execution metadata.",
      inputSchema: {
        type: "object",
        properties: {
          route: { const: "binding" },
          binding: { const: "credentials" },
        },
      },
    });
    expect(Object.keys(projectedMCP ?? {}).toSorted()).toEqual(["description", "inputSchema", "kind", "name"]);
    expect(lookupToolEntry(catalog, "content_canary")).toMatchObject({
      route: { kind: "gateway", operation: "RunMcpTool", mcpServerName: "content-canary" },
      defaultPermissionPolicy: "always_ask",
      required: false,
    });

    expect(() =>
      createToolCatalog({
        family: "claude",
        configs: [{ name: "memory", enabled: false }],
        includeSubAgentTools: false,
      }),
    ).toThrow("required tool memory cannot be disabled");
  });

  test("GPT declares required apply_patch as the sole freeform family tool", () => {
    const gpt = createToolCatalog({ family: "gpt", includeSubAgentTools: false });
    const familyNames = new Set(["exec_command", "write_stdin", "view_image", "apply_patch"]);
    const familyDefinitions = providerToolDefinitions(gpt).filter((definition) => familyNames.has(definition.name));

    expect(familyDefinitions.map((definition) => [definition.name, definition.kind])).toEqual([
      ["exec_command", "function"],
      ["write_stdin", "function"],
      ["view_image", "function"],
      ["apply_patch", "freeform"],
    ]);
    const patch = lookupToolEntry(gpt, "apply_patch");
    expect(patch).toMatchObject({
      required: true,
      inputContract: { kind: "freeform_string", executionField: "patch" },
    });
    expect(patch?.definition.larkGrammar).toContain('filename: /[^\\r\\n]*[^\\s\\r\\n][^\\r\\n]*/');
    expect(() => createToolCatalog({ family: "gpt", configs: [{ name: "apply_patch", enabled: false }] }))
      .toThrow("required tool apply_patch cannot be disabled");
    expect(() => createToolCatalog({ family: "claude", configs: [{ name: "apply_patch", enabled: false }] }))
      .not.toThrow();
    expect(lookupToolEntry(createToolCatalog({ family: "claude" }), "apply_patch")).toBeUndefined();
    expect(gpt.entries.length).toBeGreaterThan(familyDefinitions.length);
  });

  test("apply_patch grammar admits the helper-safe envelope and rejects incomplete hunks", () => {
    expect(ApplyPatchLarkGrammar).toBe(String.raw`start: begin_patch hunk+ end_patch
begin_patch: "*** Begin Patch" LF
end_patch: "*** End Patch" LF?

hunk: add_hunk | delete_hunk | update_hunk
add_hunk: "*** Add File: " filename LF add_line+
delete_hunk: "*** Delete File: " filename LF
update_hunk: "*** Update File: " filename LF change_move? change

filename: /[^\r\n]*[^\s\r\n][^\r\n]*/
add_line: "+" /(.*)/ LF -> line

change_move: "*** Move to: " filename LF
change: change_context* change_line (change_context | change_line)* eof_line?
change_context: ("@@" | "@@ " /(.+)/) LF
change_line: ("+" | "-" | " ") /(.*)/ LF
eof_line: "*** End of File" LF

%import common.LF`);

    const cases = [
      { name: "add", accepted: true, patch: "*** Begin Patch\n*** Add File: notes.txt\n+hello\n*** End Patch\n" },
      { name: "delete", accepted: true, patch: "*** Begin Patch\n*** Delete File: notes.txt\n*** End Patch" },
      { name: "update", accepted: true, patch: "*** Begin Patch\n*** Update File: notes.txt\n@@\n-old\n+new\n*** End Patch\n" },
      { name: "move with change", accepted: true, patch: "*** Begin Patch\n*** Update File: old.txt\n*** Move to: new.txt\n@@ section\n-old\n+new\n*** End Patch" },
      { name: "explicit eof", accepted: true, patch: "*** Begin Patch\n*** Update File: notes.txt\n@@\n line\n*** End of File\n*** End Patch" },
      { name: "blank path", accepted: false, patch: "*** Begin Patch\n*** Add File:   \n+hello\n*** End Patch" },
      { name: "add without body", accepted: false, patch: "*** Begin Patch\n*** Add File: notes.txt\n*** End Patch" },
      { name: "empty update", accepted: false, patch: "*** Begin Patch\n*** Update File: notes.txt\n*** End Patch" },
      { name: "move only", accepted: false, patch: "*** Begin Patch\n*** Update File: old.txt\n*** Move to: new.txt\n*** End Patch" },
      { name: "context marker only", accepted: false, patch: "*** Begin Patch\n*** Update File: notes.txt\n@@\n*** End Patch" },
    ] as const;

    for (const scenario of cases) {
      expect(helperSafePatchGrammarAccepts(scenario.patch), scenario.name).toBe(scenario.accepted);
    }
  });

  test("Runtime registration converts freeform patch input exactly once", () => {
    const toolScheduler = new ToolScheduler();
    const state = {
      executionPolicy: { toolCatalog: createToolCatalog({ family: "gpt" }) },
      toolScheduler,
      toolEntries: {},
      nextToolModelOrder: 0,
    };
    const patch = "*** Begin Patch\n*** Add File: note.txt\n+hello\n*** End Patch\n";

    expect(registerRuntimeToolCall("mreq_patch", state, {
      type: "tool-call",
      id: "call_patch",
      toolName: "apply_patch",
      input: patch,
      inputPreview: { preview: patch, truncated: false },
    })).toEqual({ type: "registered", jobId: "mreq_patch:call_patch" });
    expect(toolScheduler.jobs()[0]?.input).toEqual({ patch });

    expect(registerRuntimeToolCall("mreq_patch", state, {
      type: "tool-call",
      id: "call_patch_object",
      toolName: "apply_patch",
      input: { patch },
      inputPreview: { preview: "object", truncated: false },
    })).toEqual({ type: "invalid" });
    expect(toolScheduler.jobs()).toHaveLength(1);
  });

  test("SubAgent provider tool schemas expose the durable snake_case contract", () => {
    const catalog = createToolCatalog({ family: "claude", configs: [], includeSubAgentTools: true });
    const definitions = providerToolDefinitions(catalog);
    const names = [
      "spawn_agent",
      "send_message",
      "wait_agent",
      "interrupt_agent",
      "close_agent",
      "resume_agent",
      "list_agents",
    ];
    const byName = new Map(definitions.map((definition) => [definition.name, definition]));
    const schemaFor = (name: string) => byName.get(name)?.inputSchema as {
      readonly additionalProperties?: unknown;
      readonly properties?: Record<string, unknown>;
      readonly required?: readonly string[];
    };

    for (const name of names) {
      expect(byName.has(name)).toBe(true);
      expect(schemaFor(name).additionalProperties).toBe(false);
    }

    const spawn = schemaFor("spawn_agent");
    expect(spawn.required).toEqual(["task_name", "prompt"]);
    expect(spawn.properties?.task_name).toMatchObject({ type: "string", minLength: 1, maxLength: 128 });
    expect(spawn.properties?.prompt).toMatchObject({ type: "string", minLength: 1 });
    expect(withoutDescriptions(spawn.properties?.agent_type)).toEqual({ enum: ["general", "research", "worker"] });
    expect(withoutDescriptions(spawn.properties?.fork_turns)).toEqual({
      anyOf: [{ enum: ["none", "all"] }, { type: "string", pattern: "^(?:[1-9][0-9]{0,2}|1000)$" }],
    });

    const send = schemaFor("send_message");
    expect(send.required).toEqual(["task_name", "message"]);
    expect(send.properties?.task_name).toMatchObject({ type: "string", minLength: 1, maxLength: 128 });
    expect(send.properties?.message).toMatchObject({ type: "string", minLength: 1 });

    const wait = schemaFor("wait_agent");
    expect(wait.required).toEqual(["task_name"]);
    expect(wait.properties?.task_name).toMatchObject({ type: "string", minLength: 1, maxLength: 128 });
    expect(withoutDescriptions(wait.properties?.timeout_ms)).toEqual({ type: "integer", minimum: 0 });

    for (const name of ["interrupt_agent", "close_agent", "resume_agent"]) {
      const schema = schemaFor(name);
      expect(schema.required).toEqual(["task_name"]);
      expect(withoutDescriptions(schema.properties)).toEqual({ task_name: { type: "string", minLength: 1, maxLength: 128 } });
    }

    const list = schemaFor("list_agents");
    expect(list.properties).toEqual({});
    expect(list.required).toBeUndefined();

    const serialized = JSON.stringify(names.map((name) => byName.get(name)?.inputSchema));
    for (const legacyField of ["taskName", "agentType", "forkTurns", "timeoutMs", "session_thread_id"]) {
      expect(serialized).not.toContain(legacyField);
    }
  });

  test("approval reviewer provider catalog is read-only and approval-free", () => {
    const catalog = createApprovalReviewerToolCatalog();
    const names = providerToolDefinitions(catalog).map((definition) => definition.name);

    expect(names).toEqual(["Read", "Grep", "Glob"]);
    for (const name of names) {
      const entry = lookupToolEntry(catalog, name);
      expect(entry !== undefined ? effectivePermissionPolicy(entry, catalog.configs) : undefined).toBe("always_allow");
    }
    for (const forbidden of ["Bash", "exec_command", "write_stdin", "Write", "Edit", "apply_patch", "memory", "spawn_agent"]) {
      expect(lookupToolEntry(catalog, forbidden)).toBeUndefined();
    }
  });

  test("built-in tools use the contract default permission policies", () => {
    const catalog = createToolCatalog({ family: "claude" });
    const expected = new Map<string, "always_allow" | "always_ask">([
      ["Read", "always_allow"],
      ["Grep", "always_allow"],
      ["Glob", "always_allow"],
      ["Bash", "always_ask"],
      ["Write", "always_ask"],
      ["Edit", "always_ask"],
      ["web", "always_ask"],
      ["memory", "always_ask"],
      ["spawn_agent", "always_ask"],
      ["send_message", "always_ask"],
      ["wait_agent", "always_ask"],
      ["interrupt_agent", "always_ask"],
      ["close_agent", "always_ask"],
      ["resume_agent", "always_ask"],
      ["list_agents", "always_ask"],
    ]);

    expect(catalog.entries.map((entry) => entry.name).toSorted()).toEqual([...expected.keys()].toSorted());
    for (const entry of catalog.entries) {
      const permissionPolicy = expected.get(entry.name);
      if (permissionPolicy === undefined) {
        throw new Error(`missing expected permission policy for ${entry.name}`);
      }
      expect(effectivePermissionPolicy(entry, catalog.configs)).toBe(permissionPolicy);
    }

    const mcpCatalog = createToolCatalog({
      family: "claude",
      mcpManifests: [{
        mcpServerName: "github",
        manifestETag: "etag_1",
        manifestGeneration: 1,
        tools: [{ name: "github_get_issue", description: "Get an issue.", inputSchema: { type: "object" } }],
      }],
    });
    const mcpTool = lookupToolEntry(mcpCatalog, "github_get_issue");
    expect(mcpTool !== undefined ? effectivePermissionPolicy(mcpTool, mcpCatalog.configs) : undefined).toBe("always_ask");
  });

  test("Grep provider schema exposes the helper contract controls", () => {
    const catalog = createToolCatalog({ family: "claude", configs: [], includeSubAgentTools: false });
    const grep = providerToolDefinitions(catalog).find((definition) => definition.name === "Grep");
    const schema = grep?.inputSchema as {
      readonly additionalProperties?: unknown;
      readonly properties?: Record<string, unknown>;
      readonly required?: readonly string[];
    };

    expect(schema.additionalProperties).toBe(false);
    expect(schema.required).toEqual(["pattern"]);
    expect(withoutDescriptions(schema.properties)).toEqual({
      pattern: { type: "string" },
      path: { type: ["string", "null"] },
      globs: { type: "array", items: { type: "string" } },
      file_type: { type: ["string", "null"] },
      mode: { enum: ["files", "content", "count"] },
      case_insensitive: { type: "boolean" },
      line_numbers: { type: "boolean" },
      before_context: { type: "integer", minimum: 0 },
      after_context: { type: "integer", minimum: 0 },
      context: { type: "integer", minimum: 0 },
      multiline: { type: "boolean" },
      head_limit: { type: "integer", minimum: 1, maximum: 1000 },
      offset: { type: "integer", minimum: 0 },
    });
  });

  test("ToolGate keeps availability separate from approval mode", () => {
    const catalog = createToolCatalog({
      family: "claude",
      configs: [
        { name: "web", enabled: true, permissionPolicy: "always_allow" },
        { name: "Write", enabled: true, permissionPolicy: "always_ask" },
      ],
      includeSubAgentTools: false,
    });

    expect(evaluateToolGate({ catalog, toolName: "unknown", approvalMode: "full_access" })).toEqual({
      type: "invalid",
      reason: "unknown_or_disabled",
      publicEvent: false,
    });
    expect(evaluateToolGate({ catalog, toolName: "Read", approvalMode: "full_access" })).toEqual({
      type: "run",
      approval: "skipped",
      evaluatedPermission: "allow",
      approvalSource: "config",
    });
    expect(evaluateToolGate({ catalog, toolName: "Write", approvalMode: "ask_for_approval" })).toEqual({
      type: "ask",
      evaluatedPermission: "ask",
      approvalSource: "user",
    });
    expect(evaluateToolGate({ catalog, toolName: "web", approvalMode: "approve_for_me" })).toEqual({
      type: "run",
      approval: "allowed",
      evaluatedPermission: "allow",
      approvalSource: "config",
    });
    expect(evaluateToolGate({ catalog, toolName: "Write", approvalMode: "approve_for_me" })).toEqual({
      type: "review_required",
      evaluatedPermission: "ask",
      approvalSource: "auto_reviewer",
    });
    expect(evaluateToolGate({
      catalog,
      toolName: "Write",
      approvalMode: "approve_for_me",
      reviewerOutcome: { type: "decision", riskLevel: "high", userAuthorization: "unknown", outcome: "deny", message: "too risky" },
    })).toEqual({
      type: "deny",
      evaluatedPermission: "deny",
      approvalSource: "auto_reviewer",
      message: "too risky",
    });
  });

  test("ToolScheduler serializes same-target writers and lets different targets run in parallel", () => {
    const same = new ToolScheduler();
    same.addJob(job({ id: "a", modelOrder: 0, name: "Write", value: { file_path: "src/a.ts" } }));
    same.addJob(job({ id: "b", modelOrder: 1, name: "Edit", value: { file_path: "/workspace/src/a.ts" } }));
    expect(same.startReady().map((item) => item.id)).toEqual(["a"]);
    same.finishJob("a");
    expect(same.startReady().map((item) => item.id)).toEqual(["b"]);

    const different = new ToolScheduler();
    different.addJob(job({ id: "a", modelOrder: 0, name: "Write", value: { file_path: "src/a.ts" } }));
    different.addJob(job({ id: "b", modelOrder: 1, name: "Edit", value: { file_path: "src/b.ts" } }));
    expect(different.startReady().map((item) => item.id)).toEqual(["a", "b"]);
  });

  test("MCP gateway tool routes stay parallel-safe regardless of tool name", () => {
    const mcpRoute: ToolRoute = { kind: "gateway", operation: "RunMcpTool", mcpServerName: "github" };

    expect(inferToolRunPolicy({ name: "github_create_issue", route: mcpRoute }, { title: "one" })).toEqual({
      mode: "parallel_safe",
      conflictKeys: null,
    });
  });

  test("ToolScheduler applies stable runPolicy keys for commands, memory, and subagents", () => {
    const stdin = new ToolScheduler();
    stdin.addJob(job({ id: "stdin-1", modelOrder: 0, name: "write_stdin", value: { session_id: "task_1" } }));
    stdin.addJob(job({ id: "stdin-2", modelOrder: 1, name: "write_stdin", value: { session_id: "task_1" } }));
    stdin.addJob(job({ id: "stdin-3", modelOrder: 2, name: "write_stdin", value: { session_id: "task_2" } }));
    expect(stdin.startReady().map((item) => item.id)).toEqual(["stdin-1", "stdin-3"]);

    const memory = new ToolScheduler();
    memory.addJob(job({ id: "memory", modelOrder: 0, name: "memory", value: { action: "create", path: "x", content: "y" } }));
    memory.addJob(job({ id: "exec", modelOrder: 1, name: "exec_command", value: { cmd: "pwd" } }));
    expect(memory.startReady().map((item) => item.id)).toEqual(["memory"]);
    memory.finishJob("memory");
    expect(memory.startReady().map((item) => item.id)).toEqual(["exec"]);

    const subagents = new ToolScheduler();
    subagents.addJob(job({ id: "spawn-a", modelOrder: 0, name: "spawn_agent", value: { task_name: "a" } }));
    subagents.addJob(job({ id: "spawn-b", modelOrder: 1, name: "spawn_agent", value: { task_name: "b" } }));
    subagents.addJob(job({ id: "close-a", modelOrder: 2, name: "close_agent", value: { task_name: "child_a" } }));
    subagents.addJob(job({ id: "resume-a", modelOrder: 3, name: "resume_agent", value: { task_name: "child_a" } }));
    expect(subagents.startReady().map((item) => item.id)).toEqual(["spawn-a", "spawn-b", "close-a"]);
    subagents.finishJob("close-a");
    expect(subagents.startReady().map((item) => item.id)).toEqual(["resume-a"]);

    expect(inferBuiltinRunPolicy("write_stdin", { task_id: "legacy" })).toEqual({ mode: "exclusive", conflictKeys: null });
    expect(inferBuiltinRunPolicy("close_agent", { session_thread_id: "legacy" })).toEqual({ mode: "exclusive", conflictKeys: null });
  });

  test("ToolScheduler serializes apply_patch jobs that move different sources to the same target", () => {
    const scheduler = new ToolScheduler();
    scheduler.addJob(job({
      id: "patch-1",
      modelOrder: 0,
      name: "apply_patch",
      value: "*** Begin Patch\n*** Update File: src/old-a.ts\n*** Move to: src/shared.ts\n@@\n-old\n+new\n*** End Patch\n",
    }));
    scheduler.addJob(job({
      id: "patch-2",
      modelOrder: 1,
      name: "apply_patch",
      value: "*** Begin Patch\n*** Update File: src/old-b.ts\n*** Move to: /workspace/src/shared.ts\n@@\n-old\n+new\n*** End Patch\n",
    }));

    expect(scheduler.startReady().map((item) => item.id)).toEqual(["patch-1"]);
    scheduler.finishJob("patch-1");
    expect(scheduler.startReady().map((item) => item.id)).toEqual(["patch-2"]);
  });

  test("ToolScheduler starts an approved waiting job in a fresh tool fiber", () => {
    const scheduler = new ToolScheduler();
    scheduler.addJob(job({ id: "approval", modelOrder: 0, name: "Write", value: { file_path: "src/approved.ts" } }));

    expect(scheduler.startReady().map((item) => item.id)).toEqual(["approval"]);
    scheduler.waitForApproval("approval");
    expect(scheduler.startReady().map((item) => item.id)).toEqual([]);
    scheduler.resolveApproval("approval", "allow");

    expect(scheduler.startReady().map((item) => item.id)).toEqual(["approval"]);
  });

  test("SessionToolCoordinator enforces session-wide exclusion, keyed conflicts, aggregate limits, and session isolation", async () => {
    const events = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const scope = yield* Scope.Scope;
      const coordinator = new SessionToolCoordinator({ maxConcurrentTools: 2 });
      const otherSession = new SessionToolCoordinator({ maxConcurrentTools: 2 });
      const releaseMemory = yield* Deferred.make<void>();
      const releasePathA = yield* Deferred.make<void>();
      const releasePathB = yield* Deferred.make<void>();
      const releaseOtherSession = yield* Deferred.make<void>();
      const observed: string[] = [];
      const held = (label: string, release: Deferred.Deferred<void>) => Effect.sync(() => observed.push(label)).pipe(
        Effect.andThen(Deferred.await(release)),
      );

      const memoryFiber = yield* Effect.forkIn(
        coordinator.withPermit(inferBuiltinRunPolicy("memory", {}), held("memory", releaseMemory)),
        scope,
      );
      yield* Effect.yieldNow;
      const blockedParallel = yield* Effect.forkIn(
        coordinator.withPermit(inferBuiltinRunPolicy("Read", {}), Effect.sync(() => observed.push("after-memory"))),
        scope,
      );
      const isolatedFiber = yield* Effect.forkIn(
        otherSession.withPermit(inferBuiltinRunPolicy("Read", {}), held("other-session", releaseOtherSession)),
        scope,
      );
      yield* Effect.yieldNow;
      expect(observed).toEqual(["memory", "other-session"]);

      yield* Deferred.succeed(releaseMemory, undefined);
      yield* Fiber.join(memoryFiber);
      yield* Fiber.join(blockedParallel);
      expect(observed).toEqual(["memory", "other-session", "after-memory"]);

      const firstPath = yield* Effect.forkIn(
        coordinator.withPermit(inferBuiltinRunPolicy("Write", { file_path: "src/a.ts" }), held("path-a", releasePathA)),
        scope,
      );
      yield* Effect.yieldNow;
      const samePath = yield* Effect.forkIn(
        coordinator.withPermit(inferBuiltinRunPolicy("Edit", { file_path: "/workspace/src/a.ts" }), Effect.sync(() => observed.push("same-path"))),
        scope,
      );
      const differentPath = yield* Effect.forkIn(
        coordinator.withPermit(inferBuiltinRunPolicy("Write", { file_path: "src/b.ts" }), held("different-path", releasePathB)),
        scope,
      );
      const cappedParallel = yield* Effect.forkIn(
        coordinator.withPermit(inferBuiltinRunPolicy("Read", {}), Effect.sync(() => observed.push("after-cap"))),
        scope,
      );
      yield* Effect.yieldNow;
      expect(observed).toContain("different-path");
      expect(observed).not.toContain("same-path");
      expect(observed).not.toContain("after-cap");

      yield* Deferred.succeed(releasePathB, undefined);
      yield* Fiber.join(differentPath);
      yield* Fiber.join(cappedParallel);
      expect(observed).toContain("after-cap");
      expect(observed).not.toContain("same-path");

      yield* Deferred.succeed(releasePathA, undefined);
      yield* Fiber.join(firstPath);
      yield* Fiber.join(samePath);
      yield* Deferred.succeed(releaseOtherSession, undefined);
      yield* Fiber.join(isolatedFiber);
      return observed;
    })));

    expect(events.indexOf("same-path")).toBeGreaterThan(events.indexOf("path-a"));
    expect(events).toContain("after-cap");
  });

  test("SessionToolCoordinator releases an interrupted permit and removes interrupted waiters", async () => {
    const starts = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const scope = yield* Scope.Scope;
      const coordinator = new SessionToolCoordinator({ maxConcurrentTools: 1 });
      const held = yield* Deferred.make<void>();
      const observed: string[] = [];
      const active = yield* Effect.forkIn(
        coordinator.withPermit(inferBuiltinRunPolicy("Read", {}), Effect.sync(() => observed.push("active")).pipe(Effect.andThen(Deferred.await(held)))),
        scope,
      );
      yield* Effect.yieldNow;
      const cancelled = yield* Effect.forkIn(
        coordinator.withPermit(inferBuiltinRunPolicy("Read", {}), Effect.sync(() => observed.push("cancelled-waiter"))),
        scope,
      );
      yield* Fiber.interrupt(cancelled);
      yield* Fiber.interrupt(active);
      const replacement = yield* Effect.forkIn(
        coordinator.withPermit(inferBuiltinRunPolicy("Read", {}), Effect.sync(() => observed.push("replacement"))),
        scope,
      );
      yield* Fiber.join(replacement);
      return observed;
    })));

    expect(starts).toEqual(["active", "replacement"]);
  });
});

// Executes the fixed Lark productions as a line grammar. The helper parser's own
// table tests consume the same positive and negative envelope shapes at the execution boundary.
function helperSafePatchGrammarAccepts(patch: string): boolean {
  if (patch.includes("\r")) {
    return false;
  }
  const lines = patch.split("\n");
  if (lines.at(-1) === "") {
    lines.pop();
  }
  if (lines.shift() !== "*** Begin Patch" || lines.pop() !== "*** End Patch" || lines.length === 0) {
    return false;
  }
  const pathAfter = (line: string, prefix: string): boolean =>
    line.startsWith(prefix) && line.slice(prefix.length).trim().length > 0;
  const isHunkHeader = (line: string): boolean =>
    line.startsWith("*** Add File: ") || line.startsWith("*** Delete File: ") || line.startsWith("*** Update File: ");
  let index = 0;
  let hunks = 0;
  while (index < lines.length) {
    const header = lines[index] ?? "";
    if (pathAfter(header, "*** Add File: ")) {
      index += 1;
      let additions = 0;
      while (index < lines.length && !isHunkHeader(lines[index] ?? "")) {
        if (!(lines[index] ?? "").startsWith("+")) {
          return false;
        }
        additions += 1;
        index += 1;
      }
      if (additions === 0) {
        return false;
      }
    } else if (pathAfter(header, "*** Delete File: ")) {
      index += 1;
    } else if (pathAfter(header, "*** Update File: ")) {
      index += 1;
      if (index < lines.length && (lines[index] ?? "").startsWith("*** Move to: ")) {
        if (!pathAfter(lines[index] ?? "", "*** Move to: ")) {
          return false;
        }
        index += 1;
      }
      let changeLines = 0;
      while (index < lines.length && !isHunkHeader(lines[index] ?? "")) {
        const line = lines[index] ?? "";
        if (line === "*** End of File") {
          index += 1;
          if (index < lines.length && !isHunkHeader(lines[index] ?? "")) {
            return false;
          }
          break;
        }
        if (line === "@@" || (line.startsWith("@@ ") && line.length > 3)) {
          index += 1;
          continue;
        }
        if (line.startsWith("+") || line.startsWith("-") || line.startsWith(" ")) {
          changeLines += 1;
          index += 1;
          continue;
        }
        return false;
      }
      if (changeLines === 0) {
        return false;
      }
    } else {
      return false;
    }
    hunks += 1;
  }
  return hunks > 0;
}
