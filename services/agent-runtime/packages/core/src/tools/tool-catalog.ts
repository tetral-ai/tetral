/**
 * @packageDocumentation
 * Materializes the Runtime's enabled tool catalog from one installed builtin family, platform
 * tools, session policy, and ready MCP manifests. It guards family-exact builtin installation,
 * required-tool availability, route and formatter pairing, permission defaults, and the absence
 * of execution metadata from provider-visible definitions. Runtime Pod command handling builds
 * catalogs; provider-call assembly, ThreadLoop, ToolGate, and session hot state consume them. This
 * module calls the provider-copy accessors in tool-copy and otherwise performs no I/O or route
 * execution.
 */
import { builtinToolDescription, builtinToolParameterDescription } from "./tool-copy.js";
import type { DocumentedBuiltinToolName } from "./tool-copy.js";
import { ApplyPatchLarkGrammar } from "./apply-patch-grammar.js";

/** Approval policy attached to an enabled tool after catalog materialization. */
export type ToolPermissionPolicy = "always_allow" | "always_ask";

/** Provider-visible tool shape after Runtime-only execution fields are removed. */
export type ProviderToolDefinition =
  | {
      readonly kind: "function";
      readonly name: string;
      readonly description: string;
      readonly inputSchema: unknown;
      readonly outputSchema?: unknown;
      readonly larkGrammar?: never;
    }
  | {
      readonly kind: "freeform";
      readonly name: string;
      readonly description: string;
      readonly larkGrammar: string;
      readonly inputSchema?: never;
      readonly outputSchema?: never;
    };

/** Runtime-only normalization applied once when a completed provider Tool Call is registered. */
export type ToolInputContract =
  | { readonly kind: "json_object" }
  | { readonly kind: "freeform_string"; readonly executionField: "patch" };

/** Runtime-only dispatch route paired with each materialized provider definition. */
export type ToolRoute =
  | {
      readonly kind: "sandbox";
      readonly operation: "RunTool" | "CommandIO";
      readonly helperSubcommand: SandboxHelperSubcommand;
    }
  | {
      readonly kind: "gateway";
      readonly operation: "RunWeb";
    }
  | {
      readonly kind: "gateway";
      readonly operation: "RunMcpTool";
      readonly mcpServerName: string;
    }
  | {
      readonly kind: "bridge";
      readonly operation: "RunMemory";
    }
  | {
      readonly kind: "subagent";
      readonly operation:
        | "spawn_agent"
        | "send_message"
        | "wait_agent"
        | "interrupt_agent"
        | "close_agent"
        | "resume_agent"
        | "list_agents";
    };

/** Sandbox helper operation selected by a sandbox tool route. */
export type SandboxHelperSubcommand =
  | "exec"
  | "stdin"
  | "read"
  | "write"
  | "edit"
  | "grep"
  | "glob"
  | "view_image"
  | "apply_patch";

/** Model-visible result and bounding requirements paired with one tool route. */
export interface ToolFormatterContract {
  readonly successShape: string;
  readonly errorShape: string;
  readonly runningShape?: string;
  readonly truncationShape?: string;
  readonly continuationHint?: string;
  readonly forbiddenFields: readonly string[];
  readonly visibleCaps?: Readonly<Record<string, number>>;
}

/** Complete Runtime tool registration shared by request assembly, gating, and execution. */
export interface ToolEntry {
  readonly name: string;
  readonly definition: ProviderToolDefinition;
  readonly inputContract: ToolInputContract;
  readonly route: ToolRoute;
  readonly formatter: ToolFormatterContract;
  readonly defaultPermissionPolicy: ToolPermissionPolicy;
  readonly required: boolean;
}

/** Session-effective availability and optional permission override for one tool. */
export interface ToolConfig {
  readonly name: string;
  readonly enabled: boolean;
  readonly permissionPolicy?: ToolPermissionPolicy;
}

/** Materialized tool entries and the session configuration used to evaluate them. */
export interface ToolCatalog {
  readonly entries: readonly ToolEntry[];
  readonly configs: readonly ToolConfig[];
}

/** One provider-visible tool declared by a ready MCP server manifest. */
export interface MCPManifestTool {
  readonly name: string;
  readonly description: string;
  readonly inputSchema: unknown;
}

/** MCP manifest defaults applied when a tool has no matching override. */
export interface MCPManifestDefaultConfig {
  readonly enabled?: boolean | undefined;
  readonly permissionPolicy?: ToolPermissionPolicy | undefined;
}

/** Per-tool MCP availability and permission overrides. */
export interface MCPManifestToolConfig {
  readonly name: string;
  readonly enabled?: boolean | undefined;
  readonly permissionPolicy?: ToolPermissionPolicy | undefined;
}

/** Ready MCP server generation from which Runtime materializes gateway-routed tools. */
export interface MCPManifest {
  readonly mcpServerName: string;
  readonly manifestETag: string;
  readonly manifestGeneration: number;
  readonly tools: readonly MCPManifestTool[];
  readonly defaultConfig?: MCPManifestDefaultConfig | undefined;
  readonly configs?: readonly MCPManifestToolConfig[] | undefined;
}

/** Inputs required to build a family-pinned session tool catalog. */
export interface ToolCatalogOptions {
  readonly family: InstalledBuiltinFamily;
  readonly configs?: readonly ToolConfig[];
  readonly includeSubAgentTools?: boolean;
  readonly mcpManifests?: readonly MCPManifest[];
}

/** Session-immutable builtin family that selects the installed sandbox tool set. */
export type InstalledBuiltinFamily = "claude" | "gpt";

const ApprovalReviewerToolNames = ["Read", "Grep", "Glob"] as const;

const AlwaysAllowSandboxToolNames = new Set(["Read", "Grep", "Glob", "view_image"]);

const CommandFormatter: ToolFormatterContract = {
  successShape: "bounded text stdout/stderr plus exit status",
  errorShape: "bounded model-visible command error",
  runningShape: "task_id plus bounded initial stdout/stderr",
  truncationShape: "truncated text with byte and line counts",
  continuationHint: "call write_stdin with the returned session_id and empty chars to poll",
  forbiddenFields: ["provider_command_id", "daytona_command_id", "sandbox_process_id", "payload_path"],
  visibleCaps: {
    defaultVisibleBytesPerStream: 50 * 1024,
    defaultVisibleLinesPerStream: 2_000,
    hardCaptureBytesPerStream: 1024 * 1024,
  },
};

const FileFormatter: ToolFormatterContract = {
  successShape: "bounded text or changed-file summary",
  errorShape: "bounded file-tool error",
  truncationShape: "explicit truncated marker with continuation details when supported",
  forbiddenFields: ["payload_path", "absolute_internal_temp_path", "sandbox_driver_ref", "raw_bytes"],
  visibleCaps: {
    defaultReadLines: 2_000,
    defaultGlobPaths: 100,
    defaultGrepHeadLimit: 250,
  },
};

const WebFormatter: ToolFormatterContract = {
  successShape: "text/Markdown web result",
  errorShape: "tool_error or runtime_error text",
  truncationShape: "windowed result with ref_id and next_lineno when available",
  continuationHint: "call web.open with ref_id and next_lineno",
  forbiddenFields: ["raw_html", "binary", "base64", "object_store_ref", "credential"],
};

const MemoryFormatter: ToolFormatterContract = {
  successShape: "memory mutation summary",
  errorShape: "validation/conflict/runtime error with reread signal when stale",
  forbiddenFields: ["memory_store_id", "postgres_id", "bindingId", "sandbox_binding", "mnt_memory_path"],
};

const SubAgentFormatter: ToolFormatterContract = {
  successShape: "child thread/task summary",
  errorShape: "child lifecycle or delivery error",
  runningShape: "child task handle where applicable",
  forbiddenFields: ["bridge_delivery_id", "runtime_binding_token", "child_internal_visibility", "raw_child_context"],
};

const BuiltinEntries: readonly ToolEntry[] = [
  sandboxTool("Bash", builtinToolDescription("Bash"), "exec", {
    type: "object",
    additionalProperties: false,
    properties: {
      command: { type: "string", description: builtinToolParameterDescription("Bash", "command") },
      cwd: { type: "string", description: builtinToolParameterDescription("Bash", "cwd") },
      timeout: { type: "integer", maximum: 600000, description: builtinToolParameterDescription("Bash", "timeout") },
      run_in_background: { type: "boolean", description: builtinToolParameterDescription("Bash", "run_in_background") },
    },
    required: ["command"],
  }),
  sandboxTool("exec_command", builtinToolDescription("exec_command"), "exec", {
    type: "object",
    additionalProperties: false,
    properties: {
      cmd: { type: "string", description: builtinToolParameterDescription("exec_command", "cmd") },
      workdir: { type: "string", description: builtinToolParameterDescription("exec_command", "workdir") },
      yield_time_ms: { type: "integer", description: builtinToolParameterDescription("exec_command", "yield_time_ms") },
      max_output_tokens: { type: "integer", description: builtinToolParameterDescription("exec_command", "max_output_tokens") },
    },
    required: ["cmd"],
  }),
  sandboxTool("write_stdin", builtinToolDescription("write_stdin"), "stdin", {
    type: "object",
    additionalProperties: false,
    properties: {
      session_id: { type: "string", description: builtinToolParameterDescription("write_stdin", "session_id") },
      chars: { type: "string", description: builtinToolParameterDescription("write_stdin", "chars") },
      yield_time_ms: { type: "integer", description: builtinToolParameterDescription("write_stdin", "yield_time_ms") },
      max_output_tokens: { type: "integer", description: builtinToolParameterDescription("write_stdin", "max_output_tokens") },
    },
    required: ["session_id"],
  }),
  sandboxTool("Read", builtinToolDescription("Read"), "read", readInputSchema()),
  sandboxTool("Write", builtinToolDescription("Write"), "write", {
    type: "object",
    additionalProperties: false,
    properties: {
      file_path: { type: "string", description: builtinToolParameterDescription("Write", "file_path") },
      content: { type: "string", description: builtinToolParameterDescription("Write", "content") },
    },
    required: ["file_path", "content"],
  }),
  sandboxTool("Edit", builtinToolDescription("Edit"), "edit", {
    type: "object",
    additionalProperties: false,
    properties: {
      file_path: { type: "string", description: builtinToolParameterDescription("Edit", "file_path") },
      old_string: { type: "string", description: builtinToolParameterDescription("Edit", "old_string") },
      new_string: { type: "string", description: builtinToolParameterDescription("Edit", "new_string") },
      replace_all: { type: "boolean", description: builtinToolParameterDescription("Edit", "replace_all") },
    },
    required: ["file_path", "old_string", "new_string"],
  }),
  sandboxTool("Grep", builtinToolDescription("Grep"), "grep", {
    type: "object",
    additionalProperties: false,
    properties: {
      pattern: { type: "string", description: builtinToolParameterDescription("Grep", "pattern") },
      path: { type: ["string", "null"], description: builtinToolParameterDescription("Grep", "path") },
      globs: { type: "array", items: { type: "string" }, description: builtinToolParameterDescription("Grep", "globs") },
      file_type: { type: ["string", "null"], description: builtinToolParameterDescription("Grep", "file_type") },
      mode: { enum: ["files", "content", "count"], description: builtinToolParameterDescription("Grep", "mode") },
      case_insensitive: { type: "boolean", description: builtinToolParameterDescription("Grep", "case_insensitive") },
      line_numbers: { type: "boolean", description: builtinToolParameterDescription("Grep", "line_numbers") },
      before_context: { type: "integer", minimum: 0, description: builtinToolParameterDescription("Grep", "before_context") },
      after_context: { type: "integer", minimum: 0, description: builtinToolParameterDescription("Grep", "after_context") },
      context: { type: "integer", minimum: 0, description: builtinToolParameterDescription("Grep", "context") },
      multiline: { type: "boolean", description: builtinToolParameterDescription("Grep", "multiline") },
      head_limit: { type: "integer", minimum: 1, maximum: 1000, description: builtinToolParameterDescription("Grep", "head_limit") },
      offset: { type: "integer", minimum: 0, description: builtinToolParameterDescription("Grep", "offset") },
    },
    required: ["pattern"],
  }),
  sandboxTool("Glob", builtinToolDescription("Glob"), "glob", {
    type: "object",
    additionalProperties: false,
    properties: {
      pattern: { type: "string", description: builtinToolParameterDescription("Glob", "pattern") },
      path: { type: "string", description: builtinToolParameterDescription("Glob", "path") },
    },
    required: ["pattern"],
  }),
  sandboxTool("view_image", builtinToolDescription("view_image"), "view_image", pathOnlySchema("view_image", "path")),
  {
    name: "apply_patch",
    definition: {
      kind: "freeform",
      name: "apply_patch",
      description: builtinToolDescription("apply_patch"),
      larkGrammar: ApplyPatchLarkGrammar,
    },
    inputContract: { kind: "freeform_string", executionField: "patch" },
    route: { kind: "sandbox", operation: "RunTool", helperSubcommand: "apply_patch" },
    formatter: FileFormatter,
    defaultPermissionPolicy: "always_ask",
    required: true,
  },
  {
    name: "web",
    definition: {
      kind: "function",
      name: "web",
      description: builtinToolDescription("web"),
      inputSchema: {
        type: "object",
        additionalProperties: false,
        description: "At least one item across the three arrays is required; the three combined are capped at 8 items per call.",
        properties: {
          search_query: {
            type: "array",
            description: builtinToolParameterDescription("web", "search_query"),
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
            description: builtinToolParameterDescription("web", "open"),
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
            description: builtinToolParameterDescription("web", "find"),
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
      },
    },
    inputContract: { kind: "json_object" },
    route: { kind: "gateway", operation: "RunWeb" },
    formatter: WebFormatter,
    defaultPermissionPolicy: "always_ask",
    required: false,
  },
  {
    name: "memory",
    definition: {
      kind: "function",
      name: "memory",
      description: builtinToolDescription("memory"),
      inputSchema: {
        type: "object",
        additionalProperties: false,
        properties: {
          action: { enum: ["create", "replace", "delete", "rename"], description: builtinToolParameterDescription("memory", "action") },
          path: { type: "string", description: builtinToolParameterDescription("memory", "path") },
          new_path: { type: "string", description: builtinToolParameterDescription("memory", "new_path") },
          content: { type: "string", description: builtinToolParameterDescription("memory", "content") },
          old_text: { type: "string", description: builtinToolParameterDescription("memory", "old_text") },
          new_text: { type: "string", description: builtinToolParameterDescription("memory", "new_text") },
          replace_all: { type: "boolean", description: builtinToolParameterDescription("memory", "replace_all") },
          expected_text: { type: "string", description: builtinToolParameterDescription("memory", "expected_text") },
        },
        required: ["action", "path"],
      },
    },
    inputContract: { kind: "json_object" },
    route: { kind: "bridge", operation: "RunMemory" },
    formatter: MemoryFormatter,
    defaultPermissionPolicy: "always_ask",
    required: true,
  },
  subAgentTool("spawn_agent"),
  subAgentTool("send_message"),
  subAgentTool("wait_agent"),
  subAgentTool("interrupt_agent"),
  subAgentTool("close_agent"),
  subAgentTool("resume_agent"),
  subAgentTool("list_agents"),
] as const;

export const BuiltinToolNames = BuiltinEntries.map((entry) => entry.name);

/** Builds the family-exact catalog used by a session's provider, gate, and route paths. */
export function createToolCatalog(options: ToolCatalogOptions): ToolCatalog {
  if (options.family !== "claude" && options.family !== "gpt") {
    throw new Error("installed builtin family is required");
  }
  const includeSubAgentTools = options.includeSubAgentTools ?? true;
  const configs = options.configs ?? [];
  const installedBuiltinEntries = builtinEntriesForFamily(options.family)
    .filter((entry) => includeSubAgentTools || entry.route.kind !== "subagent");
  rejectDisabledRequiredTools(installedBuiltinEntries, configs);
  const builtinEntries = installedBuiltinEntries
    .filter((entry) => entry.required || toolEnabled(entry.name, configs));
  const builtinNames = new Set(installedBuiltinEntries.map((entry) => entry.name));
  const mcpEntries = mcpManifestEntries(options.mcpManifests ?? [], builtinNames);
  return {
    entries: [...builtinEntries, ...mcpEntries],
    configs,
  };
}

const ClaudeFamilyToolNames = ["Bash", "Read", "Write", "Edit", "Glob", "Grep"] as const;
const GPTFamilyToolNames = ["exec_command", "write_stdin", "view_image", "apply_patch"] as const;
const FamilyToolNames = new Set<string>([...ClaudeFamilyToolNames, ...GPTFamilyToolNames]);

function builtinEntriesForFamily(family: InstalledBuiltinFamily): readonly ToolEntry[] {
  const selectedNames = family === "claude" ? ClaudeFamilyToolNames : GPTFamilyToolNames;
  const byName = new Map(BuiltinEntries.map((entry) => [entry.name, entry]));
  const selected = selectedNames.map((name) => {
    const entry = byName.get(name);
    if (entry === undefined) {
      throw new Error(`installed ${family} family tool ${name} is missing from the catalog`);
    }
    return entry;
  });
  const platformEntries = BuiltinEntries.filter((entry) => !FamilyToolNames.has(entry.name));
  return [...selected, ...platformEntries];
}

function rejectDisabledRequiredTools(entries: readonly ToolEntry[], configs: readonly ToolConfig[]): void {
  const requiredNames = new Set(entries.filter((entry) => entry.required).map((entry) => entry.name));
  const disabledRequired = configs.find((config) => requiredNames.has(config.name) && !config.enabled);
  if (disabledRequired !== undefined) {
    throw new Error(`required tool ${disabledRequired.name} cannot be disabled`);
  }
}

/** Builds the fixed, always-allowed read-only catalog used by approval reviewer threads. */
export function createApprovalReviewerToolCatalog(): ToolCatalog {
  const names = new Set<string>(ApprovalReviewerToolNames);
  return {
    entries: BuiltinEntries.filter((entry) => names.has(entry.name)),
    configs: ApprovalReviewerToolNames.map((name) => ({
      name,
      enabled: true,
      permissionPolicy: "always_allow" as const,
    })),
  };
}

/** Resolves an enabled tool by its provider-visible name. */
export function lookupToolEntry(catalog: ToolCatalog, name: string): ToolEntry | undefined {
  return catalog.entries.find((entry) => entry.name === name);
}

/** Projects catalog entries into definitions that contain no Runtime execution metadata. */
export function providerToolDefinitions(catalog: ToolCatalog): readonly ProviderToolDefinition[] {
  return catalog.entries.map((entry) => stripExecutionFields(entry.definition));
}

/** Resolves a tool's session override or falls back to its catalog permission default. */
export function effectivePermissionPolicy(entry: ToolEntry, configs: readonly ToolConfig[]): ToolPermissionPolicy {
  return configs.find((config) => config.name === entry.name)?.permissionPolicy ?? entry.defaultPermissionPolicy;
}

function toolEnabled(name: string, configs: readonly ToolConfig[]): boolean {
  return configs.find((config) => config.name === name)?.enabled ?? true;
}

function mcpManifestEntries(manifests: readonly MCPManifest[], builtinNames: ReadonlySet<string>): readonly ToolEntry[] {
  const entries: ToolEntry[] = [];
  const registered = new Set<string>();
  for (const manifest of manifests) {
    for (const tool of manifest.tools) {
      if (builtinNames.has(tool.name) || registered.has(tool.name) || !mcpToolEnabled(tool.name, manifest)) {
        continue;
      }
      entries.push({
        name: tool.name,
        definition: {
          kind: "function",
          name: tool.name,
          description: tool.description,
          inputSchema: tool.inputSchema,
        },
        inputContract: { kind: "json_object" },
        route: { kind: "gateway", operation: "RunMcpTool", mcpServerName: manifest.mcpServerName },
        formatter: mcpFormatter(manifest.mcpServerName),
        defaultPermissionPolicy: mcpToolPermissionPolicy(tool.name, manifest),
        required: false,
      });
      registered.add(tool.name);
    }
  }
  return entries;
}

function mcpToolEnabled(toolName: string, manifest: MCPManifest): boolean {
  const config = manifest.configs?.find((item) => item.name === toolName);
  return config?.enabled ?? manifest.defaultConfig?.enabled ?? true;
}

function mcpToolPermissionPolicy(toolName: string, manifest: MCPManifest): ToolPermissionPolicy {
  const config = manifest.configs?.find((item) => item.name === toolName);
  return config?.permissionPolicy ?? manifest.defaultConfig?.permissionPolicy ?? "always_ask";
}

function mcpFormatter(mcpServerName: string): ToolFormatterContract {
  return {
    successShape: `formatted ${mcpServerName} MCP result text`,
    errorShape: `formatted ${mcpServerName} MCP tool_error or runtime_error text`,
    forbiddenFields: ["authorization", "credential", "token", "runtimeBindingToken", "bindingId"],
  };
}

function stripExecutionFields(definition: ProviderToolDefinition): ProviderToolDefinition {
  if (definition.kind === "freeform") {
    return {
      kind: "freeform",
      name: definition.name,
      description: definition.description,
      larkGrammar: definition.larkGrammar,
    };
  }
  return {
    kind: "function",
    name: definition.name,
    description: definition.description,
    inputSchema: definition.inputSchema,
    ...(definition.outputSchema !== undefined ? { outputSchema: definition.outputSchema } : {}),
  };
}

function sandboxTool(
  name: string,
  description: string,
  helperSubcommand: SandboxHelperSubcommand,
  inputSchema: unknown,
): ToolEntry {
  return {
    name,
    definition: { kind: "function", name, description, inputSchema },
    inputContract: { kind: "json_object" },
    route: { kind: "sandbox", operation: helperSubcommand === "stdin" ? "CommandIO" : "RunTool", helperSubcommand },
    formatter: helperSubcommand === "exec" || helperSubcommand === "stdin" ? CommandFormatter : FileFormatter,
    defaultPermissionPolicy: AlwaysAllowSandboxToolNames.has(name) ? "always_allow" : "always_ask",
    required: false,
  };
}

function subAgentTool(name: Extract<ToolRoute, { readonly kind: "subagent" }>["operation"]): ToolEntry {
  return {
    name,
    definition: {
      kind: "function",
      name,
      description: builtinToolDescription(name),
      inputSchema: subAgentInputSchema(name),
    },
    inputContract: { kind: "json_object" },
    route: { kind: "subagent", operation: name },
    formatter: SubAgentFormatter,
    defaultPermissionPolicy: "always_ask",
    required: false,
  };
}

function subAgentInputSchema(name: Extract<ToolRoute, { readonly kind: "subagent" }>["operation"]): unknown {
  const parameter = (field: string): string => builtinToolParameterDescription(name, field);
  const taskName = (): unknown => ({ type: "string", minLength: 1, description: parameter("task_name") });
  switch (name) {
    case "spawn_agent":
      return {
        type: "object",
        additionalProperties: false,
        properties: {
          task_name: taskName(),
          prompt: { type: "string", minLength: 1, description: parameter("prompt") },
          agent_type: { enum: ["general", "research", "worker"], description: parameter("agent_type") },
          fork_turns: {
            anyOf: [{ enum: ["none", "all"] }, { type: "string", pattern: "^[1-9][0-9]*$" }],
            description: parameter("fork_turns"),
          },
        },
        required: ["task_name", "prompt"],
      };
    case "send_message":
      return {
        type: "object",
        additionalProperties: false,
        properties: {
          task_name: taskName(),
          message: { type: "string", minLength: 1, description: parameter("message") },
        },
        required: ["task_name", "message"],
      };
    case "wait_agent":
      return {
        type: "object",
        additionalProperties: false,
        properties: {
          task_name: taskName(),
          timeout_ms: { type: "integer", minimum: 0, description: parameter("timeout_ms") },
        },
        required: ["task_name"],
      };
    case "interrupt_agent":
    case "close_agent":
    case "resume_agent":
      return {
        type: "object",
        additionalProperties: false,
        properties: {
          task_name: taskName(),
        },
        required: ["task_name"],
      };
    case "list_agents":
      return {
        type: "object",
        additionalProperties: false,
        description: "None. This tool takes no input.",
        properties: {},
      };
  }
}

function pathOnlySchema(toolName: DocumentedBuiltinToolName, field: "file_path" | "path"): unknown {
  return {
    type: "object",
    additionalProperties: false,
    properties: {
      [field]: { type: "string", description: builtinToolParameterDescription(toolName, field) },
    },
    required: [field],
  };
}

function readInputSchema(): unknown {
  return {
    type: "object",
    additionalProperties: false,
    properties: {
      file_path: { type: "string", description: builtinToolParameterDescription("Read", "file_path") },
      offset: { type: "integer", description: builtinToolParameterDescription("Read", "offset") },
      limit: { type: "integer", description: builtinToolParameterDescription("Read", "limit") },
      page_range: { type: "string", description: builtinToolParameterDescription("Read", "page_range") },
    },
    required: ["file_path"],
  };
}
