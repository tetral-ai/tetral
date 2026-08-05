import { describe, expect, test } from "bun:test";
import { readdir, readFile, stat } from "node:fs/promises";
import { extname, relative, sep } from "node:path";

const packageRoot = new URL("../../", import.meta.url);
const sourceRoot = new URL("../../src/", import.meta.url);
const lifecycleLayerRoots = [
  new URL("../../src/thread-loop/", import.meta.url),
  new URL("../../src/session/", import.meta.url),
  new URL("../../src/session-run-host/", import.meta.url),
] as const;

interface ForbiddenPattern {
  readonly label: string;
  readonly pattern: RegExp;
}

const lifecycleForbiddenPatterns: readonly ForbiddenPattern[] = [
  { label: "forbidden process.env", pattern: /\bprocess\.env\b/ },
  { label: "forbidden production console.log", pattern: /\bconsole\.log\s*\(/ },
  { label: "forbidden as any", pattern: /\bas\s+any\b/ },
  { label: "forbidden ts suppression", pattern: /@ts-ignore|@ts-expect-error/ },
  { label: "forbidden SessionRunService", pattern: /\bSessionRunService\b/ },
  { label: "forbidden SessionRunState", pattern: /\bSessionRunState\b/ },
  { label: "forbidden runSession", pattern: /\brunSession\b/ },
  { label: "forbidden cancelSession", pattern: /\bcancelSession\b/ },
  { label: "forbidden cancelLocalSession", pattern: /\bcancelLocalSession\b/ },
  { label: "forbidden unloaded thread ensure", pattern: /\b(?:ensureRunning|handleEnsureRunning|EnsureRunningResult)\b/ },
  { label: "forbidden TTL timer", pattern: /\bttl\b|\bidle(?:Timer|Timeout|Fiber)\b/i },
  { label: "forbidden cleanup timer", pattern: /\bcleanup(?:Timer|Timeout|Fiber)\b/i },
  { label: "forbidden payload ingress", pattern: /\b(?:eventBody|providerRequest|runtimeMessage|modelInfo)\b/ },
  { label: "forbidden HTTP/RPC transport", pattern: /\b(?:http|rpc)(?:Handler|Server|Adapter|Transport|Route|Endpoint)?\b/i },
  { label: "forbidden Bun.serve transport", pattern: /\bBun\.serve\b/ },
  { label: "forbidden WebSocket transport", pattern: /\bWebSocket\b/ },
] as const;

function sourceUrl(relativePath: string): URL {
  return new URL(`../../${relativePath}`, import.meta.url);
}

function normalizeSource(text: string): string {
  return text.replace(/\s+/g, "");
}

function namedInterfaceBody(text: string, interfaceName: string): string {
  const match = new RegExp(`interface\\s+${interfaceName}\\s*\\{([\\s\\S]*?)^\\}`, "m").exec(text);
  if (match?.[1] === undefined) {
    throw new Error(`missing interface ${interfaceName}`);
  }
  return match[1]
    .replace(/\/\*[\s\S]*?\*\//g, "")
    .replace(/\/\/.*$/gm, "");
}

function packageRelativePath(filePath: string): string {
  return relative(packageRoot.pathname, filePath).split(sep).join("/");
}

async function pathExists(url: URL): Promise<boolean> {
  try {
    await stat(url);
    return true;
  } catch {
    return false;
  }
}

async function collectTypeScriptFiles(directoryUrl: URL): Promise<readonly string[]> {
  const entries = await readdir(directoryUrl, { withFileTypes: true });
  const files: string[] = [];
  for (const entry of entries) {
    const entryUrl = new URL(`${entry.name}${entry.isDirectory() ? "/" : ""}`, directoryUrl);
    if (entry.isDirectory()) {
      files.push(...(await collectTypeScriptFiles(entryUrl)));
      continue;
    }
    if (entry.isFile() && entry.name.endsWith(".ts")) {
      files.push(entryUrl.pathname);
    }
  }
  return files;
}

async function collectLifecycleSourcePaths(): Promise<readonly string[]> {
  const files = (await Promise.all(lifecycleLayerRoots.map((root) => collectTypeScriptFiles(root)))).flat();
  return files.sort((left, right) => packageRelativePath(left).localeCompare(packageRelativePath(right)));
}

async function readLifecycleSources(): Promise<ReadonlyMap<string, string>> {
  const filePaths = await collectLifecycleSourcePaths();
  const entries = await Promise.all(
    filePaths.map(async (filePath) => [packageRelativePath(filePath), await readFile(filePath, "utf8")] as const),
  );
  return new Map(entries);
}

function importSpecifiers(text: string): readonly string[] {
  return [...text.matchAll(/\b(?:import|export)\s+(?:[^"']*?\s+from\s+)?["']([^"']+)["']/g)]
    .map((match) => match[1])
    .filter((specifier): specifier is string => specifier !== undefined);
}

function collectLifecycleBoundaryViolations(relativePath: string, text: string): readonly string[] {
  const violations: string[] = [];
  for (const forbiddenPattern of lifecycleForbiddenPatterns) {
    if (forbiddenPattern.pattern.test(text)) {
      violations.push(`${relativePath}: ${forbiddenPattern.label}`);
    }
  }
  for (const importSpecifier of importSpecifiers(text)) {
    const allowedThreadLoopRuntimeImport =
      (relativePath === "src/thread-loop/thread-loop.ts" &&
        (importSpecifier === "../runtime/message-projection.js" ||
          importSpecifier === "../runtime/accumulator.js" ||
          importSpecifier === "../runtime/conversation-turns.js" ||
          importSpecifier === "../runtime/runtime-declaration.js" ||
          importSpecifier === "../runtime/metrics.js" ||
          importSpecifier === "../runtime/turn-retry-budget.js")) ||
      (relativePath === "src/thread-loop/provider-request.ts" &&
        importSpecifier === "../runtime/metrics.js") ||
      (relativePath === "src/thread-loop/tool-execution.ts" &&
        importSpecifier === "../runtime/accumulator.js") ||
      (relativePath === "src/thread-loop/closeout.ts" &&
        importSpecifier === "../runtime/runtime-declaration.js");
    const allowedSessionManagerRuntimeImport =
      relativePath === "src/session/session-manager.ts" &&
      importSpecifier === "../runtime/metrics.js";
    if (importSpecifier.startsWith(".") && extname(importSpecifier) !== ".js") {
      violations.push(`${relativePath}: relative import without .js suffix (${importSpecifier})`);
    }
    if (!allowedThreadLoopRuntimeImport && !allowedSessionManagerRuntimeImport && /^(?:.*\/)?(?:providers|runtime)\//.test(importSpecifier)) {
      violations.push(`${relativePath}: guarded runtime/provider import (${importSpecifier})`);
    }
  }
  return violations;
}

describe("session run static boundaries", () => {
  test("lifecycle roots expose the new Agent Pod ownership files and no removed service/state files", async () => {
    const sources = await readLifecycleSources();
    const expectedLifecycleFiles = [
      "src/thread-loop/thread-loop.ts",
      "src/thread-loop/thread-runtime.ts",
      "src/thread-loop/thread-state.ts",
      "src/thread-loop/thread-turn-checkpoint.ts",
      "src/thread-loop/thread-turn-reducer.ts",
      "src/thread-loop/provider-request.ts",
      "src/thread-loop/tool-execution.ts",
      "src/thread-loop/compaction.ts",
      "src/thread-loop/closeout.ts",
      "src/session/approval-reviewer-manager.ts",
      "src/session/context-manager.ts",
      "src/session/session-configuration.ts",
      "src/session/session-manager.ts",
      "src/session/thread-command-channel.ts",
      "src/session-run-host/session-run-host.ts",
    ].sort((left, right) => left.localeCompare(right));

    expect([...sources.keys()]).toEqual(expectedLifecycleFiles);
    expect(await pathExists(sourceUrl("src/session-run-service/session-run-service.ts"))).toBe(false);
    expect(await pathExists(sourceUrl("src/session/session-run-state.ts"))).toBe(false);
    expect(await pathExists(sourceUrl("test/unit/session-run-service.test.ts"))).toBe(false);
    expect(await pathExists(sourceUrl("test/unit/session-run-state.test.ts"))).toBe(false);
  });

  test("lifecycle sources reject legacy names, payload ingress, timers, transports, and unsafe primitives", async () => {
    const sources = await readLifecycleSources();
    const violations: string[] = [];

    for (const [relativePath, text] of sources) {
      violations.push(...collectLifecycleBoundaryViolations(relativePath, text));
    }

    expect(violations).toEqual([]);
  });

  test("boundary scanner rejects realistic forbidden variants", () => {
    const violations = collectLifecycleBoundaryViolations(
      "src/session/bad.ts",
      [
        'import * as RuntimeStream from "../runtime/stream-service";',
        "const service = SessionRunService;",
        "const state = SessionRunState;",
        "const result = runSession('sesn_1');",
        "const rejected = cancelSession('sesn_1');",
        "const timer = cleanupTimer;",
        "const body = eventBody;",
        "const request = providerRequest;",
        "Bun.serve({});",
      ].join("\n"),
    );

    expect(violations).toContain("src/session/bad.ts: relative import without .js suffix (../runtime/stream-service)");
    expect(violations).toContain("src/session/bad.ts: guarded runtime/provider import (../runtime/stream-service)");
    expect(violations).toContain("src/session/bad.ts: forbidden SessionRunService");
    expect(violations).toContain("src/session/bad.ts: forbidden SessionRunState");
    expect(violations).toContain("src/session/bad.ts: forbidden runSession");
    expect(violations).toContain("src/session/bad.ts: forbidden cancelSession");
    expect(violations).toContain("src/session/bad.ts: forbidden cleanup timer");
    expect(violations).toContain("src/session/bad.ts: forbidden payload ingress");
    expect(violations).toContain("src/session/bad.ts: forbidden Bun.serve transport");
  });

  test("string-keyed maps are limited to residency, reviewer memo, and pure checkpoint extraction", async () => {
    const managerSource = await readFile(sourceUrl("src/session/session-manager.ts"), "utf8");
    const stateSource = await readFile(sourceUrl("src/thread-loop/thread-state.ts"), "utf8");
    const normalizedManager = normalizeSource(managerSource);
    const normalizedState = normalizeSource(stateSource);
    const normalizedSessionEntry = normalizeSource(namedInterfaceBody(managerSource, "SessionEntry"));
    const otherSources = await readLifecycleSources();
    const mapOwners: string[] = [];

    for (const [relativePath, text] of otherSources) {
      if (/\bnew Map\s*<\s*string\s*,/.test(text)) {
        mapOwners.push(relativePath);
      }
    }

    expect(mapOwners).toEqual([
      "src/session/approval-reviewer-manager.ts",
      "src/session/session-manager.ts",
      "src/thread-loop/thread-turn-checkpoint.ts",
    ]);
    expect(normalizedManager).toContain("interfaceThreadRunSlot{");
    expect(normalizedManager).toContain("pendingWakeAfterStop:boolean;");
    expect(normalizedManager).toContain("interfaceThreadEntry{");
    expect(normalizedManager).toContain("runSlot:ThreadRunSlot|undefined;");
    expect(normalizedSessionEntry).toBe("workspaceId:string;readonlysessionId:string;bindingId:string;bindingGeneration:number;readonlythreads:Map<string,ThreadEntry>;readonlytoolCoordinator:SessionToolCoordinator;readonlyruntimeShutdown:RuntimeShutdownObservation;readonlycontrolGate:Semaphore.Semaphore;readonlyconfiguration:SessionConfiguration;sharedStateStatus:\"initializing\"|\"ready\"|\"failed\";readonlysharedStateInitializerThreadId:string;readonlysharedStateReady:Promise<boolean>;readonlycompleteSharedStateReady:(ready:boolean)=>void;");
    expect(normalizedManager).toContain("toolCoordinator:newSessionToolCoordinator({maxConcurrentTools:options.maxConcurrentTools??8})");
    expect(normalizedManager).toContain("newThreadRuntime.ThreadRuntime(identity,approvalReviewer,toolCoordinator,sessionConfiguration,)");
    expect(normalizedManager).toContain("constsessions=newMap<string,SessionEntry>();");
    expect(normalizedManager).toContain("constsessionKey=(workspaceId:string,sessionId:string):string=>`${workspaceId}\\u0000${sessionId}`;");
    expect(normalizedManager).toContain("constcommandSessionKey=(command:{readonlyworkspaceId:string;readonlysessionId:string}):string=>sessionKey(command.workspaceId,command.sessionId);");
    expect(normalizedManager).toContain("constawaitRunSlot=(runSlot:ThreadRunSlot,):Effect.Effect<Exit.Exit<ThreadLoop.ThreadLoopRunResult,unknown>>=>Deferred.await(runSlot.doneDeferred).pipe(Effect.exit)");
    expect(normalizedManager).toContain("construnScope=yield*Scope.make();");
    expect(normalizedManager).toContain("constfiber=yield*Effect.forkIn(run,runScope);");
    expect(normalizedManager).toContain("constcontrolIdentity=(command:RuntimeThreadControlState):ThreadRuntime.RuntimeThreadIdentity=>({workspaceId:command.workspaceId,sessionId:command.sessionId,sessionThreadId:command.sessionThreadId,bindingId:command.bindingId,bindingGeneration:command.bindingGeneration");
    expect(normalizedManager).toContain("constsessionEntry=sessions.get(commandSessionKey(command));if(sessionEntry===undefined){return{ok:true,sessionId,created:false,applied:false,noResidency:true};}");
    expect(normalizedManager).toContain("sessionEntry.sharedStateStatus!==\"ready\"||[...sessionEntry.threads.values()].some((threadEntry)=>threadEntry.installationState!==\"ready\"||threadEntry.runSlot!==undefined,)");
    expect(normalizedManager).toContain("for(constthreadEntryofsessionEntry.threads.values()){");
    expect(normalizedState).toContain("interfaceRuntimeThreadControlStateextendsRuntimeCommandScopeState{readonlyruntimeInputId:string;readonlyeventIds:readonlystring[];readonlysequenceFrom:number;readonlysequenceTo:number;}");
    expect(normalizedManager).not.toContain("runInFlight");
    expect(normalizedManager).not.toContain("SessionRunMarker");
    expect(normalizedManager).not.toContain("constthreadResult=getOrCreateThreadEntry(controlIdentity(command));if(threadResult===undefined){return{ok:false,sessionId,reason:\"local_session_capacity_exceeded\"};}if(threadResult.threadEntry.runSlot!==undefined)");
    expect(normalizedState).not.toMatch(/pending(?:Wake|Notify)/);
    expect(normalizedManager.match(/setTimeout/g) ?? []).toHaveLength(1);
    expect(normalizedManager).toContain("consttimeout=setTimeout(()=>resolve(true),durationMs);signal.addEventListener(\"abort\",()=>{clearTimeout(timeout);resolve(false);},{once:true});");
    expect(normalizedManager).not.toContain("setInterval");
    expect(normalizedManager).not.toMatch(/\bttl\b/i);
  });

  test("SessionRunHost and SessionManager are the only command-routing Effect services", async () => {
    const sources = await readLifecycleSources();
    const serviceTags: string[] = [];

    for (const [relativePath, text] of sources) {
      for (const match of text.matchAll(/Context\.Service<[^>]+>\(\)\("([^"]+)"\)/g)) {
        const tag = match[1];
        if (tag !== undefined) {
          serviceTags.push(`${relativePath}: ${tag}`);
        }
      }
    }

    expect(serviceTags.sort()).toEqual(
      [
      "src/thread-loop/thread-loop.ts: tetral-agent/ThreadLoop",
      "src/session/session-manager.ts: tetral-agent/SessionManager",
      "src/session-run-host/session-run-host.ts: tetral-agent/SessionRunHost",
      ].sort(),
    );
  });

  test("cleanup path owns local memory cleanup only and cannot read context or write events", async () => {
    const managerSource = await readFile(sourceUrl("src/session/session-manager.ts"), "utf8");
    const cleanupStart = managerSource.indexOf("const cleanupSession");
    const cleanupEnd = managerSource.indexOf("\n      const shutdownActiveRuns", cleanupStart);
    const cleanupSection = managerSource.slice(cleanupStart, cleanupEnd);

    expect(cleanupSection).toContain('reason: "session_busy"');
    expect(cleanupSection).toContain("clearSessionEntry(entry)");
    expect(cleanupSection).not.toContain("unbindSessionBinding");
    expect(cleanupSection).not.toContain("sessionBinding.unbind");
    expect(cleanupSection).not.toContain("buildContext");
    expect(cleanupSection).not.toContain("loadPendingInput");
    expect(cleanupSection).not.toContain("RuntimeInternalToolRepairStore");
    expect(cleanupSection).not.toContain("SessionEventWriter");
    expect(cleanupSection).not.toContain("writeMessage");
    expect(cleanupSection).not.toContain("writePart");
    expect(cleanupSection).not.toContain(".append(");
  });

  test("production source has no durable active-run or run-status ownership", async () => {
    const sourceFiles = await collectLifecycleSourcePaths();
    const violations: string[] = [];
    const blockedPatterns: readonly ForbiddenPattern[] = [
      { label: "ActiveRun", pattern: /\bActiveRun\b/ },
      { label: "RunStatus", pattern: /\bRunStatus\b/ },
      { label: "active_run", pattern: /\bactive_run\b/ },
      { label: "run_status", pattern: /\brun_status\b/ },
      { label: "runStatus", pattern: /\brunStatus\b/ },
      { label: "SessionStatus", pattern: /\bSessionStatus\b/ },
      { label: "SQL table", pattern: /\b(?:active_runs|run_statuses|CREATE\s+TABLE)\b/i },
      { label: "database client import", pattern: /\bfrom\s+["'](?:pg|postgres|postgres-js|sqlite|better-sqlite3|bun:sqlite|drizzle|kysely|prisma)["']/ },
    ] as const;

    for (const filePath of sourceFiles) {
      const relativePath = packageRelativePath(filePath);
      const text = await readFile(filePath, "utf8");
      for (const blockedPattern of blockedPatterns) {
        if (!blockedPattern.pattern.test(text)) {
          continue;
        }
        if (blockedPattern.label === "SessionStatus" && relativePath === "src/contracts/runtime.ts") {
          continue;
        }
        violations.push(`${relativePath}: durable run-state term ${blockedPattern.label}`);
      }
    }

    expect(violations).toEqual([]);
  });

  test("cold preload publishes installing residency before the shared-state gate and pending-tool hydration", async () => {
    const managerSource = await readFile(sourceUrl("src/session/session-manager.ts"), "utf8");
    const threadLoopSource = await readFile(sourceUrl("src/thread-loop/thread-loop.ts"), "utf8");
    const normalizedThreadLoop = normalizeSource(threadLoopSource);
    const preloadStart = managerSource.indexOf("const preloadThread");
    const preloadEnd = managerSource.indexOf("\n      const interruptThread", preloadStart);
    const preloadSection = managerSource.slice(preloadStart, preloadEnd);
    const installIndex = preloadSection.indexOf("threadLoop.installLoadedPendingToolUses");
    const pendingAssignmentIndex = preloadSection.indexOf("const pendingToolUseInstall");
    const residencyIndex = preloadSection.indexOf("getOrCreateThreadEntry(identity, metadata, \"installing\")");
    const sharedStateGateIndex = preloadSection.indexOf("threadResult.sessionEntry.sharedStateReady");

    expect(preloadStart).toBeGreaterThanOrEqual(0);
    expect(preloadEnd).toBeGreaterThan(preloadStart);
    expect(installIndex).toBeGreaterThanOrEqual(0);
    expect(normalizedThreadLoop).toContain(
      "installLoadedPendingToolUses:(session,pendingToolUses,messages)=>Effect.sync(()=>installLoadedPendingToolUses(session,()=>toolCatalogForSession(session,options),pendingToolUses,messages))",
    );
    expect(pendingAssignmentIndex).toBeGreaterThanOrEqual(0);
    expect(residencyIndex).toBeGreaterThanOrEqual(0);
    expect(sharedStateGateIndex).toBeGreaterThan(residencyIndex);
    expect(pendingAssignmentIndex).toBeGreaterThan(sharedStateGateIndex);
    expect(preloadSection.indexOf("completeSharedStateReady(true)")).toBeLessThan(pendingAssignmentIndex);
  });

  test("pending approval hot state stores settlement descriptors, never SessionProcessor", async () => {
    const stateSource = await readFile(sourceUrl("src/thread-loop/thread-state.ts"), "utf8");
    const stateStart = stateSource.indexOf("export interface RuntimePendingApprovalToolJobState");
    const stateEnd = stateSource.indexOf("\n}\n", stateStart);
    const pendingState = stateSource.slice(stateStart, stateEnd);

    expect(stateStart).toBeGreaterThanOrEqual(0);
    expect(stateEnd).toBeGreaterThan(stateStart);
    expect(pendingState).toContain("assistantMessage: DurableRuntimeMessage");
    expect(pendingState).toContain('toolPart: Extract<RuntimePart, { readonly type: "tool" }>');
    expect(pendingState).not.toContain("SessionProcessor");
    expect(pendingState).not.toMatch(/\bprocessor\s*:/);
  });
});
