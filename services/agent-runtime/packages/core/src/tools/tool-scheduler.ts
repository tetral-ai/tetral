/**
 * @packageDocumentation
 * Defines tool-job scheduling state, route run-policy inference, and the session-wide permit
 * coordinator used by active request turns. It schedules and projects jobs by caller-assigned model
 * order, and guards bounded concurrency, keyed and session-wide exclusion, approval waits without
 * live-fiber ownership, and permit release on completion or interruption. ThreadLoop creates per-turn
 * ToolSchedulers and runs route Effects through the SessionToolCoordinator owned by SessionManager;
 * this module calls Effect Deferred primitives and path normalization but no tool route, Bridge,
 * database, queue, or sandbox helper.
 */
import path from "node:path";
import { Deferred, Effect } from "effect";
import type { RuntimeJsonValue } from "../contracts/runtime.js";
import type { ToolApprovalSource, ToolGateDecision } from "./tool-gate.js";
import type { ToolRoute } from "./tool-catalog.js";

const DefaultWorkspaceRoot = "/workspace";

/** Origin class of a normalized tool job. */
export type ToolKind = "builtin" | "mcp";
/** Concurrency mode assigned to a tool job before scheduling. */
export type ToolRunPolicyMode = "parallel_safe" | "exclusive";
/** Hot gate state that determines whether a tool job may start. */
export type ToolGateState = "runnable" | "waiting_approval" | "terminal";
/** Final allow or deny decision retained with a tool job. */
export type ToolDecision = "allow" | "deny";

/** Concurrency mode and optional conflict keys enforced across active session tools. */
export interface ToolRunPolicy {
  readonly mode: ToolRunPolicyMode;
  readonly conflictKeys: readonly string[] | null;
}

/** Model-ordered hot state for one normalized tool call in a request turn. */
export interface ToolJob {
  readonly id: string;
  readonly modelOrder: number;
  readonly toolUseEventId?: string;
  readonly modelToolCallId: string;
  readonly kind: ToolKind;
  readonly name: string;
  readonly route: ToolRoute;
  readonly input: RuntimeJsonValue;
  readonly runPolicy: ToolRunPolicy;
  readonly gateState: ToolGateState;
  readonly decision?: ToolDecision;
  readonly approvalSource?: ToolApprovalSource;
  readonly result?: RuntimeJsonValue;
}

/** Aggregate tool-concurrency limit applied by schedulers and session coordinators. */
export interface ToolSchedulerOptions {
  readonly maxConcurrentTools?: number;
}

interface SessionToolPermitWaiter {
  readonly id: symbol;
  readonly policy: ToolRunPolicy;
  readonly ready: Deferred.Deferred<() => void>;
  granted: boolean;
}

/**
 * Coordinates tool permits across every active request turn in one session.
 * SessionManager owns one instance per session, and ThreadLoop runs each route Effect through it so
 * aggregate limits and conflict keys remain effective across concurrent thread runs.
 */
export class SessionToolCoordinator {
  readonly #maxConcurrentTools: number;
  readonly #running = new Map<symbol, ToolRunPolicy>();
  readonly #waiters: SessionToolPermitWaiter[] = [];

  constructor(options: ToolSchedulerOptions = {}) {
    this.#maxConcurrentTools = options.maxConcurrentTools ?? 8;
  }

  withPermit<A, E, R>(policy: ToolRunPolicy, effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> {
    return Effect.flatMap(this.#acquire(policy), (release) =>
      effect.pipe(Effect.ensuring(Effect.sync(release))),
    );
  }

  #acquire(policy: ToolRunPolicy): Effect.Effect<() => void> {
    const coordinator = this;
    return Effect.gen(function* () {
      const ready = yield* Deferred.make<() => void>();
      const waiter: SessionToolPermitWaiter = {
        id: Symbol("session-tool-permit"),
        policy,
        ready,
        granted: false,
      };
      yield* Effect.sync(() => {
        coordinator.#waiters.push(waiter);
        coordinator.#drain();
      });
      return yield* Deferred.await(ready).pipe(
        Effect.onInterrupt(() => Effect.sync(() => coordinator.#cancel(waiter))),
      );
    });
  }

  #cancel(waiter: SessionToolPermitWaiter): void {
    const waitingIndex = this.#waiters.indexOf(waiter);
    if (waitingIndex >= 0) {
      this.#waiters.splice(waitingIndex, 1);
    }
    if (waiter.granted) {
      this.#release(waiter.id);
    }
  }

  #release(id: symbol): void {
    if (!this.#running.delete(id)) {
      return;
    }
    this.#drain();
  }

  #drain(): void {
    for (let index = 0; index < this.#waiters.length && this.#running.size < this.#maxConcurrentTools;) {
      const waiter = this.#waiters[index];
      if (waiter === undefined || !this.#canStart(waiter.policy)) {
        index += 1;
        continue;
      }
      this.#waiters.splice(index, 1);
      waiter.granted = true;
      this.#running.set(waiter.id, waiter.policy);
      const release = (): void => this.#release(waiter.id);
      Deferred.doneUnsafe(waiter.ready, Effect.succeed(release));
    }
  }

  #canStart(candidate: ToolRunPolicy): boolean {
    if (this.#running.size >= this.#maxConcurrentTools) {
      return false;
    }
    for (const running of this.#running.values()) {
      if (conflicts(candidate, running)) {
        return false;
      }
    }
    return true;
  }
}

// ToolScheduler is a per-request-turn coordinator over toolJobs[]. Boundary: it reads
// no databases, consumes no queue jobs, and does not own Bridge — it only starts
// ToolFibers when gate state and run policy permit, bounded by maxConcurrentTools
// (default 8) across the whole scheduled-running set. settled() means this scheduler's
// running-id bookkeeping is empty; ThreadLoop separately owns and joins the actual
// ToolFibers, and a job may sit in waiting_approval after its fiber exits.
// Here "starts" means marks and returns eligible jobs. Normal settlement calls back
// into finishJob; interruption and repair paths settle their fibers outside this
// bookkeeping surface.
//
// Gate state drives scheduler action (startReady):
//   | gateState               | scheduler action                                              |
//   | ----------------------- | ------------------------------------------------------------- |
//   | runnable, parallel_safe | start a ToolFiber within maxConcurrentTools                    |
//   | runnable, exclusive     | start only when no conflicting job runs (null key = session-wide) |
//   | waiting_approval        | never started; contributes to requires_action after request-end ACK |
//   | terminal                | inert                                                         |
/** Selects caller-ordered tool jobs and tracks their scheduled running state for one request turn. */
export class ToolScheduler {
  readonly #maxConcurrentTools: number;
  readonly #jobs = new Map<string, ToolJob>();
  readonly #running = new Set<string>();
  readonly #completedStarts: string[] = [];

  constructor(options: ToolSchedulerOptions = {}) {
    this.#maxConcurrentTools = options.maxConcurrentTools ?? 8;
  }

  addJob(job: ToolJob): void {
    if (this.#jobs.has(job.id)) {
      throw new Error(`duplicate ToolJob id ${job.id}`);
    }
    this.#jobs.set(job.id, job);
  }

  jobs(): readonly ToolJob[] {
    return [...this.#jobs.values()].sort((left, right) => left.modelOrder - right.modelOrder);
  }

  runningJobIds(): readonly string[] {
    return [...this.#running];
  }

  settled(): boolean {
    return this.#running.size === 0;
  }

  startReady(): readonly ToolJob[] {
    const started: ToolJob[] = [];
    for (const job of this.jobs()) {
      if (this.#running.size >= this.#maxConcurrentTools) {
        break;
      }
      if (job.gateState !== "runnable" || this.#running.has(job.id) || this.#completedStarts.includes(job.id)) {
        continue;
      }
      if (this.#canStart(job)) {
        this.#running.add(job.id);
        this.#completedStarts.push(job.id);
        started.push(job);
      }
    }
    return started;
  }

  finishJob(jobId: string, result?: RuntimeJsonValue): void {
    const job = this.#requiredJob(jobId);
    this.#running.delete(jobId);
    this.#jobs.set(jobId, {
      ...job,
      gateState: "terminal",
      ...(result !== undefined ? { result } : {}),
    });
  }

  waitForApproval(jobId: string, approvalSource: ToolApprovalSource = "user"): void {
    const job = this.#requiredJob(jobId);
    this.#running.delete(jobId);
    const completedStartIndex = this.#completedStarts.indexOf(jobId);
    if (completedStartIndex >= 0) {
      this.#completedStarts.splice(completedStartIndex, 1);
    }
    this.#jobs.set(jobId, {
      ...job,
      gateState: "waiting_approval",
      approvalSource,
    });
  }

  applyGateDecision(jobId: string, decision: ToolGateDecision): void {
    const job = this.#requiredJob(jobId);
    if (decision.type === "run") {
      this.#jobs.set(jobId, {
        ...job,
        gateState: "runnable",
        decision: "allow",
        approvalSource: decision.approvalSource,
      });
      return;
    }
    if (decision.type === "ask" || decision.type === "review_required") {
      this.#jobs.set(jobId, {
        ...job,
        gateState: "waiting_approval",
        approvalSource: decision.approvalSource,
      });
      return;
    }
    if (decision.type === "deny") {
      this.#jobs.set(jobId, {
        ...job,
        gateState: "terminal",
        decision: "deny",
        approvalSource: decision.approvalSource,
        result: decision.message,
      });
      return;
    }
    this.#jobs.set(jobId, {
      ...job,
      gateState: "terminal",
      decision: "deny",
      result: decision.reason,
    });
  }

  resolveApproval(jobId: string, decision: "allow" | "deny", denyMessage?: string): void {
    const job = this.#requiredJob(jobId);
    if (job.gateState !== "waiting_approval") {
      throw new Error(`ToolJob ${jobId} is not waiting for approval`);
    }
    if (decision === "allow") {
      this.#jobs.set(jobId, {
        ...job,
        gateState: "runnable",
        decision: "allow",
        approvalSource: "user",
      });
      return;
    }
    this.#jobs.set(jobId, {
      ...job,
      gateState: "terminal",
      decision: "deny",
      approvalSource: "user",
      result: denyMessage ?? "The user denied this tool call.",
    });
  }

  #canStart(candidate: ToolJob): boolean {
    if (candidate.runPolicy.mode === "parallel_safe") {
      return !this.#hasRunningSessionWideExclusive();
    }
    if (candidate.runPolicy.conflictKeys === null || candidate.runPolicy.conflictKeys.length === 0) {
      return this.#running.size === 0;
    }
    for (const runningId of this.#running) {
      const running = this.#requiredJob(runningId);
      if (conflicts(candidate.runPolicy, running.runPolicy)) {
        return false;
      }
    }
    return true;
  }

  #hasRunningSessionWideExclusive(): boolean {
    for (const runningId of this.#running) {
      const running = this.#requiredJob(runningId);
      if (running.runPolicy.mode === "exclusive" && running.runPolicy.conflictKeys === null) {
        return true;
      }
    }
    return false;
  }

  #requiredJob(jobId: string): ToolJob {
    const job = this.#jobs.get(jobId);
    if (job === undefined) {
      throw new Error(`unknown ToolJob ${jobId}`);
    }
    return job;
  }
}

/**
 * Infers the closed builtin run-policy table. `parallel_safe` applies only where listed; an
 * unlisted builtin receives session-wide exclusion. Gateway MCP routes are parallel-safe through
 * {@link inferToolRunPolicy}.
 *
 * | route(s)                                                      | mode          | conflictKey                     |
 * | ------------------------------------------------------------- | ------------- | ------------------------------- |
 * | Read, Grep, Glob, view_image, web, spawn_agent, send_message, | parallel_safe | n/a                             |
 * |   wait_agent, list_agents, Bash, exec_command                 |               |                                 |
 * | Write, Edit                                                   | exclusive     | normalized absolute target path |
 * | apply_patch                                                   | exclusive     | every changed file path         |
 * | write_stdin                                                   | exclusive     | `session_id`                    |
 * | interrupt_agent, close_agent, resume_agent                    | exclusive     | `task_name`                     |
 * | memory                                                        | exclusive     | null (session-wide)             |
 * | any unlisted built-in                                         | exclusive     | null (session-wide)             |
 *
 * `send_message` is parallel-safe here because Runtime Pod's parent/task-name operation queue
 * serializes same-task resolution and operations; after resolution, a child-id lock guards
 * delivery and lifecycle mutation. `spawn_agent` is parallel-safe because durable `task_name`
 * uniqueness, not a scheduler lock, resolves concurrent creation of the same child name.
 */
export function inferBuiltinRunPolicy(toolName: string, input: RuntimeJsonValue): ToolRunPolicy {
  switch (toolName) {
    case "Read":
    case "Grep":
    case "Glob":
    case "view_image":
    case "Bash":
    case "exec_command":
    case "web":
    case "spawn_agent":
    case "send_message":
    case "wait_agent":
    case "list_agents":
      return parallelSafe();
    case "Write":
    case "Edit":
      return exclusiveKeys([normalizeInputPath(input)]);
    case "apply_patch":
      return exclusiveKeys(applyPatchPaths(input));
    case "write_stdin":
      return exclusiveKeys([stringField(input, "session_id")]);
    case "interrupt_agent":
    case "close_agent":
    case "resume_agent":
      return exclusiveKeys([stringField(input, "task_name")]);
    case "memory":
      return sessionWideExclusive();
    default:
      return sessionWideExclusive();
  }
}

/** Infers run policy from a materialized route, including parallel-safe MCP dispatch. */
export function inferToolRunPolicy(entry: { readonly name: string; readonly route: ToolRoute }, input: RuntimeJsonValue): ToolRunPolicy {
  if (entry.route.kind === "gateway" && entry.route.operation === "RunMcpTool") {
    return parallelSafe();
  }
  return inferBuiltinRunPolicy(entry.name, input);
}

export function parallelSafe(): ToolRunPolicy {
  return { mode: "parallel_safe", conflictKeys: null };
}

export function sessionWideExclusive(): ToolRunPolicy {
  return { mode: "exclusive", conflictKeys: null };
}

export function exclusiveKeys(keys: readonly (string | undefined)[]): ToolRunPolicy {
  const normalized = [...new Set(keys.filter((key): key is string => key !== undefined && key.length > 0))];
  if (normalized.length === 0) {
    return sessionWideExclusive();
  }
  return { mode: "exclusive", conflictKeys: normalized };
}

function conflicts(left: ToolRunPolicy, right: ToolRunPolicy): boolean {
  if (left.mode === "parallel_safe" && right.mode === "parallel_safe") {
    return false;
  }
  if (left.mode === "exclusive" && left.conflictKeys === null) {
    return true;
  }
  if (right.mode === "exclusive" && right.conflictKeys === null) {
    return true;
  }
  if (left.mode === "parallel_safe" || right.mode === "parallel_safe") {
    return false;
  }
  const leftKeys = left.conflictKeys;
  const rightKeys = right.conflictKeys;
  if (leftKeys === null || rightKeys === null) {
    return true;
  }
  return leftKeys.some((key) => rightKeys.includes(key));
}

function normalizeInputPath(input: RuntimeJsonValue): string | undefined {
  const rawPath = stringField(input, "file_path") ?? stringField(input, "path");
  if (rawPath === undefined) {
    return undefined;
  }
  return normalizePath(rawPath, stringField(input, "workdir") ?? stringField(input, "cwd") ?? DefaultWorkspaceRoot);
}

function applyPatchPaths(input: RuntimeJsonValue): readonly string[] {
  const patch = typeof input === "string" ? input : stringField(input, "patch") ?? stringField(input, "content");
  if (patch === undefined) {
    return [];
  }
  const paths: string[] = [];
  for (const line of patch.split(/\r?\n/)) {
    const match = /^\*\*\* (?:(?:Add|Update|Delete) File|Move to): (.+)$/.exec(line);
    if (match?.[1] !== undefined) {
      paths.push(normalizePath(match[1], DefaultWorkspaceRoot));
    }
  }
  return paths;
}

function stringField(input: RuntimeJsonValue, field: string): string | undefined {
  if (typeof input !== "object" || input === null || Array.isArray(input)) {
    return undefined;
  }
  const value = (input as Readonly<Record<string, RuntimeJsonValue>>)[field];
  return typeof value === "string" ? value : undefined;
}

function normalizePath(inputPath: string, cwd: string): string {
  const base = path.posix.isAbsolute(cwd) ? cwd : path.posix.join(DefaultWorkspaceRoot, cwd);
  const resolved = path.posix.isAbsolute(inputPath) ? inputPath : path.posix.join(base, inputPath);
  return path.posix.normalize(resolved);
}
