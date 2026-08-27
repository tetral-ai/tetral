/**
 * @packageDocumentation
 * Ordered provider-visible hot context for one Runtime thread: sealed entries,
 * one optional open Assistant draft and an immutable child prefix. Cold preload
 * rebuilds it from durable projections; ThreadLoop mutates it only after the
 * owning write is durably acknowledged. It owns no turn lifecycle decisions,
 * dispatch, Runtime write identities, policy or external I/O.
 */

import type {
	RuntimeContextEntry,
	RuntimeContextPart,
	RuntimeInterruptToolResult,
	RuntimeOpenRequestDraft,
} from "../contracts/runtime.js";

/** Durable, non-sequenced parent context installed separately from child history. */
export interface ThreadContextPrefix {
	readonly childThreadId: string;
	readonly parentThreadId: string;
	readonly parentBoundaryEventId: string;
	readonly entries: readonly RuntimeContextEntry[];
}

// New writes are projected only after durable ACK; cold hydration may install
// projections already read from durable state. The write discipline lives at
// the ThreadLoop/SessionManager call site:
//
// | hot mutation                       | gated on durable ACK            |
// | ---------------------------------- | ------------------------------- |
// | append model-visible message input | CommitInputs ACK                |
// | append agent event content         | WriteEvent ACK                  |
// | replace terminal assistant message | WriteRequestEnd ACK             |
// | apply compaction (prefix replace)  | compaction event/projection ACK |
//
// ThreadState separately owns the last-request usage hint and route-effective
// limits. Request-end settlement seals the open draft into provider context only
// after the durable operation succeeds.
//
// A tool-confirmation commit appends its Bridge-derived user decision before
// waking the approved tool. A task notification uses the background-task settlement
// path and projects a bounded runtime note. A thread context prefix is loaded from the CHILD
// thread's durable context and is never rebuilt from the current parent. The
// generation counter advances on any non-append-only rewrite (compaction or in-place
// update) so the approval-reviewer feed cursor can detect invalidation within one hot
// lifetime. A cold return creates a successor reviewer manager and trunk, which starts
// with a full feed instead of inheriting this generation.
// UPDATE-WITH: services/agent-runtime/packages/core/src/thread-loop/thread-loop.ts,
//              services/agent-runtime/packages/core/src/session/session-manager.ts
/** Mutable hot message list whose generation invalidates reviewer feed cursors on rewrites. */
export class ContextManager {
	readonly sessionId: string;
	#entries: RuntimeContextEntry[];
	#openRequestDraft: RuntimeOpenRequestDraft | undefined;
	#threadContextPrefix: ThreadContextPrefix | undefined;
	#generation = 0;

	constructor(
		sessionId: string,
		initialEntries: readonly RuntimeContextEntry[] = [],
	) {
		this.sessionId = sessionId;
		this.#entries = [...initialEntries];
	}

	entries(): readonly RuntimeContextEntry[] {
		return [...this.#entries];
	}

	threadContextPrefix(): ThreadContextPrefix | undefined {
		return this.#threadContextPrefix;
	}

	installThreadContextPrefix(prefix: ThreadContextPrefix | undefined): void {
		this.#threadContextPrefix = prefix;
	}

	providerEntries(): readonly RuntimeContextEntry[] {
		return [...(this.#threadContextPrefix?.entries ?? []), ...this.#entries];
	}

	providerEntrySegments(): readonly (readonly RuntimeContextEntry[])[] {
		const prefix = this.#threadContextPrefix?.entries ?? [];
		return prefix.length === 0
			? [this.entries()]
			: [[...prefix], this.entries()];
	}

	entryListSnapshot(): {
		readonly generation: number;
		readonly entries: readonly RuntimeContextEntry[];
	} {
		return {
			generation: this.#generation,
			entries: this.entries(),
		};
	}

	replaceEntries(entries: readonly RuntimeContextEntry[]): void {
		const appendOnly =
			this.#entries.length <= entries.length &&
			this.#entries.every(
				(entry, index) =>
					JSON.stringify(entry) === JSON.stringify(entries[index]),
			);
		this.#entries = [...entries];
		if (!appendOnly) {
			this.#generation += 1;
		}
	}

	replaceEntriesThroughSequence(
		boundarySequence: number,
		entries: readonly RuntimeContextEntry[],
	): readonly RuntimeContextEntry[] {
		const preserved = this.#entries.filter(
			(entry) => entry.messageSequence > boundarySequence,
		);
		this.#entries = [...entries, ...preserved];
		this.#generation += 1;
		return this.entries();
	}

	appendEntry(entry: RuntimeContextEntry): void {
		this.#entries = [...this.#entries, entry];
	}

	entry(messageSequence: number): RuntimeContextEntry | undefined {
		return this.#entries.find(
			(entry) => entry.messageSequence === messageSequence,
		);
	}

	updateEntry(entry: RuntimeContextEntry): void {
		this.#entries = this.#entries.map((existingEntry) =>
			existingEntry.messageSequence === entry.messageSequence
				? entry
				: existingEntry,
		);
		this.#generation += 1;
	}

	openRequestDraft(): RuntimeOpenRequestDraft | undefined {
		return this.#openRequestDraft;
	}

	installOpenRequestDraft(draft: RuntimeOpenRequestDraft | undefined): void {
		this.#openRequestDraft = draft;
	}

	/** Appends one terminal Tool Result to its sole sealed-entry or open-draft owner. */
	appendToolResult(
		messageSequence: number,
		modelToolCallId: string,
		result: Extract<
			RuntimeContextPart,
			{ readonly type: "tool_result" }
		>["result"],
	): "sealed" | "open" {
		const sealedEntry = this.entry(messageSequence);
		const openDraft =
			this.#openRequestDraft?.messageSequence === messageSequence
				? this.#openRequestDraft
				: undefined;
		if ((sealedEntry === undefined) === (openDraft === undefined)) {
			throw new Error(
				"Tool result must target exactly one sealed entry or open Request draft",
			);
		}
		const append = (
			parts: readonly RuntimeContextPart[],
		): RuntimeContextPart[] => {
			const callCount = parts.filter(
				(part) =>
					part.type === "tool_call" && part.modelToolCallId === modelToolCallId,
			).length;
			const resultCount = parts.filter(
				(part) =>
					part.type === "tool_result" &&
					part.modelToolCallId === modelToolCallId,
			).length;
			if (callCount !== 1 || resultCount !== 0) {
				throw new Error("Tool result target is missing or already terminal");
			}
			return [...parts, { type: "tool_result", modelToolCallId, result }];
		};
		if (sealedEntry !== undefined) {
			if (sealedEntry.contextKind !== "assistant") {
				throw new Error("Tool result target is not Assistant context");
			}
			this.updateEntry({ ...sealedEntry, parts: append(sealedEntry.parts) });
			return "sealed";
		}
		this.#openRequestDraft = { ...openDraft!, parts: append(openDraft!.parts) };
		return "open";
	}

	appendInterruptToolResults(
		routes: readonly {
			readonly toolUseEventId: string;
			readonly assistantMessageSequence: number;
			readonly modelToolCallId: string;
		}[],
		results: readonly RuntimeInterruptToolResult[],
	): void {
		for (const result of results) {
			const route = routes.find(
				(candidate) => candidate.toolUseEventId === result.toolUseEventId,
			);
			if (route === undefined)
				throw new Error("interrupt Tool result has no active route");
			this.appendToolResult(
				route.assistantMessageSequence,
				route.modelToolCallId,
				result.result,
			);
		}
	}

	sealOpenRequestDraft(): RuntimeContextEntry {
		const draft = this.#openRequestDraft;
		if (draft === undefined)
			throw new Error("cannot seal a missing open Request draft");
		const entry: RuntimeContextEntry = {
			messageSequence: draft.messageSequence,
			contextKind: "assistant",
			parts: draft.parts,
		};
		this.appendEntry(entry);
		this.#openRequestDraft = undefined;
		return entry;
	}

	clear(): void {
		this.#entries = [];
		this.#openRequestDraft = undefined;
		this.#threadContextPrefix = undefined;
		this.#generation += 1;
	}
}
