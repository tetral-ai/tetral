/** Partitions sealed provider context into complete user-led turns. */
import type { RuntimeContextEntry } from "../contracts/runtime.js";

export interface RuntimeConversationTurn {
	readonly entries: readonly RuntimeContextEntry[];
	readonly userLed: boolean;
	readonly compactable: boolean;
}

export function partitionRuntimeConversationTurns(
	entries: readonly RuntimeContextEntry[],
): readonly RuntimeConversationTurn[] {
	const turns: RuntimeContextEntry[][] = [];
	for (const entry of entries) {
		if (
			(isUserBoundary(entry) && entry.contextKind !== "compaction") ||
			turns.length === 0
		) {
			turns.push([entry]);
			continue;
		}
		turns[turns.length - 1]?.push(entry);
	}
	return turns.map((turn) => ({
		entries: turn,
		userLed:
			turn[0] !== undefined &&
			isUserBoundary(turn[0]) &&
			turn[0].contextKind !== "compaction",
		compactable:
			turn[0] !== undefined &&
			isUserBoundary(turn[0]) &&
			!turn.some((entry) => entry.contextKind === "compaction"),
	}));
}

export function selectRecentUserLedTurns(
	entries: readonly RuntimeContextEntry[],
	count: number,
): readonly RuntimeContextEntry[] {
	return partitionRuntimeConversationTurns(entries)
		.filter((turn) => turn.userLed)
		.slice(-count)
		.flatMap((turn) => turn.entries);
}

function isUserBoundary(entry: RuntimeContextEntry): boolean {
	return (
		entry.contextKind === "user" || entry.contextKind === "runtime_notification"
	);
}
