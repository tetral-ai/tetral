import { readFile } from "node:fs/promises";
import { Metadata } from "@grpc/grpc-js";
import type { ApprovalReviewerThreadClose } from "../../src/approval-reviewer.js";
import { BridgeAPIApprovalReviewerThreadCreator } from "../../src/bridge-client.js";

const inputPath = process.argv[2];
if (inputPath === undefined) {
	throw new Error("reviewer close composition input is required");
}

const input = JSON.parse(await readFile(inputPath, "utf8")) as {
	readonly bridgeAddress: string;
	readonly closes: readonly ApprovalReviewerThreadClose[];
};
const creator = new BridgeAPIApprovalReviewerThreadCreator({
	address: input.bridgeAddress,
	tokenPath: "/unused/service-account-token",
	metadataFactory: async () => new Metadata(),
});
const results = [];
for (const close of input.closes) {
	results.push(await creator.closeApprovalReviewerThread(close));
}
process.stdout.write(JSON.stringify({ results }));
