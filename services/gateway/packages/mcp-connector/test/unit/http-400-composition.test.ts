import { expect, test } from "bun:test";
import { resolve } from "node:path";

test("an ordinary MCP HTTP 400 never consumes the authentication refresh budget", async () => {
	const child = Bun.spawn({
		cmd: [
			process.execPath,
			resolve(
				import.meta.dir,
				"../fixtures/http-400-no-refresh-composition.ts",
			),
		],
		cwd: resolve(import.meta.dir, "../../../.."),
		env: process.env,
		stdout: "pipe",
		stderr: "pipe",
	});
	const [exitCode, stdout, stderr] = await Promise.all([
		child.exited,
		new Response(child.stdout).text(),
		new Response(child.stderr).text(),
	]);
	const diagnostic = `composition exit=${exitCode} stdout=${stdout.slice(0, 2_000)} stderr=${stderr.slice(0, 2_000)}`;
	expect(exitCode, diagnostic).toBe(0);
	const result = JSON.parse(stdout) as {
		readonly ordinaryBadRequests: number;
		readonly refreshes: number;
		readonly errorCode?: string | number;
		readonly retryStatus?: string;
		readonly authClassified: boolean;
	};
	expect(result.ordinaryBadRequests).toBe(1);
	expect(result.refreshes).toBe(0);
	expect(result.errorCode).toBe(400);
	expect(result.authClassified).toBe(false);
	expect(result.retryStatus).toBeUndefined();
}, 10_000);
