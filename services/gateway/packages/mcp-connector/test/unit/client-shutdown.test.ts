import { fixtureServerResolver } from "../fixtures/registered-server.js";
import { expect, test } from "bun:test";
import { McpSDKClient } from "../../src/client.js";
import { runMcpConnectorCommand } from "../../src/command.js";
import { SQLMcpCredentialResolver } from "../../src/credential.js";
import type { McpCredentialSQL } from "../../src/credential.js";
import { loadMcpConnectorConfigFromProcessEnv } from "../../src/config.js";
import { commandEnv, commandFixture } from "../fixtures/command-process.js";

const identity = {
  workspaceId: "wksp_shutdown",
  sessionId: "sesn_shutdown",
  mcpServerName: "github",
};

test("SDK close retains a timed-out credential transaction until actual join and preserves its cached timeout rejection", async () => {
  const fixture = heldCredentialSQL();
  const client = credentialClient(fixture.sql);
  const listing = client.listTools(identity);
  void listing.catch(() => undefined);
  let state = "pending";
  let failure: unknown;
  let closing: Promise<void> | undefined;
  try {
    await fixture.entered;
    await expect(listing).rejects.toMatchObject({ code: "mcp_timeout" });
    const deadline = new Date(Date.now() + 25);
    closing = client.closeAll(deadline);
    void closing.then(
      () => {
        state = "success";
      },
      (error) => {
        state = "failed";
        failure = error;
      },
    );
    expect(client.closeAll()).toBe(closing);
    await Bun.sleep(40);
    expect(state).toBe("pending");
    expect(fixture.events).not.toContain("credential.joined");
    fixture.release();
    await expect(closing).rejects.toThrow(
      "MCP client shutdown deadline exceeded",
    );
    expect(state).toBe("failed");
    expect(failure).toBeInstanceOf(Error);
    expect(fixture.events).toEqual(["credential.held", "credential.joined"]);
    expect(client.closeAll()).toBe(closing);
    await expect(client.closeAll()).rejects.toBe(failure);
  } finally {
    fixture.release();
    await listing.catch(() => undefined);
    await (closing ?? client.closeAll()).catch(() => undefined);
  }
});

test("reusable MCP command retains SQL past client deadline until raw credential join; cooperative close succeeds", async () => {
  const saved = { ...process.env };
  Object.assign(process.env, commandEnv(), {
    TETRAL_DRAIN_TIMEOUT_MS: "200",
    TETRAL_SERVICE_CANCEL_JOIN_TIMEOUT_MS: "1000",
    TETRAL_MCP_CREDENTIAL_TIMEOUT_MS: "10",
    TETRAL_DATABASE_STATEMENT_TIMEOUT_MS: "30000",
  });
  try {
    const config = loadMcpConnectorConfigFromProcessEnv();
    if (!config.ok) throw new Error("invalid shutdown fixture config");
    expect(config.config.drainTimeoutMs).toBe(200);
    expect(config.config.cancelJoinTimeoutMs).toBe(1000);
    expect(config.config.databasePool.statementTimeoutMs).toBe(30000);
    for (const cooperative of [false, true]) {
      const fixture = heldCredentialSQL();
      const client = credentialClient(fixture.sql);
      const closeEntered = deferred<Date>();
      const originalClose = client.closeAll.bind(client);
      client.closeAll = (deadline) => {
        if (deadline === undefined)
          throw new Error("command close deadline missing");
        closeEntered.resolve(deadline);
        return originalClose(deadline);
      };
      const command = commandFixture("none", (event) =>
        fixture.events.push(event),
      );
      let state = "pending";
      let failure: unknown;
      const running = runMcpConnectorCommand({
        ...command.options,
        sql: fixture.sql,
        client,
        logger: { info: () => undefined, error: () => undefined },
        registerSignalHandlers: () => undefined,
        waitForever: async () => {
          const listing = client.listTools(identity);
          void listing.catch(() =>
            fixture.events.push("public.listing.returned"),
          );
          await fixture.entered;
          await expect(listing).rejects.toMatchObject({ code: "mcp_timeout" });
          return undefined as never;
        },
      }).then(
        () => {
          state = "success";
        },
        (error) => {
          state = "failed";
          failure = error;
        },
      );
      try {
        const deadline = await closeEntered.promise;
        if (!cooperative) {
          await Bun.sleep(Math.max(1, deadline.getTime() - Date.now() + 20));
          expect(state).toBe("pending");
          expect(fixture.events).toContain("public.listing.returned");
          expect(fixture.events).not.toContain("credential.joined");
          expect(fixture.events).not.toContain("database.close");
        }
        fixture.release();
        await running;
        expect(state).toBe(cooperative ? "success" : "failed");
        if (!cooperative)
          expect(failure).toMatchObject({
            message: "MCP client shutdown deadline exceeded",
          });
        expect(
          fixture.events.filter((event) => event === "database.close"),
        ).toHaveLength(1);
        expect(fixture.events.indexOf("database.close")).toBeGreaterThan(
          fixture.events.indexOf("credential.joined"),
        );
      } finally {
        fixture.release();
        await running;
      }
    }
  } finally {
    for (const key of Object.keys(process.env)) delete process.env[key];
    Object.assign(process.env, saved);
  }
}, 5000);

function credentialClient(sql: McpCredentialSQL) {
  return new McpSDKClient({serverResolver: fixtureServerResolver,
    credentialResolver: new SQLMcpCredentialResolver(
      sql,
      "00".repeat(32),
    ),
    credentialTimeoutMs: 10,
    onToolsListChanged: async () => undefined,
  });
}

function heldCredentialSQL() {
  const events: string[] = [];
  const held = deferred<void>();
  const entered = deferred<void>();
  const sql = Object.assign(
    async <T = unknown>(strings: TemplateStringsArray): Promise<T> => {
      if (strings.join("").includes("WITH session_vaults")) {
        events.push("credential.held");
        entered.resolve();
        await held.promise;
        events.push("credential.joined");
      }
      return [] as T;
    },
    {
      begin: async <T>(
        operation: (tx: McpCredentialSQL) => Promise<T>,
      ): Promise<T> => await operation(sql),
      close: async () => {
        events.push("database.close");
      },
    },
  );
  return {
    sql,
    events,
    entered: entered.promise,
    release: () => held.resolve(),
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((complete) => {
    resolve = complete;
  });
  return { promise, resolve };
}
