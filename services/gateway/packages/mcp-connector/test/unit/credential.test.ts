import { describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { SQLGitHubMcpCredentialResolver } from "../../src/credential.js";
import { MCP_REFRESH_HTTP_TIMEOUT_MS } from "../../src/phase-budgets.js";
import type { McpCredentialSQL } from "../../src/credential.js";
import type { GitHubMcpCredentialRefreshWriter } from "../../src/credential-update-path.js";

describe("SQLGitHubMcpCredentialResolver", () => {
  test("sets the workspace RLS GUC inside the credential read transaction", async () => {
    const calls: string[] = [];
    const tx = (async <T = unknown>(strings: TemplateStringsArray) => {
      const query = strings.join("?");
      calls.push(query);
      return [] as T;
    }) as McpCredentialSQL;
    const sql = (async <T = unknown>() => [] as T) as McpCredentialSQL;
    sql.begin = async <T>(fn: (transaction: McpCredentialSQL) => Promise<T>) => await fn(tx);
    const resolver = new SQLGitHubMcpCredentialResolver(sql, "00".repeat(32));

    await resolver.resolve({
      workspaceId: "wksp_rls",
      sessionId: "sesn_rls",
      mcpServerName: "github",
    });

    expect(calls).toHaveLength(2);
    expect(calls[0]).toContain("set_config('tetral.workspace_id'");
    expect(calls[1]).toContain("FROM credentials c");
  });
  test("resolves a static bearer credential from the session vault set", async () => {
    const keyHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const encrypted = await encryptAES256GCM(new TextEncoder().encode(JSON.stringify({
      type: "static_bearer",
      mcp_server_url: "https://api.githubcopilot.com/mcp",
      token: "github-token-sentinel",
    })), keyHex);
    const resolver = new SQLGitHubMcpCredentialResolver(asyncSQL([{
      id: "cred_1",
      vault_id: "vlt_1",
      mcp_server_url: "https://stale.example.com/mcp",
      auth_public_json: publicAuthJSON("https://api.githubcopilot.com/mcp/"),
      encrypted_auth: encrypted,
    }]), keyHex);

    const resolved = await resolver.resolve({
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      mcpServerName: "github",
    });

    expect(resolved).toEqual({
      ok: true,
      mode: "bearer",
      token: "github-token-sentinel",
      tokenHash: "9cb8fbfae398764beea48e31233c2f137a029d266d589c7aca98be5707198265",
      vaultId: "vlt_1",
      credentialId: "cred_1",
    });
  });

  test("does not match credential URL with more than one trailing slash stripped", async () => {
    const keyHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const encrypted = await encryptAES256GCM(new TextEncoder().encode(JSON.stringify({
      type: "static_bearer",
      mcp_server_url: "https://api.githubcopilot.com/mcp//",
      token: "github-token-sentinel",
    })), keyHex);
    const resolver = new SQLGitHubMcpCredentialResolver(asyncSQL([{
      id: "cred_1",
      vault_id: "vlt_1",
      mcp_server_url: "https://api.githubcopilot.com/mcp//",
      auth_public_json: publicAuthJSON("https://api.githubcopilot.com/mcp//"),
      encrypted_auth: encrypted,
    }]), keyHex);

    const resolved = await resolver.resolve({
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      mcpServerName: "github",
    });

    expect(resolved).toEqual({ ok: false, error: "credential_required" });
  });

  test("does not match credential URL with query, fragment, or userinfo", async () => {
    const keyHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const resolver = new SQLGitHubMcpCredentialResolver(asyncSQL([
      {
        id: "cred_query",
        vault_id: "vlt_1",
        mcp_server_url: "https://api.githubcopilot.com/mcp/?token=secret#fragment",
        auth_public_json: publicAuthJSON("https://api.githubcopilot.com/mcp/?token=secret#fragment"),
        encrypted_auth: await encryptAES256GCM(new TextEncoder().encode(JSON.stringify({
          type: "static_bearer",
          mcp_server_url: "https://api.githubcopilot.com/mcp/?token=secret#fragment",
          token: "github-token-sentinel",
        })), keyHex),
      },
      {
        id: "cred_userinfo",
        vault_id: "vlt_1",
        mcp_server_url: "https://user:pass@api.githubcopilot.com/mcp/",
        auth_public_json: publicAuthJSON("https://user:pass@api.githubcopilot.com/mcp/"),
        encrypted_auth: await encryptAES256GCM(new TextEncoder().encode(JSON.stringify({
          type: "static_bearer",
          mcp_server_url: "https://user:pass@api.githubcopilot.com/mcp/",
          token: "github-token-sentinel",
        })), keyHex),
      },
    ]), keyHex);

    const resolved = await resolver.resolve({
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      mcpServerName: "github",
    });

    expect(resolved).toEqual({ ok: false, error: "credential_required" });
  });

  test("does not normalize credential URL host case or default port", async () => {
    const keyHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const resolver = new SQLGitHubMcpCredentialResolver(asyncSQL([
      {
        id: "cred_upper_host",
        vault_id: "vlt_1",
        mcp_server_url: "https://API.GITHUBCOPILOT.COM/mcp/",
        auth_public_json: publicAuthJSON("https://API.GITHUBCOPILOT.COM/mcp/"),
        encrypted_auth: await encryptAES256GCM(new TextEncoder().encode(JSON.stringify({
          type: "static_bearer",
          mcp_server_url: "https://API.GITHUBCOPILOT.COM/mcp/",
          token: "github-token-uppercase",
        })), keyHex),
      },
      {
        id: "cred_default_port",
        vault_id: "vlt_1",
        mcp_server_url: "https://api.githubcopilot.com:443/mcp/",
        auth_public_json: publicAuthJSON("https://api.githubcopilot.com:443/mcp/"),
        encrypted_auth: await encryptAES256GCM(new TextEncoder().encode(JSON.stringify({
          type: "static_bearer",
          mcp_server_url: "https://api.githubcopilot.com:443/mcp/",
          token: "github-token-default-port",
        })), keyHex),
      },
    ]), keyHex);

    const resolved = await resolver.resolve({
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      mcpServerName: "github",
    });

    expect(resolved).toEqual({ ok: false, error: "credential_required" });
  });

  test("runs every MCP credential vector", async () => {
    const vectors = await loadGitHubCredentialVectors();
    expect(vectors.cases).toHaveLength(10);
    for (const vector of vectors.cases) {
      const rows = await encryptedRowsForVector(vector);
      const state = statefulSQL(rows);
      const refreshRequests: Array<{ readonly url: string; readonly body: string }> = [];
      const resolver = new SQLGitHubMcpCredentialResolver(
        state.sql,
        vectorKeyHex,
        () => new Date(vectors.now),
        async (url, init) => {
          refreshRequests.push({ url: String(url), body: String(init?.body) });
          const refresh = vector.refresh_response;
          if (refresh === undefined) {
            return new Response("unexpected refresh", { status: 500 });
          }
          if (refresh.status < 200 || refresh.status > 299) {
            return new Response(refresh.body ?? "{\"error\":\"invalid_grant\"}", { status: refresh.status });
          }
          return Response.json({
            access_token: refresh.access_token,
            refresh_token: refresh.refresh_token,
            expires_in: refresh.expires_in,
          }, { status: refresh.status });
        },
      );

      const resolved = await resolver.resolve({
        workspaceId: "wksp_1",
        sessionId: "sesn_1",
        mcpServerName: "github",
      });

      switch (vector.expected.outcome) {
        case "use":
          expect(resolved.ok, vector.name).toBe(true);
          const expectedToken = requireVectorString(vector.expected.token, `${vector.name}: expected token`);
          const expectedBearer = requireVectorString(vector.expected.bearer_authorization, `${vector.name}: expected bearer authorization`);
          if (resolved.ok && resolved.mode === "bearer") {
            expect(resolved.token, vector.name).toBe(expectedToken);
            expect(`Bearer ${resolved.token}`, vector.name).toBe(expectedBearer);
            expect(resolved.refreshTriggered === true, vector.name).toBe(vector.expected.refresh_triggered === true);
          } else {
            throw new Error(`${vector.name}: expected bearer material`);
          }
          expect(refreshRequests, vector.name).toHaveLength(vector.expected.refresh_triggered ? 1 : 0);
          break;
        case "error":
          expect(resolved, vector.name).toEqual({ ok: false, error: requireVectorError(vector.expected.error, vector.name) });
          break;
      }
    }
  });

  test("fails closed when more than one matching credential is present", async () => {
    const resolver = new SQLGitHubMcpCredentialResolver(asyncSQL([
      { id: "cred_1", vault_id: "vlt_1", mcp_server_url: "https://api.githubcopilot.com/mcp/", auth_public_json: publicAuthJSON("https://api.githubcopilot.com/mcp/"), encrypted_auth: new Uint8Array([1]) },
      { id: "cred_2", vault_id: "vlt_2", mcp_server_url: "https://api.githubcopilot.com/mcp", auth_public_json: publicAuthJSON("https://api.githubcopilot.com/mcp"), encrypted_auth: new Uint8Array([2]) },
    ]), "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");

    await expect(resolver.resolve({
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      mcpServerName: "github",
    })).resolves.toEqual({ ok: false, error: "ambiguous" });
  });

  test("fails closed on the live connector path when no credential exists", async () => {
    const resolver = new SQLGitHubMcpCredentialResolver(asyncSQL([]), vectorKeyHex);

    await expect(resolver.resolve({
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      mcpServerName: "github",
    })).resolves.toEqual({ ok: false, error: "credential_required" });
  });

  test("refreshes expired OAuth credentials under the row lock and writes back rotated material", async () => {
    const keyHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const now = new Date("2026-01-01T00:00:00.000Z");
    const state = statefulSQL([{
      id: "cred_oauth",
      vault_id: "vlt_1",
      mcp_server_url: "https://api.githubcopilot.com/mcp/",
      auth_public_json: publicAuthJSON("https://api.githubcopilot.com/mcp/"),
      encrypted_auth: await encryptAES256GCM(new TextEncoder().encode(JSON.stringify({
        type: "mcp_oauth",
        mcp_server_url: "https://api.githubcopilot.com/mcp/",
        access_token: "old-access",
        expires_at: "2025-12-31T23:59:00.000Z",
        refresh: {
          refresh_token: "old-refresh",
          client_id: "github-client",
          token_endpoint: "https://tokens.example/oauth",
          scope: "repo",
          resource: "https://api.githubcopilot.com/mcp/",
          token_endpoint_auth: { type: "client_secret_post", client_secret: "client-secret" },
        },
      })), keyHex),
    }]);
    const refreshRequests: Array<{ readonly url: string; readonly body: string }> = [];
    const resolver = new SQLGitHubMcpCredentialResolver(
      state.sql,
      keyHex,
      () => now,
      async (url, init) => {
        refreshRequests.push({ url: String(url), body: String(init?.body) });
        return Response.json({ access_token: "new-access", refresh_token: "new-refresh", expires_in: 3600 });
      },
    );

    const resolved = await resolver.resolve({
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      mcpServerName: "github",
    });

    expect(resolved).toMatchObject({ ok: true, token: "new-access" });
    expect(refreshRequests).toEqual([{
      url: "https://tokens.example/oauth",
      body: "grant_type=refresh_token&refresh_token=old-refresh&scope=repo&resource=https%3A%2F%2Fapi.githubcopilot.com%2Fmcp%2F&client_id=github-client&client_secret=client-secret",
    }]);
    const decrypted = JSON.parse(new TextDecoder().decode(await decryptAES256GCM(state.row().encrypted_auth, keyHex))) as {
      readonly access_token: string;
      readonly expires_at: string;
      readonly refresh?: { readonly refresh_token?: string };
    };
    expect(decrypted.access_token).toBe("new-access");
    expect(decrypted.refresh?.refresh_token).toBe("new-refresh");
    expect(decrypted.expires_at).toBe("2026-01-01T01:00:00.000Z");
    expect(state.publicAuthJSON).not.toContain("new-access");
    expect(state.publicAuthJSON).not.toContain("new-refresh");
    expect(state.publicAuthJSON).not.toContain("client-secret");
  });

  test("refreshes OAuth credentials when expiry is within the sixty second skew", async () => {
    const keyHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const now = new Date("2026-01-01T00:00:00.000Z");
    const state = statefulSQL([{
      id: "cred_oauth",
      vault_id: "vlt_1",
      mcp_server_url: "https://api.githubcopilot.com/mcp/",
      auth_public_json: publicAuthJSON("https://api.githubcopilot.com/mcp/"),
      encrypted_auth: await encryptAES256GCM(new TextEncoder().encode(JSON.stringify({
        type: "mcp_oauth",
        mcp_server_url: "https://api.githubcopilot.com/mcp/",
        access_token: "near-expiry-access",
        expires_at: "2026-01-01T00:00:30.000Z",
        refresh: {
          refresh_token: "near-expiry-refresh",
          client_id: "github-client",
          token_endpoint: "https://tokens.example/oauth",
          token_endpoint_auth: { type: "none" },
        },
      })), keyHex),
    }]);
    const refreshRequests: Array<{ readonly url: string; readonly body: string }> = [];
    const resolver = new SQLGitHubMcpCredentialResolver(
      state.sql,
      keyHex,
      () => now,
      async (url, init) => {
        refreshRequests.push({ url: String(url), body: String(init?.body) });
        return Response.json({ access_token: "fresh-access", refresh_token: "fresh-refresh", expires_in: 3600 });
      },
    );

    const resolved = await resolver.resolve({
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      mcpServerName: "github",
    });

    expect(resolved).toMatchObject({ ok: true, token: "fresh-access" });
    expect(refreshRequests).toHaveLength(1);
    expect(state.beginCount).toBe(2);
    expect(state.workspaceSetCount).toBe(2);
    expect(state.selectForUpdateCount).toBe(1);
    expect(state.updateCount).toBe(1);
  });

  test("aborts a hung OAuth refresh HTTP request within its phase budget", async () => {
    const keyHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const now = new Date("2026-01-01T00:00:00.000Z");
    const state = statefulSQL([{
      id: "cred_oauth",
      vault_id: "vlt_1",
      mcp_server_url: "https://api.githubcopilot.com/mcp/",
      auth_public_json: publicAuthJSON("https://api.githubcopilot.com/mcp/"),
      encrypted_auth: await encryptAES256GCM(new TextEncoder().encode(JSON.stringify({
        type: "mcp_oauth",
        mcp_server_url: "https://api.githubcopilot.com/mcp/",
        access_token: "expired-access",
        expires_at: "2025-12-31T23:59:00.000Z",
        refresh: {
          refresh_token: "refresh-token",
          client_id: "github-client",
          token_endpoint: "https://tokens.example/oauth",
          token_endpoint_auth: { type: "none" },
        },
      })), keyHex),
    }]);
    let fetchSignal: AbortSignal | undefined;
    const resolver = new SQLGitHubMcpCredentialResolver(
      state.sql,
      keyHex,
      () => now,
      async (_url, init) => {
        fetchSignal = init?.signal ?? undefined;
        await new Promise<void>((_resolve, reject) => {
          fetchSignal?.addEventListener("abort", () => reject(fetchSignal?.reason), { once: true });
        });
        throw new Error("unreachable");
      },
      undefined,
      1,
    );

    await expect(resolver.resolve({
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      mcpServerName: "github",
    })).resolves.toEqual({ ok: false, error: "refresh_failed" });
    expect(fetchSignal?.aborted).toBe(true);
    expect(MCP_REFRESH_HTTP_TIMEOUT_MS).toBe(10_000);
  });

  test("keeps the OAuth refresh deadline armed while reading the response body", async () => {
    const keyHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const now = new Date("2026-01-01T00:00:00.000Z");
    const state = statefulSQL([{
      id: "cred_oauth",
      vault_id: "vlt_1",
      mcp_server_url: "https://api.githubcopilot.com/mcp/",
      auth_public_json: publicAuthJSON("https://api.githubcopilot.com/mcp/"),
      encrypted_auth: await encryptAES256GCM(new TextEncoder().encode(JSON.stringify({
        type: "mcp_oauth",
        mcp_server_url: "https://api.githubcopilot.com/mcp/",
        access_token: "expired-access",
        expires_at: "2025-12-31T23:59:00.000Z",
        refresh: {
          refresh_token: "refresh-token",
          client_id: "github-client",
          token_endpoint: "https://tokens.example/oauth",
          token_endpoint_auth: { type: "none" },
        },
      })), keyHex),
    }]);
    const resolver = new SQLGitHubMcpCredentialResolver(
      state.sql,
      keyHex,
      () => now,
      async (_url, init) => new Response(new ReadableStream<Uint8Array>({
        start(controller) {
          const timer = setTimeout(() => {
            controller.enqueue(new TextEncoder().encode(JSON.stringify({
              access_token: "too-late-access",
              refresh_token: "too-late-refresh",
              expires_in: 3600,
            })));
            controller.close();
          }, 25);
          init?.signal?.addEventListener("abort", () => {
            clearTimeout(timer);
            controller.error(init.signal?.reason);
          }, { once: true });
        },
      }), { status: 200, headers: { "content-type": "application/json" } }),
      undefined,
      1,
    );

    await expect(resolver.resolve({
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      mcpServerName: "github",
    })).resolves.toEqual({ ok: false, error: "refresh_failed" });
    expect(state.updateCount).toBe(0);
  });

  test("delegates OAuth write-back to the injected Vault update path", async () => {
    const keyHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const now = new Date("2026-01-01T00:00:00.000Z");
    const encrypted = await encryptAES256GCM(new TextEncoder().encode(JSON.stringify({
      type: "mcp_oauth",
      mcp_server_url: "https://api.githubcopilot.com/mcp/",
      access_token: "old-access",
      expires_at: "2025-12-31T23:59:00.000Z",
      refresh: {
        refresh_token: "old-refresh",
        client_id: "github-client",
        token_endpoint: "https://tokens.example/oauth",
        token_endpoint_auth: { type: "none" },
      },
    })), keyHex);
    const calls: Array<Parameters<GitHubMcpCredentialRefreshWriter["refreshOAuthCredential"]>[0]> = [];
    const writer: GitHubMcpCredentialRefreshWriter = {
      refreshOAuthCredential: async (input) => {
        calls.push(input);
        return {
          ok: true,
          mode: "bearer",
          token: "delegated-access",
          tokenHash: sha256("delegated-access"),
          vaultId: "vlt_1",
          credentialId: "cred_oauth",
          refreshTriggered: true,
        };
      },
    };
    const resolver = new SQLGitHubMcpCredentialResolver(
      asyncSQL([{
        id: "cred_oauth",
        vault_id: "vlt_1",
        mcp_server_url: "https://api.githubcopilot.com/mcp/",
        auth_public_json: publicAuthJSON("https://api.githubcopilot.com/mcp/"),
        encrypted_auth: encrypted,
      }]),
      keyHex,
      () => now,
      async () => {
        throw new Error("resolver must not refresh directly");
      },
      writer,
    );

    const resolved = await resolver.resolve({
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      mcpServerName: "github",
    });

    expect(resolved).toEqual({
      ok: true,
      mode: "bearer",
      token: "delegated-access",
      tokenHash: sha256("delegated-access"),
      vaultId: "vlt_1",
      credentialId: "cred_oauth",
      refreshTriggered: true,
    });
    expect(calls).toHaveLength(1);
    expect(calls[0]).toMatchObject({
      workspaceId: "wksp_1",
      force: false,
      previousTokenHash: undefined,
      row: {
        id: "cred_oauth",
        vault_id: "vlt_1",
        mcp_server_url: "https://api.githubcopilot.com/mcp/",
      },
    });
  });

  test("refresh loser reuses the locked rotated token inside skew without a second upstream refresh", async () => {
    const keyHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const state = statefulSQL([{
      id: "cred_oauth",
      vault_id: "vlt_1",
      mcp_server_url: "https://api.githubcopilot.com/mcp/",
      auth_public_json: publicAuthJSON("https://api.githubcopilot.com/mcp/"),
      encrypted_auth: await encryptAES256GCM(new TextEncoder().encode(JSON.stringify({
        type: "mcp_oauth",
        mcp_server_url: "https://api.githubcopilot.com/mcp/",
        access_token: "already-rotated-access",
        expires_at: "2026-01-01T00:00:30.000Z",
        refresh: {
          refresh_token: "already-rotated-refresh",
          client_id: "github-client",
          token_endpoint: "https://tokens.example/oauth",
          token_endpoint_auth: { type: "none" },
        },
      })), keyHex),
    }]);
    const refreshRequests: string[] = [];
    const resolver = new SQLGitHubMcpCredentialResolver(
      state.sql,
      keyHex,
      () => new Date("2026-01-01T00:00:00.000Z"),
      async (url) => {
        refreshRequests.push(String(url));
        return Response.json({ access_token: "should-not-be-used", refresh_token: "should-not-be-used", expires_in: 3600 });
      },
    );

    const resolved = await resolver.refresh({
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      mcpServerName: "github",
      vaultId: "vlt_1",
      credentialId: "cred_oauth",
      previousTokenHash: sha256("stale-access"),
      force: true,
    });

    expect(resolved).toMatchObject({ ok: true, token: "already-rotated-access" });
    expect("refreshTriggered" in resolved).toBe(false);
    expect(refreshRequests).toEqual([]);
    expect(state.beginCount).toBe(2);
    expect(state.workspaceSetCount).toBe(2);
    expect(state.selectForUpdateCount).toBe(1);
    expect(state.updateCount).toBe(0);
  });

  test("proactive refresh loser reuses a same-token credential whose locked expiry moved forward", async () => {
    const keyHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const staleAuth = {
      type: "mcp_oauth",
      mcp_server_url: "https://api.githubcopilot.com/mcp/",
      access_token: "same-access",
      expires_at: "2026-01-01T00:00:30.000Z",
      refresh: {
        refresh_token: "same-refresh",
        client_id: "github-client",
        token_endpoint: "https://tokens.example/oauth",
        token_endpoint_auth: { type: "none" },
      },
    };
    const staleRow = {
      id: "cred_oauth",
      vault_id: "vlt_1",
      mcp_server_url: "https://api.githubcopilot.com/mcp/",
      auth_public_json: publicAuthJSON("https://api.githubcopilot.com/mcp/"),
      encrypted_auth: await encryptAES256GCM(new TextEncoder().encode(JSON.stringify(staleAuth)), keyHex),
    };
    const state = statefulSQL([staleRow], [{
      ...staleRow,
      encrypted_auth: await encryptAES256GCM(new TextEncoder().encode(JSON.stringify({
        ...staleAuth,
        expires_at: "2026-01-01T00:00:45.000Z",
      })), keyHex),
    }]);
    const refreshRequests: string[] = [];
    const resolver = new SQLGitHubMcpCredentialResolver(
      state.sql,
      keyHex,
      () => new Date("2026-01-01T00:00:00.000Z"),
      async (url) => {
        refreshRequests.push(String(url));
        return Response.json({ access_token: "should-not-be-used", refresh_token: "should-not-be-used", expires_in: 3600 });
      },
    );

    const resolved = await resolver.resolve({
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      mcpServerName: "github",
    });

    expect(resolved).toMatchObject({ ok: true, token: "same-access" });
    expect("refreshTriggered" in resolved).toBe(false);
    expect(refreshRequests).toEqual([]);
    expect(state.updateCount).toBe(0);
  });

  test("forced refresh loser reuses a same-token credential whose locked expiry moved forward", async () => {
    const keyHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const staleAuth = {
      type: "mcp_oauth",
      mcp_server_url: "https://api.githubcopilot.com/mcp/",
      access_token: "same-access",
      expires_at: "2025-12-31T23:59:00.000Z",
      refresh: {
        refresh_token: "same-refresh",
        client_id: "github-client",
        token_endpoint: "https://tokens.example/oauth",
        token_endpoint_auth: { type: "none" },
      },
    };
    const staleRow = {
      id: "cred_oauth",
      vault_id: "vlt_1",
      mcp_server_url: "https://api.githubcopilot.com/mcp/",
      auth_public_json: publicAuthJSON("https://api.githubcopilot.com/mcp/"),
      encrypted_auth: await encryptAES256GCM(new TextEncoder().encode(JSON.stringify(staleAuth)), keyHex),
    };
    const state = statefulSQL([staleRow], [{
      ...staleRow,
      encrypted_auth: await encryptAES256GCM(new TextEncoder().encode(JSON.stringify({
        ...staleAuth,
        expires_at: "2026-01-01T01:00:00.000Z",
      })), keyHex),
    }]);
    const refreshRequests: string[] = [];
    const resolver = new SQLGitHubMcpCredentialResolver(
      state.sql,
      keyHex,
      () => new Date("2026-01-01T00:00:00.000Z"),
      async (url) => {
        refreshRequests.push(String(url));
        return Response.json({ access_token: "should-not-be-used", refresh_token: "should-not-be-used", expires_in: 3600 });
      },
    );

    const resolved = await resolver.refresh({
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      mcpServerName: "github",
      vaultId: "vlt_1",
      credentialId: "cred_oauth",
      previousTokenHash: sha256("same-access"),
      force: true,
    });

    expect(resolved).toMatchObject({ ok: true, token: "same-access" });
    expect("refreshTriggered" in resolved).toBe(false);
    expect(refreshRequests).toEqual([]);
    expect(state.updateCount).toBe(0);
  });

  test("fails closed when the locked row no longer matches the originally selected identity", async () => {
    const keyHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const auth = {
      type: "mcp_oauth",
      mcp_server_url: "https://api.githubcopilot.com/mcp/",
      access_token: "stale-access",
      expires_at: "2025-12-31T23:59:00.000Z",
      refresh: {
        refresh_token: "stale-refresh",
        client_id: "github-client",
        token_endpoint: "https://tokens.example/oauth",
        token_endpoint_auth: { type: "none" },
      },
    };
    const original = {
      id: "cred_original",
      vault_id: "vlt_original",
      mcp_server_url: auth.mcp_server_url,
      auth_public_json: publicAuthJSON(auth.mcp_server_url),
      encrypted_auth: await encryptAES256GCM(new TextEncoder().encode(JSON.stringify(auth)), keyHex),
    };
    const replacement = {
      ...original,
      id: "cred_replacement",
      vault_id: "vlt_replacement",
    };
    const state = statefulSQL([original], [replacement]);
    const refreshRequests: string[] = [];
    const resolver = new SQLGitHubMcpCredentialResolver(
      state.sql,
      keyHex,
      () => new Date("2026-01-01T00:00:00.000Z"),
      async (url) => {
        refreshRequests.push(String(url));
        return Response.json({ access_token: "unexpected", refresh_token: "unexpected", expires_in: 3600 });
      },
    );

    await expect(resolver.refresh({
      workspaceId: "wksp_1",
      sessionId: "sesn_1",
      mcpServerName: "github",
      vaultId: "vlt_original",
      credentialId: "cred_original",
      previousTokenHash: sha256("stale-access"),
      force: true,
    })).resolves.toEqual({ ok: false, error: "credential_required" });
    expect(refreshRequests).toEqual([]);
    expect(state.updateCount).toBe(0);
  });
});

function asyncSQL(rows: readonly unknown[]) {
  const sql = (async <T = unknown>(strings: TemplateStringsArray) => {
    const query = strings.join("?");
    return (query.includes("set_config('tetral.workspace_id'") ? [] : rows) as T;
  }) as McpCredentialSQL;
  sql.begin = async <T>(fn: (tx: McpCredentialSQL) => Promise<T>) => await fn(sql);
  return sql;
}

function statefulSQL(initialRows: ReadonlyArray<{
  readonly id: string;
  readonly vault_id: string;
  readonly mcp_server_url: string;
  readonly auth_public_json: string | Record<string, unknown>;
  readonly encrypted_auth: Uint8Array;
}>, rowsAfterLockWait?: ReadonlyArray<{
  readonly id: string;
  readonly vault_id: string;
  readonly mcp_server_url: string;
  readonly auth_public_json: string | Record<string, unknown>;
  readonly encrypted_auth: Uint8Array;
}>) {
  let rows = initialRows.map((row) => ({ ...row }));
  const state = {
    beginCount: 0,
    publicAuthJSON: "",
    selectForUpdateCount: 0,
    updateCount: 0,
    workspaceSetCount: 0,
    row: () => rows[0]!,
    sql: undefined as unknown as McpCredentialSQL,
  };
  const sql = (async <T = unknown>(strings: TemplateStringsArray, ...values: unknown[]) => {
    const text = strings.join("?");
    if (text.includes("set_config('tetral.workspace_id'")) {
      state.workspaceSetCount += 1;
      return [] as T;
    }
    if (text.includes("FOR UPDATE")) {
      state.selectForUpdateCount += 1;
      if (rowsAfterLockWait !== undefined) {
        rows = rowsAfterLockWait.map((row) => ({ ...row }));
      }
    }
    if (text.includes("UPDATE credentials")) {
      state.updateCount += 1;
      state.publicAuthJSON = String(values[0]);
      rows = rows.map((row) => row.id === values[7] ? {
        ...row,
        auth_public_json: String(values[0]),
        mcp_server_url: String(values[1]),
        encrypted_auth: values[3] as Uint8Array,
      } : row);
      return [] as T;
    }
    return rows as T;
  }) as McpCredentialSQL;
  sql.begin = async <T>(fn: (tx: McpCredentialSQL) => Promise<T>) => {
    state.beginCount += 1;
    return await fn(sql);
  };
  state.sql = sql;
  return state;
}

function sha256(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function publicAuthJSON(mcpServerURL: string): string {
  return JSON.stringify({ mcp_server_url: mcpServerURL });
}

async function encryptAES256GCM(plaintext: Uint8Array, hexKey: string): Promise<Uint8Array> {
  const key = await crypto.subtle.importKey("raw", arrayBuffer(bytesFromHex(hexKey)), "AES-GCM", false, ["encrypt"]);
  const nonce = new Uint8Array(12);
  const sealed = await crypto.subtle.encrypt({ name: "AES-GCM", iv: arrayBuffer(nonce), tagLength: 128 }, key, arrayBuffer(plaintext));
  const output = new Uint8Array(nonce.byteLength + sealed.byteLength);
  output.set(nonce, 0);
  output.set(new Uint8Array(sealed), nonce.byteLength);
  return output;
}

async function decryptAES256GCM(ciphertext: Uint8Array, hexKey: string): Promise<Uint8Array> {
  const key = await crypto.subtle.importKey("raw", arrayBuffer(bytesFromHex(hexKey)), "AES-GCM", false, ["decrypt"]);
  const nonce = ciphertext.slice(0, 12);
  const sealed = ciphertext.slice(12);
  return new Uint8Array(await crypto.subtle.decrypt({ name: "AES-GCM", iv: arrayBuffer(nonce), tagLength: 128 }, key, arrayBuffer(sealed)));
}

function bytesFromHex(value: string): Uint8Array {
  const output = new Uint8Array(value.length / 2);
  for (let index = 0; index < output.length; index += 1) {
    output[index] = Number.parseInt(value.slice(index * 2, index * 2 + 2), 16);
  }
  return output;
}

function arrayBuffer(bytes: Uint8Array): ArrayBuffer {
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer;
}

const vectorKeyHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

interface GitHubCredentialVectorFile {
  readonly catalog_url: string;
  readonly now: string;
  readonly cases: readonly GitHubCredentialVectorCase[];
}

interface GitHubCredentialVectorCase {
  readonly name: string;
  readonly credentials: readonly GitHubCredentialVectorCredential[];
  readonly refresh_response?: GitHubCredentialVectorRefreshResponse | undefined;
  readonly expected: GitHubCredentialVectorExpected;
}

interface GitHubCredentialVectorCredential {
  readonly auth_type: "mcp_oauth" | "static_bearer";
  readonly mcp_server_url: string;
  readonly secret_mcp_server_url?: string | undefined;
  readonly token?: string | undefined;
  readonly access_token?: string | undefined;
  readonly expires_at?: string | undefined;
  readonly refresh?: GitHubCredentialVectorRefreshBlock | undefined;
  readonly archived?: boolean | undefined;
  readonly encrypted_auth?: "corrupt" | undefined;
}

interface GitHubCredentialVectorRefreshBlock {
  readonly refresh_token: string;
  readonly client_id: string;
  readonly token_endpoint: string;
  readonly token_endpoint_auth?: { readonly type?: string | undefined } | undefined;
}

interface GitHubCredentialVectorRefreshResponse {
  readonly status: number;
  readonly access_token?: string | undefined;
  readonly refresh_token?: string | undefined;
  readonly expires_in?: number | undefined;
  readonly body?: string | undefined;
}

interface GitHubCredentialVectorExpected {
  readonly outcome: "use" | "error";
  readonly error?: GitHubCredentialVectorError | undefined;
  readonly token?: string | undefined;
  readonly bearer_authorization?: string | undefined;
  readonly refresh_triggered?: boolean | undefined;
}

type GitHubCredentialVectorError =
  | "credential_required"
  | "ambiguous"
  | "undecryptable"
  | "expired"
  | "refresh_failed";

interface GitHubCredentialVectorRow {
  readonly id: string;
  readonly vault_id: string;
  readonly mcp_server_url: string;
  readonly auth_public_json: string;
  readonly encrypted_auth: Uint8Array;
}

async function loadGitHubCredentialVectors(): Promise<GitHubCredentialVectorFile> {
  const body = await readFile(new URL("../testdata/mcp-credential-vectors.json", import.meta.url), "utf8");
  const parsed = JSON.parse(body) as GitHubCredentialVectorFile;
  expect(parsed.catalog_url).toBe("https://api.githubcopilot.com/mcp/");
  return parsed;
}

async function encryptedRowsForVector(vector: GitHubCredentialVectorCase): Promise<readonly GitHubCredentialVectorRow[]> {
  const rows: GitHubCredentialVectorRow[] = [];
  for (const [index, credential] of vector.credentials.entries()) {
    if (credential.archived === true) {
      continue;
    }
    const secret = {
      type: credential.auth_type,
      mcp_server_url: credential.secret_mcp_server_url ?? credential.mcp_server_url,
      token: credential.token,
      access_token: credential.access_token,
      expires_at: credential.expires_at,
      refresh: credential.refresh,
    };
    rows.push({
      id: `cred_${index}`,
      vault_id: `vlt_${index}`,
      mcp_server_url: credential.mcp_server_url,
      auth_public_json: publicAuthJSON(credential.mcp_server_url),
      encrypted_auth: credential.encrypted_auth === "corrupt"
        ? new Uint8Array([0])
        : await encryptAES256GCM(new TextEncoder().encode(JSON.stringify(secret)), vectorKeyHex),
    });
  }
  return rows;
}

function requireVectorString(value: string | undefined, message: string): string {
  if (value === undefined) {
    throw new Error(message);
  }
  return value;
}

function requireVectorError(value: GitHubCredentialVectorError | undefined, vectorName: string): GitHubCredentialVectorError {
  if (value === undefined) {
    throw new Error(`${vectorName}: expected error`);
  }
  return value;
}
