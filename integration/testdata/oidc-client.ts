import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { createInterface } from 'node:readline';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

// The Engine owns this driver. The SDK source at the runner's immutable pin is
// imported unchanged; assertions and cache credentials never cross stdout.
const sdkRoot = process.env.TETRAL_ENGINE_SDK_ROOT;
const bootstrapPath = process.argv[2];
assert(sdkRoot && bootstrapPath);
const bootstrap = JSON.parse(await readFile(bootstrapPath, 'utf8'));
const fixtureName = bootstrap.fixtureName;
assert(fixtureName === 'oidc-sdk-human' || fixtureName === 'oidc-sdk-service');
const { default: Tetral } = await import(pathToFileURL(join(sdkRoot, 'src/index.ts')).href);
const { retrieveAttributedMemoryVersion, redactAttributedMemoryVersion } = await import(
  pathToFileURL(join(sdkRoot, 'tests/integration/fixtures/oidc-memory-actors.ts')).href,
);
assert(typeof bootstrap.identityID === 'string' && bootstrap.identityID.length > 0);
const expectedActor = fixtureName === 'oidc-sdk-service'
  ? { type: 'service_actor', service_id: bootstrap.identityID }
  : { type: 'user_actor', user_id: bootstrap.identityID };
const ca = bootstrap.caPath === undefined ? undefined : await readFile(bootstrap.caPath, 'utf8');
const client = new Tetral({
  ...(ca === undefined ? {} : { fetch: (input: RequestInfo | URL, init?: RequestInit) => Bun.fetch(new Request(input, init), { tls: { ca } }) }),
  apiKey: null,
  authToken: null,
  baseURL: bootstrap.baseURL,
  config: {
    authentication: {
      type: 'oidc_federation',
      federation_rule_id: bootstrap.ruleID,
      identity_token: { source: 'file', path: bootstrap.assertionPath },
      ...(bootstrap.serviceAccountID ? { service_account_id: bootstrap.serviceAccountID } : {}),
    },
    organization_id: bootstrap.organizationID,
    workspace_id: bootstrap.workspaceID,
  },
  maxRetries: 1,
  timeout: 30_000,
  logLevel: 'off',
});

async function execute(command: Record<string, string>): Promise<unknown> {
  const stores = client.beta.memoryStores;
  switch (command.operation) {
    case 'session': {
      const environment = await client.beta.environments.create({ name: `${fixtureName}-environment`, config: { type: 'cloud', networking: { type: 'blocked' } } });
      const agent = await client.beta.agents.create({ name: `${fixtureName}-agent`, model: 'anthropic/claude-opus-4-8', approval_mode: 'ask_for_approval', tools: [{ type: 'tetral_agent_toolset', family: 'claude' }] });
      const created = await client.beta.sessions.create({ agent: { type: 'agent', id: agent.id, version: agent.version }, environment_id: environment.id, vault_ids: [] });
      const retrieved = await client.beta.sessions.retrieve(created.id);
      const listed = await client.beta.sessions.list();
      assert.equal(retrieved.id, created.id);
      assert(listed.data.some((row: { id: string }) => row.id === created.id));
      return { sessionID: created.id, environmentID: environment.id, agentID: agent.id };
    }
    case 'store': {
      const store = await stores.create({ name: `${fixtureName}-memory` });
      assert.equal((await stores.retrieve(store.id)).id, store.id);
      assert((await stores.list()).data.some((row: { id: string }) => row.id === store.id));
      return { storeID: store.id };
    }
    case 'create': {
      const memory = await stores.memories.create(command.storeID, { path: command.path, content: command.content });
      const version = await retrieveAttributedMemoryVersion(client, memory.memory_version_id, { memory_store_id: command.storeID }, expectedActor);
      return { memoryID: memory.id, versionID: memory.memory_version_id, version };
    }
    case 'read': {
      const memory = await stores.memories.retrieve(command.memoryID, { memory_store_id: command.storeID });
      const listed = await stores.memories.list(command.storeID);
      const versions = await stores.memoryVersions.list(command.storeID);
      assert.equal(memory.id, command.memoryID);
      assert(listed.data.some((row: { id: string }) => row.id === command.memoryID));
      assert(versions.data.some((row: { id: string }) => row.id === memory.memory_version_id));
      return { memoryID: memory.id, versionID: memory.memory_version_id };
    }
    case 'update': {
      const memory = await stores.memories.update(command.memoryID, { memory_store_id: command.storeID, content: command.content });
      const version = await retrieveAttributedMemoryVersion(client, memory.memory_version_id, { memory_store_id: command.storeID }, expectedActor);
      return { memoryID: memory.id, versionID: memory.memory_version_id, version };
    }
    case 'delete': {
      const deleted = await stores.memories.delete(command.memoryID, { memory_store_id: command.storeID });
      assert.equal(deleted.id, command.memoryID);
      return deleted;
    }
    case 'redact':
      return await redactAttributedMemoryVersion(client, command.versionID, { memory_store_id: command.storeID }, expectedActor, expectedActor);
    default:
      throw new Error('unknown Engine-owned SDK fixture command');
  }
}

for await (const line of createInterface({ input: process.stdin, crlfDelay: Infinity })) {
  let id = 'invalid';
  try {
    assert(line.length <= 64 * 1024);
    const command = JSON.parse(line);
    id = command.id;
    const result = await execute(command);
    console.log(JSON.stringify({ id, ok: true, result }));
  } catch {
    // SDK errors may include request credentials. Emit only a fixed diagnostic.
    console.log(JSON.stringify({ id, ok: false, error: 'SDK operation failed' }));
  }
}
