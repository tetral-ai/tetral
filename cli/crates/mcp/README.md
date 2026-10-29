# mcp

The **MCP manager** belongs here, one task per App Server, which owns the local
MCP servers this machine runs and each binding's MCP connections to them.

## Design rules

- The manager follows the owner rules of `cli/README.md`.

## Where things are

- `lib.rs`: `spawn_mcp_manager`, `McpManagerMessage` and `McpManagerMailbox`.
