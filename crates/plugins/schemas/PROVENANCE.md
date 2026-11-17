# Agent Plugins 1.0.0

Unmodified canonical schemas from agentplugins/agent-plugins-spec,
revision `ff8ab5e392cc87bd88d87c060815a87490e51003` (schemas: Apache-2.0; see LICENSE.md and Apache-2.0.txt).

* `https://agent-plugins.org/schemas/1.0.0/plugin.schema.json`
  SHA256 `0a4aad95ce337878ad38802ebf0daa3fde76abe3f65400c86bcbb1ec0b3ab883`
* `https://agent-plugins.org/schemas/1.0.0/mcp.schema.json`
  SHA256 `6539175bfcdf43085855183e86da40ea94b166547a72b47ae9a0a390516d3acb`

Plugin manifests use the canonical 1.0.0 rules, including their exceptions for
unknown manifest fields and unimplemented extension namespaces. The 1.1.0
plugin-manifest working draft is unsupported. No network retrieval occurs.

MCP configurations use Pioneer's existing native MCP parser for both standalone
and package installation. The vendored MCP schema is an unchanged upstream
reference, not a second restrictive validation contract. Its identifier is not
required to install an MCP configuration, and native OAuth, policies, secrets
and timeouts remain supported. Package loading adds only bounds and package
path/ownership handling.
