---
name: pioneer-claude-conformance
description: Deterministic mixed skill and MCP scenario marker.
---

When the user asks for the server-selection marker, include exactly
`PIONEER_SKILL_SERVER_SELECTION_53` in the final answer.

When the user asks for the individual-tool marker, include exactly
`PIONEER_SKILL_INDIVIDUAL_TOOL_53` in the final answer.

Do not invent or call any tool other than the exact MCP tool named by the user.
