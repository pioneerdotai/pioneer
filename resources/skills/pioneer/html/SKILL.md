---
name: html
slug: html
owner: pioneer
description: "Create inline HTML documents with CSS and embedded JavaScript in Pioneer messages, including interactive calculators, charts, and small working interfaces. Do not use for ordinary application edits merely involving HTML."
version: "0.1.0"
user-invocable: true
disable-model-invocation: false
implicit-invocation: required
catalog-hide: true
---

# Pioneer Inline HTML and JavaScript

Inline HTML supports CSS and embedded JavaScript that runs inside the document. Use JavaScript for working calculators, filters, tabs, scenario controls, and charts that respond to user input. Keep reading-focused documents static when interaction adds no value.

Use inline HTML to make relationships, comparisons, data, or interactive calculations easier to understand. Choose the form from the user's question: a document for reading, aligned rows for comparison, a diagram for relationships, a chart or table for data, or a small working interface for exploration. Plain text is sufficient for simple answers.

## Workflow

1. Prepare one complete HTML document with ready-to-run CSS and JavaScript. No TypeScript, TSX, or required build step.
2. When `html_preview` is available and image input is supported, call it using the callable name in the current runtime catalog.
3. For a successful preview, inspect all six real images and each frame's diagnostics: light at 360, 768, 1024 CSS px, then dark at the same widths. Correct clipping, overflow, contrast, layout, script errors, and truncated captures; preview again as needed.
4. Publish the finished version with permitted `html_render`. It already saves and publishes the document; do not add `artifact_register`. Iterate through preview rather than publishing drafts.
5. Add only necessary text that complements the HTML. Follow the current answer delivery and review rules; HTML does not bypass them.

If preview is unavailable, or image capability is explicitly unsupported or unconfirmed, do not repeat a known unsupported preview without changed conditions. Do not claim visual verification. You may publish with permitted `html_render` while clearly telling the user that visual verification was unavailable. If visual verification is necessary for a reliable result, provide a text answer instead. The document must still support both themes.

Preview checks initial rendering in both standard Pioneer themes, not every interaction or every future custom palette. Exercise interactions when an authorized tool supports it; never claim actions were tested from static screenshots alone.

## Host contract

Adapt to message width and let content determine height. Do not add an outer decorative page shell, horizontal page padding, or viewport-height layouts that interfere with height measurement. Contain intentionally wide tables or diagrams locally.

Use host-provided `--pioneer-*` variables for primary styles. Support both light and dark and live theme updates. Never redefine reserved host tokens with a fixed palette. The application supplies the actual theme; `prefers-color-scheme` alone cannot determine the user's Pioneer choice. JavaScript-rendered Canvas/charts must reread computed tokens and redraw on `pioneer-theme`.

Internal controls must work within the document. HTML buttons cannot call agent tools, send chat messages, or grant permissions. Do not promise persistent DOM state: the webview may be recreated.

## Read the matching references

- Always read [references/theme-and-layout.md](references/theme-and-layout.md) before styling.
- Read [references/tools-and-runtime.md](references/tools-and-runtime.md) for tool inputs, diagnostics, publication, and environment limits.
- Read [references/documents-and-plans.md](references/documents-and-plans.md) for documents, comparisons, plans, and roadmaps.
- Read [references/diagrams.md](references/diagrams.md) for architecture, sequence, topology, and state.
- Read [references/charts-and-data.md](references/charts-and-data.md) for charts, tables, and quantitative claims.
- Read [references/interactive-content.md](references/interactive-content.md) for calculators and working interfaces.

In native runtime, use the exact `skill:<id>` from Internal Skill References; resolve package paths under the returned `skill_asset_root`. In Codex, Claude, and OpenCode, `system:pioneer/html` means the exported `pioneer-system-html` skill; resolve references relative to its loaded directory. Use only tools actually present in the runtime catalog. Skills do not grant permissions.
