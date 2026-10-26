# Snapshots and refs

<!-- Modified for Pioneer. -->
All shell examples assume the task session from [SKILL.md](../SKILL.md).

## Contents

- [Choose a snapshot](#choose-a-snapshot)
- [Ref lifetime](#ref-lifetime)
- [Delta snapshots](#delta-snapshots)
- [Tabs and iframes](#tabs-and-iframes)
- [Recover from missing elements](#recover-from-missing-elements)

## Choose a snapshot

A snapshot gives an accessibility tree with compact refs such as `@e3`.
Use refs actually returned by the current session, not numbers inferred from
examples, an earlier unrelated task or a screenshot taken without annotation.

```bash
agent-browser snapshot
agent-browser snapshot -i
agent-browser snapshot -i -u
agent-browser snapshot -i -c
agent-browser snapshot -i -d 3
agent-browser snapshot -s "#main"
agent-browser snapshot -i --json
```

`-i` focuses on interactive controls, `-u` includes hrefs, `-c` compacts
structure, `-d` limits depth and `-s` scopes to a known selector. Full/scoped
snapshots and `read` are useful for non-interactive article/label text that
`-i` omits.

Example output (actual roles/format can vary by page and CLI version):

```text
@e1 [heading] "Edit profile"
@e2 [textbox] "Name"
@e3 [button] "Save"
```

After inspecting the actual output:

```bash
agent-browser fill @e2 "Example User"
agent-browser click @e3
agent-browser wait --text "Profile saved"
agent-browser snapshot -i
```

Use an app-specific result condition. A snapshot establishes observable
structure, not proof a preceding action completed successfully.

## Ref lifetime

A ref points at its DOM node while that node survives. Same-document updates
can preserve refs. Replaced elements and page/iframe document replacements
invalidate the corresponding refs. Invalidated ids are not recycled within
the session; virtual accessibility nodes have snapshot-local refs. JSON
snapshots report disappeared refs in `removedRefs`.

For a replaced node, the CLI may recover by unique role/name only when the
original match was unique. Ambiguous duplicates fail without acting. Do not
infer which duplicate it should have selected; snapshot again and choose the
right currently observed control.

Take a fresh snapshot after navigation, tab/frame changes or relevant dynamic
changes such as a menu opening. Reuse surviving refs when appropriate, but do
not assume every ref changes on every snapshot or that any ref survives a
navigation. After context loss, obtain a current snapshot rather than
reconstructing ids from a description.

## Delta snapshots

```bash
agent-browser snapshot -i --delta
agent-browser snapshot -i --delta
agent-browser snapshot -i --delta --full
```

History is per tab and option set. Responses are `full`, `unchanged` or
`delta`; URL changes and large changes can force a full response. A delta
must apply to its exact `baseRevision`.

For machine consumers, apply both:
- `changes` (`add`, `remove`, `replace`) to ref metadata;
- `treeChange` to the previous tree: split on newlines, replace
  `deleteCount` lines at zero-based `startLine` with `lines`, then join.

Only then advance to `revision`. If the baseline is missing after compaction,
a tool limit or a tab/option change, use `--delta --full`. A lone patch is not
a complete current tree.

## Tabs and iframes

Refs belong to the tab that was active for the snapshot. Switch with a returned
`tN` id, label or CDP target id, then snapshot; positional `tab 2` is invalid.
For shared Chrome, see [tab pinning](session-management.md#isolation-and-shared-chrome).

Main-frame snapshots inline one iframe level. Child refs carry their frame
context, so normal click/fill operations can use them directly. Nested frames
need explicit selection:

```bash
agent-browser frame @e4
agent-browser snapshot -i
agent-browser frame main
```

Use a real iframe ref; CSS selectors and frame name/URL matching are also
supported. Empty/inaccessible cross-origin frames may be absent. Inspect the
frame tree and select the appropriate frame rather than treating an omitted
tree as proof the page has no content.

## Recover from missing elements

For `Ref not found`, stale/ambiguous matches or a changed document, re-snapshot.
For an offscreen or virtualized control, scroll the relevant container and
wait for its actual render condition, then inspect again.

```bash
agent-browser scroll down 500
agent-browser wait --text "Expected item"
agent-browser snapshot -i
```

If a click is covered, handle the observed overlay first. For an overly large
tree, scope with `-s`, limit depth or read only the needed region. Prefer
semantic `find role/text/label` locators over guessed selectors when the
accessible control is identifiable without another full tree.

An annotated screenshot's numeric labels map to refs; an ordinary screenshot's
pixels do not identify refs by themselves. See [core workflows](core.md) for
capture and [tools and runtime](tools-and-runtime.md) for image inspection.
