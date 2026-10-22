---
name: runtime-diagnostics
slug: runtime-diagnostics
owner: pioneer
description: "Investigate Pioneer Gateway or desktop CPU, memory, slow work, queues, and runtime stalls using process metrics, local snapshots, bounded native profiles, and exact release symbols. Use for Pioneer performance diagnosis and interpreting its diagnostic artifacts."
version: "0.1.0"
user-invocable: true
disable-model-invocation: false
implicit-invocation: user-controlled
---

# Pioneer Runtime Diagnostics

Find which process consumes resources, what work initiated it, where it waits,
and which functions run during the problem. Use the existing `pioneer diagnostics`
commands and telemetry before proposing new instrumentation or changing code.

## Protect the session that is doing the investigation

The investigating agent may run inside the Gateway, desktop, or a child of the
process it is investigating. Treat that dependency as present unless verified
otherwise. **Never stop, restart, replace, upgrade, or relaunch those processes
from the agent's tools.** Do not run `pioneer stop`, `pioneer start`,
`gateway-service`, `launch-memory`, service-manager mutations, quit commands, or
signals against them. Do not stop their containers, remote hosts, or supervisors.
A detached shell, background task, or subagent does not make a restart safe.

Snapshot, bundle, symbol download/validation, and a bounded native attach capture
can usually run autonomously under an authorized diagnostic request. Preserve the
user's telemetry preference and OS attach permissions. If a restart is required,
prepare the complete user handoff in [references/restart-memory.md](references/restart-memory.md):
exact paths and commands, what to reproduce, when to stop, how to restore ordinary
launch, and how to continue this investigation. The **user** executes that handoff
from a terminal outside Pioneer. Do not execute its restart blocks yourself.

## Four capabilities

1. **Process resources and identity:** one run/build identity; CPU and OS memory/IO
   snapshots for the target and registered children, with freshness and coverage.
2. **Native CPU and memory captures:** bounded on-demand profiles, manifests,
   private artifacts, and exact Mach-O UUID / ELF build ID / PE GUID+age symbols.
3. **Work ownership:** parent/linked work, trigger, attempt, executor and available
   trace/span context carried across instrumented execution boundaries.
4. **Live diagnosis:** active work and waits, queue accounting, database pool
   state, runtime/foreground responsiveness, export health and bounded prehistory.

These are complementary signals. A long span is not measured CPU. An allocation
stack is not an ownership/retainer graph. A successful export is not proof that
the backend retained every event.

## Normal autonomous workflow

1. Discover the actual installed CLI, OS/architecture, runtime home, live runs,
   process ownership and available tools. Read [references/signals-and-triage.md](references/signals-and-triage.md).
   The skill may be available before a binary supporting diagnostics is installed;
   report that mismatch instead of installing or restarting the hosting app.
2. Save an initial snapshot, a later snapshot after a few normal sample periods,
   and a bundle for the exact run. Keep capture output in a private case directory.
   Prefer exact run IDs over `gateway`/`desktop` when several instances exist.
3. Separate target CPU/memory from registered children and profiler cost. Check
   freshness, consent generation, unknown fields, queue/liveness evidence and
   instrumentation loss before naming a cause.
4. If function-level evidence is needed, read [references/captures.md](references/captures.md)
   and take the smallest relevant supported capture. Use attach first. Prepare
   user instructions for macOS allocation history only when attach cannot answer
   the question; read the restart reference before planning it.
5. Read [references/symbols.md](references/symbols.md) to download the matching private
   `Build Debug Symbols` artifact, validate it against the capture and executable,
   and resolve addresses. Prefer existing artifacts; local LLVM builds and release
   rebuilds are not a normal diagnostic step.
6. Follow the evidence to the initiating work and concrete functions. Give a
   short verdict, evidence paths/time window, missing coverage and a scoped next
   action. A single failed native capture is not proof that ordinary telemetry
   failed. Finish when the requested question is answered; do not launch a broad
   benchmark or repeated capture campaign without a reason.

## Operating bounds

- Keep at least **5 GiB free** on every volume holding inputs, symbols, profiles,
  or temporary logs. Check before downloads and capture and during large work;
  leave headroom for growth between checks. The product's artifact quotas are
  not a whole-disk free-space guard. Stop diagnostic work before crossing the floor.
- Native profilers can consume several GiB of RAM and substantial CPU/IO. Report
  their cost separately from the target. Do not promise free or zero-overhead diagnosis.
- Run one capture per target at a time. Use `diagnostics cancel --target RUN_ID`
  to stop the native capture worker, never kill the Gateway/desktop to cancel it.
- Preserve private endpoint tokens, paths, native output and symbol artifacts.
  Do not print endpoint descriptor JSON or upload raw bundles to public services.
  A request to investigate does not authorize publishing diagnostic data.
- Cleanup only case-owned files after identifying the exact path, size, ownership,
  active readers/writers and evidence that must remain. Never remove a shared
  Cargo target, running preparation logs, retained release inputs, or unrelated
  files merely to make room.
- Use only callable tools and installed commands. Skill references are accessed
  through the same package/authority as this file, or relative to its filesystem
  directory when exported locally. Do not treat a `skill://` identifier as a path.
