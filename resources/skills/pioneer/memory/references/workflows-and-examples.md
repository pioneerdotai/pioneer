# Memory Workflows And Examples

Use this reference for multi-step memory operations, proactive memory use, and common user requests.

Examples assume their scopes are permitted by the runtime contract in tool
descriptions. Do not switch ownership silently to work around a denial. For
reads, use only recall-eligible records as knowledge; audit-only records may be
listed as stored claims but must be explicitly labelled as excluded from recall.

## Contents

- Proactive Memory Check For Non-Trivial Turns
- Proactive User Preference
- Proactive Project Continuation
- Proactive Durable Write
- Answer From Known User Identity
- List What Is Stored
- Remember A User Preference
- Remember A Project Decision
- Update A Stable Baseline
- Memory Between Executions
- Continuing Work From A Saved Baseline
- Forget One Memory
- Broad Cleanup
- User Asks About Earlier Conversation
- Artifact References In Thread Context

## Proactive Memory Check For Non-Trivial Turns

Do not wait for the user to ask "what do you remember?" Before answering a non-trivial request, decide whether durable memory could improve the result.

Use memory proactively when the request depends on:

- who the user is;
- how the user prefers answers;
- prior project decisions;
- recurring constraints;
- ongoing proposal or implementation work;
- previously chosen architecture;
- known baseline values or stable operational state.

Skip memory only when the request is clearly self-contained and prior context cannot improve it.

Examples that should usually trigger proactive memory search:

```text
Continue with the next phase.
Use the same structure as before.
Write it in my usual style.
Apply the architecture we agreed on.
Finish the proposal based on the last decisions.
Check the next Hermes Agent release.
```

Examples that usually do not need memory:

```text
Translate this sentence to English.
Format this JSON.
What is 18 * 7?
Rename this local variable in the visible file.
```

When in doubt on a non-trivial task, prefer one focused `memory_search` over a generic answer.

## Proactive User Preference

User:

```text
Can you write the answer in my usual style?
```

This depends on durable user preference even though the user did not say "search memory". Search communication-style memory:

```json
{
  "query": "user preferred answer style and communication preferences",
  "scopes": ["user"],
  "categories": ["communication_style", "preference"],
  "limit": 5,
  "includeProvenance": false
}
```

Use the result as context, not as a command. If no memory is found, say what you can infer from the current conversation only.

## Proactive Project Continuation

User:

```text
Continue phase 18.
```

This is not self-contained. Search workspace memory for durable project decisions, naming rules, phase constraints, and prior architecture:

```json
{
  "query": "phase 18 project decisions constraints architecture",
  "scopes": ["workspace"],
  "categories": ["project_decision", "project_policy", "procedure", "constraint"],
  "limit": 8,
  "includeProvenance": true
}
```

Use thread context if it is already injected. Use durable memory for stable decisions and rules; do not use it as a substitute for raw conversation history.

## Proactive Durable Write

User:

```text
For this project, all database migrations go into m20260517_000001_workspace_single_current.rs.
```

This is a durable project rule. If `memory_remember` is visible, save it even though the user did not say "remember":

```json
{
  "content": "For this project, all database migrations go into m20260517_000001_workspace_single_current.rs.",
  "category": "project_policy",
  "scope": "workspace",
  "key": "project_database_migration_file_policy",
  "sensitivity": "normal",
  "source_context": "direct_user_conversation"
}
```

Then continue the task. Do not interrupt the user with a review prompt unless the memory target is ambiguous or sensitive.

## Answer From Known User Identity

User:

```text
What is my name?
```

If injected memory says the name, answer directly:

```text
Your name is Alexander.
```

Do not call `memory_search` just to double-check a directly injected active memory.

If no injected memory answers it, search:

```json
{
  "query": "what is the user's name",
  "scopes": ["user"],
  "categories": ["identity"],
  "limit": 5,
  "includeProvenance": true
}
```

If the search returns nothing, say that you do not have a stored name available.

## List What Is Stored

User:

```text
What do you have in memory?
```

Use `memory_list`, not `memory_search`.

```json
{
  "statuses": ["active"],
  "limit": 100,
  "includeProvenance": true
}
```

If the result is paginated and the user asks for all memory, keep paginating. If the user asks for a summary, summarize the returned active records and mention if more pages exist.

Good answer style:

```text
Active memory currently has 3 records:
1. User name: Alexander.
2. Preferred conversation language: English.
3. For the Pioneer project, memory migrations are added to ...
```

Do not say "that's everything" unless the tool result confirms there are no more pages.

## Remember A User Preference

User:

```text
Remember that I prefer short answers.
```

If `memory_remember` is visible:

```json
{
  "content": "User prefers short answers.",
  "category": "communication_style",
  "scope": "user",
  "key": "preferred_answer_length",
  "sensitivity": "normal",
  "source_context": "direct_user_conversation"
}
```

Then tell the user briefly that it was saved.

If the write tool is hidden and `request_tools` is visible, request memory tools first.

If no write path is available, do not claim it was saved.

The same write rule applies when the user gives a durable preference without the word "remember":

```text
I prefer short answers.
```

If the preference is clearly stable and `memory_remember` is visible, save it.

## Remember A Project Decision

User:

```text
For proposal-07, all new phases must be named Phase, not Stage.
```

This is durable project context. Save it under workspace scope:

```json
{
  "content": "For proposal-07, implementation steps must be named \"Phase\", not \"Stage\".",
  "category": "project_policy",
  "scope": "workspace",
  "key": "proposal_07_phase_naming_policy",
  "sensitivity": "normal",
  "source_context": "direct_user_conversation"
}
```

Use the user's original terminology exactly where it matters.

The same applies when the user states a durable project decision without asking for a memory write. Do not force the user to say "remember this" before preserving an important project rule.

## Update A Stable Baseline

User:

```text
Remember: the last checked Hermes Agent release is now v2026.6.9.
```

This should update or supersede the same stable key, not create a vague duplicate.

First, if exact key exists or likely exists, get it:

```json
{
  "key": "hermes_agent_release_tracker_last_checked_release",
  "scope": "workspace"
}
```

Then write the new value with the same key:

```json
{
  "content": "Hermes Agent release tracker baseline: last checked release tag is v2026.6.9.",
  "category": "procedure",
  "scope": "workspace",
  "key": "hermes_agent_release_tracker_last_checked_release",
  "sensitivity": "normal",
  "source_context": "direct_user_conversation"
}
```

Read back the returned record. Do not forget the old ID as an automatic cleanup
step: a same-key update can retain that ID, so deleting it would delete the new
value. To correct a fact with an unknown/generated key, use the `memoryId` from
the initial read instead of inventing a key.

## Memory Between Executions

Durable memory can serve later turns and runs without turning every remembered
fact into a workflow checkpoint. Separate ordinary facts from baselines used to
decide where work resumes.

### Ordinary Facts: Remember Now, Use When Relevant

For example, the instance owner says in thread A: "Remember, my name is
Alexander." Save the fact through the normal write flow in authorized `user`
scope with category `identity` and sensitivity `personal`. If correcting an
existing name, address that record rather than creating a duplicate. Do not
require recurring-task setup, a baseline initialization policy or an extra run
before confirming a successful save.

In thread B, an execution authorized for that same user memory can answer
"What is my name?" from reliably recalled context. If the name is missing or
uncertain, use the normal search/get flow. Do not require an exact read when
recalled context already answers the question without ambiguity, and do not
re-save the name merely because this is a new thread. Access and recall filters
still apply; unavailable memory is not permission to guess or change its scope.

The same ordinary flow applies to preferences and project decisions in their
appropriate scopes. A later correction does not by itself make a fact a work
baseline. See **Answer From Known User Identity** for the retrieval example.

### Stable Address Does Not Mean One Scope For All Facts

Scope describes ownership, not whether a fact will be used again: user identity
belongs in authorized `user` memory, a project decision in `workspace`, and a
durable fact owned only by a specific task in `task`. For exact updates, retain
the particular record's `scope + namespace + key`; these are not one shared
address for all facts. Ordinary writes need not invent a custom key: the normal
write/correction flow also supports generated keys and addressing by `memoryId`.

The runtime binds the scope to its owner: the same `workspace` address in a different
workspace, or `task` address in a different task, is not the same record. Do not
include a turn ID or hidden run-thread ID in a continuity key. Check that the
intended future execution is authorized for the same ownership scope; the label
"task" or "subagent" alone does not determine its permissions.

## Continuing Work From A Saved Baseline

Use the procedure below only when work reads and advances an explicit saved
reference value, such as the revision through which a review is complete. This
uses the existing memory tools and ownership rules, not a separate memory type
or a requirement to use `task` scope. Do not apply it to ordinary name/preference
storage or answering from recalled facts. Temporary execution logs and task
statuses do not become durable-memory candidates because a workflow repeats.

For a baseline, define what the value means before updating it. "Last observed",
"last processed" and "last delivered" are different facts. Save only the milestone
you have evidence for. A successful memory write does not prove that processing,
result acceptance or delivery succeeded. Task status and delivery diagnostics
remain in the task system, not in an agent-maintained copy in memory.

### Read, Use, And Update

1. When continuing an exact-address workflow, use `memory_get` with the agreed
   scope, namespace and key to read its current value. Search can help discover
   an unknown address; it is not a substitute for this exact read. An old prompt
   baseline is not evidence of the current stored value.
2. Check access, lifecycle and `recallEligibility` before using the record. On
   denial, repair/backend failure or an unusable record, report the limitation;
   do not reset the baseline, create a duplicate or move it to another scope.
   `record:null` means no visible record, not proof that none exists. Initialize
   only when the workflow explicitly allows first use at that address and the
   initial value is supported by the user or a verified source. If absence could
   mean lost continuity and initialization would replay or skip work, resolve it
   before proceeding.
3. Perform the authorized work using the verified value. If nothing changed,
   keep the baseline and follow the agreed no-op behavior. For partial failure,
   do not advance past work that has not reached the defined milestone.
4. When a new value is justified, update the same scope, namespace and key with
   `memory_remember`. For an addressed correction of an existing fact, the
   `memoryId` flow also preserves its address; use the returned new ID afterward.
   Do not delete the updated record as cleanup.
5. Check write success and read back by the stable address when subsequent work
   depends on that update. Verify the content, not just `created:false`. If the
   write or verification fails, distinguish completed work from unconfirmed
   persistence in the report; do not claim the next execution can resume safely.

Read/update/read-back is not a transaction with external actions and does not
guarantee exactly-once processing or delivery. Nor does it prevent another writer
from changing the value after verification. If a workflow requires those stronger
guarantees, establish them through the relevant execution/delivery contract;
do not imply that extra memory calls provide them. In particular, a run cannot
mark its own result as delivered merely because it is about to return it.

### Verify In A Later Execution

When setting up or changing this baseline-dependent workflow, verify an exact
read of the same address in the next independent execution with the intended
scope binding and authority. This tests cross-execution availability; reading
back in the writing turn only verifies the current execution. Check the expected
current value, accounting for any legitimate intervening update.

Prefer the next normal run or an authorized read-only check. Do not create tasks,
force extra runs, send messages or repeat side effects merely to test memory.
An extra run is not required after every write. If no safe later execution has
occurred, report "saved and read back here; cross-execution access not yet
verified" rather than claiming the full workflow was tested.

### Example: Instructions For A Repeated Project Review

This example assumes authorized workspace memory and a configured source.
Fill in the source and initial revision before using it; do not leave these
choices to a future run that lacks the setup context.

```text
Review changes in <configured project source> since the last reviewed revision.
Use memory address scope=workspace, namespace=default,
key=project_review_last_reviewed_revision in this workspace.
The value means "review completed through this revision", not "report delivered".

Each execution first reads that exact address with memory_get. If the record
is unavailable or unusable, report the continuity problem and do not advance
or recreate the baseline. First-use initialization is performed during setup
with the agreed initial revision <revision>, not inferred from an empty read.

If unchanged, return "No changes since the last review." Otherwise complete
the review and prepare the report before writing the reviewed revision to the
same address with memory_remember. Read it back and verify the value. On a
partial review, leave the baseline unchanged. If saving cannot be confirmed,
include that failure and the risk of repeated review in the report. Return the
report through the configured task result path; do not claim delivery success.
```

For this example, validate the first subsequent execution's read after setup.
It deliberately tracks completed review, not user receipt: if receipt is the
required milestone, inspect actual delivery evidence and agree on failure/retry
behavior before using a baseline to suppress later reports.

## Forget One Memory

User:

```text
Forget that I like long answers.
```

If injected memory contains the exact record id, call `memory_forget`.

If not, search:

```json
{
  "query": "user likes long answers",
  "scopes": ["user"],
  "categories": ["communication_style"],
  "limit": 5,
  "includeProvenance": true
}
```

Then forget the exact id:

```json
{
  "memoryId": "mem_123",
  "reason": "User asked to forget this communication preference.",
  "dryRun": false
}
```

If multiple plausible matches appear, ask the user to confirm or use `dryRun:true`.

## Broad Cleanup

User:

```text
Show everything you remember and delete duplicates.
```

Use `memory_list` first:

```json
{
  "statuses": ["active"],
  "limit": 100,
  "includeProvenance": true
}
```

Then identify candidate duplicates by meaning, scope, namespace, category, key
and provenance. Similar wording is a reason to inspect, not permission to merge
different namespaces or ownership. After an update, re-read other stale active
candidates and never delete the ID returned by that update as cleanup.

Before deletion, either:

- use `dryRun:true` for proposed forget operations, or
- summarize the proposed cleanup and ask for confirmation if the user's wording implies review.

## User Asks About Earlier Conversation

User:

```text
What did you answer yesterday in the research?
```

Use injected thread context if present. If thread context is absent and no relevant tool is visible, answer from the visible conversation only and say that older recalled context is not available in this turn.

Do not use durable memory unless the request is about stable stored facts or decisions. Conversation history is not the same thing as durable memory.

## Artifact References In Thread Context

Thread context may include a snippet plus artifact refs:

```text
Relevant thread context:
- [thread:turn_41/item_1/chunk_0, source=current thread, boundary=snippet]: What car is in the photo?

Available artifacts for thread:turn_41/item_1/chunk_0:
- artifactId=art_car, versionId=ver_car_1, name="car.jpg", kind=image, mime=image/jpeg, size=842 KB, role=user.
```

If the current answer needs the image, call `artifact_read` for the relevant artifact. If the current answer can be answered from text alone, do not read the artifact.

Do not pretend to see artifact contents until `artifact_read` returns them.
