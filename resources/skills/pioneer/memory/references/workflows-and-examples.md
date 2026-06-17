# Memory Workflows And Examples

Use this reference for multi-step memory operations, proactive memory use, and common user requests.

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

If the old memory should be removed and the user asked for cleanup, forget the stale id after resolving it.

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

Then identify duplicates by meaning, scope, category, and key. Do not rely only on exact text match: "User's name is Alexander" and "The user's name is Alexander" are duplicates.

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
