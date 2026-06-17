# Scopes, Categories, Keys, And Write Quality

Use this reference when writing memory or choosing filters for reads. The goal is to store the right fact in the right place, with a stable shape that future turns can find and update.

## Contents

- Scopes
- Categories
- Keys
- Sensitivity
- Confidence
- Importance
- Durable Memory Versus Thread Context

## Scopes

Choose the narrowest truthful scope.

### user

Use `user` for facts that follow the person across projects and threads:

- name and identity;
- stable preferences;
- communication style;
- durable biography;
- relationships;
- recurring personal instructions.

Examples:

```text
User's name is Alexander.
User prefers concise answers.
User prefers Russian for direct conversation.
```

### workspace

Use `workspace` for project/repository/product context:

- project rules;
- architecture decisions;
- migration policy;
- coding conventions;
- recurring project procedures;
- operational baselines tied to a project.

Examples:

```text
Proposal phases must keep the "Phase" naming.
Memory migrations for the current workspace are added to m20260517_000001_workspace_single_current.rs.
Hermes Agent release tracker baseline: last checked release tag is v2026.6.5.
```

### thread

Use `thread` rarely. Most conversation history belongs to thread context, not durable memory. Use durable `thread` memory only for a long-lived thread-local fact that is clearly meant to survive as memory for this thread.

### agent

Use `agent` only for stable, user-approved facts about the agent itself. Do not save assistant self-description just because the assistant said it.

### task

Use `task` rarely for durable task-scoped facts. Do not store ordinary temporary task progress.

## Categories

Use the narrowest correct category:

- `identity`: names and identity facts.
- `preference`: stable preferences not specifically about communication.
- `biography`: durable life/background facts.
- `relationship`: durable relationship facts.
- `recurring_instruction`: standing instructions for future turns.
- `project_policy`: durable project rules and constraints.
- `project_fact`: stable facts about a project.
- `project_decision`: decisions made for a project.
- `procedure`: repeatable workflows or operational baselines.
- `todo`: durable todo-like facts meant to survive the turn.
- `constraint`: durable limits or requirements.
- `communication_style`: tone, language, brevity, formatting, review style.
- `custom`: only when no typed category fits.

If more than one category seems plausible, choose the one that will help future retrieval most. For example, "always answer me in Russian" is `communication_style`, not generic `preference`.

## Keys

Use a stable `key` when future lookup or update should be exact.

Good keys are lowercase, stable, and domain-specific:

```text
preferred_response_language
preferred_answer_length
project_migration_policy
proposal_phase_naming_policy
hermes_agent_release_tracker_last_checked_release
```

Avoid vague keys:

```text
info
memory
preference
note
misc
```

Use keys for:

- recurring workflows;
- baselines that will be updated;
- preferences that should not duplicate;
- project policies;
- facts with a natural canonical identity.

Do not force a key when the memory is a one-off durable fact without a stable update path.

## Sensitivity

Default to `normal` for non-sensitive project and preference records.

Use `personal` for ordinary personal facts such as name, language preference, location preference, birthday, relationships, or biography.

Use `secret_like` for anything that resembles a credential, token, API key, private key, password, or authentication material. Normally do not store these.

Use `regulated` for sensitive health, legal, financial, or similarly regulated personal information. Do not store it unless the user explicitly asks and policy allows it.

## Confidence

Confidence is about whether the fact is supported by the conversation, not whether it is important.

High confidence:

- the user explicitly says the fact;
- the user confirms a proposed fact;
- the user asks to remember the fact;
- the fact is a stable project decision stated directly in the current conversation.
- the user states a current durable rule, preference, baseline, or constraint without using a "remember this" phrase.

Lower confidence:

- the fact is inferred indirectly;
- the statement is uncertain, sarcastic, hypothetical, or quoted from someone else;
- the assistant produced the claim without user confirmation;
- the fact conflicts with existing memory.

When confidence is low, do not write automatically. Only write if the user explicitly requested it and the wording preserves uncertainty.

## Importance

Importance is about future utility.

High importance:

- identity;
- durable user preference;
- recurring instruction;
- project rule or decision;
- operational baseline used repeatedly;
- fact likely to prevent future mistakes.

Low importance:

- temporary task progress;
- one-off debugging details;
- short-lived planning state;
- raw command output;
- facts that will not matter after this turn.

Do not save low-importance facts just because they are true.

## Durable Memory Versus Thread Context

Durable memory is for facts that should survive independently.

Thread context is for conversation history, prior answers, snippets, files, and artifacts. Use recalled thread context for earlier-discussion and continuation questions when it is available. Do not turn raw conversation history into durable memory unless the user preserves a stable conclusion, rule, or decision from it.
