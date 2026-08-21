# Attached Subagent Workflows And Examples

Use this reference for common current-turn subagent workflows.

## Contents

- Parallel Code Investigation
- Independent Review Subagent
- Parent Synthesis After Subagents
- Revision Workflow
- Cancel Or Detach

## Parallel Code Investigation

User:

```text
Find why explicit user identity facts are not being extracted into durable memory.
```

Good split:

- Subagent A: inspect post-turn extractor prompt and parser.
- Subagent B: inspect memory quality gate and scoring.
- Subagent C: inspect write provider calls and diagnostics.

Create all three first, then wait for the next actionable result:

```json
{
  "title": "Memory extraction investigation",
  "goal": "Find how post-turn extraction turns completed turns into durable memory write candidates.",
  "agentRole": "researcher",
  "instructions": [
    "Inspect the repository read-only.",
    "Return exact file:line references.",
    "Do not modify files."
  ],
  "inputText": "Focus on extractor eligibility, provider JSON parsing, quality gate routing, and write diagnostics.",
  "outputInstructions": "Return findings with evidence and a product-level explanation."
}
```

Then:

```json
{
  "runIds": ["RUN_A", "RUN_B", "RUN_C"],
  "timeoutMs": 120000,
  "returnCompleted": true,
  "returnPending": true
}
```

Review every returned candidate immediately. Accept evidence-backed results, revise incomplete results, or cancel unusable work. Then call `task_wait` again with the remaining active `runIds`. Repeat until all three runs are resolved, then synthesize one parent answer.

## Independent Review Subagent

Use when you made a change and want independent verification.

Child task:

```json
{
  "title": "Review memory prompt rewrite",
  "goal": "Review the edited memory prompt for incorrect semantics or missing safety constraints.",
  "agentRole": "reviewer",
  "instructions": [
    "Read the changed prompt files.",
    "Look for factual errors, missing warnings, and confusing instructions.",
    "Do not edit files."
  ],
  "inputText": "Changed files: crates/promt/src/render/memory_post_turn_extractor.rs.",
  "outputInstructions": "Return a code-review-style list of findings with file:line references. If no issues, say so and list residual risks."
}
```

Do not pass your intended conclusion. The point is independent validation.

## Parent Synthesis After Subagents

When all attached child work is accepted, do not paste raw child outputs blindly. Integrate them.

Good synthesis:

- start with the direct answer;
- mention child work only if useful;
- combine duplicate findings;
- keep evidence from children when important;
- call out uncertainty and failures.

Bad synthesis:

```text
Subagent A said...
Subagent B said...
Subagent C said...
```

unless the user asked for an audit trail.

## Revision Workflow

When a candidate is close but incomplete:

1. Identify the missing requirement.
2. Request revision with precise feedback.
3. Wait again.
4. Review the revised candidate.
5. Accept only the revised satisfactory candidate.

Example feedback:

```json
{
  "feedback": "The answer explains parser validation but does not say how weak candidates are rejected. Add a section 'Quality gate' with file:line evidence for rejection and suppression paths.",
  "additionalInstructions": [
    "Keep the existing correct parser explanation.",
    "Return the complete revised answer, not a diff."
  ]
}
```

## Cancel Or Detach

Cancel when:

- the user's goal changed;
- the child is working on the wrong thing;
- the result would be unsafe or irrelevant;
- a duplicate child is no longer needed.

Detach when:

- the task should keep running in the background;
- the parent can answer now without waiting;
- the user explicitly wants background work.

Do not detach review-required work. Resolve the candidate first.
