# Threads Troubleshooting

## Tool Missing Or Operation Denied

Native: request_tools can open the registered `threads` domain when permitted. Request a domain, never individual names. CLI: the current turn catalog is fixed; inspect its callable names and available subset. A skill being installed does not grant a missing operation.

Access denial is not proof of source absence. Reads depend on current user rights and admitted workspace/thread scope; writes and starts also require valid opaque routes. Do not retry through a different tool to bypass denial, guess option IDs, or widen grants. Report the unavailable action without claiming success.

## Stale Options Or Cursors

Refresh threads_start_options for a stale/expired/revoked destination and re-evaluate the intended action. A fresh option does not grant new rights. If the operation may already have committed, resolve its replay outcome before issuing a fresh mutation.

Keep history, list, children, search, model/options, and result-reader cursors separate. Preserve the original operation parameters. History continuations omit anchor and match threadId, limit, direction; reverse only with reverseCursor/reverseDirection after a fragmented event is complete. Result-reader next is an object with version and Unicode scalar offset, not a string.

Source edits/deletion, changed authorization, or stale authenticated tokens can invalidate continuation. Restart the relevant read within current access when appropriate. Do not splice text from different source versions or claim an uninterrupted complete read after invalidation.

## Incomplete Search Coverage Or Missing Index

Inspect `coverageIncomplete` and `warnings`. Index/semantic unavailability can reduce coverage; unindexed history remains history. Empty matches do not prove absence. Use an accessible known source or threads_list plus selective history reading when the user's request requires it. Do not describe nextCursor as scanning leftover files, impose an artificial corpus limit, or claim a fixed ANN order.

## Incomplete Large Records Or Saved Results

Assemble search pageFragment text for the same source/version through complete. Assemble history/list metadata JSON by byte offsets through fragment eof. Follow the appropriate nextCursor before changing direction. For saved tool results, concatenate reader text with structured next until eof. Preserve the saved content's own truncation markers.

A short native working-context excerpt is not the full search page. Read its saved reference. After decoding the saved search page, use that page's search nextCursor to get further results. Reader EOF ends that saved result, not the complete search. A fallback fullResultSaved:false cannot restore unsaved output.

## Mutation Outcome Unknown

Creation, message append, and turn start have effects. An interrupted transport or missing response does not prove failure. Preserve the existing logical call identity and use the runtime's Agent action ledger/outbox replay; inspect the recovered result before deciding whether another distinct operation is necessary. Do not invent a JSON idempotencyKey, issue fresh create/send/start merely to see if the previous one worked, or claim completion without confirmation.

`status:denied` and an authorization outcome differ from `status:failed` or an execution/runtime-integrity failure. `accepted`/`queued` acknowledge admission or queued work; they do not prove execution or a final model answer. Outcomes such as agent_route_required, agent_route_revoked, agent_route_expired, agent_destination_unavailable, agent_execution_stale, agent_action_conflict, and agent_runtime_integrity_lost should be reported according to the actual safe result. Public unavailable outcomes may intentionally combine source disappearance and access loss; do not infer hidden existence.

## Identity Or Reasoning Rejected

Refresh options and the selected identity's models. Choose compatible current IDs and an admitted effort. `useDefaultReasoning:true` conflicts with explicit reasoning; remove the explicit field if default is intended. Legacy reasoning:null retains inheritance. Missing supported/effort metadata is unknown, not a promise that an effort works or that reasoning is absent. Do not substitute an invented model or a different identity without matching the user's intent and current permissions.
