# Interactive HTML and JavaScript

Read this for editors, calculators, control panels, and other HTML documents people operate rather than read straight through.

Implement local behavior with plain JavaScript in the document, for example recalculating totals as inputs change, filtering or sorting a table, switching tabs, or updating a chart from a scenario slider. Include the finished CSS and JavaScript in the HTML; no separate app or build step is needed. Use real event handlers and update the visible result when the user acts. These controls operate inside the document, not through agent tools or chat messages.

Lead with the current state and the next useful action. Organize the interface around the user's task, not the shape of the underlying data model.

- Make controls look actionable and label them with the result they produce.
- Show feedback close to the action: pending, success, empty, validation, and error states should all be designed.
- Preserve state visibly. Selected filters, active modes, changed values, and unsaved work should never be mysteries.
- Put summaries before detail when scanning matters, while keeping the path to the underlying evidence obvious.
- Use status color for status, independently from the artifact's decorative accent.
- Keep forms keyboard-friendly, labels explicit, targets comfortably sized, and destructive actions difficult to trigger accidentally.
- Prefer a few strong regions over a uniform grid of interchangeable cards.

Implement the important path when behavior is part of the request. A convincing static shell with dead controls is worse than a simpler interface whose important path works end to end.

On narrow screens, preserve the primary task. Reflow secondary panels, turn dense toolbars into deliberate controls, and contain wide data rather than shrinking it until it is illegible.


Apply the host theme, layout, runtime, and preview contract in SKILL.md.
