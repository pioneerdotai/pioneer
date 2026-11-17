# Issue taxonomy

<!-- Modified for Pioneer. -->

Use this reference to calibrate an [exploratory review](dogfood.md), rather than
turning every entry into a required test or finding. Severity reflects the
observed impact in the target environment, not the amount of evidence captured.

## Severity

| Severity | Meaning |
| --- | --- |
| **critical** | Core workflow blocked for the relevant users, data loss or app crash with severe impact |
| **high** | Major feature broken or unusable, without a practical workaround |
| **medium** | Feature works with a meaningful problem; a practical workaround exists |
| **low** | Minor cosmetic, copy or polish problem with limited impact |

Explain the affected workflow and conditions when assigning severity. A single
failed request or console warning is not automatically a high-severity defect.
Separate confirmed impact from suspected broader impact, and label intermittent
behavior or limited reproduction honestly.

## Categories

### Visual / UI

- Broken layout, alignment, spacing or stacking.
- Overlap or clipping, missing images/icons, incorrect typography.
- Light/dark appearance problems or insufficient contrast.
- Responsive problems at relevant viewport sizes.
- Animation glitches or jank.

View the actual screenshot before asserting a visual defect. A DOM/snapshot
alone cannot establish clipping, overlap, color or contrast.

### Functional

- Broken links, incorrect destinations or redirects.
- Buttons that fail or silently do nothing.
- Validation accepting invalid input or rejecting valid input.
- Unexpected state loss across navigation/refresh.
- Double submission, stale data or other race symptoms.
- Broken search, filtering or pagination.
- Upload/download failures.

Record the expected success signal and the actual resulting state. Do not
replay writes or destructive actions outside the user's authorized test scope.

### UX

- Confusing navigation, defaults or inconsistent patterns.
- Missing loading/progress or success/failure feedback.
- Unclear error messages and dead ends.
- Missing safeguards for consequential actions, where relevant to the product.
- Poor keyboard navigation or focus handling.
- Missing or unhelpful empty states.

Distinguish a personal preference from a demonstrable usability problem. Explain
what a user cannot understand or accomplish, with a concrete example.

### Content

- Typos, grammatical mistakes or inconsistent terminology.
- Incorrect/outdated text or leftover placeholders.
- Truncated text with no usable way to read it.
- Missing or misleading labels.

### Performance

- Slow loading or interactions under recorded conditions.
- Janky scrolling/animation, layout shifts, degradation over time.
- Excessive requests, large assets or suspected memory growth.

Use measurements and conditions instead of universal cutoffs. Network/provider
latency or a visible delay does not by itself identify the app's bottleneck.
Use [profiling](profiling.md) or Web Vitals from [core workflows](core.md) when
they help establish impact; label suspected causes separately.

### Console / errors

- JavaScript exceptions or unhandled promise rejections.
- Failed requests, CORS/mixed-content errors or relevant warnings.
- Errors correlated with a specific user action or load state.

Record the correlated behavior and redact sensitive URLs/payloads. Distinguish
an intentional rejected request, harmless warning or extension noise from an
app defect. Avoid duplicate reports for one root symptom.

### Accessibility

- Unlabeled inputs, missing meaningful alternative text.
- Inaccessible keyboard controls, lost focus or focus traps.
- Contrast and semantic/ARIA problems affecting users.
- Dynamic content changes that are not conveyed appropriately.

State which keyboard/manual or automated checks were performed. An axe result
is evidence for its reported checks, not a complete accessibility certification.

## Exploration checklist

Select items that fit the request and environment:

1. Inspect relevant layouts and visible states; capture images when useful.
2. Exercise controls and observe feedback within authorized scope.
3. Try realistic form inputs, empty/invalid states and relevant boundaries.
4. Check navigation, history, deep links and return paths.
5. Inspect loading, empty, error and overflow states.
6. Correlate console/network failures with visible behavior.
7. Test viewport/appearance variants when relevant.
8. Check sign-in/role boundaries using accounts and environments already in scope.

Keep evidence proportional and coverage explicit. Do not invent missing
findings, roles or measurements to fill a checklist or report template.
