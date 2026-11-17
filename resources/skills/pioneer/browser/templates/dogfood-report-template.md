<!-- Modified for Pioneer.
Copy this template from the skill package. Remove unused sections and template
instructions in the report. Use only observed values; keep auth state outside the
deliverable directory. Replace evidence placeholders with links to actual files
relative to the completed report, or returned artifact references. -->

# Quality review: {APP_NAME}

| Field | Value |
| --- | --- |
| Date/time and timezone | {OBSERVED_AT} |
| Target | {APP_URL_WITHOUT_SECRETS} |
| Environment and role | {ENVIRONMENT_AND_TEST_ROLE} |
| Scope | {REQUESTED_SCOPE} |
| Coverage | {PAGES_AND_FLOWS_CHECKED} |
| Limitations | {BLOCKED_OR_UNTESTED_AREAS_OR_NONE} |

## Findings

{MOST_CONSEQUENTIAL_OBSERVED_FINDINGS_OR_NO_ISSUES_IN_CHECKED_SCOPE}

| Severity | Confirmed issues |
| --- | --- |
| Critical | {COUNT} |
| High | {COUNT} |
| Medium | {COUNT} |
| Low | {COUNT} |
| Total | {COUNT} |

{INTERMITTENT_OBSERVATIONS_OR_UNCONFIRMED_HYPOTHESES_IF_ANY}

## Issues

<!-- Repeat this block per distinct finding. Keep steps and evidence proportional:
a static typo may need a location and one image; timing/motion may need a video.
Do not require an image for every step. Omit unused evidence rows. -->

### ISSUE-001: {SHORT_TITLE}

| Field | Value |
| --- | --- |
| Severity | {CRITICAL_HIGH_MEDIUM_LOW_AND_IMPACT} |
| Category | {VISUAL_FUNCTIONAL_UX_CONTENT_PERFORMANCE_CONSOLE_ACCESSIBILITY} |
| Location | {PAGE_URL_OR_VIEW_WITHOUT_SECRETS} |
| Reproducibility | {REPRODUCIBLE_OR_INTERMITTENT_WITH_ATTEMPTS} |
| Screenshot | {ACTUAL_PATH_E_G_screenshots/issue-001-result.png_OR_NOT_NEEDED} |
| Video | {ACTUAL_PATH_E_G_videos/issue-001-repro.webm_OR_NOT_NEEDED} |

**Initial state:** {ACCOUNT_ROLE_DATA_AND_PRECONDITIONS}

**Steps**

1. {NAVIGATE_TO_THE_RELEVANT_VIEW}
2. {PERFORM_THE_RELEVANT_ACTION_WITH_NON_SECRET_INPUT}
3. {OBSERVE_THE_RESULT}

**Expected:** {EXPECTED_BEHAVIOR}

**Observed:** {ACTUAL_BEHAVIOR_AND_USER_IMPACT}

**Supporting evidence:** {RELEVANT_INSPECTED_CAPTURE_MEASUREMENT_OR_REDACTED_ERROR}

**Limits or diagnosis:** {REPRODUCTION_LIMITS_OR_SEPARATELY_LABELED_CAUSE_HYPOTHESIS}

## Checks without findings

{IMPORTANT_COMPLETED_CHECKS_IF_USEFUL}

## Blocked checks and follow-up

{EXACT_MISSING_ACCESS_DATA_TOOL_OR_ENVIRONMENT_AND_EFFECT_ON_COVERAGE}
