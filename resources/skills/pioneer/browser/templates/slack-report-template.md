<!-- Modified for Pioneer.
Remove unused sections and template instructions after copying. Preserve only
task-relevant excerpts. Replace evidence placeholders with actual message links,
report-relative file links or returned artifact references. Do not paste raw
snapshots, credentials, invented metrics or unrelated personal information. -->

# Slack findings: {REQUESTED_TOPIC}

| Field | Value |
| --- | --- |
| Observed at | {DATE_TIME_AND_TIMEZONE} |
| Workspace | {WORKSPACE_NAME} |
| Scope | {CHANNELS_DMS_QUERY_DATE_RANGE_OR_OTHER_REQUESTED_SCOPE} |
| Source | {CONNECTOR_OR_BROWSER_VIEWS_ACTUALLY_INSPECTED} |
| Coverage | {COMPLETE_WITHIN_SCOPE_OR_OBSERVED_SUBSET_WITH_LIMITS} |

## Findings

{KEY_FINDINGS_WITH_SUPPORTING_MESSAGE_LINKS}

## Unread observations

<!-- Activity entries, unread conversations and unread messages are distinct.
Views can overlap. Do not add them into one total. Capture the baseline before
opening conversations, which can mark them read. Use unknown when unmeasured. -->

| View/filter | Observed measure | Count or status | Coverage/time |
| --- | --- | --- | --- |
| Activity / {FILTER} | {ACTIVITY_ENTRIES_OR_OTHER_MEASURE} | {OBSERVED_OR_UNKNOWN} | {COVERAGE} |
| DMs | Unread conversations | {OBSERVED_OR_UNKNOWN} | {COVERAGE} |
| Sidebar / {SECTION} | Channels with unread indicators | {OBSERVED_OR_UNKNOWN} | {COVERAGE} |

| Conversation | Indicator or exact count if shown | Relevant context | Evidence |
| --- | --- | --- | --- |
| {CHANNEL_OR_DM} | {OBSERVED_BADGE_OR_UNKNOWN} | {TASK_RELEVANT_NOTE} | {LINK_OR_INSPECTED_CAPTURE} |

## Channel inventory

{STATE_WHETHER_THIS_IS_JOINED_SIDEBAR_CHANNELS_A_DIRECTORY_OR_ANOTHER_SUBSET}

| Channel | Visibility if shown | Section or topic if relevant | Evidence |
| --- | --- | --- | --- |
| {CHANNEL} | {PUBLIC_PRIVATE_OR_UNKNOWN} | {OBSERVED_CONTEXT} | {CHANNEL_LINK} |

## Conversations and search results

**Query/filter:** {EXACT_QUERY_AND_DATE_RANGE_IF_SEARCHED}

**Result coverage:** {PAGES_OR_RANGES_INSPECTED_AND_UNSEEN_REPLIES}

| Sender | Displayed time | Conversation | Relevant excerpt or summary | Source |
| --- | --- | --- | --- | --- |
| {OBSERVED_NAME} | {TIME_AND_KNOWN_TIMEZONE_OR_UNKNOWN} | {CHANNEL_OR_DM} | {RELEVANT_EXCERPT} | {MESSAGE_PERMALINK} |

{NECESSARY_THREAD_CONTEXT_DECISIONS_AND_ACTION_ITEMS_WITH_SOURCE_LINKS}

## Pins and reactions

{OBSERVED_PINS_REACTION_COUNTS_AND_VISIBLE_REACTORS_IF_REQUESTED}

## Activity analysis

<!-- Optional. Include rankings, participant counts or timing statistics only
when collected data and the user's request justify them. Never infer a team-wide
trend from one rendered viewport or fabricate rows to complete the template. -->

{MEASURE_DEFINITION_TIME_WINDOW_DATA_COVERAGE_AND_OBSERVED_RESULT}

## Evidence and limitations

| Evidence | What it supports |
| --- | --- |
| {ACTUAL_LINK_OR_FILE} | {SPECIFIC_FINDING} |

{MISSING_ACCESS_VIRTUALIZATION_FILTER_LIMITS_UNKNOWN_COUNTS_AND_OTHER_UNCERTAINTY}

## Follow-up

{RECOMMENDED_NEXT_STEPS_IF_USEFUL_DISTINGUISHED_FROM_ACTIONS_ACTUALLY_TAKEN}
