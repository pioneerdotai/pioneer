# Contextual voice ASR: repeat-review handoff

Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/voice-contextual-transcription`.
Branch: `fix/voice-contextual-transcription`.
Base: `df89ef2442622f2c6c4275caa9ae1b8a10eae7c2`.
The user accepted this implementation for commit and PR publication.
No tests, compilation, application, models, benchmarks or audio experiments
were run during implementation and review.

Changed files:

- `voice/incremental.rs`: bounded window ownership, real timing validation,
  global text projection, retained left context, Commit draining and regressions.
- `voice/transcription.rs`: private hypothesis with optional SDK metadata and
  text-only default/Box forwarding; final protocol transcript is unchanged.
- `voice/runtime.rs`: retain SDK results for all engines; request word timing
  from the existing Parakeet adapter, without changing recognition parameters.
- `voice/supervisor.rs`: forward hypotheses under the existing lock/fences and
  test metadata forwarding/cancellation/generation handling.
- `voice/session_buffer.rs`: the existing revision's private skip-preview hint;
  queue/close/sequence/budget behavior is unchanged.
- `message/tests.rs`: actual owned-ingress regression cases and native PCM/work
  assertions. No production handler changes.
- This document: contract, limits, audits and deferred validation.

Rust paths above are relative to `crates/gateway/src/`.

## Contract recorded before implementation, finalized after static review

One existing tracked worker owns VAD, frame remainder, unique source counters,
a confirmed prefix and a replaceable tail. Ordered PCM/duplicates/admission,
leases, cancellation, generation fences and publication retain their existing
owners. No per-packet native task, inference queue, database work or public
configuration is introduced.

The PCM window remains bounded to 18 s, including up to 1.5 s of already confirmed
left context. Reaching 12, 15 or 18 s never starts partition probes. Only an
attempt to accept further speech into a full window requires advancement. A
VAD end or Commit seals the outstanding span. During open recording, a preview
runs after at least 3 s of new speech when ingress is caught up. Skipped previews
are coalesced into the latest available hypothesis before waiting for input.

The former whole == left + right acceptance gate and content-dependent recording
refusal have been removed. Punctuation, case, spelling or rephrasing differences
are input to alignment, never a transcription error. Real native errors still
propagate and prevent any partial user turn.

## Native metadata and timestamp path

The installed SDK inspected is `transcribe-rs 0.3.12`, exactly as pinned in
`Cargo.lock`. Its `TranscriptionResult` has text and optional segments, without
common confidence. All eight existing native adapters now preserve that result
through the private `VoiceSpeechHypothesis` boundary and the existing supervisor
mutex/fences. Legacy text consumers still receive String. The trait's default
hypothesis method wraps text-only transcribers; Box forwards overrides.

Parakeet's existing model-specific call requests Word instead of Segment
metadata; the decoder/model, language and all other inference parameters remain
unchanged. Its SDK still adds and compensates 250 ms leading silence. Whisper
and SenseVoice can also return timing; Moonshine Base/Streaming, GigaAM, Canary
and Cohere provide no timing here. No model-name switch selects window policy.

Timing is usable only if finite, ordered, non-overlapping, within actual source
input and exactly covering the full text in order (whitespace between segments
is allowed). Metadata beyond 2,048 segments is dropped from retained hypotheses.
Reported seconds are rounded to source-sample coordinates. Empty, invalid,
text-mismatched or unusably coarse metadata takes the text fallback, without
turning an otherwise successful inference into Failed.

For advancement, select a reported end inside [confirmed context + 6 s,
window end - 3 s], nearest the low-energy candidate. Also require its text end
to be between whitespace-delimited units: the SDK's generic segments can be
subword tokens, so a token timestamp alone must not confirm a partial word.
On later refreshes, use timing to exclude confirmed context only if its fence
lies between segments at such a text boundary. A segment straddling that fence
cannot separate the context and triggers text alignment instead. These remain
model time estimates, not proven acoustic or linguistic boundaries.

## Common fallback without usable timestamps

Choose a deterministic low-energy 60 ms neighborhood inside the same admissible
interval, then decode non-overlapping source prefix and suffix. The low-energy
candidate is never used by itself. Project that known audio partition into the
full contextual result using two global weighted edit-distance rows, minimizing:

    distance(prefix probe, whole[..split]) + distance(suffix probe, whole[split..])

A unit is normally a whitespace-delimited token. Han (including supplementary
planes), kana and Hangul runs are instead split at UTF-8 scalar boundaries,
with following punctuation/combining kana marks attached. Thus `甲乙丙丁` and
probes `甲乙` / `丙丁` have a zero-cost projection after `乙`, rather than a
single indivisible whole that can project to zero. This is comparison
segmentation, not a claim that a scalar is a linguistic word.

Construct bounded coarse units for all three strings. If the final left unit
followed by the first right unit forms a prefix of any coarse whole unit, that
unit may contain the partition internally: refine **all three sequences** to
Unicode scalars for this projection, regardless of surrounding correct words.
Check raw units first, then nonempty normalized keys; raw matching matters when
punctuation at a probe edge is internal punctuation in the whole. Also keep the
single-whole-unit fallback when both references contain text but disagree.
This cue selects comparison resolution only; it does not confirm a text/audio
boundary. The full global alignment still chooses ownership, including repeats.
Construction groups units to the same 2,048-unit limit and performs just one
edit-distance projection. For `ทดสอบระบบ` / `ทดสอบ` / `ระบบ`, identical scalar
sequences give a zero-cost interior split after `ทดสอบ`. Adding ` OK` or ` OK OK`
to the whole and right no longer disables that split; the relevant coarse unit
remains `ทดสอบระบบ`. The same applies with other correct words before it or with
new text after the shared suffix on a later refresh.
Using one shared resolution also avoids comparing a one-word probe's scalars
against word units in a multiword whole. No additional script ranges, dependency,
language detection or native calls are needed. The separate SDK
subword/timestamp boundary guard remains unchanged. Costs are 0 for identical raw
units, 1 for equal lowercase keys with edge punctuation trimmed, 3 for a
substitution and 2 for insertion/deletion. Internal punctuation and word
separators remain in keys. This handles extra/missing words and rephrasing as
well as punctuation/case, rather than merely stripping punctuation and requiring
equality. Every repeated occurrence participates in the full alignment; there
is no suffix/prefix stitching or deletion of equal repeated words. The textual
prefix cue above selects resolution only; it never confirms or removes output.

Tie order is total cost, prefix fit, distance from the probe token-count ratio,
then earlier index. Better prefix fit prevents newly inserted repeated tail
words from being assigned to already confirmed context solely because the tail
grew. The ratio is only a deterministic textual prior, never a timestamp.
Ambiguity does not block audio progress or fail a normal recording.

Only byte slices of the contextual whole become confirmed prefix and revisable
tail. UTF-8 offsets come from `char_indices`; grouping never truncates characters.
Joining a continuation slice uses the separator actually supplied by the
contextual whole; no synthetic space is inserted into adjacent characters,
including after ASCII punctuation or at a mixed-script boundary. A supplied
leading separator still yields a space. Raw tail separators survive the carried
hypothesis and finalization. Separate VAD spans, or a new whole that omits all
recognizable context, retain the ordinary space join; text-only data cannot
establish the missing join in that latter case.
Probe text is not appended as independent fragments. For example, whole
`Привет, мир.` and probes `Привет.` / `Мир.` select the split between the two
words while preserving the whole's punctuation and case. Likewise an early
`ошибка` can become `черновик`; a probe-only `новый черновик` need not leak into
the final contextual result. Natural `да, да` and repeated phrases retain all
occurrences represented in the contextual hypothesis.

Retain 1.5 s of actual left PCM before the chosen boundary and at least 3 s of
right PCM. A necessary 1.5 s context probe records the prefix reference. Later
text-only refreshes align this context reference plus the previous revisable
tail against the new full window result, then replace only the tail. They do
not append each preview or deduplicate overlapping strings. An unchanged source
end can reuse the carried hypothesis recognized with the larger context; no
fake timing is attached to it.

A successful **empty isolated context probe** does not erase known confirmed
ownership. First try to recover the retained PCM's text from the already validated
whole's timestamps. Both the context start and cut must lie between segments and
at complete-text boundaries, using the existing SDK subword guard. Preserve the
entire resulting reference, including multiple repeated words, up to 256 UTF-8
bytes. A valid empty interval returns `Some("")`, which is distinct from missing
evidence (`None`): it must not invoke the nearest-unit fallback. A
straddling/coincident partial subword, unusable timing,
or an over-budget reference takes the bounded heuristic fallback instead; it
does not claim full timed coverage after truncation.

Without that evidence, retain the nearest coarse unit of the contextualized
prefix, capped to a 256-byte UTF-8 suffix. Neither branch reads the accumulated
final transcript. Retaining the entire old prefix would incorrectly make words
outside replayed PCM compete with new repeated words. A nonempty context probe
remains the primary reference; usable new timing still takes precedence on
refresh. No extra decode is introduced. An empty final whole result still
replaces the old tail with empty; the reference is alignment input, never text
to publish.

For `начало да конец`, probes `начало да` / `конец` confirm `начало да`.
If the 1.5 s probe is empty and no usable timing locates the context, the
heuristic reference is `да`, not empty or `начало да`.
The next whole `да конец дальше` projects after the first `да`, yielding
`начало да конец дальше`. For a genuinely new repetition represented by
`да да конец дальше`, both candidate splits have equal total cost, but prefix
fit prefers the first; the result is `начало да да конец дальше`. This remains
a bounded textual ownership heuristic, not inferred acoustic timing.

When the whole is `начало да да конец`, with `начало` at 0–8 s, `да` at
8.6–9.2 s, `да` at 9.2–10 s and `конец` at 10–18 s, the selected 10 s cut
retains PCM 8.5–10 s. The available timing locates **both** confirmed `да`
inside this context. An empty isolated probe therefore keeps `да да`, not just
the last word. The next text-only whole `да да конец дальше` yields
`начало да да конец дальше`; `да да да конец дальше` preserves the genuinely new
third word. Missing, invalid or fence-straddling timing in the new whole does
not discard that previously obtained reference. SDK times are still estimates,
not a guarantee of true acoustic word positions.

For `начало да конец` with segments at 0–2 s, 2–5 s and 16–18 s, no reported
end lies in the 6–15 s admissible interval. Prefix/suffix probes therefore select
the 10.5 s candidate. Both retained-context fences (9 s and 10.5 s) lie in the
same timestamp gap after `да`, so the timed reference is successfully empty.
Carrying it yields `context_text = ""`, `owned_text_start = 0` and full carried
hypothesis `конец`. The next whole `конец да дальше` keeps its entire text,
including the new `да`; final joining gives `начало да конец да дальше`.
An empty left reference cannot improve global alignment by consuming target
units: doing so costs their insertion gaps, and prefix-fit ties prefer zero.
Thus later text-only/coarse refreshes retain ownership offset zero. Final joining
then inserts the ordinary separator after the confirmed prefix. This represents
what the supplied timestamps say about text coverage, not an acoustic silence
assertion: the regression PCM remains Speech under the existing energy VAD.

## Ownership, resource bounds and Commit

`window_start_sample + speech.len() == speech_samples`, in the unique VAD-emitted
source timeline. `window_start_sample + context_samples` is the confirmed audio
fence; the retained prefix before it is replayable context. `owned_text_start`
marks that prefix's byte extent in the full hypothesis. `hypothesis` contains
only the replaceable text after it. `decoded_samples` records the last decoded
source end relative to the current window. Advancing/refreshing never increments
unique source counts. `segment_count` counts confirmed spans, not native calls.
Signal statistics still consume original normalized PCM exactly once.

- PCM allocation: 288,000 f32 samples / 1,152,000 bytes per worker, including
  context. Each prepared native input and runtime flattening copy is at most
  this size. SDK/model internal allocations are not claimed to have this bound.
- Left context: 24,000 samples / 96,000 bytes, retained inside that allocation.
  A first advance discards at least 4.5 s of PCM; subsequent advances discard at
  least 6 s. The confirmed audio fence always advances by at least 6 s.
- One necessary advance makes at most four serial calls (whole, prefix, suffix,
  context), totaling at most 37.5 s of source input. Usable timing avoids the
  prefix/suffix calls: at most two calls and 19.5 s. Cached whole results reduce
  this further. Each single input remains at most 18 s.
- Preview cadence is source-based (3 s), not a wall-clock rate guarantee. VAD
  closure and Commit may require a final changed-span decode. Native serialization
  and model speed still determine actual latency.
- Alignment has at most 2,048 units per sequence, two rows of at most 2,049 usize
  costs and at most 8,388,608 cell updates per projection. Longer texts are
  grouped into adjacent units without truncating output; mapping becomes coarser.
  Raw text and comparison keys remain proportional to bounded native results.
  Each construction makes two linear scans. Resolution selection scans bounded
  coarse units, then may rebuild all three sets once with scalar resolution;
  there is no unbounded scalar-boundary array or additional edit-distance pass.
  During replacement at most four unit vectors coexist, each at most 2,048 units;
  comparison-key storage remains proportional to the three native texts.
  Empty-probe references are at most 256 bytes (including known empty),
  either the entire timestamp-located context or a nearest-unit heuristic.
  Nonempty context references still come from the bounded 1.5 s probe.
- Existing admission is still at most four workers. The session's cumulative
  payload + normalized budget remains 6,144,000 bytes; draining does not renew it.
  There is no separate backlog of native jobs. Received PCM retains the existing
  inbox/lifetime cap even while inference is slow.

Commit closes ingress and drains all accepted source in order. Optional previews
are skipped for queued/closed input, and crossing intermediate thresholds cannot
start probes. A continuous closed 13 s or 17 s span has one decode totaling
13 s or 17 s; with a blocked 3 s preview already in flight, there are two calls
totaling 16 s or 20 s. A span exceeding the actual 18 s cap requires only the
advances needed to retain bounded memory/input, then one final tail decode.
Previously sealed spans are not replayed except the explicit 1.5 s context.

An already executing call keeps its lease and original startup scope. Every
subsequent call uses the same supervisor generation/cancellation fence. A real
late error, including after window advancement or VAD confirmation, discards the
whole recording outcome and follows the existing Failed path. No turn exists
before Commit; terminal claim, authorization, context, attachments and the one
final turn/result publication remain in the existing handler.

Moonshine Base's minimum 1,600-sample preparation is unchanged and native-only.
Overlap, SDK padding and that short-input padding never inflate logical source
or signal counters. Empty successful results retain the existing empty/no-speech
semantics; disagreement and missing timing are not native errors.

## Written regressions, not executed

- 50 s continuous PCM across packet sizes 317/960/1600; both timestamp-bearing
  and text-only fakes actually enter the production hypothesis boundary. Exact
  repeated text, original source diagnostics, native-size/work/call bounds.
- Another context-dependent 50 s fake changes words, case and punctuation between
  whole/probes, with an early incorrect word and probe-only rephrasing. Checks
  the entire ordered word-occurrence sequence across packetizations, rather than
  relying solely on the additive frame oracle.
- Text-only 50 s + seven-sample CJK cases with repeated characters, punctuation
  and supplementary-plane Han check the exact entire transcript after several
  advances and final refresh, for packets of 317/1600 samples. Both unspaced and
  explicitly spaced variants preserve all characters/repetitions; source PCM,
  unique diagnostics, signal statistics and native work bounds are checked.
- Additional text-only 50 s + seven-sample Thai and Arabic cases check the entire
  ordered text and repetitions through multiple advances, changed final refresh
  and Commit, with packets of 317/1600 samples. Every captured native input must
  be an exact source slice. Unique samples, signal statistics and native work
  bounds remain separate from replay. Existing CJK fixtures are unchanged.
- A small projection fixture checks the Thai counterexample with common prefixes,
  suffixes `OK`, `OK OK` and Russian words, including punctuation internal to the
  unspaced whole. A production 20 s + seven-sample fixture places `OK` / `OK OK`
  in actual source PCM near the first window end, so left/context probes lack it
  while the whole/right include it. The final refresh adds new speech AFTER it.
  Exact entire text, repeats, separators, five native PCM slices, unique sample
  counts and native input volume are asserted across packet sizes 317/1600.
- The empty-context-probe counterexample and its genuine new-`да` neighbor run
  through the production processor, VAD, advancement and Commit at 20 s + seven
  samples. Context-sensitive final hypotheses depend on actual new PCM markers.
  Exact five native PCM slices and total 776,007 native input samples are checked
  separately from 320,007 unique samples. Empty final and real late-error variants
  retain empty-tail and Failed behavior, without publishing a stale tail.
- A production `HypothesisTranscriber` regression supplies the exact four timed
  segments above, an empty context probe, then a whole with missing, coarse or
  nonfinite timing. Both the two-old-words case and genuinely new third `да`
  assert the full final text across packetizations. Exact native inputs are
  source `[0..288000]`, `[136000..160000]` and `[136000..320007]`: three calls,
  496,007 native samples versus 320,007 unique samples. Timestamp evidence avoids
  prefix/suffix probes; keeping multiple words adds no recovery calls.
- The known-empty timed-gap counterexample uses energetic varying-sign PCM and
  the exact 0–2/2–5/16–18 s metadata. A production fixture checks missing/coarse
  final timing, with and without a new `да`, empty final and genuine late error.
  It asserts complete text, source diagnostics and exact five native inputs
  totaling 776,007 samples versus 320,007 unique samples. It also directly checks
  carried `owned_text_start = 0`, empty reference, full tail and unchanged-source
  final joining. No recovery calls or partial error transcript are allowed.
- Exactly 18 s, its next sample/seven-sample tail, and VAD hangover beginning at
  the cap all succeed. Prefix/suffix/context/final native PCM slices and source
  counts are checked separately from intentional replay.
- Closed 13/17 s cases assert exactly one native call and its complete input
  volume. Actual owned-dispatcher cases block the 3 s preview, Commit with 13/17 s
  queued, assert exactly two calls, input sums, PCM and corrected single turn.
- Actual owned dispatcher also exercises 20 s backlog with punctuation/case and
  phrase repeats: six calls including the in-flight preview, 824,007 total input
  samples, maximum 288,000 per call, exact prefix/suffix/context/tail PCM and one
  expected transcript. A matching late-error case fails without a partial turn.
- Coarse, nonfinite and text-mismatched timing falls back successfully. A subword
  timing fixture confirms a whole word instead of a partial token. Supervisor
  native-result forwarding retains actual times while generation/cancel fences
  still reject calls. No fake manufactures timestamps for a real engine.
  A coincident-end fixture keeps repeated/zero-duration words on the same side
  of the audio fence instead of leaving one in a tail whose time was discarded.
- Existing correction/empty-tail, pre-Commit inference, 4 s catch-up, short
  Moonshine padding/diagnostics, sessions, cancel/disconnect/revocation/shutdown,
  lease/capacity, authorization, attachments, terminal and late-error coverage
  remains. Obsolete refusal assertions were replaced with successful advancement,
  not weakened PCM/duplicate/padding checks.

## Intermediate audit and full self-review

The first intermediate audit traced `ทดสอบระบบ OK` with probes `ทดสอบ` /
`ระบบ OK`. The coarse Thai unit contains both boundary parts; shared scalar
resolution gives a zero-cost split after `ทดสอบ`, independent of the common
suffix. Checked common prefixes, repeated suffixes, an interior punctuation mark,
new PCM after the suffix, UTF-8 byte offsets, exact full output, no deduplication,
the 2,048-unit cap and the single edit-distance pass. Existing CJK and ordinary
Russian word-boundary fixtures retain coarse resolution where adequate.

The second intermediate audit traced the 9–10.5 s timestamp gap. Equal text
offsets at both valid PCM fences now give `Some("")`; nearest-unit fallback is
not evaluated. Checked carried `context_text`, `owned_text_start = 0`, full tail
`конец`, subsequent whole `конец да дальше` and final space joining. A zero left
reference has optimal split zero (or wins by prefix-fit on ties); it cannot
consume the new `конец да`. Rechecked the nonempty multiword timed reference,
absent/invalid/coarse timing, size cap, empty final and late native error. No
reference replaces a required changed-source decode.

The separate final static review covered the entire diff against the fixed
base: original incremental implementation, current processor, all native/trait/
supervisor callers, session/VAD lifecycle, owned handlers, old and new fixtures,
and this document. It found that using only normalized keys for resolution could
miss punctuation internal to a whole but at the end of a probe. Added the raw
unit cue and a comma regression, then rechecked both counterexamples and all
projection/append sites. Reviewed extra bounded coarse-key allocations and
updated the resource contract: at most one scalar rebuild, no second DP pass.

Rechecked prior fixes throughout: actual continuation separators (including
ASCII punctuation in CJK), arbitrary-script refinement, natural repeats and
prefix-fit ties; validation/coalescing of coincident timestamps before SDK
subword guards; context fences in gaps or inside coarse segments; timing-to-byte
mapping, rounded non-grid 10 s progress, cap/next sample and VAD hangover; closed
13/17 s coalescing and blocked previews; distinct source/context/padding counts;
real errors before publication, generation/cancel fences and lease ownership.
The new gap fixture varies signs at constant Speech energy so exact PCM checks
cover order as well as length. No production changes outside `incremental.rs`
were necessary in this round. These are static conclusions, not executed test
results or evidence of actual recognition quality/latency.

Source normalization, sequence/duplicate semantics, ordering, VAD smoothing,
prefill/hangover and frame remainders remain unchanged. Engine locks are not
held while waiting for audio. No DB transaction/snapshot/permit, model selection,
language, production setting, protocol, SDK dependency, install/deployment,
content logging, commit or PR was introduced. Only formatting, reading/static
analysis and git diff whitespace checking are authorized/performed.

## Remaining assumptions and future validation after explicit acceptance

Neither timing nor global alignment proves correct recognition. Time estimates
may drift; lexical ambiguity and severe omissions/rephrasing can misassign a word
at the ownership fence. The nearest-unit empty-probe reference cannot recover
the exact amount of confirmed speech represented by replayed PCM: if that context
contains several words, or its nearest unit is itself omitted/reworded, ownership
can remain ambiguous. This is an explicit limitation, not a timestamp guarantee.
The timed branch can locate that text (including an empty interval) only when
both fences and its size bound are usable. Scalar resolution is selected from
available boundary-part cues, not a universal word/grapheme segmenter: severe
rephrasing or omissions may hide the cue in a mixed spaced/unspaced result,
scalar cuts can separate combining sequences, and absent probe words supply no
evidence for an interior split. These are text-projection limits, not relaxed
SDK timestamp confirmation.
Grouped alignment of unusually long output is coarser. Left PCM context
reduces abrupt cuts but cannot guarantee error-free word ownership or correct
already confirmed text. The 1.5/3/6/18 s bounds and alignment costs are engineering
choices requiring actual-audio review, not measured quality/latency optima.
There is no cross-pause decoder state, guessed confidence, LLM repair, translation
or model-name fallback registry.

Final whitespace checks: `git diff --check` and the untracked review document's
`git diff --no-index --check` completed without findings. Formatting used the
installed rustfmt directly; no Cargo command or executable test/check was run.

After complete repeat review and acceptance, first compile/run the focused
incremental, supervisor, session/VAD and owned_voice_context_/owned_voice_incremental_
fixtures plus existing authorization/attachments cases. Then compare the base
and revised implementation on the same saved PCM/settings with real-time pacing,
selected Parakeet and a timestamp-free engine. Measure WER/CER and boundary errors,
Commit -> ready transcript p50/p95, native call/input volume, model work and peak
memory for ordinary/continuous/repeated/noisy/short speech and blocked-inference
Commit cases. Do not infer WER or latency gains from deterministic fakes.
