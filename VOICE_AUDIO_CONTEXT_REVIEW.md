# Блок А: передача на внешнее ревью

Publication status: The user accepted the implementation and P2 correction,
and authorized a commit, push, pull request, and remote CI monitoring. Earlier
review statuses and restrictions below describe the work at that time. Local
builds and tests remain prohibited.

Base/HEAD: `7f226499d3fa8beac5dd15b3d241040e7a99016e`.
Branch: `feat/voice-audio-context`.
Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/voice-audio-context`.

Реализация блока А подготовлена к внешнему ревью, ещё не принята;
тесты и сборки не запускались. Diff незакоммичен. Суффиксы не понадобились.
Основной checkout и соседние worktree не менялись.

## Полный инвентарь файлов

| Файл | Изменения |
| --- | --- |
| `Cargo.lock` | Один dependency edge gateway → существующий pinned ort |
| `crates/gateway/Cargo.toml` | Прямой ort вместо vad-silero SDK wrapper |
| `crates/gateway/src/voice/vad.rs` | 512 framing, smoothing, metadata, тесты |
| `crates/gateway/src/voice/silero.rs` | Узкий ONNX v4 adapter, deferred smoke |
| `crates/gateway/src/voice/incremental.rs` | Mapping, boundaries, ownership, тесты |
| `resources/voice/silero-v4/NOTICE.md` | Контракт локального adapter |
| `VOICE_AUDIO_CONTEXT_REVIEW.md` | Единственный новый/untracked файл |

Модель ONNX и LICENSE не менялись. Нет новых публичных metadata протоколов.

## Pre-edit function and invariant map

- `silero::create_voice_detector/load_verified_detector`: embedded pinned v4,
  SHA256/size verification, private temporary staging, recording-owned session,
  cancellation before/after native load, native session drops before resource.
- `vad::push_incremental_frame/push_frame/finish_incremental_tail`: one recurrent
  detector across all source frames, smoothing onset/prefill/hangover, no replay
  of emitted prefill; final native padding must never become source PCM.
- `incremental::process/process_buffered`: assemble frames across packets;
  buffered has one post-Commit ASR call; source stats count pre-VAD audio.
- `IncrementalSpeech::accept_speech/decode`: unique prepared stream separate
  from source clock, left-context replay and native-only padding; 18 s PCM cap.
- `pause/prepare_pause/resume/flush`: finite context lifetime/continuations,
  immutable confirmed text, original 1.2 s final quiet target.
- `advance_window/project_window`: timestamps first, bounded partition probes,
  evict audio only after ownership transfer; retain 1.5 s real left context.
- `timed_boundaries/timed_text_start`: finite ordered call-relative timestamps,
  exact UTF-8 text correspondence, coincident ends coalesced, no subword split.
- `edit_costs/project_boundary`: bounded global alignment, occurrence-preserving
  text; current ties are hidden by deterministic priors and need explicit costs.

No changes to ingress, capture, transport, native ASR adapters, terminal claims,
generation fences or finalization leases. No execution checks except scoped fmt.

## Intermediate audits

1. Framing/VAD (static): local v4 ONNX inputs/outputs checked against installed
   ort API and pinned upstream Python helper. Fixed real tail classification
   during established speech so recurrent input stays sequential through Commit.
   Padding is absent from interval lengths and quiet time. Session/resource field
   drop order and all cancellation checks preserved. No native execution.
2. Pauses/coordinates (static): emitted PCM intervals are transferred once,
   clipped on partial acceptance/eviction; removed source gaps are not measured
   as ASR samples. Buffered preparation retains coalesced source spans, not
   whole-recording probabilities. Incremental probabilities cover only retained
   real PCM plus bounded pending prefill. Pure silence cannot emit Speech.
   Corrected pause-seal budget to 19200 source samples rather than 40 new frames.
3. Boundary/alignment (static): pause cuts use asymmetric [len-3 s, len-1.5 s]
   bounds, further constrained by context/progress. Added ownership filtering and
   UTF-8 checks. Equal minimum edit-cost splits now defer optional projection,
   with one attempt per unchanged span. Mandatory cap uses a finite positional
   tie fallback; all disputed text retains its full audio during deferral.
4. После тестов (только чтение): исправлены старые 480-sample VAD fixtures,
   ожидаемые hangover/prefill и положение pause cut. Размер условного слова
   alignment fixtures отделён от кадра VAD: прежние 30 мс — свойство fixture.
   Planner-only tests явно задают source ownership уже подготовленного PCM,
   без production test hook. Проверены partial frame на лимите 18 с, повторное
   исключение consensus prefix, quiet retries и исключение native padding.
   Smoke больше не объявляется независимым oracle качества/recurrent state.
5. Финальный статический аудит: просмотрены весь diff относительно base,
   единственный untracked файл и затронутые consumers. Проверены сохранённые
   интерфейсы и fences, eviction PCM вместе с metadata, coalesced end times,
   source gaps, finite fallback, buffered one-call. Нет заявлений об успешной
   компиляции/inference. Замечания в scope исправлены; никаких запусков тестов.

## Соответствие пяти исходным пунктам

1. `prepare_pause` больше не требует timestamp == cut. Ненулевой интервал
   выводится из retained budget, left context, progress и минимального хвоста;
   ближайший пригодный word end выбирается внутри него. Сохранены finite/
   ordered timestamps, точное соответствие тексту, UTF-8/subword guards,
   объединение coincident ends, ownership и bounded text-only fallback.
2. Локальный `SileroV4Session`: 512 samples /16 kHz, `input`, scalar `sr`,
   отдельные `[2,1,64]` h/c, outputs `output/hn/cn`. Состояние принадлежит одной
   записи и проходит через тишину. Проверяются shape/type/finite outputs.
   Существующий `ort 2.0.0-rc.12`, без SDK fork/registry edits. Сохранены asset
   SHA256/size, staging 0700 Unix, notices, session → TempDir drop order,
   cancellation и embedding/packaging. Padding только у последнего native frame.
3. Один VAD передаёт probabilities/intervals подготовки. Incremental хранит
   только текущий retained PCM и bounded prefill. Quiet runs >=64 мс дают
   кандидатов: более длинная пауза, её source midpoint, затем положение и
   mean probability при равенстве. Midpoint в удалённой тишине отображается
   в шов реальных краёв. Пригодные ASR timestamps сохраняют приоритет, энергия
   остаётся конечным fallback. Source duration не подменяется prepared duration.
4. Ограниченная политика реальных краёв сохранена и формализована без второй
   очереди тишины. Gap до 27 quiet frames (864 мс при двухкадровом onset)
   сохраняется целиком; длинный gap сохраняет 14 hangover +13 quiet prefill
   frames (448 +416 мс). Final short onset может сохранить ещё реальную часть
   края в пределах prefill +tail. Переданный hangover никогда не возвращается
   как prefill. Тишина не становится Speech; buffered не получает chunking.
6. `BoundaryProjection` хранит selected/first/last offsets, minimum cost и
   лучший alternative cost. Равные minimum costs означают явную неоднозначность,
   без статистического confidence. Optional cut подтверждает общий префикс всех
   optimal candidates, если reference <=512 bytes; спорный суффикс остаётся
   вместе со всем PCM. Неизменный span не повторяет probes. На новом целом
   prefix исключается по пригодным timestamps либо occurrence-aware alignment.
   Hard cap применяет конечный positional tie fallback; confirmed text immutable.

## Инварианты, параметры и ограничения

- Mono PCM 16000 Hz; 512 samples =32 мс. Onset 2 =64 мс (60 вверх), hangover
  14 =448 мс (450 вниз), prefill setting 14 =448 мс, включая предыдущий onset
  frame. Queue максимум 15×512=7680 samples, как прежние 16×480. Реальный
  pre-onset quiet edge при двух onset frames — 13×512=6656 samples.
- Порог onset 0.3 сохранён, continuation 0.2 — небольшой hysteresis margin
  0.1. Quiet clock/pause candidates используют тот же decision, без прибавления
  hangover. Это engineering choice для согласованного smoothing, не измеренный
  optimum и не перенос порогов Handy. Нужны реальные fixtures внешней приёмки.
- Final pause target сохранён: 19200 source samples =1.2 с. С новым framing
  непрерывная quiet достигает его за 38 кадров =1216 мс; overshoot <1 кадра.
- Не увеличены hard PCM 18 с, refresh 3 с, left context 1.5 с, advancement 6 с,
  pause lifetime 6 с, две continuations, pause text 512 bytes, alignment 2048
  units. Energy scan 480 samples /30 мс — только прежняя сетка cut search,
  не размер входа модели. При неоднозначном optional cut целевое pause окно
  4.5 с временно не достигается, но 18-секундный cap и pause budgets действуют.
- Для `L=len`, `C=context_samples`, `left=24000`: pause
  `earliest=max(L+left-72000, C+512, left)`, `latest=L-left`.
  Значит `left+(L-cut)<=72000`, хвост >=24000, есть progress. Full window:
  `earliest=C+96000`, `latest=L-48000`. Симметричного допуска нет.
- Source diagnostics считают input до VAD; `speech_samples` — уникальный
  реальный prepared PCM, включая сохранённые края, без replay/padding.
  `window_start_sample` — offset в unique prepared stream. `AudioTimeline`
  хранит half-open source intervals и prepared offsets; eviction клиппирует
  оба. Стык принадлежит следующему sample. Удалённая тишина и native padding
  не имеют mapping. Buffered хранит coalesced source spans через свой один
  ASR call, без probabilities всей записи.
- Для `speech[a..b]` ASR timestamp относится к actual native PCM этого вызова.
  Реальный `i=round(t*16000)` отображается через
  `timeline.source_at(window_start_sample+a+i)` только при `i<b-a`.
  Timing внутри native-only padding не подтверждает ownership. Moonshine
  minimum 1600 остаётся native-only и не меняет source diagnostics.
- Projection ограничена whole +двумя disjoint probes +left context. Новых
  recovery probes нет. Text-only проекция остаётся эвристикой, не доказательством
  точной акустической границы. Повторы сохраняются как отдельные occurrences.

## Карта shared правок для интеграции с Б

- `incremental.rs`: imports/constants; `process_buffered` (source mapping и
  real sample accounting); новые private mapping structs; поля/init state;
  `prepare_pause`, `accept_speech`, `refresh`, `advance_window`, новый
  `preferred_boundary`, `project_window`, `flush`; `partition_boundary_in`,
  `is_text_boundary`, `BoundaryProjection/boundary_projection`; tests.
- `process`, worker lifecycle, `SessionSpeechTranscriber`, native `decode`,
  generation/cancellation/lease fences не менялись. Сохранить timeline updates
  и partial acceptance frame при интеграции native streaming блока Б.
- `PreparedSpeechBuffer`, `VoiceSpeechTranscriber`, `VoiceSpeechHypothesis` и
  `transcription.rs` не менялись. Если Б расширяет hypothesis, согласовать
  literals в tests при интеграции. ASR runtime/SDK, capture, transport,
  session_buffer и finalize integrity не редактировались.
- Manifest: сохранить прямой pinned ort и local v4 adapter; не возвращать
  480-only SDK VAD. Lock меняет только dependency edge, без обновления версий.

## Написанные, но не запущенные тесты

Новые в `vad.rs`:

- `pause_edges_preserve_real_short_gaps_and_bound_long_quiet_history`;
- `hysteresis_and_quiet_clock_share_the_detector_decision_not_hangover`;
- `pure_quiet_and_noise_never_become_speech_even_at_commit`.

Новые в `incremental.rs`:

- `real_samples_cross_packet_and_v4_frame_boundaries_once_in_both_modes`;
- `pause_uses_nearest_admissible_word_end_and_coincident_repetitions`;
- `equal_alignment_costs_hold_text_with_audio_once_then_force_finite_progress`;
- `prepared_mapping_excludes_removed_silence_and_survives_partial_evictions`;
- `timing_validation_rejects_bad_order_padding_and_non_utf8_text_fences`;
- `timestamp_cut_cannot_escape_the_retained_window_and_tail_bounds`;
- `vad_pause_duration_beats_nearby_low_energy_during_window_planning`.
- `final_pause_seal_keeps_the_original_source_time_target_with_32ms_frames`.

Сохранены существующие buffered one-call >18 с, Moonshine padding, несколько
advances/resource/replay bounds, repetitions/unspaced text, coincident end и
subword, cached candidate/generation/cancellation/leases tests. Обновлены только
fixtures, связанные с framing/cuts. Final-tail matrices дополнены 511 samples.
Native smoke по-прежнему ignored: это isolation smoke, не independent quality
oracle. Ни baseline proof, ни компиляция новых тестов не выполнялись.

## Журнал реально выполненных команд

Команды чтения с повторяющимися `sed -n`/`rg -n` ниже сгруппированы. Правки
выполнялись `apply_patch` и `python3 - <<'PY'` только для замены текста файлов
inventory; тела редакторских heredoc не дублируются. Они не запускали код
проекта, тесты, модели, generators или subprocess checks.

1. Основной checkout: `pwd`, `git status --short`, `git worktree list`,
   `git cat-file -t 7f226499d3fa8beac5dd15b3d241040e7a99016e`,
   `git branch --list 'feat/voice-audio-context*'`,
   `ls -ld .worktrees/voice-audio-context`.
2. `rg --files -g AGENTS.md -g '!node_modules' -g '!target'` по pioneer/Handy;
   `cat AGENTS.md`; `ls` ancestor AGENTS, включая `.worktrees/AGENTS.md`.
   Отсутствующие пути дали обычные read errors; нет чужих мутаций.
3. `git worktree add -b feat/voice-audio-context /Users/alexander/Code/pioneer/pioneer/.worktrees/voice-audio-context 7f226499d3fa8beac5dd15b3d241040e7a99016e`.
4. `cat`, `sed -n`, `rg -n` исходников voice, manifests/lock, NOTICE,
   `.github/workflows/ci.yml`, `.cargo/config.toml`, `rust-toolchain.toml`.
   Read-only cat Handy AGENTS, chunking.rs, audio/sink.rs; read-only transcribe-rs
   0.3.12 VAD wrapper/manifest и ort 2.0.0-rc.12 tensor API в Cargo registry.
5. Прочитаны test-audit SKILL и Pioneer reference: использованы authoring gates.
   User scope production implementation и запрет запусков имеют приоритет над
   его test-only/run guidelines. Другие приложенные skills не потребовали
   task/thread/HTML операций; новых агентов или durable tasks не создавалось.
6. Web open, только чтение pinned v4 `utils_vad.py` по ссылке в NOTICE.
   Из сети не запускались модели или Python helpers.
7. `apply_patch` создал pre-edit map и обновлял отчёт; одна попытка дополнения
   отчёта не применилась из-за контекста, затем исправлена. Редакторские
   `python3` heredoc меняли только inventory paths.
8. Три успешных запуска scoped formatting:
   `cargo fmt --manifest-path crates/gateway/Cargo.toml --package pioneer-gateway -- --config skip_children=true crates/gateway/src/voice/vad.rs crates/gateway/src/voice/silero.rs crates/gateway/src/voice/incremental.rs`.
   Все exit 0; вне трёх файлов Rust изменений нет.
9. `git status --short`, `git diff --stat`, `git diff --numstat`,
   `git diff 7f226499d3fa8beac5dd15b3d241040e7a99016e -- <inventory paths>`
   (частями через sed), `git rev-parse HEAD`, `git branch --show-current`,
   `git ls-files --others --exclude-standard`; повторное чтение итоговых файлов.

10. Финальный scoped check:
    `cargo fmt --manifest-path crates/gateway/Cargo.toml --package pioneer-gateway -- --config skip_children=true --check crates/gateway/src/voice/vad.rs crates/gateway/src/voice/silero.rs crates/gateway/src/voice/incremental.rs`.
    Exit 0. Это единственная выполненная исполняемая проверка; кроме разрешённых
    fmt/fmt --check не запускалось ничего из test/build/check/lint/typecheck/CI.

## Отложенные проверки: только после внешней приёмки объединения

Эти команды НЕ запускались, включая сборочные этапы:

```sh
cargo check --locked -p pioneer-gateway
cargo nextest run --locked -p pioneer-gateway --test-threads 2 --no-fail-fast --retries 0 -E 'test(voice::vad::tests::) | test(voice::incremental::tests::) | test(voice::silero::tests::)'
cargo nextest run --locked -p pioneer-gateway --run-ignored only --test-threads 2 --no-fail-fast --retries 0 -E 'test(silero_v4_bundled_load_inference_and_recording_state_isolation_smoke)'
```

Внешняя приёмка также должна сравнить adapter с независимым pinned v4
Python/ONNX reference на одинаковых 512-frame sequences (тишина, speech,
partial tail): probabilities, h/c, scalar sr и runtime на поддержанных targets.
Реальные короткие/шумные fixtures нужны для оценки hysteresis, onset, pause
quality и text projections во всех ASR моделях. Затем packaging Unix/Windows,
native error/cancellation/cleanup и согласованные workspace checks/CI блоков
А/Б/В. До внешней приёмки всё это запрещено. Fake tests не доказывают качество
модели; native inference здесь не проверялся.

## Повторное внешнее ревью: исправление P2 quiet-run continuity

Статус после исправления: передано на повторное внешнее ревью, ещё не принято.
Base/HEAD и ветка прежние; весь предыдущий незакоммиченный diff сохранён.
Дополнительные изменения только в `vad.rs`, `incremental.rs` и этом отчёте.

### Замечание и исправление

Planner объединял тихие source edges через любой удалённый промежуток, хотя
prefill мог вытеснить положительный VAD frame, не прошедший двухкадровый onset.
Последовательность 15 quiet /1 positive /16 quiet ошибочно считалась одной
32-кадровой паузой и выигрывала у настоящей непрерывной паузы 20 кадров.

`VadInterval::quiet_run_start` теперь несёт начало непрерывной последовательности
отрицательных решений на полном входе VAD. Для quiet frame оно вычисляется как
`source_cursor - quiet_samples` **до** обновления существующих clocks. Любое
положительное решение обнуляет quiet clock независимо от onset/emission; поэтому
следующий quiet run получает другое начало даже после eviction positive frame.
Поле voiced interval не используется planner для сравнения тихих runs.

`AudioTimeline::pause_boundary` завершает текущего quiet candidate при смене
этого marker. Source gaps с одинаковым marker продолжают объединяться: реальная
длинная пауза сохраняет длительность между её retained edges. Marker не заменяет
source/prepared coordinates и не добавляет историю решений записи: один `usize`
на уже ограниченный retained interval, без новых очередей и persistent counters.

### Промежуточные статические самоаудиты

1. Передача continuity metadata: просмотрены `classify`, emission/Commit tail,
   `take_emitted_intervals`, `accept_speech`, `AudioTimeline::append/retain_from`
   и buffered consumer. Marker создаётся до prefill eviction; обычные emission
   и tail переносят его вместе с interval. Частичный приём копирует marker через
   struct update; eviction меняет только source/prepared endpoints. Buffered
   coalesced mapping по-прежнему не хранит probabilities/quiet history и не
   меняет свой единственный ASR call. Пороги, smoothing, source clock, padding
   и паузные бюджеты не менялись.
2. Planner: смена marker завершает candidate даже без сохранённого voiced PCM;
   одинаковый marker доказывает непрерывность через source gap. Длительность и
   midpoint остаются в source coordinates, результат — в prepared coordinates.
   При clipping учитываются только текущие retained edges. Приоритет timestamps,
   fallback, ownership и текстовая проекция не редактировались.

### Регрессия — написана, НЕ запущена

`evicted_positive_vad_frame_separates_pauses_but_a_quiet_gap_keeps_continuity`
использует существующие `IncrementalSpeech`, `open_store`, test transcriber и
production-путь `push_incremental_frame` → `accept_speech` (emitted metadata) →
`preferred_boundary`/`pause_boundary`. Два table cases задают только probabilities,
не подставляют continuity markers и проверяют выбранную границу, а не run ID.

- После 200 voiced frames: 15 quiet, один frame 0.9, 16 quiet, два onset,
  40 voiced, конкурирующая пауза 20 quiet и продолжение. Ожидается prepared cut
  **142848** → source **145408**: выигрывает настоящая 20-кадровая пауза.
- Тот же вход с probability 0.0 у среднего frame: непрерывная 32-кадровая пауза
  выигрывает, ожидается prepared cut **109568** → source **112128**, шов её
  retained edges. Удалены те же пять source frames в обоих cases.
- Проверяется, что средний frame действительно отсутствует в prepared mapping.
  Приём проходит через refresh boundaries, где один interval делится на части.
  Дополнительно timeline клиппируется внутри первого quiet edge, после чего
  planner должен вернуть тот же абсолютный cut в новых локальных координатах.

Старые test-only `VadInterval` literals дополнены новым обязательным полем;
ожидаемое поведение прежних tests не менялось. До исправления положительный case
статически выбирал cut контрольного case; baseline/новые tests не исполнялись,
их успешность и компиляция не заявляются. Fake probabilities не доказывают
качество реальной модели.

### Финальный статический самоаудит дополнительного diff

Просмотрены дополнительные hunks против копий состояния до P2, новые/изменённые
tests и все producers/consumers `VadInterval`. В `vad.rs` добавлены только поле
и его вычисление. В production `incremental.rs` изменено только группирование
quiet runs. Координатные функции, размер очередей, PCM/replay limits и native
calls сохранены. Silero adapter, SDK, manifest/lock, capture/transport/UI,
модель, LICENSE/NOTICE и соседние Б/В не менялись этим исправлением.

### Реально выполненные команды этого исправления

Все shell-команды выполнялись в прежнем worktree. Чтения сгруппированы;
`git diff --no-index` используется для просмотра текста изменений, его exit 1
при наличии diff не является запуском теста/проверкой приложения.

1. `git status --short`, `git rev-parse HEAD`, `git branch --show-current`,
   `cat AGENTS.md`, `rg --files -g AGENTS.md -g '!target' -g '!node_modules' crates resources`,
   `ls` для ancestor AGENTS (отсутствующие пути дали обычные read errors).
2. `mktemp -d /tmp/pioneer-voice-audio-p2.XXXXXX` создал
   `/tmp/pioneer-voice-audio-p2.RdPNzY`; `git diff --binary HEAD` сохранён в
   `existing.patch`, `cp` сохранил исходные `vad.rs`, `incremental.rs` и отчёт.
   Это снимки текстового diff для ревью, без запуска кода проекта.
3. `sed -n`, `rg -n`, `cat` для VAD, timeline, acceptance, factories/tests,
   отчёта, test-audit SKILL/Pioneer reference, gateway manifest и CI. Использованы
   authoring gates test-audit; прямое user-разрешение production fix и запрет
   запусков имеют приоритет над его test-only/run guidelines.
4. `apply_patch`: узкое исправление, адаптация literals, новая regression table
   и дополнение этого отчёта. Подагенты и вспомогательные проверяющие скрипты
   не запускались.
5. `git diff --no-index` между снимками и текущими `vad.rs`, `incremental.rs`,
   отчётом; просмотр существующего и итогового `git diff --binary HEAD`,
   `git diff --stat`, `git status --short`, `git ls-files --others --exclude-standard`.
6. `cargo fmt --manifest-path crates/gateway/Cargo.toml --package pioneer-gateway -- --config skip_children=true crates/gateway/src/voice/vad.rs crates/gateway/src/voice/incremental.rs` — exit 0.
7. `cargo fmt --manifest-path crates/gateway/Cargo.toml --package pioneer-gateway -- --config skip_children=true --check crates/gateway/src/voice/vad.rs crates/gateway/src/voice/incremental.rs` — exit 0.

Тесты, cargo check/build/clippy, inference, приложения, lint/typecheck,
generators, benchmarks и CI не запускались. Отложенные проверки из предыдущего
раздела остаются запрещёнными до внешней приёмки объединения. Commit/push/PR/
merge не выполнялись; исправление и предыдущий diff незакоммичены.
