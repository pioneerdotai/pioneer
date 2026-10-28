# Voice pause context — ожидает повторного полного внешнего ревью

## Изоляция и prerequisite

- Base и HEAD (diff не закоммичен): `afe798675a082425b25898bf31d72f9f60d1973d`.
- Ветка: `feat/voice-pause-context`.
- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/voice-pause-context`.
- После `git fetch origin main` origin/main равен base; ancestor-проверка успешна.
- В истории и коде изучены #205 (`186cb6c1`: общий stateful resampler и capture/FFI callers), #204 (`38e75bd4`: перенос ownership перед eviction PCM), #206 (`9cd27576`, `4a46b2d0`: Silero, incremental VAD и детерминированные fixtures).
- Корневой AGENTS.md прочитан; вложенных инструкций на voice/message путях нет. Database/UI/FFI не изменяются. Mobile PR #16 не prerequisite и не проверялся.

## Карта до изменения кода

- Ingress нормализует и считает принятый PCM; SignalAccumulator учитывает всю запись. Worker собирает фиксированные 480-sample frames независимо от пакетов. VAD выдаёт уникальную речь, включая onset/prefill/hangover, но отбрасывает часть тишины. Поэтому speech_samples не часы паузы.
- Inbox передаёт PCM единственному blocking worker. VAD владеет ещё не выданным prefill; IncrementalSpeech — выданной речью и bounded replay. Native получает копию; padding не принадлежит source counters.
- text — immutable confirmed prefix; hypothesis/full_hypothesis и owned_text_start описывают неподтверждённый suffix; context_samples/context_text исключают технический replay. Продвижение окна сначала переносит ownership, затем удаляет PCM.
- До изменения decoded_samples идентифицирует только длину span. На Noise flush немедленно подтверждает suffix и сбрасывает контекст. Проекция окна несёт кандидат с изменёнными границами, что требует отдельного окончательного decode перед его reuse.
- Commit закрывает inbox, подавляет previews, дренирует вход, завершает короткий onset/tail и flush. Ошибка decode прерывает всю запись. Cancellation проверяется вокруг native; lease живёт до возврата worker и публикации. Handler сохраняет terminal claim и повторную authorization; native не запускается из ingress/store mutex.

## Итоговая политика

Состояния находятся в существующем IncrementalSpeech; отдельного coordinator, timer или task нет.

| Состояние / вход | Переход |
| --- | --- |
| Speech без owned PCM + Noise (включая первый onset) | Ничего не подтверждает |
| Speech с PCM + первый Noise после hangover | Holding; подготовка bounded candidate, без окончательного подтверждения хвоста |
| Holding + повторный Noise | Только source-time проверка; тот же PCM не декодируется снова |
| Holding + Speech / Commit-tail | Resumed с тем же deadline, +1 continuation; новый PCM делает candidate неактуальным |
| Resumed + Noise | Holding, deadline и счётчик не сбрасываются |
| Quiet >= 1.2 s, lifetime >= 6 s, третье продолжение или oversized text | Окончательно завершить текущий span; сбросить pause/context; последующий source начинает новый span |
| Commit | Дренировать inbox, сохранить короткий onset/tail, окончательно завершить актуальный span один раз |
| Decode error / cancellation | Ошибка всей записи; ранее подготовленный кандидат не становится успешным итогом |

Подготовка кандидата (`prepare_pause` / `refresh`), переходы паузы (`pause` / `resume`) и окончательное подтверждение (`flush`) разделены. При необходимости проекция переносит только левую часть в confirmed prefix перед eviction. Хвост остаётся replaceable. Используется прежняя alignment эвристика без изменений. Timestamps необязательны: при отсутствии пригодной границы используются два disjoint PCM probes.

## Пределы и их обоснование

- На паузе сохраняется максимум **72 000 samples = 4.5 s**: до 3 s неподтверждённой речи и 1.5 s настоящего confirmed left context. Если outstanding PCM короче, сохраняется весь span. Fence — `len - 48000`, без увеличения существующего 18 s hard cap. После продолжения действует тот же исходный cap, до следующего pause/deadline.
- Через паузу сохраняются full candidate и context reference не более **512 UTF-8 bytes каждый**; provisional — срез full candidate. При превышении span завершается целиком, без обрезания текста. Не подготовленный из-за coalescing кандидат не удерживает новую текстовую историю. Confirmed output всей записи в этот лимит не входит и никогда не передаётся ASR как prompt.
- Quiet deadline — **19 200 source samples = 1.2 s**, включая 450 ms hangover. Это оставляет примерно 750 ms после hangover для короткой паузы. Lifetime удерживаемого контекста — **96 000 source samples = 6 s** от последнего positive frame перед первой паузой; возобновление deadline не продлевает.
- Максимум **два продолжения** одного held span. Третье начинает новый span после seal. За удержание возможны максимум три входа в Holding; обычный preview stride остаётся 3 s. Подготовка одной паузы делает максимум пять decode: whole, два partition probes, left-context probe, новый bounded candidate. При пригодном timing или актуальном whole вызовов меньше. Одни и те же неизменные samples и границы повторный Noise/Commit не декодируют. Нет retry-until-agreement.
- Это консервативные инженерные настройки, не измеренный языковой/модельный оптимум: 3 s использует существующий stride; 1.5 s — существующий left context; text cap допускает короткий многоязычный хвост, но ограничивает повторное выравнивание. Два продолжения и 6 s ограничивают repeated replay независимо от ASR.
- Существующие limits не повышены: окно 288 000 samples, alignment 2048 units, fallback anchor 256 bytes, ingress lifetime budget. Тишина хранится только в исходном bounded VAD prefill, а не бесконечно дописывается в speech PCM.

## Ownership и актуальность

- `source_samples` — часы обработанного входа: +480 за полный frame, остаток учитывается на Commit. `vad.quiet_samples()` считает detector-negative source frames, включая hangover. Ни число пакетов, ни SignalAccumulator, который получает chunk целиком, ни длина speech-only PCM не используются для часов паузы. Точность переходов — один VAD frame (30 ms); классификация неполного frame при Commit прежняя.
- VAD classification, threshold, onset, hangover, prefill, wrapper и asset неизменны. Ранее выданные frames по-прежнему удаляются из prefill. Новый prefill содержит только ещё не выданное аудио; технический ASR replay живёт в отдельном retained span и не увеличивает source counters.
- Инвариант `window_start_sample + speech.len() == speech_samples` сохраняется: append увеличивает unique counter; projection меняет cursor/PCM, flush освобождает весь span. Native padding остаётся только в копии decode-buffer. Buffered source counters и объединение речи неизменны.
- `candidate_current && decoded_samples == speech.len()` означает результат именно текущего PCM и ownership. Append и любая проекция PCM/ownership сбрасывают validity; carried text служит reference, но не окончательным кандидатом. Только успешный refresh устанавливает актуальность. Отдельный `decoded_samples` после проекции сохраняет origin preview stride, чтобы invalidation не запускала обычный preview до 3 s нового source. Flush без source — no-op.
- Отмена проверяется вокруг native и перед completion. После обоих режимов добавлена проверка pinned generation, в том числе для Commit без нового decode. Getter generation читает отдельный атомарный снимок и не захватывает engine mutex; snapshot публикуется при существующем serialized generation transition. Настройки уже запрещают reconfiguration при активной сессии/lease (`message/dispatch.rs`), поэтому lease защищает промежуток до публикации.
- Terminal claim, повторная authorization и admission/единственность результата не менялись. Disconnect/access invalidation удаляют authoritative session и отменяют buffer; shutdown закрывает admission, отменяет buffers и дожидается tracker. Lease остаётся у blocking worker до реального native-return, затем передаётся completion. Ни одного native вызова из ingress или под store mutex не добавлено.
- `transcribe_while_recording=false`: исходный `process_buffered` не изменён; один ASR вызов по объединённой VAD-речи после Commit. Выбор режима по-прежнему фиксируется при Start.

## Статические самоаудиты

1. **До правок:** зафиксирована карта выше; прочитаны buffering, native/session transcriber, VAD и finalization callers.
2. **После pause transitions:** перепроверены frame/source clocks, hangover/prefill и нулевой onset. Повторный Noise не создаёт decode/tasks; pause/lifetime thresholds считаются по PCM. Projection helper имеет два production caller: обычное окно и pause cap.
3. **После интеграции Commit/candidate:** найден и исправлен reuse перенесённой гипотезы после изменения границ. Проверены no-speech/empty/error, закрытый backlog, короткий хвост, передача lease и generation. Добавлен completion generation fence для cached Commit.
4. **После тестов:** обновлены старые ожидания вызовов при cap+pause; fake unchanged-source decode получил корректный отдельный PCM case. Исправлены setup реального processing inbox и закрытие/join до assertion preview timeout. Добавлены empty, oversized и cancel-paused случаи. Text cap проверяет также отдельный context reference.
5. **Полное итоговое ревью:** полностью перечитаны diff относительно base и новый отчёт; проверены все изменённые файлы, оба режима, source counters, cap arithmetic, ownership transfer-before-drain, cancellation/error exits и callers. Найден `cfg(test)` на getter generation и снят только этот gate для реального worker caller. При повторном проходе отделена validity кандидата от preview stride: сброс длины после проекции мог бы ускорить обычные previews без нового stride source; теперь отдельный bool инвалидирует candidate, сохраняя прежнюю cadence. Повторно проверен guard reconfiguration до завершения lease. Нет изменений соседних задач или запрещённых файлов. Это статическое ревью, не доказательство компиляции или прохождения тестов.

## Написанные и пересмотренные тесты

### Исправления по внешнему ревью (текущий проход)

1. **Completion без ожидания чужого native.** В supervisor добавлен `current_generation: AtomicU64`, инициализируемый из initial state. Единственный production transition generation (`apply_desired`) публикует новый номер `Release` store под существующим inner mutex; getter выполняет `Acquire` load. Проверка перед native-входом по-прежнему выполняется под inner mutex. Проверка перед completion остаётся на прежнем месте и не использует ни ожидание engine mutex, ни try_lock/refusal при занятости.
2. **Отмена после принятия кандидата.** Общий test-only `CheckpointDetector` использует существующую generic detector границу. Первый pause decode выполняется при обработке кадра 18: два speech frames и шестнадцать quiet, из которых пятнадцать — hangover. Вход в кадр 19 возможен только после возврата decode и принятия результата владельцем; `cancel_paused` отменяет только после этого checkpoint. Сигнал из fake ASR используется лишь для допуска следующего кадра, не как доказательство принятия. Отдельный `cancel` по-прежнему отменяет внутри последующего decode.
3. **Повторные Noise в открытом inbox.** В существующую live-матрицу добавлен один вариант `seal`. После checkpoint 19 подаются тихие кадры при открытом inbox. Checkpoint 29 подтверждает обработку предыдущих тихих кадров и один ASR-вызов; checkpoint 43 — обработку кадра 42, на котором достигнут существующий quiet seal 40 frames. До Commit ASR всё ещё вызван один раз. Итог проверяет один decode, один fragment, отсутствие повторного текста, 8160 unique speech samples, 43 × 480 total source samples и точный PCM с единственным hangover. Остальные live-варианты также проверяют реальные PCM, два ещё не выданных quiet frames как prefill при продолжении и source counters. Closed-input проверки сохранены.

Новый тест `cached_completion_does_not_wait_for_peer_native_and_rejects_stale_generation` проходит через `start_with_detector`, настоящий `SessionSpeechTranscriber` (`fake: None`), supervisor `transcribe_for_session`/`transcribe_inner` и completion с lease. Обе сессии уже стартовали; кандидат A принят до входа B. Существующий `LoadedVoiceEngine::TestStub` расширен callback вместо постоянной строки **только под cfg(test)**: callback B сигнализирует о входе под настоящим engine mutex и ждёт явного release. Production adapters, параметры моделей и native inference не изменены.

Написанный сценарий (не запускался) требует completion A **до** release B; старый mutex getter приводит к timeout. Второй вариант тем же барьером проверяет no-speech completion без ASR в A. Третий меняет generation после принятия кандидата A без нового speech PCM: cached Commit должен вернуть ошибку, а B старой generation не должен войти в engine. Для живого B тест отменяет его после completion A и требует сохранения lease до фактического возврата callback; после release проверяется typed cancellation. RAII release освобождает барьер при неудаче assertion/timeout. Все ожидания — явные каналы/barriers, без sleep, polling и предположений о скорости worker.

### Статические аудиты исправлений

- После исправления 1 перечитаны constructor/getter/apply_desired и все production записи generation, native entry, completion callers и существующие тестовые конструкторы TestStub. Инициализация и successful transition согласованы; no-op/error transition не меняют snapshot. Settings busy guard, terminal claim, reauthorization и передача lease остаются прежними.
- После исправления 2 перечитаны последовательный frame loop и обновлённый live fixture: checkpoint находится в следующем вызове detector, а не внутри ASR. Проверены обе отмены, empty/error/oversized, prefill из двух quiet frames и отсутствие native padding в source counters.
- После исправления 3 перечитаны quiet transition, coalescing и live seal progression: inbox остаётся открытым до checkpoint 43, существующие thresholds/limits не изменены. У native-регрессии проверен порядок cleanup: completion lease освобождается до `TaskTracker::wait`, чтобы сам тест не ожидал удерживаемый им lease.
- Итоговый повторный проход охватывает весь текущий diff от указанного base, включая test-only runtime fixture и этот отчёт, оба режима, PCM/text ownership, candidate validity, cancellation/generation, lease и terminal publication callers.

**Установлено только статическим чтением:** независимость atomic getter от engine mutex; полнота публикации production generation transitions; happens-before checkpoint 19 относительно принятия кандидата; порядок checkpoint 43 после source-time seal frame; отсутствие нового coordinator/tasks/polling и изменений pause limits/alignment; сохранение buffered one-call пути и publication/lease guards. Поведение и assertions новых тестов запуском не подтверждены; компиляция не проверялась. Первоначальный проход не принят, исправления ожидают повторного полного внешнего ревью.

### Покрытие первоначального прохода

Новые, только в `incremental.rs`, через production pipeline и существующий transcriber boundary:

- `short_pause_revises_only_owned_tail_with_packet_independent_source`: разные ASR hypotheses, immutable prefix, Commit в паузе/после продолжения, точные реальные retained PCM и counters при пакетах 137/480/1600.
- `long_silence_seals_once_without_decoding_or_retaining_each_noise_frame`: 40 и 1000 quiet frames дают два ограниченных source spans с одинаковой политикой, без ASR на каждом Noise.
- `real_repetition_across_short_pause_is_not_text_deduplicated`: сохраняются четыре реальные повторённые единицы.
- `pause_candidate_commit_resume_error_and_cancel_use_current_source`: живой inbox, принятие candidate подтверждено следующим detector frame, один finalize, reuse без нового source, correction после продолжения, поздняя ошибка, обе точки отмены, oversized text, EmptyTranscript и повторные Noise до seal при открытом inbox.
- `pause_context_expires_after_two_continuations_or_source_lifetime`: третье продолжение и 6 s реального входа завершают старый span.

Пересмотрены:

- Прежний `vad_pause_seals_contextual_span_and_preserves_short_moonshine_tail` заменён контрактом `vad_short_pause_keeps_context_and_preserves_seven_sample_moonshine_tail`: короткая пауза теперь сохраняет контекст, семь последних samples доходят до native. Независимые padding-проверки `moonshine_native_short_commit_preserves_source_at_minimum_boundary`, `moonshine_native_preparation_pads_only_short_input_with_silence`, `moonshine_native_padding_does_not_admit_silence_to_inference` сохранены без изменений.
- `voice_incremental_next_onset_does_not_replay_previous_hangover`: один contextual decode вместо прежних двух seals; точный source PCM и final onset coverage сохранены для обоих режимов.
- `window_limit_next_sample_and_vad_hangover_do_not_refuse_recording`: сохранены cap/next-sample/hangover checks; новый bounded final PCM после дополнительной pause projection.
- `known_empty_timed_context_preserves_the_entire_current_tail`: fake поддерживает настоящий decode retained unchanged-source span вместо прежнего reuse projected candidate.

Существующие buffered one-call/Commit, source ownership, timestamp/unspaced alignment, message lifecycle (cancel/disconnect/revocation/shutdown/generation/late error), lease и authorization тесты не дублировались в UI/FFI. Никакие тесты не запускались; baseline regressions запуском также не подтверждались.

## Полный список файлов

1. `crates/gateway/src/voice/incremental.rs` — pause policy, candidate validity, completion generation fence и тесты.
2. `crates/gateway/src/voice/vad.rs` — только счётчик тихого исходного аудиовремени и accessor.
3. `crates/gateway/src/voice/supervisor.rs` — атомарный generation snapshot для completion без engine mutex.
4. `crates/gateway/src/voice/runtime.rs` — исключительно существующий cfg(test) TestStub, расширенный callback для native barrier.
5. `VOICE_PAUSE_CONTEXT_REVIEW.md` — новый отчёт, обновлён после внешнего ревью.

## Журнал команд

Повторяющиеся read-only просмотры сгруппированы; все изменения текста сделаны `apply_patch` в указанном worktree. Команды сборки/тестов из прочитанных инструкций не исполнялись.

| Этап | Выполненные команды / чтения |
| --- | --- |
| Основной checkout, исходное состояние | `pwd`, `git status --short`, `cat AGENTS.md`, `git worktree list`, `git log -8 --oneline` |
| Инструкции | `cat` четырёх переданных Pioneer SKILL.md; `cat` test-audit/SKILL.md; `sed -n '1,100p'` test-audit/references/pioneer.md; `rg --files -g AGENTS.md /Users/alexander/Code/pioneer` |
| Origin / история | `git fetch origin main`; `git rev-parse origin/main`; `git merge-base --is-ancestor afe798675a082425b25898bf31d72f9f60d1973d origin/main`; `git branch --list 'feat/voice-pause-context*'`; `git show --stat` для 186cb6c1, 38e75bd4, 9cd27576; `git log -1 --format='%H %s'` для них и 4a46b2d0 |
| Изоляция | `git worktree add -b feat/voice-pause-context /Users/alexander/Code/pioneer/pioneer/.worktrees/voice-pause-context origin/main` — коллизий не было |
| Предыдущие изменения | `git show 38e75bd4 -- crates/gateway/src/voice/incremental.rs`; `git show 186cb6c1 -- crates/client/src/voice/resampling.rs`; `git show 9cd27576 -- crates/gateway/src/voice/vad.rs`; `git show 186cb6c1 -- crates/desktop/src/audio/capture.rs crates/client-ffi/src/voice_capture.rs`; дополнительный `git show 186cb6c1 -- crates/client-ffi/src/voice_capture.rs` с `head -180` |
| Исходники | `cat` incremental.rs, vad.rs; серии `sed -n` и `rg -n` по ним, session_buffer.rs, transcription.rs, supervisor.rs, runtime.rs, silero.rs, finalization.rs, session_store.rs и desktop capture.rs. Просмотрены production части и применимые тесты |
| Callers / lifecycle | `rg -n` и `sed -n` в message/voice_handlers.rs, message/tests.rs (voice harness), message/dispatch.rs (settings guard и connection_closed), message/access_invalidation.rs, message/mod.rs (shutdown), protocol/settings.rs; `rg --files` для поиска owners; `cat` gateway/Cargo.toml и .github/workflows/ci.yml |
| Неудачные read-only поиски | Несуществующие voice/session.rs, voice/sessions.rs, connection.rs, message.rs, settings_handlers.rs и glob message/settings*; владельцы затем найдены через `rg --files` / поиск по src. Часть больших выводов была обрезана, нужные участки перечитаны узкими `sed` |
| Промежуточные аудиты | Серии `git diff -- <affected paths>`, `git diff --stat`, `git diff --numstat`, `git status --short`; дополнительные `rg -n` по refresh/flush/counters/тестам и `sed -n` по найденным диапазонам |
| Единственная исполняемая проверка | `cargo fmt -p pioneer-gateway` — пять вызовов по мере правок, exit 0. Без `--all`, без check/build/test/typecheck/lint |
| Финальная Git-инспекция | `git diff afe798675a082425b25898bf31d72f9f60d1973d -- <affected paths>` (полный diff), `git ls-files --others --exclude-standard`, `git diff --name-only <base>`, `git rev-parse HEAD`, `git branch --show-current`, `cat VOICE_PAUSE_CONTEXT_REVIEW.md`; повторный status основного checkout |

Дополнение журнала текущего прохода исправлений (без fetch, новой ветки или нового worktree):

- `git status --short`, `git rev-parse HEAD`, `git branch --show-current`, `cat AGENTS.md`, `rg --files -g AGENTS.md crates/gateway`, `cat VOICE_PAUSE_CONTEXT_REVIEW.md` — существующая ветка/base и полный первоначальный diff сохранены.
- Серии `sed -n`/`rg -n` по voice/supervisor.rs (constructor, generation transitions, native entry и fixtures), voice/runtime.rs (TestStub и match), voice/incremental.rs (worker, frame loop, pause policy и тесты), voice/session_buffer.rs, voice/finalization.rs и voice/transcription.rs. `rg -n 'TestStub\(' crates --glob '*.rs'` и поиск всех `VoiceInputSupervisor` constructors проверили callers изменённых fixtures/field.
- Прочитаны message/voice_handlers.rs (completion, terminal claim, authorization) и message/dispatch.rs (settings reconfiguration guard). Несколько длинных read-only выводов перечитаны отдельными диапазонами.
- Правки внесены только через `apply_patch` в существующем worktree. Промежуточно перечитаны `git diff --` для supervisor.rs/runtime.rs и изменённые тестовые диапазоны incremental.rs.
- `cargo fmt -p pioneer-gateway` — два дополнительных вызова текущего прохода, exit 0; первоначальные пять выше — отдельный исторический журнал. Других исполняемых проверок не было.
- Итог: полный `git diff afe798675a082425b25898bf31d72f9f60d1973d` прочитан частями, включая новые файлы через `cat`/`sed`; `git status --short`, `git diff --stat`, `git diff --name-only`, `git ls-files --others --exclude-standard`, `git rev-parse HEAD`, `git branch --show-current`. Основной checkout просмотрен только read-only status/HEAD/branch.

## Передача и ограничения

Diff оставлен незакоммиченным. Реализация **ожидает повторного полного внешнего ревью**, не принята и не объявляется компилируемой или проверенной тестами. Тесты, любые сборки/check/clippy/metadata/generators, приложения, модели, inference и исполняемые эксперименты не запускались. Commit/push/PR/merge/rebase/install/deploy не выполнялись; чужие worktrees/ветки не изменялись, prune не выполнялся.

После явной повторной внешней приёмки отложены компиляция пакета, focused nextest для voice incremental/buffered/lifecycle/supervisor/runtime (включая новый peer-native barrier и open-inbox seal сценарий) и относящиеся CI-проверки согласно AGENTS.md. До приёмки их запуск запрещён.

Ограничения: text/audio projection без точных word timestamps остаётся эвристикой и не гарантирует качество распознавания; oversized output закрывает span раньше. Silence clock зависит от решений VAD и имеет 30 ms frame granularity; если входное аудио не поступает, часы не идут (wall-clock timer не вводился). Native inference остаётся синхронным и не прерывается cancellation, публикация подавляется после возврата. Настройки требуют будущей качественной оценки на реальной речи.

**Обрезание длинной диктовки не решено этой задачей.** Существующий lifetime ingress budget примерно на 64 s переданного PCM не повышался; ingest/ACK не менялись. Он не доказывает причину наблюдавшегося результата 15–30 s. Mobile PR #16, обновление native-библиотек и mobile app не входят в diff.
