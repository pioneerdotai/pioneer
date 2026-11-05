# Finalized voice chunks — кандидат для внешнего ревью

Дата: 2026-11-05. Реализация не принята. Все выводы ниже — статическое чтение исходников, кроме явно указанного форматирования. Компилируемость, прохождение тестов и улучшение точности не подтверждены.

## Рабочая копия

- Ветка: `feat/voice-finalized-audio-chunks`.
- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/voice-finalized-audio-chunks`.
- Base SHA: `03abf10c428c790e0910b5aca1aabcdfe21698b5`.
- Перед созданием выполнен `git fetch origin main`; `origin/main` совпадал с этой базой. Worktree создан от коммита, без копирования локальных изменений.
- В основном checkout первоначально были изменены `.github/workflows/ci.yml`, `scripts/packaging/symbol-inputs.py`, `scripts/packaging/test_symbol_inputs.py`. Они не редактировались, не переносились и не прятались через stash.
- Во время работы основной checkout независимо продвинулся до `63127af6e92dacfd41c6018bf9c2c99bd4a2c079` и стал чистым. Этот агент не выполнял там записей в исходники, commit или reset. Данная реализация намеренно остаётся на указанной исходной базе; совместимость с последующими изменениями main требует отдельного ревью при интеграции.
- Mobile checkout содержит многочисленные посторонние изменения inbox/generated-файлов. В него не записывались изменения; отдельный mobile worktree не понадобился.

Полный список файлов результата:

| Статус | Файл |
|---|---|
| Изменён | `crates/gateway/src/voice/incremental.rs` |
| Изменён | `crates/gateway/src/voice/session_buffer.rs` |
| Новый | `VOICE_FINALIZED_CHUNKS_REVIEW.md` |

Удалённых файлов нет. Staged-изменений нет. Весь результат оставлен незакоммиченным.

## Общий путь и три приоритета

Сохранён снимок модели/режима при Start в `voice_handlers.rs` и существующий `SessionSpeechTranscriber`. Desktop и mobile отправляют Start/audio/Finalize общему gateway; mobile-capture.ts выполняет drain перед Finalize. Новый алгоритм находится только в gateway. UI, протокол, адаптеры и зависимости не изменены.

`transcribe_while_recording=true`: существующий единственный blocking worker последовательно нормализованный PCM пропускает через прежний SmoothedVoiceVad. `accept_speech` забирает уже вычисленные emitted intervals; `push` передаёт их вместе с PCM в ограниченный pending-диапазон. Закрытые паузы дают кандидатов. На 45 секундах подготовленного PCM окончательно выбирается один разрез и вызывается `decode_until`. PCM левой части принадлежит только этому вызову, правая часть остаётся следующему диапазону. Повторного распознавания готовых частей нет.

После полного drain `process` вызывает прежний `finish_incremental_tail`, принимает реальный хвост с его mapping, затем вызывает тот же planner через `finish`. При остатке >40 секунд выбираются границы; последняя непустая часть передаётся без минимального порога. Результаты последовательно добавляются в итоговую строку. Ошибка немедленно возвращается из worker целиком; строки предыдущих успешных частей не являются успешным outcome.

`transcribe_while_recording=false`: тело `process_buffered` сохранено. Он ждёт Commit, готовит всю VAD-запись и вызывает transcriber один раз. На chunking не переведён.

## Параметры, шкалы, пределы

Исходный согласованный набор Handy, без заявления об оптимальности для Pioneer:

| Параметр | Секунды | Реальные samples @16 kHz |
|---|---:|---:|
| Target | 30 | 480000 |
| Максимальный ASR-фрагмент | 40 | 640000 |
| Минимальная плановая часть | 5 | 80000 |
| Lookahead | 5 | 80000 |
| Жёсткий pending cap | 45 | 720000 |

1. Source — все реальные нормализованные samples, включая тишину, удалённую VAD. `source_samples`, `VadInterval.start/end/quiet_run_start` находятся в этой шкале.
2. Prepared PCM — только выдача существующей VAD-политики, включая её onset/prefill/hangover. `speech_samples`, `chunk_start_sample`, prepared-координаты AudioTimeline и все ограничения planner находятся в этой шкале.
3. ASR-вход — одна неперекрывающаяся часть prepared PCM. Размер проверяется относительно фактически передаваемого в адаптер буфера; построение диапазонов ограничивает его 640000 samples.
4. Moonshine base сохраняет существующее дополнение короткого native-входа до 1600 samples. Оно не входит в реальные counters, source mapping или lookahead. Оно не может превысить 40-секундный предел. Padding неполного VAD-frame остаётся только классификационным.

Продолжительность паузы берётся из source-интервалов, позиция разреза — из prepared mapping. Середина удалённой тишины переходит в реальный concatenation seam. Размер пакета не является длиной фрагмента.

Live-разрез выполняется на точном горизонте 720000 prepared samples. Входной блок и его mapping делятся перед добавлением; oversized append отсутствует. Закрытая пауза оценивается `min(gap_seconds,1) - penalty`: penalty равен 0.05 за секунду до target и 0.1 после target. Сначала выбираются кандидаты с остатком >=5 секунд, затем fallback; при отсутствии кандидата — 40 секунд либо половина финального остатка 40–45 секунд.

Открытая пауза исключена из live-кандидатов: середина не движется вместе с текущим концом. Даже при открытой паузе cap не откладывается. На finish конец паузы известен, но её source-длительность продлевается только при совпадении последнего полного quiet-run identity. Удалённый положительный frame не объединяет независимые паузы. Если тишина перестала выходить из VAD, pending больше не растёт; новая ASR-работа не создаётся из source-часов или нулей.

Pending PCM: максимум 720000 реальных samples (2.88 MB f32). Во время decode выделены два Vec с запрошенной capacity до 720000 каждый: принадлежащая вызову левая часть и резерв pending. Итого запросы PCM-capacity planner до 1.44 млн f32 (5.76 MB), не считая allocator overhead. После возврата вызова левая часть освобождается. Runtime по существующему контракту может отдельно скопировать до 640000 samples в native input; это не архив записи. Ещё существуют текущий входной пакет, фиксированный VAD frame 512 и прежний ограниченный VAD prefill. Метаданные ограничены живым prepared-диапазоном и удаляются при продвижении; production append идёт по VAD-frame и максимум одному дополнительному split на границе.

Существующие ingress/session бюджеты не изменены: `VOICE_AUDIO_MAX_CHUNK_BYTES=3200`, lifetime raw+normalized budget `3200*1920=6144000` bytes. Для target PCM16/16k mono это примерно 64 секунды допускаемой записи, включая удалённую тишину. Planner не увеличивает этот бюджет; более длинные synthetic prepared-stream unit fixtures обходят ingress исключительно для проверки planner. Очередь ASR не добавлена, дополнительная полная запись не хранится. Минимальный live-cut >=80000 samples обеспечивает конечный прогресс.

## Источники Handy и отличия

Прочитаны закреплённые локальные файлы `cjpais/Handy`, commit `82dfab9fda18c4bc24e1f6e56d36474d38eca0bc`:

- `src-tauri/src/audio_toolkit/chunking.rs`: `plan_cuts`, `push`, `finish`, midpoint и join.
- `src-tauri/src/managers/transcription/chunked.rs`: последовательный однократный decode окончательных частей.
- `src-tauri/src/managers/transcription/live.rs`: feed/finish общего caller.

Перенесены принцип владения аудио, параметры, score/предпочтение полезного остатка и порядок результатов. Не скопированы toolkit, scorer, native streaming или выбор движков. Используются существующие VAD-сведения Pioneer; второй Silero не создаётся. Capture VAD может фильтровать PCM также у Handy, поэтому принцип не основан на предположении о непрерывном raw audio у Handy.

Отличия: точный ограниченный горизонт вместо ожидания закрытия открытой паузы; при отсутствии закрытого кандидата выполняется hard cut. Ошибка VAD/ASR фатальна всей записи, без отключения нарезки, retry или buffered fallback. Нет unbounded feed channel и архива полной записи. Пакетизация не сдвигает горизонт: push режет блок до cap. Finish использует реальный VAD-tail Pioneer. Unicode-склейка использует существующий локальный helper, дополненный диапазонами Thai/Lao/Khmer/Myanmar/Bopomofo/CJK punctuation; Korean сохраняет обычный пробел. ASCII закрывающая пунктуация не получает лишний предшествующий пробел.

## Удалённые механизмы

Удалены 3-секундные refresh растущего окна, 18-секундное окно, подтверждённый левый context/replay, left/right/context ASR-пробы, timestamps→text-cut projection, alignment/edit-cost agreement, context anchor/prefix exclusion, heuristic energy cut grid, PauseState/prepare_pause/resume/short-pause seal и старые pause lifetime/text-size ограничения. Удалён единственный ставший ненужным метод `processing_skip_preview` из session store. Старый путь не оставлен fallback.

Общие VoiceSpeechHypothesis, native timestamps, adapters, PreparedSourceMap, AudioTimeline, integrity и cancellation не удалены. Тесты удалённых проекций/проб заменены проверками нового наблюдаемого контракта; общие tail/mapping/session/buffered проверки сохранены.

## Инварианты по коду и сохранённые гарантии

- `push`: каждый consumed sample добавляется один раз с соответствующим clipped source interval. Предел проверяется внутри блока, а не после пакета.
- `decode_until`: split_off(count) делит без пересечения; только левая часть входит в PreparedSpeechBuffer. После успеха cursor сдвигается ровно на count, timeline.retain_from удаляет устаревшие данные. Правый контекст не распознаётся повторно.
- `finish`: не использует старую гипотезу; вызывает ASR на окончательном реальном остатке, включая хвост <5 секунд. Запись <=40 секунд не режется.
- `append_fragment`: не ищет общие suffix/prefix и не добавляет терминальную пунктуацию. Реальные повторы сохраняются.
- Любая ошибка/отмена возвращает Err, а не prefix. Число fragments считает успешные реальные вызовы; итоговые speech_samples — уникальную выдачу VAD; signal total — source, включая тишину. Padding не увеличивает эти значения.
- `next_processing_chunk` продолжает извлекать очередь до проверки closed; исходный порядок drain/Commit/resampler сохранён в нетронутых слоях.
- Большие буферы и planner принадлежат blocking worker вне session-store mutex. Startup-key захватывает lock только для чтения небольшого ключа. Нативный engine mutex и проверки session generation остаются в supervisor.
- Lease остаётся у worker/VoiceProcessingCompletion до фактического возврата синхронной операции; task/cancel ownership не изменён.
- Финальная cancellation/generation проверка worker сохранена. Устаревшая сессия/ошибка целостности приводит к отказу через прежние owner/store fences.
- Единственный terminal claim в `message/voice_handlers.rs` / `session_store.rs` не изменён; по-прежнему защищает публикацию turn от cancel/disconnect/повторного Finalize. Новых публикаций между фрагментами нет.

## Тесты, только написаны/прочитаны

Новые primary owner проверки:

- `finalized_ranges_are_packet_independent_disjoint_and_bounded`: continuous, closed pauses, open pause, фактические PCM-входы, точный порядок, отсутствие потерь/overlap и packetization invariance.
- `finish_preserves_short_recordings_and_prefers_a_useful_rest`: 1/7 samples, 5/40 секунд, выбор полезного остатка.
- `finish_keeps_a_short_tail_after_the_only_available_pause`: реальный остаток 1.5 секунды не отбрасывается.
- `open_pause_waits_and_evicted_positive_frames_separate_quiet_runs`: нет live-кандидата открытой паузы; identity не склеивает удалённый положительный frame.
- `fragment_join_preserves_repeats_punctuation_and_unicode`: повтор, незавершённая фраза, punctuation, UTF-8/CJK supplementary/Thai/Korean/empty.
- `long_vad_recording_matches_buffered_preparation_and_failure_is_not_partial_success`: одинаковый фактический подготовленный PCM в двух режимах, диагностика и ошибка третьего вызова после двух успешных.
- `live_worker_decodes_finalized_audio_before_stop_and_retains_open_pause`: VAD checkpoint после возврата обработки предыдущих frames доказывает фазу до Stop; source-пауза сама не запускает ASR.
- `generation_changed_during_last_native_call_rejects_completed_text`: generation действительно меняется внутри fake-вызова, затем worker возвращает ошибку вместо готового текста.

Расширен lease/cancel/shutdown тест на оба режима. Сохранены отдельные detector-blocking/error/lease, onset/subframe, next-onset hangover, seven-sample Moonshine tail, padding/source stats, two-session isolation, prepared-source mapping/partial eviction и buffered one-call/drain/backlog/error тесты. Silence fixture продлена до 60 секунд в пределах session budget. Неприкосновенное покрытие supervisor проверяет generation/cancellation перед native-entry; session_store — terminal claim и integrity.

Fake transcriber захватывает фактические переданные samples, а не вычисляет ожидаемые границы за planner. Конкурентный live-тест использует VAD checkpoints, lease-тест — подтверждённый вход в блокирующий fake ASR; timeout не является доказательством нужной фазы. Генерация в конечном вызове проверяется на границе worker, а engine-lock fences остаются у supervisor.

Ограничения: ни один тест не запускался и не компилировался; тесты используют deterministic Energy VAD/fake transcriber, не оценивают Silero/ASR качество. Prepared fixtures проверяют planner отдельно от transport limits; полные gateway fixtures соблюдают lifetime budget. Реальные адаптеры/разные платформы/features требуют последующей проверки. Успех fmt не означает успех типов, borrow checking или тестов.

## Самоаудиты

1. После определения границ: проверены четыре шкалы, source↔prepared seam, раздельное владение PCM и точный cap внутри блока. Отказ от moving open-pause midpoint, deterministic hard progress.
2. После ASR: проверены единственное место вызова на готовую часть, порядок worker, отсутствие старого алгоритма/fallback, неизменность buffered и generation/lease owners. Удалена мёртвая preview hint.
3. После finish/тестов: проверены реальный VAD-tail и onset, короткая финальная часть, пунктуация/Unicode, fail-closed, реальные counters. Исправлены budget предусловия длинных тестов и extension финального quiet-run через evicted positive frame; восстановлены независимые source-mapping и seven-sample tail регрессии.
4. Итоговый проход: рассмотрены оба изменённых tracked файла, staged/unstaged/untracked inventory, новые тесты и отчёт; пройдены текущие вызовы и типы PreparedSpeechBuffer/VoiceSpeechHypothesis/VoiceTranscript, cfg(test) helpers, worker/supervisor/store/terminal callers. Не добавлены зависимости/feature gates/model branches. Изменений mobile/desktop нет. Это статический самоаудит, а не внешняя приёмка.

## Журнал команд

Команды выполнялись из отдельного worktree, кроме явных абсолютных read-only путей и создания worktree. Повторные поиски сгруппированы; shell-скрипты редактирования не являлись запуском кода продукта/тестов.

1. `pwd`; `git -C .../pioneer status --short`; `git -C .../pioneer rev-parse HEAD`; `cat .../research/voice-streaming-app-audit-20261104/AUDIT.md` (полностью).
2. `ls -a .../Code/pioneer`; `find .../Code -name AGENTS.md ...`; `ls .../pioneer/agents.d` (каталога нет); `git branch --list`; `git log -1 origin/main`; `ls /tmp/pioneer-dictation-apps-20261104`.
3. `cat .../pioneer/AGENTS.md`; поиск agents.d/вложенных AGENTS (применимых voice-инструкций не найдено); `cat` трёх закреплённых Handy файлов chunking/chunked/live.
4. `git -C .../pioneer fetch origin main`; `git -C .../pioneer worktree add -b feat/voice-finalized-audio-chunks .../.worktrees/voice-finalized-audio-chunks origin/main`.
5. `cat .../test-audit/SKILL.md`, `references/pioneer.md`; чтение `references/audit.md`; чтение Cargo.toml, rust-toolchain.toml, CI. Пользовательский запрет любых тестов/компиляции имеет приоритет над validation-командами инструкций.
6. `rg -n`, `rg --files --hidden`, `sed -n`, `cat`, `head` для incremental.rs, vad.rs, session_buffer.rs, supervisor.rs, runtime.rs, transcription.rs, finalization.rs, session_store.rs, voice_handlers.rs и mobile-capture references. Одна попытка чтения несуществующего voice_session_handlers.rs и glob modules/*/Cargo.toml не дала содержимого; использованы реальные найденные пути.
7. `python3 - <<'PY' ...` — последовательные текстовые правки только двух scoped Rust-файлов, сохранение исходной копии `/tmp/voice-incremental-before.rs`, перенос сохранённых test-regions и вставка новых fixtures. `cat > /tmp/finalized-tests.txt`, `cat > /tmp/generation-test.txt` — промежуточные тексты новых тестов. Они не исполнялись.
8. `cargo fmt -p pioneer-gateway` — выполнен после нескольких порций правок, успешно.
9. `cargo fmt -p pioneer-gateway --check` — выполнен после правок и в последнем проходе, успешно (exit 0). Это единственная исполняемая проверка вместе с разрешённым форматированием.
10. `git diff --stat`, `git diff --numstat`, `git diff -U3/-U0 -- ...`, `git diff -- .../session_buffer.rs`, `git diff --name-status`, `git diff --cached --stat`, `git ls-files --others --exclude-standard`, `git status --porcelain=v1`, `git branch --show-current`, `git rev-parse HEAD`; повторные read-only status/log основного и status mobile checkout.
11. `cat > VOICE_FINALIZED_CHUNKS_REVIEW.md` — этот отчёт; итоговое чтение отчёта и inventory изменений.

Ни cargo check/build/clippy/test/nextest, ни приложения, inference, benchmark, генераторы, установки, commit/push/PR/merge/deployment не запускались. Настройка tracking branch выполнена самим `git worktree add`; main checkout не переключался.

## Отложено до внешней приёмки

Внешний полный review diff; разрешение компиляции и тестов; focused nextest gateway voice tests с нужными платформенными возможностями, затем остальные согласованные checks; реальные adapter/model проверки и измерение качества; отдельное рассмотрение интеграции с main, продвинувшимся во время задачи. Ни один из этих шагов данным результатом не считается выполненным.
