# Voice text ownership: передача на внешнее ревью

- Branch: `fix/voice-incremental-text-ownership`.
- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/voice-incremental-text-ownership`.
- Base и HEAD: `f85e19f7166d0e5484f66beafcf1b3592a196fca`.
- Изменения оставлены незакоммиченными. Основной checkout, другие worktree и mobile repo не изменялись; база не обновлялась, prune не выполнялся.
- **Тесты и компиляция не подтверждены: они не запускались. Ожидается полная внешняя приёмка другой моделью и её разрешение на проверки.** Этот документ фиксирует статический self-review, не внешнюю приёмку.

## Дефект и исправление

До правок прослежена цепочка `voice_handlers → incremental::start → process → accept_speech → advance_window/refresh → flush`. Inbox отдаёт уникальный нормализованный PCM; `speech` хранит ограниченное окно. `context_samples` — уже подтверждённый PCM для replay, `text` — неизменяемый префикс, `full_hypothesis[owned_text_start..]` и `hypothesis` — заменяемый хвост. Commit закрывает inbox; processor дренирует его, обрабатывает остаток и вызывает `flush`; completion переносит lease до публикации.

Статический контрпример: `whole="ทดสอบระบบ OK"`, `left="ทดสอป"`, `right="ระบบ OK"`. Coarse whole имеет две единицы; опечатка блокировала прежний exact-prefix gate для scalar refinement. Равная edit cost разрешалась в пользу prefix fit, то есть границы 0. Whole оставался в заменяемом хвосте после удаления левого PCM; новый whole мог его потерять. Это вывод чтения исходной базы, не выполненный baseline-тест.

Теперь несовпадающая граничная единица любой пробы также включает общее scalar refinement всех трёх последовательностей. Нет новых языковых диапазонов или словарей. Native timing проверяется раньше и по-прежнему имеет приоритет.

`project_boundary` разделяет две задачи. При продвижении PCM минимальна сначала суммарная edit cost, затем расстояние до округлённого положения PCM-разреза в последовательности whole, затем prefix fit. Это устраняет систематическое назначение пропущенного пробами текста заменяемому хвосту при равной стоимости. В `refresh` остаётся прежний порядок tie-break: prefix fit, затем позиция по размерам проб; рост хвоста не доказывает увеличение подтверждённого replay. `advance_window` переносит выбранный срез **whole** в `text` до `drain`; только правый срез остаётся заменяемым. Пробы не поставляют текст результата.

## Ограничения эвристики

PCM prior используется только при равной edit cost и предполагает примерно равномерную плотность текста среди равнозначных вариантов. Он не задаёт timestamps. Пустая проба может означать пропуск ASR; полностью противоречащие пробы не дают точного акустического ownership. Возможны раннее подтверждение и неверное расположение текстовой границы, особенно при неравномерной речи, rewording или группировке сверх лимита. Проекция не восстанавливает отсутствующие в whole слова и не гарантирует акустическую точность произвольного противоречивого ASR. Проверяемое обязательство регрессии — сохранение буквального whole-текста в заданных сценариях после eviction, новых whole и Commit, без замены его текстом проб и без схлопывания повторов.

Пустой context probe без timing остаётся неопределённым: сохраняется прежний ограниченный nearest-unit anchor. **Известная пустота** `Some("")` из валидного timing по-прежнему не заменяется старым словом. Логику `timed_context_anchor` не меняли.

## Ownership на двух продвижениях (вариант с опечаткой)

| Момент | Подтверждённый `text` | Заменяемый хвост / replay |
| --- | --- | --- |
| Whole первого окна | пусто | `ทดสอบระบบ OK` |
| Eviction 1: cut 168000, удалено 144000 samples | `ทดสอบ` | хвост `ระบบ OK`, replay `อบ` |
| Новый whole после добавления PCM | `ทดสอบ` | из `อบระบบ OK OK FIN` исключается только replay `อบ` |
| Eviction 2: глобальный cut 324000, начало окна 300000 | `ทดสอบระบบ OK` | хвост `OK FIN`, replay `OK` |
| Новый PCM и Commit | `ทดสอบระบบ OK OK FIN fin.` | final whole `OK OK FIN fin.` исключает первый replay `OK`, сохраняет новое повторение |

Для пустых/противоречащих проб промежуточная эвристическая граница может отличаться; тот же буквальный финальный результат задан независимо во всех семи вариантах. Совпадение слов само по себе не используется как дедупликация.

## Сохранённые инварианты и промежуточные аудиты

- Окно PCM ≤ 288000 samples (18 s), context PCM = 24000 (1.5 s), refresh = 48000 (3 s), minimum advance = 96000 (6 s). На cap удаляется положительный PCM-интервал независимо от текста проб; caps и cadence не изменены.
- Alignment: ≤ 2048 единиц на последовательность, прежняя группировка и UTF-8 offsets; две DP-строки с O(N) памятью и O(N·(L+R)) шагами, N/L/R ≤ 2048. Новая проверка разрешения сканирует coarse whole для двух граничных единиц, без дополнительных DP или inference. Сравнение строк и подготовка ключей зависят также от байтов входа, как прежде. Fallback context anchor ≤ 256 bytes; PCM для любой context probe ограничен 1.5 s.
- Сохраняются whole spelling/case/punctuation и намеренные повторы, guards для SDK subword, straddling и coincident-end metadata, отсутствие синтетических timestamps, reuse неизменённого span и замена изменившегося хвоста (включая пустой успех/ошибку).
- После изменения проекции повторно прочитаны `refresh`, `advance_window`, `flush`, `process`, `voice_handlers`, `session_buffer` и generation/cancel fence в `supervisor`. Замечание закрыто: PCM prior нужен и при одной пустой пробе, поэтому его применяет partition tie-break независимо от размеров проб; context exclusion сохраняет отдельный порядок.
- После coverage повторно прочитаны те же владельцы и callers, новый сценарий сопоставлен с реальными PCM-срезами. Покрытие сведено к одному owner-сценарию с семью вариантами; UI/FFI дубликатов и production test seams нет. Разбор границ, сохранения повтора и пустого context выполнен статически.
- Полный diff относительно base перечитан, включая новый тест и этот отчёт. Изменения production ограничены выбором разрешения/проекции и её вызовом перед eviction. VAD/framing/final-tail, `Noise → flush`, паузы, buffered processor, capture/resampling, ASR-параметры, протокол, настройки и оболочки не менялись. Generation/cancel fences, lease, terminal claim и единственность результата/turn остаются прежними. Старый тест пауз не переписан.

## Файлы и coverage

Полный список изменённых/новых файлов:

1. `crates/gateway/src/voice/incremental.rs` — production projection и новый `inexact_mixed_probes_keep_ownership_through_two_advances_and_commit`.
2. `VOICE_TEXT_OWNERSHIP_REVIEW.md` — этот отчёт.

Новый тест использует существующий `closed_input → finish_processing → process`: реальная нормализация/VAD/PCM ownership, детерминированный fake на существующей ASR-границе. Семь вариантов: typo с отсутствующим/coarse/nonfinite timing; пустая левая, правая, обе пробы; полностью противоречащие пробы. Последние четыре включают пустой context probe. Буквальный expected: `ทดสอบระบบ OK OK FIN fin.`. Проверяются два продвижения, новая финальная гипотеза, реальные повторы против replay, уникальные diagnostics, девять точных PCM-срезов ≤ cap и суммарно 1380007 native-input samples. Это написанные assertions, не полученные результаты запуска.

Существующие meaningful регрессии timing/UTF-8/повторов, known-empty timed context, late failure/cancel и пауз оставлены без изменений. Они дополняют сценарий отдельными контрактами; новой матрицы в других слоях нет.

## Команды и отложенная валидация

Выполнено: чтение `AGENTS.md`, предоставленных Pioneer skills и `test-audit`/references, manifests/CI и Rust-кода через `cat`, `sed`, `rg`/`rg --files`; `pwd` и `ls` для путей/toolchain. Git: `status --short`, `worktree list --porcelain`, `branch --list`, `cat-file -t` для base, `worktree add -b fix/voice-incremental-text-ownership <указанный worktree> f85e19f7166d0e5484f66beafcf1b3592a196fca`, `diff`/`diff --stat`/`diff --numstat`/`diff --name-only`, `diff --no-index -- /dev/null VOICE_TEXT_OWNERSHIP_REVIEW.md` (exit 1 означает наличие нового файла), `ls-files --others --exclude-standard`, `rev-parse HEAD`, `branch --show-current`. Правки — `apply_patch`. Единственная выполненная исполняемая проверка: **`cargo fmt -p pioneer-gateway`**, exit 0; formatter затронул только scoped diff. Служебные Pioneer задачи/threads и субагенты не создавались.

Не выполнялись тесты, компиляция, cargo check/build/test/nextest/clippy/run/metadata/update/generate-lockfile, inference, генераторы, lint/typecheck, приложения и одноразовые исполняемые эксперименты. Зависимости, Cargo.lock, manifests, schemas/seals и prebuilt не менялись; commit/push/PR/merge/rebase/install/deploy не выполнялись.

После полной внешней приёмки и отдельного разрешения: focused nextest для `voice::incremental::tests` пакета `pioneer-gateway` с `--locked --test-threads 2 --no-fail-fast --retries 0`, затем согласованная компиляция/смежные проверки. В частности, ожидаются подтверждение исходного baseline failure и прогон прежних timing/UTF-8/repetition/known-empty-context/пауза/cancel регрессий. Разрешение на них этим self-review не выдаётся.
