# Блок Б: причины завершения и native ASR sessions

Статус: **блок Б подготовлен к внешнему ревью, ещё не принят**.
Тесты, сборки, lint/typecheck, inference, приложения, генераторы исходников,
CI и native rebuild не запускались. Единственная исполненная проверка —
scoped `cargo fmt` / `cargo fmt --check` изменённых Rust-файлов.
Измерений точности, задержки и производительности нет.

- Base и HEAD: `7f226499d3fa8beac5dd15b3d241040e7a99016e`.
- Ветка: `feat/voice-engine-sessions`.
- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/voice-engine-sessions`.
- Commit/push/PR/merge/deployment/установка приложения не выполнялись.
- Основной checkout и соседние worktree не редактировались. Блоки А и В не интегрированы.
- Прочитан применимый корневой `AGENTS.md`; вложенных правил в voice нет.

## Карта API/state ownership, составленная до реализации

Исходная цепочка: запись/worker → `SessionSpeechTranscriber` →
`VoiceInputSupervisor::transcribe_for_session` → `LoadedVoiceEngine` → SDK.
Supervisor владеет общей моделью под mutex и публикует поколение также через
atomic. Worker владеет lease до возврата синхронной native операции, затем
передаёт lease вместе с completion на публикацию.

В SDK 0.3.12 все gateway-адаптеры вызывали one-shot методы. Moonshine Streaming
имел отдельный `StreamingState`, но `create_state`, `process_audio_chunk`,
`encode_streaming` и decoder были приватными. `generate` создавал новое состояние,
подавал весь PCM и вызывал final encode. Frontend держит sample/convolution tails;
encoder — накопленные features и курсор стабильных кадров; adapter — position;
decoder — self/cross KV. `GreedyDecoder::next_token` возвращал одинаковый `None`
для EOS и repetition guard. Исчерпание внешнего generation loop возвращало текст.

Новая граница: additive completion sidecar SDK → `VoiceSpeechHypothesis` →
проверка пригодности перед принятием результата. Native handle принадлежит
одному worker и поколению, содержит собственный SDK state и заходит в supervisor
отдельно на begin/feed/hypothesis/finish. Он не заимствует engine на всю запись.
`PreparedSpeechBuffer` сохранён без новых полей; число реальных prepared samples
передаётся отдельным методом trait с default для существующих реализаций.

## Пункт 5: причины завершения

1. Добавлены `Unknown`, `Normal` (наблюдался EOS), `BudgetExhausted`,
   `RepetitionGuard`. В greedy сохранён прежний repeat guard, его лимит не увеличен.
2. Moonshine Base/Streaming, Canary и Cohere возвращают completion через новый
   `transcribe_with_completion`; прежние SDK методы оставлены совместимыми.
3. Известные budget/guard stops и tentative hypothesis не проходят
   `ensure_complete`. В one-shot incremental это применяется перед использованием
   гипотезы/проб; в buffered — перед единственным terminal результатом.
4. При ошибке финального сегмента worker возвращает `Err`, включая случай уже
   успешного предыдущего сегмента. В handler идёт существующий Failed путь;
   накопленный префикс не превращается в пользовательский turn.
5. Unknown у result-only backends разрешён как прежде и не переименован в Normal.
   Пустой SDK путь без decoder/EOS также не маркируется доказанным Normal.
6. Moonshine duration budgets используют реальный prepared PCM, исключая
   source gaps до VAD и native-only padding. Rate policy сохранена: Base 6,
   Streaming 6.5 tokens/s. Streaming ограничивает decoder input positions
   `config.max_seq_len` (SDK fallback 448); первая позиция принадлежит BOS,
   одна decoder операция потребляет одну позицию, включая попытку EOS.
   Canary: `min(requested_total, 1024) - prompt_tokens` с saturating subtraction.
   Cohere сохраняет upstream cap 1024 и вычитание prompt; default новых tokens
   остаётся 512. Moonshine Base сохраняет upstream предел входа 0.1–64 s;
   неизвестный дополнительный token context для Base не выдумывается.
7. Ни повтор всей записи, ни повышение всех лимитов, ни buffered chunking,
   ни fallback/новая языковая политика не добавлены.

## Пункт 7: реальные потоковые сессии

| Backend | Native begin/feed/finish в gateway | Completion через gateway |
| --- | --- | --- |
| Moonshine Streaming tiny/small/medium | Да, при incremental режиме | EOS / budget / repetition; preview всегда tentative |
| Moonshine Base | Нет; прежний one-shot incremental | EOS / budget / repetition |
| Canary Flash / v2 | Нет; прежний one-shot incremental | EOS / budget / repetition |
| Cohere | Нет; прежний one-shot incremental | EOS / budget / repetition |
| Whisper / Breeze | Нет; прежний one-shot incremental | Unknown |
| Parakeet v2/v3 | Нет; прежний one-shot incremental | Unknown |
| SenseVoice | Нет; прежний one-shot incremental | Unknown |
| GigaAM CTC | Нет; прежний one-shot incremental | Unknown |

Capability определяется реально реализованным методом загруженного адаптера,
а не названием модели или `supports_streaming`. В buffered режиме begin не
вызывается: после Commit остаётся ровно один one-shot ASR-вызов.

SDK session хранит frontend, features, encoder cursor, adapter position и
acoustic memory между feed. Новый звук подаётся ровно один раз. Полные 1280-sample
SDK chunks идут во frontend, неполный chunk остаётся в session. В finish остаток
дополняется native-only нулями до 1280, затем `encode_streaming(is_final=true)`
освобождает lookahead. `real_samples` и `padding_samples` считаются отдельно.
Padding не включается в duration token budget. Gateway source/diagnostics и
prepared PCM остаются в прежнем владельце А.

Decoder self KV очищается перед каждым re-decode с BOS: он зависел от прежнего
acoustic context. Cross KV сохраняется только при неизменном memory; существующий
encode инвалидирует его при добавлении frames. Повторный `transcribe_with` по
растущей записи не используется для native пути.

Промежуточный decode, включая EOS, имеет `tentative=true`. В native flush всегда
вызывается finish, cached preview не принимается вместо него. Native finish
требует Normal, тогда как Unknown совместим только с прежним result-only путём.

Используются существующие pause/continuation/Commit hooks. В native dispatch
нет one-shot partition/context probes: подготовка паузы только обновляет preview,
а существующие endpoint hooks закрывают сегмент. На существующей границе 18 s
весь native сегмент завершается, после успеха следующий PCM начинает новый
неперекрывающийся сегмент. Это ресурсная граница, которая может пересечь слово;
здесь не заявляется эквивалентность качества contextual one-shot пути.
Аудио на границе не отбрасывается и не переигрывается.

## Lifecycle и ограничения

- Один handle на запись/текущий сегмент. SDK привязывает его `Arc` identity к
  конкретному загруженному model object; gateway дополнительно проверяет generation.
- Native real PCM: максимум 288000 samples (18 s). SDK pending: <1280 samples;
  финальное native padding: 0–1279. Features/memory ограничены этим сегментом
  (оценка с запасом 904 encoder frames при 320 samples/frame).
- До создания состояния проверяются конфигурация frontend (frame_len 80),
  положительные размеры, bounded max_seq_len/lookahead и checked оценка основных
  persistent f32 tensors ≤256 MiB. Features/memory/self KV резервируются один раз,
  чтобы geometric growth не удвоил эту оценку. Это оценка session tensors,
  а не предел RSS: веса, ORT scratch/outputs и allocator overhead в неё не входят.
- Gateway сохраняет прежнее bounded PCM окно и admission максимум четырёх workers;
  новой очереди/задачи на каждый audio chunk нет. Накопленный текст и общая длина
  записи по-прежнему ограничиваются существующим ingress budget владельца В.
- Engine mutex живёт в пределах отдельной операции. Ожидание inbox и пауза между
  feed не держат engine. Native decoding сам по себе синхронный и не прерывается.
- Cancel/disconnect/shutdown не освобождают lease до реального возврата native.
  Результат после cancellation отбрасывается; worker transfer completion сохраняет
  lease до publication/drop. Поколение и Ready проверяются после ожидания mutex.
- Reload сериализован тем же mutex; старое состояние не может попасть в новую
  модель. Drop состояния не вызывает отложенный inference/finish.
- SDK ошибки poison session; wrapper убирает state при ошибке и take-ит его при
  finish. Новый segment begin возможен только после успешного предыдущего finish.
  После ошибки никакого скрытого восстановления/one-shot fallback нет.
- Настоящие runtime ошибки сохраняют прежний supervisor Failed/unload путь.
  Известный защитный stop — результат с metadata, его отклонение в pipeline само
  по себе не выгружает исправную модель.
- Terminal claim и публикация в `message/voice_handlers.rs` не изменялись.
  Прочитаны claim, shutdown и Failed пути, `session_buffer` и `finalization`.

## SDK provenance и собственная дельта

Подробности: `vendor/transcribe-rs/PROVENANCE.md`.
Закреплена та же версия 0.3.12 из base lock, upstream VCS
`abc6f582967c40839c1d05ec5acc98d1068d9d58`, MIT license сохранена.
Неизменённый cached `.crate` находится в `upstream/`; extracted dependency —
в `patched/`; **весь собственный патч** — в `pioneer.patch`.
Registry/global dependencies не редактировались и не устанавливались.

Изменены ровно девять SDK source paths:
`src/lib.rs`, `src/decode/greedy.rs`, `src/onnx/moonshine/mod.rs`,
`src/onnx/moonshine/model.rs`, `src/onnx/moonshine/streaming.rs`,
новый `src/onnx/moonshine/streaming/session_api.rs`, `src/onnx/cohere/mod.rs`,
`src/onnx/canary/mod.rs`, `src/onnx/canary/decoder.rs`.
Остальные extracted upstream paths сохранены без собственной дельты.
`pioneer.patch` целиком прочитан при финальном ревью.

Root Cargo.toml добавляет path patch и исключает vendored crate из workspace
members. SDK manifest, features и версии зависимостей не менялись. В Cargo.lock
вручную убраны только registry source/checksum у заменённого package 0.3.12;
список его зависимостей неизменен. Resolver не запускался; каноничность lockfile
и сборка path patch остаются отложенными проверками.
Checksum в provenance взят из base lock, новый integrity seal не вычислялся.

Дополнительное read-only исследование: reference Moonshine C++ source, snapshot
`234f60faa0eb388b01cdf7e60aca232af37aefda`, `decoder_reset`/`decode_full`/encoder;
[официальный config medium](https://huggingface.co/moonshine-ai/moonshine-streaming/commit/0d3c38e8305db8c628612a8c440d6bd9850a68e8)
подтверждает frame_len 80. Эти источники не являются новой зависимостью и не
заменяют проверку конкретных экспортов, закреплённых в Pioneer.

## Shared hunks для объединения с А и В

| Файл/метод | Изменение Б | Что сохранить при объединении |
| --- | --- | --- |
| transcription.rs / VoiceSpeechHypothesis | completion + tentative, constructors/defaults и проверка | Unknown defaults; не терять metadata при text projection/копировании |
| transcription.rs / VoiceSpeechTranscriber | default prepared-count overload и optional native begin | Старые реализации/Box forwarding совместимы |
| incremental.rs / SessionSpeechTranscriber | dispatch prepared count/native wrapper | Generation/cancellation остаются на каждую операцию |
| incremental.rs / process_buffered | один hypothesis call + пригодность; count до padding | Один ASR call после Commit; А владеет PCM/mapping |
| incremental.rs / process | optional native handle перед циклом | В владеет доставкой/Commit completeness |
| IncrementalSpeech / accept_speech | feed только новой VAD-emitted порции | Не подавать context replay/padding как новое аудио |
| prepare_pause, refresh, advance_window, flush | только native dispatch/lifecycle branches | One-shot boundary/projection алгоритмы оставлены А; существующие pause hooks общие |
| decode | prepared real count и ensure_complete | Результат guard/budget нельзя использовать как probe/подтверждение |
| incremental.rs tests | include нового test-only файла и defaults старых literal fixtures | Сохранить тесты А вместе с новыми полями hypothesis |
| supervisor.rs | короткие native операции + prepared count forwarding | Общая mutex/generation модель и терминальные fences |
| runtime.rs | SDK sidecar adapters/native state | Только Б меняет SDK |

Silero/VAD/frame size/thresholds/audio preparation/alignment/text confirmation
algorithms/capture/codec/resampler/model catalog/UI/settings не редактировались.
`session_buffer.rs`, `finalization.rs`, handlers прочитаны, но не изменены.
18 s gateway cap и SDK MAX_SESSION_SAMPLES должны оставаться согласованными.

## Написанные тесты — ни один не запускался

Gateway `engine_sessions_tests.rs`:
- native dispatch, последовательность нескольких feed и остаток 137 samples,
  два 18 s сегмента плюс хвост, независимая следующая запись;
- native finish через существующий pause endpoint;
- buffered one-shot при наличии native capability; result-only incremental;
- EOS/Unknown compatibility и отклонение budget/guard, непубликация успешного
  префикса при неуспешном втором сегменте;
- ошибка native feed: остановка без restart/fallback;
- EOS preview остаётся tentative; peer one-shot проходит при idle handle;
  смена поколения отсекает старый handle, новый получает чистое состояние;
  finish однократный;
- детерминированный барьер **внутри заблокированного feed** под реальным
  supervisor mutex: cancel, удаление session (disconnect), shutdown, cancel+
  queued reload, успешное завершение. Проверяются отсутствие раннего completion,
  удержание lease до возврата и до drop completion, один finish при успехе.

Сценарий reload+blocked feed намеренно включает cancellation перед queued reload:
он не выдаётся за доказательство generation transition во время native; переход
поколения сериализован mutex. Отдельный idle-handle тест проверяет настоящий
replacement generation. Fake coverage не проверяет ONNX/model quality.
Существующий terminal claim остаётся владельцем однократной user publication;
новые tests проверяют worker completion/finish, не полную transport notification.

Runtime: проверка real prepared count и duration budget без source gaps/padding.
SDK: дополнены настоящие greedy tests EOS/repetition; добавлен budget-exhausted
loop и Canary prompt/context cap. Отдельный **ignored** real-export тест в
session_api.rs требует `PIONEER_MOONSHINE_STREAMING_MODEL` и
`PIONEER_VOICE_SPEECH_WAV`; проверяет feed partitions, retained acoustic state,
final remainder/padding/lookahead, decoder bound, закрытие, другой model owner,
oversize/poison и новое чистое состояние. Он также не запускался.

## Самоаудиты и финальное статическое ревью

1. SDK/completion: перепроверено поведение исходной 0.3.12, все bounded decode
   loops и передачи metadata. Исправлена первоначальная классификация пути без
   decoder как Normal: теперь там Unknown. Repeat guard сохранён.
2. Native/memory: проверены acoustic cursors/lookahead, pending/final tail,
   decoder/cross KV invalidation, segment cap, identity, poison/drop. Исправлен
   риск geometric capacity growth — persistent buffers резервируются ограниченно.
3. Integration/terminal: прочитаны все новые dispatch/lifecycle ветки, обработка
   ошибок после префикса, cached preview, generation, cancel и существующий handler
   terminal claim. Проверены изменённые consumers и совместимость defaults.
4. Tests: прочитаны новые тесты и существующие fixtures, добавлены defaults во
   все hypothesis literals. Барьер действительно находится после native entry;
   ограничения fake/reload/publication coverage явно указаны выше.
5. Final tree: прочитан весь tracked diff относительно base, весь собственный
   SDK patch, новые gateway tests, provenance/license/VCS metadata и inventory
   untracked файлов. SDK source comparison показывает только перечисленные девять
   собственных paths; прочие vendored файлы — extracted upstream. Scoped fmt check
   успешен. Это статическое ревью, не компиляция и не внешняя приёмка.

## Журнал команд и действий

Рабочий каталог для изменений — только выделенный worktree. Повторные чтения
сгруппированы ниже; диапазоны `sed` использовались, чтобы дочитать большой diff
без truncation. Все Python heredoc ниже — редактирование файлов/документации,
а не тесты или вспомогательные исполняемые проверки.

1. `pwd`; `git status --short`; `git cat-file -t 7f226499d3fa8beac5dd15b3d241040e7a99016e`;
   `git worktree list`; `rg --files -g AGENTS.md` по исходному дереву.
   Read-only shell `for` также проверил наличие ancestor AGENTS.md от `/` до
   `/Users/alexander/Code/pioneer`; дополнительных правил там не оказалось.
2. `cat AGENTS.md`; `git worktree add -b feat/voice-engine-sessions .worktrees/voice-engine-sessions 7f226499d3fa8beac5dd15b3d241040e7a99016e`.
3. `cat`, `sed -n`, `rg -n`, `rg --files`, `ls -la` для root manifest/lock,
   gateway manifest/CI, voice runtime/supervisor/transcription/incremental/
   finalization/session_buffer/mod, catalog, voice_handlers, указанных Handy
   live.rs/worker.rs, SDK manifest/license/VCS, greedy и четырёх autoregressive
   backends. Прочитаны переданные Pioneer skill files и test-audit references.
4. `mkdir -p vendor/transcribe-rs/upstream vendor/transcribe-rs/patched`;
   `cp` существующего cache archive; `tar -xzf ... --strip-components=1` для
   перечисленных в provenance исходников. Ничего в Cargo registry не записывалось.
5. `apply_patch` и `python3 - <<'PY'` с `Path.read_text/write_text` для правок
   SDK/gateway, defaults test fixtures и review документов. `mkdir -p` для
   нового SDK submodule. Python `difflib.unified_diff` записывает review artifact
   pioneer.patch из исходного и изменённого текста; патч не применялся как проверка.
6. Web search/read для первичных источников Moonshine. Один web open получил
   HTTP 403; чтение продолжено через `curl -fsSL` GitHub API tree в
   `/tmp/voice-engine-moonshine-tree.json`, C++ source в
   `/tmp/voice-engine-moonshine.cpp`. Snapshot tree записал commit 234f60f… .
   Попытка чтения старого `scripts/download-moonshine-model.py` вернула 404;
   это не загрузка модели и не запуск скрипта. Дополнительный поиск нашёл
   официальный config medium; модели/зависимости не скачивались.
7. Созданы два временных fmt-only Cargo.toml в `/tmp/voice-engine-sessions-fmt/`:
   gateway edition 2024 и sdk edition 2021, с явными bin path только на изменённые
   файлы, без dependencies и с отдельным `[workspace]`.
8. По три прохода каждого из:
   `cargo fmt --manifest-path /tmp/voice-engine-sessions-fmt/gateway/Cargo.toml -- --config skip_children=true`;
   `cargo fmt --manifest-path /tmp/voice-engine-sessions-fmt/sdk/Cargo.toml -- --config skip_children=true`.
   Финально по одному соответствующему `cargo fmt ... --check -- --config skip_children=true`.
   Оба check завершились с exit 0. Полный workspace formatter не запускался.
9. Git/source inspection: `git diff --stat`, `git diff --numstat`, `git diff -- ...`,
   `git diff 7f226499d3fa8beac5dd15b3d241040e7a99016e -- ...`,
   `git diff --no-index --stat <registry SDK src> vendor/transcribe-rs/patched/src`,
   `git status --short --untracked-files=all`, `git rev-parse HEAD`,
   `git branch --show-current`, `git diff --name-only <base>`,
   `git diff --cached --stat` (index пуст). `sed -n` прочитан весь pioneer.patch
   и новые файлы; дополнительно прочитан owner test terminal claim в session_store.
10. Документ обновлён обычной записью текста; inventory ниже составлен чтением
    дерева vendor. Финальная Git-инспекция не меняет index/HEAD.

## Отложенные проверки и непроверенное поведение

До внешней приёмки **ничего из этого не выполнять**:
компиляция/typecheck/lockfile validation на поддерживаемых платформах; все новые
и затронутые тесты; ignored SDK test; real inference для всех трёх закреплённых
Moonshine Streaming экспортов и completion sidecars Base/Canary/Cohere;
фактические memory/RSS/latency measurements; качество disjoint segment boundary;
эквивалентность frontend outputs/lookahead и полного финального padding на
реальном экспорте; EOS/guard/budget на настоящей речи; общая приёмка с А и В.
Отдельно проверить SDK unit-test target при workspace exclude. Пакетный resolver,
patch reconstruction и checksum verification не запускались в этом turn.
Объективной несовместимости пятиграфового API при чтении исходников не обнаружено;
его фактическая работа не объявляется проверенной.

## Полный список файлов diff и untracked

Включены и неизменённые extracted upstream файлы, поскольку для Git они новые.

- `Cargo.lock`
- `Cargo.toml`
- `VOICE_ENGINE_SESSIONS_REVIEW.md`
- `crates/gateway/src/voice/engine_sessions_tests.rs`
- `crates/gateway/src/voice/incremental.rs`
- `crates/gateway/src/voice/runtime.rs`
- `crates/gateway/src/voice/supervisor.rs`
- `crates/gateway/src/voice/transcription.rs`
- `vendor/transcribe-rs/PROVENANCE.md`
- `vendor/transcribe-rs/patched/.cargo_vcs_info.json`
- `vendor/transcribe-rs/patched/Cargo.toml`
- `vendor/transcribe-rs/patched/Cargo.toml.orig`
- `vendor/transcribe-rs/patched/LICENSE`
- `vendor/transcribe-rs/patched/README.md`
- `vendor/transcribe-rs/patched/build.rs`
- `vendor/transcribe-rs/patched/examples/canary.rs`
- `vendor/transcribe-rs/patched/examples/cohere.rs`
- `vendor/transcribe-rs/patched/examples/gigaam.rs`
- `vendor/transcribe-rs/patched/examples/moonshine.rs`
- `vendor/transcribe-rs/patched/examples/moonshine_streaming.rs`
- `vendor/transcribe-rs/patched/examples/openai.rs`
- `vendor/transcribe-rs/patched/examples/parakeet.rs`
- `vendor/transcribe-rs/patched/examples/sense_voice.rs`
- `vendor/transcribe-rs/patched/examples/whisper.rs`
- `vendor/transcribe-rs/patched/examples/whisperfile.rs`
- `vendor/transcribe-rs/patched/src/accel.rs`
- `vendor/transcribe-rs/patched/src/audio.rs`
- `vendor/transcribe-rs/patched/src/decode/ctc.rs`
- `vendor/transcribe-rs/patched/src/decode/greedy.rs`
- `vendor/transcribe-rs/patched/src/decode/mod.rs`
- `vendor/transcribe-rs/patched/src/decode/sentencepiece.rs`
- `vendor/transcribe-rs/patched/src/decode/tokens.rs`
- `vendor/transcribe-rs/patched/src/error.rs`
- `vendor/transcribe-rs/patched/src/features/cmvn.rs`
- `vendor/transcribe-rs/patched/src/features/lfr.rs`
- `vendor/transcribe-rs/patched/src/features/mel.rs`
- `vendor/transcribe-rs/patched/src/features/mod.rs`
- `vendor/transcribe-rs/patched/src/lib.rs`
- `vendor/transcribe-rs/patched/src/onnx/PORTING.md`
- `vendor/transcribe-rs/patched/src/onnx/canary/decoder.rs`
- `vendor/transcribe-rs/patched/src/onnx/canary/mod.rs`
- `vendor/transcribe-rs/patched/src/onnx/canary/vocab.rs`
- `vendor/transcribe-rs/patched/src/onnx/cohere/mod.rs`
- `vendor/transcribe-rs/patched/src/onnx/gigaam/mod.rs`
- `vendor/transcribe-rs/patched/src/onnx/mod.rs`
- `vendor/transcribe-rs/patched/src/onnx/moonshine/mod.rs`
- `vendor/transcribe-rs/patched/src/onnx/moonshine/model.rs`
- `vendor/transcribe-rs/patched/src/onnx/moonshine/streaming.rs`
- `vendor/transcribe-rs/patched/src/onnx/moonshine/streaming/session_api.rs`
- `vendor/transcribe-rs/patched/src/onnx/parakeet/mod.rs`
- `vendor/transcribe-rs/patched/src/onnx/sense_voice/mod.rs`
- `vendor/transcribe-rs/patched/src/onnx/session.rs`
- `vendor/transcribe-rs/patched/src/remote/mod.rs`
- `vendor/transcribe-rs/patched/src/remote/openai.rs`
- `vendor/transcribe-rs/patched/src/transcriber/energy_adaptive_chunked.rs`
- `vendor/transcribe-rs/patched/src/transcriber/merge.rs`
- `vendor/transcribe-rs/patched/src/transcriber/mod.rs`
- `vendor/transcribe-rs/patched/src/transcriber/test_helpers.rs`
- `vendor/transcribe-rs/patched/src/transcriber/vad_chunked.rs`
- `vendor/transcribe-rs/patched/src/vad/mod.rs`
- `vendor/transcribe-rs/patched/src/vad/silero.rs`
- `vendor/transcribe-rs/patched/src/whisper_cpp/gpu.rs`
- `vendor/transcribe-rs/patched/src/whisper_cpp/mod.rs`
- `vendor/transcribe-rs/patched/src/whisperfile.rs`
- `vendor/transcribe-rs/patched/tests/canary.rs`
- `vendor/transcribe-rs/patched/tests/cohere.rs`
- `vendor/transcribe-rs/patched/tests/common/mod.rs`
- `vendor/transcribe-rs/patched/tests/gigaam.rs`
- `vendor/transcribe-rs/patched/tests/moonshine.rs`
- `vendor/transcribe-rs/patched/tests/openai.rs`
- `vendor/transcribe-rs/patched/tests/parakeet.rs`
- `vendor/transcribe-rs/patched/tests/sense_voice.rs`
- `vendor/transcribe-rs/patched/tests/transcribe_single_chunk.rs`
- `vendor/transcribe-rs/patched/tests/transcriber.rs`
- `vendor/transcribe-rs/patched/tests/vad_silero.rs`
- `vendor/transcribe-rs/patched/tests/whisper.rs`
- `vendor/transcribe-rs/patched/tests/whisperfile.rs`
- `vendor/transcribe-rs/pioneer.patch`
- `vendor/transcribe-rs/upstream/transcribe-rs-0.3.12.crate`
