# Voice transcription modes — external review preparation

Статус: после сообщённого полного внешнего статического аудита всех 93 изменённых/новых файлов исправлено замечание P3 к сообщению ошибки первоначальной загрузки Voice settings. Прежние исправления cancellation-тестов, runtime translation bundle и Retry сохранены. Реализация не принята: повторную внешнюю приёмку выполняет ревьюер. Компиляция, исполнение и соответствие штатной генерации не подтверждены.

## Isolation and recorded initial state

Both primary checkouts were clean (`git status --short --branch`) before work.
Their local main revisions exactly matched the task's reference SHAs; no intervening base changes required investigation.

| Repository | Worktree | Branch | Base / initial HEAD | Initial worktree state |
| --- | --- | --- | --- | --- |
| Rust | `/Users/alexander/Code/pioneer/pioneer/.worktrees/voice-transcription-modes` | `feat/voice-transcription-modes` | `dcc63b7142bf681cba96de8267a15e8383e82b11` | clean |
| Mobile | `/Users/alexander/Code/pioneer/pioneer/.worktrees/voice-transcription-modes-app` | `feat/voice-transcription-modes` | `bb2ad274ae6bb9da5accb54c0f0935a0817155c2` | clean |

No primary checkout was switched or edited. Changes remain uncommitted in these worktrees.

Финальная сверка Git: оба основных checkout остаются чистыми на приведённых main SHA; HEAD обоих task worktree равны base, staged diff пуст. Rust worktree использует common dir `/Users/alexander/Code/pioneer/pioneer/.git`, mobile worktree — `/Users/alexander/Code/pioneer/pioneer-app/.git`, несмотря на вложенное расположение. В рабочем diff 52 Rust-файла и 41 mobile-файл с учётом новых документов/тестов и добавленного после повторного аудита runtime translation bundle; точный инвентарь приведён ниже. Чужие worktree/ветки не изменялись нашей работой.

## Initial architecture map (before implementation)

- Source of settings: `GatewayVoiceConfig` plus persisted gateway Voice Input override in `crates/gateway/src/settings.rs`; wire snapshot/update are in `crates/protocol/src/settings.rs`.
- Planned canonical mode: `transcribe_while_recording: bool`, default true, optional in partial updates. Existing `transcription_strategy` is a legacy config value, not a second user switch. Both its old values must retain the actual incremental behavior when the new bool is absent.
- A session freezes its mode at admission/start. Mode changes persist and publish for future sessions without reconfiguring the model supervisor or changing engine generation.
- PCM ownership: platform capture sends existing chunks; gateway buffer store validates sequence/format and counts accepted unique data. Worker owns processing data after transfer; expensive concatenation and VAD must happen outside the global session-store mutex.
- Lifecycle: existing start admission obtains supervisor transcriber/generation and processing lease; append and input-close share the buffer-store boundary; Commit closes input and waits for the existing processing completion. Cancellation and terminal claims fence final publication and authorized turn creation.
- Cancellation/generation/native ownership: reuse current session token, supervisor generation checks, pre-inference fences after engine-lock acquisition, and lease ownership through native return. No engine mutex held while recording or waiting for Commit.
- Settings UI: desktop typed `SettingsIntent` → shared client settings runtime/planner → gateway update → persistence → existing response/notification reductions/publications. Mobile uses the same core through existing FFI/Nitro dispatch/publication. Platform shells own rows and localization only.
- Contracts: protocol settings/update, shared Voice action/SettingsIntent/publications, embedded mobile JSON schemas and generated TS snapshots. No new mobile transport ABI is planned. Native binaries, seals and source manifests cannot be regenerated before acceptance.
- Resource boundaries: keep existing cumulative byte/sample/session-capacity limits and incremental window limits. Buffered mode accumulates the accepted recording within those limits, then applies full-recording VAD and concatenates prepared speech for one ASR adapter call; the incremental window limit is not a buffered recording limit.

## Execution restrictions

Only reading, editing, static inspection, Git isolation/status/diff/log/show, and `cargo fmt` are permitted. Tests may be written but must not be executed. All builds, typechecks, lint, inference, model loads/downloads, app launches and generators are deferred until explicit external acceptance.

## Окончательный контракт

Единственный пользовательский выбор — `transcribe_while_recording` в настройках выбранного gateway. При отсутствии нового поля и в Rust `Default` значение равно `true`. В update поле имеет тип `Option<bool>`: `None` ничего не меняет. В TOML override оно также optional: отсутствие наследует config, `Some(false)` сохраняется явно и переживает перезапуск, обновления соседних настроек и смену модели.

| Значение | Во время записи | После Commit |
| --- | --- | --- |
| `true` | Существующий incremental processor, прежние context/window/tail/coalescing | Существующая финализация incremental; повтор всей записи не добавлен |
| `false` | Прежний transport принимает и накапливает чанки; ASR не вызывается | VAD всей принятой записи, объединение подготовленной речи и один вызов существующего ASR adapter при наличии речи; иначе NoSpeech без ASR |

Никакой выбор режима не зависит от имени модели. Имеющаяся специальная подготовка короткого Moonshine Base input переиспользуется для обоих режимов. Внутренняя обработка SDK и параметры ASR не изменены.

Оба legacy значения `GatewayVoiceTranscriptionStrategy` остаются допустимыми в config. До изменения они не выбирали реальную production session pipeline: после `b0866e15` та всегда была incremental. Поэтому ни старый default `BufferedGatewaySession`, ни `ExperimentalStreaming` не преобразуются в выключение нового bool. Старые диагностические enum/conversion не становятся вторым переключателем и не получают нового production consumer.

Режим фиксируется в замыкании worker при Start под существующим settings-update lock. Mode-only save не входит в `VoiceInputDesiredState`, не вызывает `apply_desired`, установку/скачивание/loading, не меняет generation и не отменяет активные сессии. `GatewayVoiceInputChangeSet.changed` отделён от `reconfigure`; существующий busy guard защищает только изменение enabled/provider/model/retry. Новая запись читает уже сохранённое effective значение независимо от клиента, который его изменил.

Сохранение выполняется существующим атомарным writer перед публикацией. При ошибке сохранения нет успешного response/notification с новым режимом; shared page сохраняет авторитетное значение и показывает существующую ошибку. Voice notification после mode-only update получает snapshot из persisted settings. Отложенные supervisor events объединяются с persisted mode под тем же lock и не возвращают старый default другому клиенту.

## Владение и конкурентные границы

| Граница | Владелец и инвариант |
| --- | --- |
| Start | `voice_session_start` проверяет доступ и Ready, читает persisted mode, закрепляет engine generation, session cancellation и один admission lease. `incremental::start` выбирает processor один раз. |
| Append | `GatewayVoiceSessionBufferStore` проверяет существующие format/sequence/byte limits. Принятые уникальные чанки остаются в порядке принятия; gaps не заполняются синтетическим PCM, duplicates не увеличивают counters. |
| Commit | `finish_processing` и append используют один mutex. Всё принятое до закрытия учитывается; после закрытия append отклоняется. Completion receiver передаётся существующему authenticated finalizer. |
| Buffered ownership | `take_closed_processing_chunks` делает `mem::take` над deque, а затем освобождает общий lock. Склейка, VAD, native preparation и ASR выполняются вне него. Raw/normalized chunks освобождаются по мере прохода. |
| VAD и хвост | Переиспользуются `push_incremental_frame` и `finish_incremental_tail`. Кадры собираются поперёк packet boundaries; последние 1–479 исходных samples и финальный onset не отбрасываются. Нулевая/низкоэнергетическая запись не становится речью из-за padding. |
| Native | По статическому чтению session adapter проверяет token/generation после получения engine mutex; дополнительная проверка token стоит после полной подготовки input, перед выбранным adapter. Возврат `None` при отмене обрабатывается supervisor отдельно от engine error, без перевода модели в Failed. Generation защищён удерживаемым engine mutex. Это описание production-кода, не результат воспроизведения точных моментов отмены в тестах. |
| Cancel/disconnect/revocation | Существующее удаление входа отменяет token и будит worker. Вызов native не прерывается искусственно; его поздний результат не приобретает terminal authority. |
| Shutdown | Закрывается admission, отменяется вход, tracker ждёт фактического завершения workers/native. |
| Lease/capacity | Lease остаётся у blocking worker до фактического возврата native и передаётся через completion в publication. Dropped receiver/cancel не освобождает native slot раньше времени. |
| Result/turn | Worker не создаёт turn. Существующий finalizer атомарно claims terminal session, заново проверяет authorization и запускает turn существующим путём. Silence/empty transcript дают NoSpeech; engine error не выдаёт частичный transcript. |

Существующие ограничения сохранены: PCM s16le/16 kHz/mono, до 3 200 payload bytes на пакет, cumulative raw+normalized budget `3_200 * 1_920 = 6_144_000` bytes на сессию и до четырёх worker leases на gateway. При этом формате ingress budget ограничивает число исходных samples значением 1 024 000 (64 секунды); это следствие прежнего byte budget, не новый предел длительности. Metadata deque ограничена количеством непустых PCM-пакетов в том же бюджете. Пустой payload отсекается transport decoder; минимальный допустимый PCM payload содержит один sample.

Buffered speech Vec резервируется один раз, максимум под число принятых samples (или минимум 1 600 samples для native Moonshine padding). Это одна дополнительная запись нормализованного PCM, без геометрического роста capacity. По завершении прохода исходная очередь уже освобождена. Существующий adapter создаёт свой native input; SDK allocation policy не изменена и отдельно не измерялась. Incremental 18-second window остаётся только внутренним пределом incremental processor. Ни permits БД, ни DB-транзакции не удерживаются во время ASR; новый scheduling/DB слой не добавлен.

## Что переиспользовано из истории

Из `4c56de135ca09d833d07e04bf899ed15391f1dd7` изучены `finalize_voice_session_audio`, `take_session_audio`/`into_session_audio`, `segment_samples`, `transcribe_prepared_speech_buffer` и supervisor. Восстановлен принцип полного VAD и одного ASR-вызова по объединённой речи. Не восстановлены старые handlers, lock-held flattening или `chunks_exact(480)`, терявший остаток.

Из `b0866e15a3361174a188452a5de88e148e0d42f0` сохранены нынешние transport/worker/session lifecycle. Из `5ede0866001c24462e2b9d827ab5d610389407eb` сохранены context window, уточняемый хвост, coalescing и hypothesis/generation ownership. Incremental text projection, политика контекста через паузы, ресемплинг, VAD detector/threshold/prefill/hangover не менялись.

## Общий settings path и два UI

Desktop: GPUI `Switch` → `apply_voice_input_transcribe_while_recording` → typed `SettingsIntent::VoiceTranscribeWhileRecording` → shared `voice_input_transcribe_while_recording_plan` → settings runtime queue/epoch → gateway `settings/update` → persistence → response/notification reduction → `SettingsPagePublication` → controlled switch.

Mobile: native `Switch` → существующий `dispatchSettings`/`ClientIntent::Settings` → существующий Nitro/FFI JSON dispatch → тот же typed intent/planner/runtime. `useSettingsPage('voice')` использует существующие scoped demand/publication subscriptions; новый polling или FFI ABI не добавлен. Epoch/connection/authorization fences остаются в core. Model sheet дополнительно сохраняет gateway/connection/workspace target и picker owner generation; старые callbacks проверяют актуальность target.

Desktop: General Settings → существующий Voice Input → model selection row → новый отдельный mode row → download/error presentation. Row построен теми же `h_flex/v_flex`, rem spacing, typography и стандартным `Switch`; есть localized accessibility label/tooltip, disabled по loading/connection/permissions. Как и существующий model row, он виден при включённом Voice Input. Положение проверено чтением renderer; существующий GPUI render test дополнен bounds assertions. Прочитаны применимые GPUI coding/design guides, актуальный Switch API сверялся с pinned source. Реальное окно не запускалось.

Mobile: текущий SettingsScreen дополнен минимальным Voice Input block перед прежним Language/Theme block. Внутри: enable row → ASR model row → divider → mode row → statuses/errors/retry. Только model picker открывает sheet; выбора режима в sheet/composer нет. Settings page управляет loading/pending/errors. Mode не хранится в TS; при старой native publication без поля UI ждёт актуальную publication, не придумывая свой default.

Refresh retry оставлен только для ошибки загрузки без authoritative Voice settings. При ошибке сохранения с имеющимися settings сохраняются сообщение ошибки и опубликованное значение, а Refresh-кнопка скрыта: Refresh не повторяет mode intent и не очищает PageAction.error. Существующие controls позволяют повторить действие через общий settings path без хранения последнего intent или локального persisted mode. Target/owner guards и отдельный Retry подготовки модели сохранены.

Новые title/description локализованы во всех восьми desktop locales и восьми mobile locales. RU: «Распознавать во время записи» и «Если выключено, распознавание начнётся после завершения записи». Соседние экраны не переоформлены; save button не добавлена.

## Промежуточные аудиты и исправления

1. Settings/persistence/session snapshot: исправлена опасность derived false Default; omission/update различаются; legacy strategy не превращается в buffered. Engine `reconfigure` отделён от persisted `changed`. Полные supervisor notifications объединяются с persisted mode под settings lock. Перепроверены callers update/start/publication и explicit Rust struct literals.
2. Buffered lifecycle/tail: режим исключён из engine desired identity. Использован O(1) transfer очереди; подготовка вне общего lock. Полная запись не попадает в incremental window planner. Переиспользован актуальный residual/onset helper; source counters отделены от padding. При статическом чтении видны проверки отмены после получения engine mutex и после подготовки native input перед adapter, а также отдельная обработка отменённого `None` в supervisor. Тесты не воспроизводят детерминированно отмену именно во время подготовки или уже начавшегося ожидания mutex. Добавлено bounded reserve вместо геометрического роста speech Vec.
3. UI/contracts: проверены порядок строк, controlled persisted value, typed/shared/FFI path, disabled/error/permissions и stale-target guards. На desktop дополнительно найден и исправлен пропуск `pending`: проекция Voice теперь блокирует новый переключатель на время сохранения. На mobile исправлены открытие model sheet по shared `needs_selection`, nested JSON default, font token и обработка stale dispatch. Ручные snapshots изменены только по новому контракту.
4. Полное ревью: весь tracked diff относительно обеих base SHA, callers и новые файлы прочитаны отдельно от локальных исправлений. Проверены settings/config/protocol, concurrency/cancellation/lease/publication, обе оболочки/локализации, FFI, JSON/TS snapshots и тесты. Найденный пробел authenticated buffered message coverage закрыт десятью случаями в прежнем `owned_voice_ingress_case`: success/full PCM, cancel/disconnect/revocation/shutdown, empty/error/silence и два направления смены режима с последующей записью другого клиента. Повторно прочитана вся изменённая матрица: сохранены прежние incremental assertions, устранены предположения о порядке ACK/result/turn completion, leases освобождаются перед tracker wait, fake освобождается и при unwind. Supervisor readiness snapshot делегирует default протоколу, не дублируя его. Дополнительно существующий тест изоляции двух записей расширен обоими processor modes с прежними PCM/text/counter assertions. После исправлений повторно прослежены затронутые цепочки; новые исполняемые доказательства по-прежнему отложены.

## Тесты: написаны, не запускались

- Config/protocol: missing/default true; обе legacy strategy; false roundtrip; absent partial update не reset.
- Gateway settings writer/readback: false при disabled/no model, inheritance config, reload, model/partial updates, независимый settings file gateway.
- Settings handler: mode-only update при активных Recording/Finalizing/Transcribing, generation не меняется, peer получает false; injected saver error не публикует успех; supervisor transitions/progress не перезаписывают persisted false.
- Shared core: точный mode-only plan и noop; model selection не содержит mode; настоящий runtime failed-save path сохраняет авторитетное значение/error. Существующие stale ingress/delayed-response тесты расширены false.
- FFI: настоящий JSON dispatch нового intent отмечает pending только Voice page.
- Buffered processor: точный PCM >18 секунд одним вызовом с четырьмя packet boundaries; объединение речи через длинную паузу; 1/137/479/480/481/959/960/1599 samples; padding/source counters; тишина и низкоэнергетический шум; empty transcript и native error; gaps/duplicates/order/backlog; concurrent append/Commit; cancel до Commit и cancel/shutdown во время blocking fake с удержанием capacity.
- Runtime: написанный `pre_cancelled_call_returns_none_and_engine_remains_usable` передаёт уже отменённый токен, содержит assertions на `Ok(None)` и последующий успешный вызов того же stub engine без токена. Он не воспроизводит отмену во время подготовки input и не проверяет переходы состояния supervisor.
- Supervisor: написанный `voice_supervisor_session_generation_and_cancellation_fence_native_entry` содержит assertions на отклонение неверного generation/заранее отменённого вызова, успешный допустимый вызов и сохранность engine. В дополнительном конкурентном сценарии родитель отменяет токен, удерживая engine mutex; worker сигнализирует до входа в `transcribe_for_session`. Сигнал не гарантирует вход в метод или начало ожидания mutex. Сценарий содержит assertions на ошибку worker и сохранность engine, но не фиксирует детерминированно момент отмены внутри метода.
- Authenticated message path: до Commit отсутствуют ASR в buffered и любые durable TurnStarted; после Commit 320 007 samples одним вызовом, один terminal result и один authorized turn. Buffered cancel/disconnect/revocation/shutdown удерживают полную native capacity до release и не публикуют поздний/двойной результат или turn. Empty/error/silence не меняют историю. Mode update в обоих направлениях не переключает активную запись, а следующая запись peer-клиента использует сохранённое новое значение, с неизменными моделью/engine/generation.
- Изоляция: прежний тест двух concurrent sessions теперь выполняется для incremental и buffered, проверяя раздельные точные PCM, результаты и unique source counters. Тишина/низкоэнергетический шум проверяются также с включённой native подготовкой Moonshine Base, чтобы padding не допускал их к ASR.
- Desktop: существующие locale parity и GPUI rendered bounds tests расширены новым row; исходный порядок непосредственно под моделью проверен статически.
- Mobile: новый UI/adapter test файл проверяет rendered order, publication-controlled false→intent→true, shared picker/model-only intent, отсутствие TS default, stale callback и pending/permission disable. Сценарий failed save расширен реальным кодом `settings_save_failed`: опубликованный false и ошибка сохраняются, Refresh retry отсутствует, доступный переключатель снова отправляет mode intent. Отдельный сценарий `settings_load_failed` без authoritative settings содержит нажатие оставшегося Retry и assertion на Refresh. Это написанное, не запущенное UI adapter proof с fake publications, а не подмена Rust/gateway lifecycle тестов. Существующий `src/locale/translations/resources.test.ts` сохранён без изменений и не запускался; он уже сравнивает runtime bundle с namespace-файлами, дублирующий тест не добавлен.

Расположение post-engine-mutex/pre-adapter проверок и отдельная обработка `None` установлены статическим чтением production-кода. Ни runtime-тест, ни конкурентный supervisor-сценарий не воспроизводят детерминированно отмену во время подготовки input или когда worker уже заблокирован на engine mutex. Описанные assertions сохранены, но тесты не запускались; их нельзя выдавать за исполняемое подтверждение этих точных конкурентных моментов.

## Ручные schemas/types и отложенные артефакты

Все изменённые `schemas/*.json`, `schemas/client/*.json`, mobile `src/client/schema/*.json` и `src/client/generated/*.ts`, перечисленные в инвентаре ниже, обновлены вручную. Это не результат генерации. Добавлены bool/default true, optional update, новые intent/action и nested voice defaults. JSON/TS ordering и byte-for-byte соответствие генератору не подтверждены.

Mobile `src/locale/translations/resources.json`, фактически импортируемый `src/locale/i18n.ts`, также обновлён вручную: добавлены только `settings.voice` из namespace-файлов en, ru, zh, hi, es, de, fr, ja, включая английский fallback. Остальные данные и minified-формат сохранены; посимвольный Git diff содержит только восемь вставок. После явной внешней приёмки необходимы штатный `bun run locale`, просмотр его diff и существующий `resources.test.ts`; генерация и исполняемое подтверждение соответствия пока отсутствуют.

Уже в базах некоторые tracked Rust/mobile schemas различались по несвязанным полям (например, remote access URL, provider/runtime enum и task intent). Эти расхождения не переносились копированием snapshots между репозиториями и не исправлялись в этой задаче. Штатная генерация после приёмки требует отдельного просмотра полного diff.

Ни boundary-source manifests, ни hashes/seals, ни prebuilt native libraries не изменялись. Старые binaries не соответствуют новым Rust исходникам и не выдаются за пересобранную реализацию. Nitro signatures не менялись: существующего dispatch/publication ABI достаточно.

Только после явной внешней приёмки, из Rust-worktree:

```sh
cargo run --locked -p pioneer-protocol --bin schema -- schemas
cargo run --locked -p pioneer-client --features schema --bin schema -- schemas/client
cargo run --locked -p pioneer-client-ffi --features schema --bin schema -- schemas/client
```

Только после приёмки, из mobile-worktree, с обязательной привязкой к изменённым исходникам:

```sh
export PIONEER_RUST_ROOT=/Users/alexander/Code/pioneer/pioneer/.worktrees/voice-transcription-modes
bun run client:schema
bun run client:types
bun run client:contract
bun run client:check
bun run rust:build:ios
bun run rust:build:android
```

После генерации — targeted Rust nextest для затронутых crates (включая client/client-ffi schema features и desktop-settings), TS/Jest/contract checks, native smoke и реальные UI/capture/model проверки по внешне согласованному порядку. Ничего из этого здесь не выполнялось. Относительный fallback `../pioneer` для вложенного mobile-worktree использовать нельзя; ни один workflow с fallback не запускался.

## Что реально выполнялось

Чтение/поиск (`cat`, `sed`, `rg`, `ls`), редактирование исходников/документов, Git status/worktree/branch/rev-parse/diff/log/show для изоляции и статического разбора. Разрешённый formatter:

```sh
cargo fmt --package pioneer-config --package pioneer-protocol --package pioneer-client --package pioneer-client-ffi
cargo fmt --package pioneer-gateway -- --config skip_children=true crates/gateway/src/settings.rs crates/gateway/src/message/mod.rs crates/gateway/src/message/dispatch.rs crates/gateway/src/message/tests.rs
cargo fmt --package pioneer-gateway
cargo fmt --package pioneer-gateway -- --check
cargo fmt --package pioneer-gateway -- --config skip_children=true crates/gateway/src/voice/incremental.rs
cargo fmt --package pioneer-desktop-settings -- --config skip_children=true crates/desktop-settings/src/actions.rs crates/desktop-settings/src/screen.rs crates/desktop-settings/src/view.rs
cargo fmt --package pioneer-config --package pioneer-protocol --package pioneer-client --package pioneer-client-ffi --package pioneer-gateway --package pioneer-desktop-settings -- --check
```

Первые gateway `fmt --check` при разработке и доработке показали отличия форматирования в собственных изменениях; они исправлены `cargo fmt`, последующие gateway `fmt --check` завершились без вывода. Desktop и изменённые processor tests форматировались повторно после финальных исправлений. Последний общий `cargo fmt --check` для всех шести затронутых пакетов завершился с exit 0 без вывода. Это не утверждение компилируемости. Mobile formatter/linters/typecheck, тесты всех видов, cargo check/build/clippy/run, приложения, native tools, ASR/VAD inference, модели и генераторы не запускались. Commit/merge/rebase/push/PR/install/deploy не выполнялись.

Уточняющий проход после внешнего аудита ограничен названием runtime-теста, комментариями тестов и точностью описания покрытия в двух отчётах. Assertions и production-код сохранены; новые тесты, hooks, sleep и polling не добавлялись. В этом проходе выполнены чтение (`cat`, `sed`, `rg`), временное копирование исходного состояния четырёх файлов, редактирование и Git `status`/`rev-parse`/`diff` для статического сравнения. Единственная исполняемая проверка — `cargo fmt --package pioneer-gateway -- --check`, exit 0 без вывода. Тесты и остальные проверки не запускались.

Следующий проход по двум mobile-замечаниям повторного внешнего аудита ограничен runtime translation bundle, условием Refresh retry, существующим UI-test файлом и двумя отчётами. После исправлений перечитаны изменённые участки, i18n import/fallback, UI callers, shared Refresh/PageAction.error и весь дополнительный diff относительно состояния в начале прохода. Authoritative settings, target/owner guards и прежние cancellation-исправления сохранены. Выполнены только чтение/поиск (`cat`, `sed`, `rg`), Git `status`/`branch`/`rev-parse`/`diff`, временные копии (`mktemp`, `cp`) и редактирование (`apply_patch`, одноразовая точечная вставка восьми объектов в bundle через Python). Штатный генератор не запускался, bundle целиком не переформатировался. Исполняемые проверки, включая formatter, в этом проходе не выполнялись; уточнённые/новые UI-тесты не запускались.

Последующий проход P3 ограничен выбором сообщения ошибки в mobile UI, ключом `voice.loadFailed` в восьми namespace-файлах и runtime bundle, двумя assertions существующего initial-load теста и двумя отчётами. `settings_load_failed` без authoritative Voice settings отображает `loadFailed`; save error и rejected/stale действия сохраняют `saveFailed`. Failed-save сценарий, условия Retry, опубликованное значение, pending и target/owner guards не изменены. Ручная синхронизация bundle добавляет только новый ключ, сохраняя остальные данные/формат. UI-ветка, обе тестовые ситуации, локализации и весь дополнительный diff перечитаны статически. Выполнялись чтение/поиск (`cat`, `sed`, `rg`), Git `status`/`branch`/`rev-parse`/`diff`, временные копии (`mktemp`, `cp`) и редактирование (`apply_patch`, точечная Python-вставка в bundle); исполняемые проверки и генераторы не запускались. Уточнённый тест и существующий `resources.test.ts` не запускались; их исполнение и штатная сверка bundle отложены до явной внешней приёмки.

## Ограничения передачи

Реальная компиляция, тесты, генерация, соответствие prebuilt, UI layout/keyboard/screen reader на устройствах, точность распознавания и latency не подтверждены. Детерминированные fakes не доказывают качество/производительность native SDK; его внутренняя обработка полной записи оставлена прежней. Сообщённый полный внешний статический аудит не означает приёмку: повторная внешняя приёмка всей реализации другой моделью ещё обязательна. Внутреннее рассмотрение результатов подзадач не является такой приёмкой. Отмена во время подготовки input и после начала ожидания engine mutex остаётся без детерминированного воспроизведения в двух уточнённых тестах; production-проверки в этих местах описаны по статическому чтению.

## Полный инвентарь рабочего diff

Пути относительны соответствующему worktree. `NEW` отмечает новые untracked файлы; остальные — изменённые tracked. HEAD в обоих worktree по-прежнему равен base из таблицы в начале. Все изменения unstaged/uncommitted.

Rust — исходники, тесты и локализации:

```text
crates/client-ffi/src/settings_binding_tests.rs
crates/client/src/gateway/settings_store.rs
crates/client/src/runtime.rs
crates/client/src/settings/runtime.rs
crates/client/src/settings/voice.rs
crates/config/src/lib.rs
crates/desktop-settings/locales/de.toml
crates/desktop-settings/locales/en.toml
crates/desktop-settings/locales/es.toml
crates/desktop-settings/locales/fr.toml
crates/desktop-settings/locales/hi.toml
crates/desktop-settings/locales/jp.toml
crates/desktop-settings/locales/ru.toml
crates/desktop-settings/locales/zh.toml
crates/desktop-settings/src/actions.rs
crates/desktop-settings/src/screen.rs
crates/desktop-settings/src/view.rs
crates/gateway/src/message/dispatch.rs
crates/gateway/src/message/mod.rs
crates/gateway/src/message/tests.rs
crates/gateway/src/message/voice_handlers.rs
crates/gateway/src/settings.rs
crates/gateway/src/voice/incremental.rs
crates/gateway/src/voice/runtime.rs
crates/gateway/src/voice/session_buffer.rs
crates/gateway/src/voice/supervisor.rs
crates/protocol/src/settings.rs
NEW VOICE_GATEWAY_IMPLEMENTATION_NOTES.md
NEW VOICE_SETTINGS_IMPLEMENTATION_NOTES.md
NEW VOICE_TRANSCRIPTION_MODES_REVIEW.md
```

Rust — все 22 вручную изменённых JSON snapshots:

```text
schemas/client/cli_runtime_provider_settings_plan.json
schemas/client/client_intent.json
schemas/client/client_intent_dispatch_dto.json
schemas/client/gateway_settings_get_response.json
schemas/client/gateway_settings_state.json
schemas/client/gateway_settings_store.json
schemas/client/gateway_settings_update_plan.json
schemas/client/gateway_settings_update_response.json
schemas/client/gateway_ws_event.json
schemas/client/settings_page_publication.json
schemas/client/voice_input_settings_action.json
schemas/client/voice_input_settings_plan.json
schemas/client/voice_input_settings_plan_request.json
schemas/gateway_notification.json
schemas/gateway_settings_get_response.json
schemas/gateway_settings_snapshot.json
schemas/gateway_settings_update.json
schemas/gateway_settings_update_params.json
schemas/gateway_settings_update_response.json
schemas/gateway_voice_input_settings.json
schemas/gateway_voice_input_settings_update.json
schemas/gateway_voice_input_status_changed_notification.json
```

Mobile — исходники, тесты и локализации:

```text
src/client/settings.ts
src/locale/resources/de/settings.json
src/locale/resources/en/settings.json
src/locale/resources/es/settings.json
src/locale/resources/fr/settings.json
src/locale/resources/hi/settings.json
src/locale/resources/ja/settings.json
src/locale/resources/ru/settings.json
src/locale/resources/zh/settings.json
src/locale/translations/resources.json
src/screens/settings/index.test.tsx
src/screens/settings/index.tsx
NEW src/screens/settings/components/voice-input.test.tsx
NEW src/screens/settings/components/voice-input.tsx
NEW VOICE_MOBILE_IMPLEMENTATION_NOTES.md
```

Mobile — все 13 вручную изменённых JSON и 13 TS snapshots:

```text
src/client/schema/cli_runtime_provider_settings_plan.json
src/client/schema/client_intent.json
src/client/schema/client_intent_dispatch_dto.json
src/client/schema/gateway_settings_get_response.json
src/client/schema/gateway_settings_state.json
src/client/schema/gateway_settings_store.json
src/client/schema/gateway_settings_update_plan.json
src/client/schema/gateway_settings_update_response.json
src/client/schema/gateway_ws_event.json
src/client/schema/settings_page_publication.json
src/client/schema/voice_input_settings_action.json
src/client/schema/voice_input_settings_plan.json
src/client/schema/voice_input_settings_plan_request.json
src/client/generated/cli_runtime_provider_settings_plan.ts
src/client/generated/client_intent.ts
src/client/generated/client_intent_dispatch_dto.ts
src/client/generated/gateway_settings_get_response.ts
src/client/generated/gateway_settings_state.ts
src/client/generated/gateway_settings_store.ts
src/client/generated/gateway_settings_update_plan.ts
src/client/generated/gateway_settings_update_response.ts
src/client/generated/gateway_ws_event.ts
src/client/generated/settings_page_publication.ts
src/client/generated/voice_input_settings_action.ts
src/client/generated/voice_input_settings_plan.ts
src/client/generated/voice_input_settings_plan_request.ts
```

Тесты, компиляция, cargo check, приложения, модели и генераторы не запускались.
Реализация подготовлена для повторной внешней приёмки и ещё не принята.
Запуск проверок — только после явной повторной внешней приёмки.
