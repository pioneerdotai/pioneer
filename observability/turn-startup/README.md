# Наблюдаемость запуска терна

Реализация предложения из [TURN_START_OBSERVABILITY_PROPOSAL.md](../../TURN_START_OBSERVABILITY_PROPOSAL.md). Код находится в `crates/observability/src/turn_startup.rs`; desktop и gateway используют общий recorder, mobile дополнительно использует `pioneer-app/src/services/telemetry/turn-startup.ts` и тонкий FFI bridge.

## Границы измерения

Запуск начинается при принятии Send/Enter или CommitVoice. Время записи голоса до CommitVoice не входит в startup. Сообщения режима Message не создают модельный startup. Первый вывод — непустой текст модели, reasoning или структурированный вызов инструмента. Ack, `turn/started`, пустые delta, usage, tool stdout и явно помеченный replay не завершают ожидание. Буферизованные текст и reasoning имеют `output.delivery=buffered`.

| Измерение | Начало → конец | Точность |
| --- | --- | --- |
| `first_output.duration`, `receive_boundary=rust_transport` | Принятие намерения в Rust → декодирование первого модельного уведомления | Одни Rust monotonic часы. На mobile не включает вход в bridge до принятия намерения |
| `first_output.duration`, `receive_boundary=js_publication` | JS Send/CommitVoice → первая JS-публикация, обнаружившая получение модельного вывода в Rust | Одни JS `performance.now()` часы; включает планирование публикации, это не timestamp WebSocket |
| `first_text.duration` | То же начало → первый непустой assistant text | Отдельно от reasoning и tool call; сохраняется после первого вывода |
| `first_presented.duration` | Начало → GPUI render или React commit с содержимым | Прокси отображения текста/reasoning, не подтверждение показанных пикселей. Свёрнутый reasoning не считается |
| `first_text_presented.duration` | JS Send/CommitVoice → React commit первого assistant text | Отдельная мобильная presentation-метрика |
| `gateway.first_output.duration` | Приём RPC gateway → вывод runtime | Gateway monotonic часы |
| `runtime.first_output.duration` | Первый dispatch runtime → вывод runtime | У native — чтение provider chunk; у CLI — событие на входе projector |
| `first_output.delivery.duration` | Вывод runtime → успешная запись первого модельного уведомления в socket инициатора | Не подтверждает получение клиентом; другие подписчики не закрывают этот замер |

Все имена в таблице имеют префикс `pioneer.turn.startup.`; единица — **ms**, гистограммы экспортируются с temporality **Delta**. В запросах обязательно выбирать `receive_boundary`: объединение Rust и JS серий удвоит количество мобильных наблюдений и смешает разные границы.

Первый структурированный tool event может появиться позже первых внутренних токенов его аргументов. Внутреннюю очередь провайдера, вычисление модели и скрытые фазы стороннего CLI Pioneer не разделяет: измеряет доступную границу и помечает `runtime.observation`. Повторный provider round не перезапускает startup clock. Вся последующая жизнь терна не удерживает startup span открытым.

## Parent и child

`launch.path=direct|delegated` различает выполнение в текущем треде и Composer → detached task → child. `thread.role=root|child|unknown` описывает **тред пользовательского ввода**, а не текущего executor. Поэтому обычный parent → child имеет `delegated/root`, уточнение внутри существующего TaskRun — `direct/child`.

У delegated запуска исходный пользовательский clock сохраняется. Завершение технического parent-turn после создания task не считается `completed_without_output`; наблюдение привязывается к первому execution child. Отдельный follow-up в том же child имеет новый turn ID и независимый clock. Второй execution child, reviewer, retry и вложенная задача не могут перехватить эту привязку. Повторная доставка первого вывода не создаёт новый sample.

Gateway переносит контекст в task executor по исходному Composer launch из существующих метаданных задачи. Отдельные стадии: `task.create`, `task.handoff.wait` и `task.child.prepare`. Handoff начинается при входе в detached admission и заканчивается при получении задачи executor (включает создание задачи; это пересекающиеся интервалы). При повторной постановке в очередь следующий интервал начинается заново; общий startup clock не сбрасывается. Перезапуск процесса не восстанавливает monotonic clock из БД: старое наблюдение теряется, replay не создаёт искусственную успешную задержку.

В существующие модельные уведомления добавляется транспортное поле `_pioneer_startup` с локальной связью parent/child и ролью. Клиент потребляет и удаляет его до domain parsing; поле не сохраняется в событиях и не экспортируется как атрибут Axiom. Даже если инициатор остаётся в parent и не подписан на child, gateway направляет ему первое настоящее модельное уведомление и первый text, либо terminal без вывода. Доставка проходит текущую проверку `ChildObserve`; подписанному получателю дополнительная копия не отправляется. Постоянная подписка не создаётся, остальные токены продолжают идти по обычной маршрутизации. Это подтверждает получение содержимого, но **не отображение в parent**: presentation завершится только при фактическом GPUI render / React commit соответствующего child.

Отмена, ошибка или блокировка task до child передаётся инициатору отдельным `turn/startup/outcome` после проверки доступа; это уведомление не считается модельным выводом. Если соединение потеряно или доступ отозван, успешная клиентская задержка не выдумывается. Все дополнительные отправки требуют существующего consent-gated наблюдения.

## Детализация

`stage.duration` и дочерние spans измеряют:

- Client: синхронный JS bridge call (duration/event на JS-часах, отдельной очереди у этого вызова нет), ожидание worker, подготовку, readiness/session refresh, подготовку/загрузку вложений, очередь отправки, socket write, очередь первого модельного события и его обработку в Rust. Если child не открыт, обработка уведомления не означает применение к видимому состоянию.
- Gateway: dispatch, admission, сохранение терна; отдельно DB admission, SQLx pool acquire, execution и commit. Scheduling classes и границы транзакций остаются прежними.
- Native: history, artifacts, skills, security, environment, context; preflight и отдельные фазы hooks; ожидание compaction coordinator и выполнение compaction; provider connect и ожидание первого вывода.
- CLI: ожидание session lock/lease, получение сессии, process spawn, initialize handshake, thread start/resume, readiness и MCP, dispatch и ожидание первого модельного события. `session.state` различает `new`, `reused`, `replaced` на gateway.
- Voice: finalize, извлечение аудиобуфера, VAD, ожидание transcriber mutex, inference и преобразование transcript в input. Текущий production путь распознавания вызывает supervisor синхронно; отдельной worker queue в этом пути нет, поэтому её время не выдумывается.
- Delivery: projection/persistence, fanout, outbound queue именно соединения-инициатора, socket write; затем Rust event queue/apply и наблюдения JS/GPUI/React.

`startup.unattributed_ms` на завершённом span и `unattributed.duration` показывают время вне измеренных интервалов **внутри одного процесса**. Интервалы объединяются и обрезаются по границам startup: вложенные/параллельные spans считаются один раз. Это покрытие верхнего уровня, не exclusive CPU time; большой родительский span всё равно нужно раскрывать до вложенных ожиданий. Время JS и Rust не вычитается друг из друга. Процентили стадий нельзя складывать.

## Корреляция и ограничения объёма

JSON-RPC `params` у `turn/start` и `voice/session/finalize` может содержать необязательное расширение:

```json
{
  "_pioneer_telemetry": {
    "version": 1,
    "traceparent": "00-12345678901234567890123456789012-1234567890123456-01",
    "input": "text",
    "runtime": "native",
    "platform": "desktop"
  }
}
```

Gateway удаляет расширение **до** типизированной десериализации и вычисления admission digest. Это не business input, idempotency key или authorization input. Неподдерживаемые/повреждённые значения не ломают RPC; gateway создаёт локальное наблюдение без родительского trace. Отсутствующий `platform` разрешён для раннего формата v1. Секреты, baggage и произвольные атрибуты через расширение не передаются.

Каждый RPC несёт собственный контекст. Для futures контекст устанавливается только на время одного poll, для spawned work передаётся явно. Завершённый startup остаётся доступным локально для первого текста/presentation. Локальные composer/turn/connection IDs не экспортируются.

Ограничения: 1024 ключа registry с учётом aliases, 128 обычных stage observations на startup, до 24 DB observations, TTL 15 минут. JS registry ограничен 128 операциями. TTL — предел хранения наблюдений, не deadline модельного терна. При opt-out registry очищается; epoch-fence исключает экспорт поздно завершившихся startup spans после повторного opt-in. Внутренний epoch-маркер удаляется перед экспортом. Consent выключает запись/экспорт; новые настройки доступа к Axiom не нужны. Конфигурация `pioneer-tracing/collector/config.yaml` пересылает OTLP traces и metrics без фильтра по имени; развёрнутую конфигурацию нужно сверить при rollout.

`model.family`, `reasoning.effort`, `runtime.kind`, `runtime.family`, `client.platform`, `input.kind`, `session.state` — ограниченные наборы. Произвольные model IDs, thread IDs, SQL, prompt, transcript, файлы, команды и тексты ошибок в новых атрибутах отсутствуют. `session.state=unknown` у клиента нормален: решение о reuse принимает gateway.

## Исходы и полнота

`outcomes` записывает один исход: `output_received`, `rejected`, `failed`, `cancelled`, `deadline_exceeded`, `blocked`, `no_speech`, `completed_without_output`, `observation_lost`. Поздние события не превращают ошибку в успех. Первый delta до RPC ack разрешён. Потеря соединения и истечение TTL завершают незакрытые наблюдения; mobile background отдельно помечает потерю UI-наблюдения.

`attempts` увеличивается при старте, имеет только `input.kind` и `observation.scope` плюс resource labels: окончательный runtime/platform ещё может быть не разрешён. Для cohorts по runtime используйте завершённые `outcomes`; для полноты по приложениям — `attempts` по `service.name`, `outcomes`, `inflight`, `oldest.age` и `observation.losses`. При сравнении коротких окон учитывайте ещё выполняющиеся старты и границы окна.

`ingress` считает RPC ingress с `correlation=remote|missing|invalid`. Это счётчик RPC, не уникальных пользовательских попыток: retries/finalize могут дать несколько ingress для одного startup. `missing` означает отсутствие контекста: причину (старая версия, неинициализированный SDK или consent) нужно проверять отдельно. `observation.losses` показывает registry/stage limits и потерянные мобильные наблюдения. Нулевые успешные latency samples не означают отсутствие медленных или неуспешных запусков.

## Axiom

В этой папке лежат готовые запросы для редактора Axiom:

- `client-received.mpl`, `mobile-action-to-observation.mpl`, `first-text.mpl`, `presentation.mpl` — клиентские задержки.
- `runtime.mpl`, `stages.mpl`, `unattributed.mpl` — разложение.
- `outcomes.mpl`, `correlation.mpl`, `losses.mpl` — исходы/полнота.
- `slow-starts.apl` → выбранный `trace_id` → `trace.apl` — конкретный waterfall.

Фильтр environment в файлах — `production`; для проверки измените его на окружение тестового запуска. Версии оставлены группировкой, поскольку версии mobile, desktop и gateway могут различаться. Для сравнения регрессии выбирайте согласованные версии каждого сервиса и одинаковые cohorts input/runtime/model/session.

Синтаксис гистограмм соответствует [Axiom histogram queries](https://axiom.co/docs/mpl/histograms-summaries), counters/группировки — [MPL language features](https://axiom.co/docs/mpl/introduction). Новые запросы ещё не исполнялись на развёрнутой версии: данные появятся после сборки и запуска компонентов с этим кодом. Dashboard/monitors в Axiom удалённо не создавались.

Для первых monitors используйте p95 вместе с числом наблюдений (например, не менее 100 за окно), отдельно по text/voice, native/Codex/Claude, platform и cold/reused session. Порог задержки выбирайте после baseline; рост `missing`, losses или failed outcomes проверяйте независимо от успешного p95. Учитывайте sampling: процентили берутся из гистограмм, не из произвольно отобранных traces.

## Проверка после развёртывания

Матрица smoke: desktop/mobile × text/voice × native/Codex/Claude × parent→child/direct child follow-up. Для parent→child повторить с открытым parent без подписки на child и с уже открытым child; сравнить отсутствие дубликатов. Для CLI повторить cold и reused session; дополнительно — вложение, длинный context с compaction, stop/no speech, отмена, timeout, disconnect, mobile background, reconnect/replay, два терна одновременно.

Для каждого запуска проверить один клиентский исход, ожидаемый gateway trace parent, отсутствие ack/пустых delta в first output, продолжение first text после reasoning и отсутствие повторного sample при дубле. Искусственная задержка на конкретном контролируемом ожидании должна увеличить соответствующий stage; первый вывод в другом терне не должен завершать наблюдение. Проверить содержимое экспортируемых атрибутов и количество series. При отключённом consent новые startup spans/metrics не должны экспортироваться.

Локальные unit/regression tests проверяют state machine, классификацию вывода, W3C validation, контекст параллельных futures, union интервалов, SDK span export и мобильные clocks/duplicates/background/bridge errors. Фактическую доставку новых данных в production эти тесты не подтверждают.
