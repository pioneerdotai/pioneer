# Подготовка desktop к Gateway

Измерения используют существующий `DesktopStartupTrace`: timeline создаётся в
`main` до чтения config/consent и передаётся storage worker при установке runtime,
до `OnboardingIntent::Initialize`. UI `StartupTelemetry` подключается после
открытия окна, но ранние platform-интервалы уже сохранены в этом же timeline.
Экспорт происходит штатным startup pipeline после завершения startup, без
синхронного network export на пути подключения. Readiness не изменена.

## Интервалы

| `startup.stage` | Начало → конец |
| --- | --- |
| `gateway_preconnect.effect.queue_wait` | Публикация конкретного native effect под lock очереди → начало его обработки единственным storage/platform worker. Включает ожидание предыдущей работы и запуска worker; не включает ожидание публикаций на пустой очереди. Повторная публикация pending plan сохраняет первоначальный enqueue instant. |
| `gateway_preconnect.keystore.prepare` | Config/runtime home для storage adapter → возврат `DesktopSecrets::open`, включая открытие DB keystore. Выполняется только при отсутствии открытого store; неуспешное открытие может повториться на следующем effect, как раньше. |
| `gateway_preconnect.retired_credentials.cleanup` | Вызов очистки retired Gateway tokens → возврат очистки. Ошибка остаётся best effort и не меняет readiness. |
| `gateway_preconnect.environment.load` | Dispatch `LoadGatewayEnvironment`, включая config/home/timings → готовый результат environment либо ошибка. Keystore и cleanup выполняются перед этим интервалом. |
| `gateway_preconnect.local.prepare` | Dispatch фактически вызванного `PrepareLocalGateway`, включая начальные config/home/timings, внутренние проверки/ожидания, credentials load и условный `create_local_pending_device_session` с внешней командой → результат либо ошибка. Пропущенный effect записи не создаёт. |
| `gateway_preconnect.registry.load` | Загрузка/создание registry → normalization, installation identity и штатное сохранение. |
| `gateway_preconnect.registry.save` | Serialization → private file write, sync и atomic replacement; также измеряет отдельный `PersistGatewayRegistry`, если он выполнен до завершения startup. |
| `gateway_preconnect.installation.check` | Вызов `managed_gateway_requires_install` → возврат bool. |
| `gateway_preconnect.update.check` | Вызов `managed_gateway_requires_update` → возврат bool. |
| `gateway_preconnect.environment.credentials.load` | Фактическое чтение local session для определения provisioned → возврат storage; отдельно от чтения при подготовке подключаемой сессии. |
| `gateway_preconnect.service.check` | Вызов проверки активности configured service → возврат bool/error. |
| `gateway_preconnect.reachability.check` | Вызов проверки reachability → возврат bool/error. |
| `gateway_preconnect.readiness.check` | Вызов проверки local Gateway readiness в `PrepareLocalGateway` → возврат состояния/error. |
| `gateway_runtime.update_check` | В `PrepareLocalGateway`: `ensure_managed_gateway_up_to_date` целиком, включая update, если он действительно понадобился. |
| `gateway_runtime.health.wait` | В `PrepareLocalGateway`: ожидание готовности существующего service целиком, включая штатные polls. |
| `gateway_runtime.service_start` | В `PrepareLocalGateway`: вызов запуска service → возврат. |
| `gateway_session.credentials.load` | Исходный session diagnostic: чтение session credentials через platform storage, включая его queue wait. |
| `gateway_session.refresh_intent.persist` | Исходный diagnostic: сохранение pending refresh intent → возврат storage. При уже сохранённом intent стадия отсутствует. |
| `gateway_session.refresh.request` | Исходный diagnostic: первый фактический вызов refresh → ответ/error. |
| `gateway_session.credentials.persist` | Исходный diagnostic: сохранение обновлённых credentials → возврат storage. |
| `gateway_session.connect_attempt` | Существующий diagnostic попытки подключения, границы не изменены. |
| `gateway_session.identity_verify` | Существующий diagnostic проверки identity, границы не изменены. |

Environment содержит registry/load, installation/update checks и реально
выполненные local storage/service/reachability checks. Registry/load содержит
registry/save. Local/prepare содержит все внутренние операции этого effect, включая
activation после последнего credentials load. Его длительность нельзя складывать
с внутренними spans. Session storage diagnostics содержат queue wait своего effect;
очередь не является дополнительным слагаемым к времени session storage.
`gateway_runtime.update_check`, `health.wait` и `service_start` также могут
содержать внутренние действия/ожидания. Все startup stages экспортируются
соседними spans под `desktop.startup`, как и раньше: вложенность интервалов
определяется этими границами, а не `parent_span_id`. Родительские и вложенные
длительности нельзя суммировать. Детальные lifecycle traces локального service
сохраняют существующее поведение.

## Исходы и ограничения

- `outcome=ok` означает завершившийся вызов без ошибки; результат проверки `false`
  тоже является успешным выполнением проверки.
- `outcome=error` означает ошибку вызова или unwind guard. Raw errors не пишутся.
- Terminal identity/storage failure остаётся `outcome=error`, даже когда сама
  verification инвалидирует epoch через retirement и `AuthFailed`. Локальное
  свидетельство фиксирует владельца до/после обеих мутаций под существующими
  session locks; цепочка должна точно соединять исходного и текущего владельца.
- `outcome=cancelled` означает внешнюю отмену, смену operation/epoch/flight или
  shutdown, в том числе при позднем `Err`. Отмена до, между или после собственных
  terminal-мутаций не маскируется как error; shutdown всегда имеет приоритет.
  Свидетельство живёт только в вызове observer, не хранится и не экспортируется.
  Результаты операций, transitions и сохранение successor credentials не меняются.
- Short-circuit branches, отсутствующий local endpoint и ранее сохранённый
  refresh intent не создают записей для невыполненных операций. Pending session
  diagnostics без duration не импортируются как завершённые. Их отсутствие
  не означает успешную операцию с нулевой длительностью.

Session diagnostics сохраняют существующую модель первого наблюдения каждой
из шести стадий. Они принадлежат первому endpoint/operation/connection epoch и конкретному flight;
другая попытка не заполняет пропуски первой, даже если connection epoch
между retries не изменился. Идентичность flight удерживается через Weak, без
удержания выполняющейся операции. Для подготовки до регистрации
transport допускается присоединение первого connection epoch. После ошибки первого connect или завершения identity capture закрыт: retry
не добавляет identity к неудачному connect, а background refresh не заполняет
ранее пропущенный intent. Повторный refresh
в той же попытке не переписывает завершённую стадию; retry/backoff и последующие
reconnect не превращаются в новые startup diagnostics. Поэтому waterfall
показывает первый refresh request, а не сумму retry requests и backoff.
Повторные snapshots не экспортируют завершение заново.

Длительности считаются через `Instant`. Queue timestamps привязаны к monotonic
origin startup; импорт session diagnostics сохраняет исходные
`started_at_unix_ms`/`duration_ms` без нового замера в observer. Никакие endpoint
IDs/URL, operation IDs, paths, registry payloads, credentials или tokens не
экспортируются. Внутренние IDs используются только для проверки принадлежности.

Новые platform/queue observations ограничены 128 reservations на startup,
включая незавершённые guards. После лимита новые подробности не записываются;
отсутствующий span может означать и достижение лимита. Завершения после
финализации startup отбрасываются и не направляются в другой timeline. Ранние
записи сохраняются локально до `bind_consent`; экспорт разрешён только при
включённом consent с той же generation. Opt-out и последующее opt-in не оживляют
старый timeline. Новых metric instruments нет: используется штатный
`pioneer.desktop.startup.stage.duration` pipeline.

## Waterfall одного startup

Схема `pioneer-traces` хранит стандартные поля span в верхнем уровне,
атрибуты — в `attributes.custom`. Выберите `trace_id` нужного `desktop.startup`:

```apl
['pioneer-traces']
| where trace_id == 'REPLACE_WITH_TRACE_ID'
| sort by _time asc
| project _time, name, duration, span_id, parent_span_id, ['service.name'], ['attributes.custom']
```

Это инструментирование помогает установить причину задержки перед connect.
Оно не является изменением производительности или обещанием ускорения.
