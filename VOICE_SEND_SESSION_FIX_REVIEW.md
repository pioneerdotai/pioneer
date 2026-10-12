# Voice + Send — статическое ревью

Изменения без коммитов. Пользовательские эпизоды не воспроизводились: устранена
установленная чтением цепочка нарушений, но вся наблюдаемая последовательность
не объявляется доказанно объяснённой или исправленной. Сервер, протокол и FFI не менялись.

## Worktree, ветки и базы

| Репозиторий | Worktree | Ветка | Исходный SHA |
| --- | --- | --- | --- |
| Rust | `/Users/alexander/Code/pioneer/pioneer/.worktrees/voice-send-session-fix` | `fix/voice-send-session-fix` | `2ed80db92fa7cd72e7a376f132e72e3d55c052b1` |
| Mobile | `/Users/alexander/Code/pioneer/pioneer/.worktrees/voice-send-session-fix-app` | `fix/voice-send-session-fix` | `2867e763c0603b0c170bab61c57e7ad1d133d0c5` |

Регистрации, ветки, пути и базовые коммиты проверены; суффиксы не понадобились.
Оба основных checkout были и остаются чистыми. Прочитаны применимые AGENTS.md и
gpui-kit-design-guides для UI copy. Чужие деревья и временные реализации не использованы.

## Production и инварианты A/B/C/D

- **A — providers/runtime.rs, composer/runtime_selection.rs:** приватная принятая
  auth-эпоха отделена от полной эпохи запроса. При ротации прежние строки сохраняются,
  а loaded/request/revision fences нового потока обновляются. Loading и Failed без
  принятого результата не меняют capability target. Старый ответ отвергается;
  актуальный, включая пустой каталог, применяется. Реальная смена сессии/прав
  инвалидирует кэш и сохраняет ограничения/cancel_operation. Кэш не разрешает
  отправку: CLI-подготовка продолжает требовать актуальное чтение.
- **B — providers/runtime.rs:** прежнее ожидание каталога допускает до двух замен
  соединения при неизменной авторизации, через актуальный запрос существующего
  controller. Demand, отмена, разрешения и полная эпоха ответа проверяются;
  запрос привязан к captured connection. Probe не повторяется, таймауты не увеличены.
  Send identity сохраняется; автоматического повтора turn/start нет.
- **C — composer/voice.rs, core.rs:** generation исходного соединения добавлена
  существующему Voice-владельцу. Start проверяется до отправки/принятия; chunks,
  finalize и cleanup используют generation владельца. Finalize проверяет ответ.
  Принятые connection events, включая retiring connection, завершают потерявшие
  соединение записи через Failed. Exact-session cleanup сохранён; поздний результат
  не изменяет новую ручную попытку.
- **D — desktop composer commands.rs/owner.rs/voice.rs; mobile use-active-thread.ts,
  thread/index.tsx, mobile-capture.ts; локализации:** Failed и cancelled native
  finalize освобождают точные native request/task/refs/busy/lease. Мобильная очистка
  ждёт ending task; поздний native start снимает свои callbacks/recorder, поздняя
  деактивация AudioManager проверяет владельца. Проверяются полная identity и scope;
  замена операции также освобождает её прежний локальный capture. Следующая ручная
  попытка доступна без перезапуска. Send показывает локализованные технические
  ошибки, включая Failed после dispatch. Cancelled тихий, NoSpeech сохранён;
  поздняя ошибка не перекрывает подтверждённый результат. Send-copy предлагает
  проверить переписку без обещания отсутствующего turn. Границы очистки draft после
  dispatch не менялись, payload автоматически не восстанавливается.

## Новые типы и механизмы

Новых production-типов, публичных DTO/схем, контроллеров и потоков нет.
После рассмотрения более простых вариантов оставлены:

1. `RuntimeScope.accepted_auth`: loaded один не отличает принятую информацию от
   ожидания нового соединения; внутреннего поля достаточно.
2. Счётчик двух переходов в прежнем ожидании. Приватный wait callback нужен для
   управляемых регрессий; production сохраняет recv_timeout(100 ms) и RPC deadlines.
3. Generation в прежней Voice authority и точное завершение в existing ingress:
   совпадения gateway/session недостаточно для серверного владения соединением.
4. `MobileVoiceCaptureSession.endingTask` и приватная identity владельца native
   audio activity: ending bool не позволяет дождаться cleanup, а общий AudioManager
   без проверки владельца может выключить новую запись. Использованы прежние
   operation identity, recorder и lease.

Test-only access setter/event adapter не входят в production. Выделенные UI-методы
обрабатывают существующие исходы, не создают систему доставки или recovery.

## Доработка по четырём замечаниям (12 октября)

1. **Mobile restart:** `activateRecordingSession` получает `this.operation`.
   Restart проверяет ending и текущий план; после ожидающей деактивации повторно
   проверяет их и отсутствие другого native owner. Отменённый, заменённый или
   уступивший аудиосессию restart не включает её заново и не забирает новую запись.
   Использованы прежние identity, ending и recorderTransition.
2. **Voice preflight:** authority конкретного voice/status захватывается портом
   запроса после wait_composer_session_refresh, перед RPC, и передаётся вместе с
   ответом в существующий reducer. Точная операция, ошибка preflight и отклонение
   authority, заменённой во время RPC, сохраняются. Активная серверная запись
   по-прежнему привязана к своему исходному соединению.
3. **Finalize-response:** допуск команды по pending/session owner не менялся.
   Ответ отдельно проверяет исходную authority и точные identity/session. Он также
   допускает уже подтверждённый терминальный исход этой операции — Completed от
   TurnStarted либо сохранённый VoiceSessionResult, включая NoSpeech. Освобождённый
   owner при таком порядке не создаёт вторичную техническую ошибку; терминальный
   reducer не открывается повторно. Новая операция и новое соединение отсеиваются.
4. **Desktop Failed → native Cancelled:** до тихой очистки обработчик читает
   актуальную Client-публикацию и представляет её существующим методом. Failed
   сохраняет локализованную ошибку даже до доставки binding; очистка exact capture
   выполняется один раз. Обычный Cancelled остаётся тихим, подтверждённый результат
   не подменяется вторичной ошибкой.

В этой итерации изменены только:

- Rust: `crates/client/src/composer/voice.rs`,
  `crates/desktop-thread/src/composer/voice.rs`,
  `crates/desktop-thread/src/composer/owner.rs`, этот отчёт.
- Mobile: `src/services/voice/mobile-capture.ts`,
  `src/services/voice/mobile-capture.e2e.test.ts`.

Добавлены управляемые регрессии через прежние reducers/controllers и native ports:

- Route change сохраняет identity/владение; cancel, недействительный план и новая
  запись во время ожидающей деактивации запрещают повторную активацию старой записи.
  Deferred promises и fake clock позволяют завершить restart до последующей очистки.
- Плановый refresh завершается через существующий refresh controller заменой
  соединения перед voice/status; Ready принимается. Замена во время RPC отклоняется.
- VoiceSessionResult(TurnStarted/NoSpeech) и соответствующий TurnStarted до
  finalize-response сохраняют точную терминальную публикацию без повторного RPC.
  Новая ручная операция либо замена соединения до старого ответа защищены.
- Client Failed → native Cancelled → поздний binding сохраняет локализованную
  ошибку; соседние варианты Cancelled/Completed тихие. Проверяются точные ресурсы
  и однократная очистка, включая дублирующий поздний callback.

Прежние регрессии не ослаблены. Накопленная реализация A/B сохранена без правок;
автоматической повторной отправки нет. Новых production-типов, DTO, состояний,
workers, контроллеров и ledger не добавлено. Приватный preflight-порт возвращает
существующую authority вместе с существующим ответом; отдельная схема не нужна.

Команды этой итерации: чтение через cat/sed/rg, git status/branch/rev-parse,
git diff (включая --binary, --stat и --numstat); правки через apply_patch;
`rustfmt --edition 2024` только трёх изменённых Rust-файлов;
`git diff --check` в обоих деревьях. Основные checkout не изменялись.
**Тесты, компиляция тестовых целей, сборки, cargo check, typecheck, приложение,
линтеры, генераторы и запускающие их скрипты не выполнялись.** Проверки с
исполнением остаются отдельным этапом после принятия кода ревьюером.

## Регрессии — написаны, не запускались

| Требования | Файлы и проверяемое поведение |
| --- | --- |
| 1–2 | composer/runtime_selection.rs: Voice/Send без skills/MCP сохраняются при Loading/Failed; старый ответ отклонён, новый принят, в том числе с новой нумерацией revision |
| 3 | providers/runtime.rs + composer/workflow.rs: 1/2 перехода разрешены, третий прекращает чтение; rights/probe запрещают переход; Send identity сохранена и turn/start однократен |
| 4 | composer/runtime_selection.rs: session/rights loss отменяет Send; подтверждённый пустой каталог меняет target |
| 5 | composer/voice.rs: старые chunks/finalize/cancel не используют новое соединение; ротация во время start отвергает ответ |
| 6 | desktop composer/owner.rs + mobile-capture.e2e.test.ts: Failed/native Cancelled освобождают владельца; новая попытка, поздний callback/start, lease и поздняя AudioManager-деактивация |
| 7 | desktop composer/owner.rs + use-active-thread-focus.test.tsx: локализованный Failed, тихий Cancelled, заменённая draft identity и inactive/unmounted scope |

Использованы реальные reducers/controllers/UI owners, fake native ports, каналы и
deferred promises. Это не воспроизведение эпизодов и не полная сетевая проверка CLI-подготовки.

## Полный список изменённых файлов

Rust (относительно Rust worktree):

- `crates/client/src/composer/runtime_selection.rs`
- `crates/client/src/composer/voice.rs`
- `crates/client/src/composer/workflow.rs`
- `crates/client/src/core.rs`
- `crates/client/src/providers/runtime.rs`
- `crates/client/src/transport/ws/runtime/command_sender.rs`
- `crates/desktop-thread/locales/de.toml`
- `crates/desktop-thread/locales/en.toml`
- `crates/desktop-thread/locales/es.toml`
- `crates/desktop-thread/locales/fr.toml`
- `crates/desktop-thread/locales/hi.toml`
- `crates/desktop-thread/locales/jp.toml`
- `crates/desktop-thread/locales/ru.toml`
- `crates/desktop-thread/locales/zh.toml`
- `crates/desktop-thread/src/composer/commands.rs`
- `crates/desktop-thread/src/composer/owner.rs`
- `crates/desktop-thread/src/composer/voice.rs`
- `VOICE_SEND_SESSION_FIX_REVIEW.md`

Mobile (относительно mobile worktree):

- `src/hooks/use-active-thread-focus.test.tsx`
- `src/hooks/use-active-thread.ts`
- `src/locale/resources/de/threads.json`
- `src/locale/resources/en/threads.json`
- `src/locale/resources/es/threads.json`
- `src/locale/resources/fr/threads.json`
- `src/locale/resources/hi/threads.json`
- `src/locale/resources/ja/threads.json`
- `src/locale/resources/ru/threads.json`
- `src/locale/resources/zh/threads.json`
- `src/locale/translations/resources.json`
- `src/screens/thread/index.tsx`
- `src/services/voice/mobile-capture.e2e.test.ts`
- `src/services/voice/mobile-capture.ts`

## Выполненные команды и ограничения

Чтение: pwd, rg/rg --files, cat, sed, ls/test/command -v; git status --short,
worktree list --porcelain, branch --list/--show-current, cat-file -t, rev-parse HEAD,
diff --stat/--numstat/--name-only и адресные git diff. Создание: два
`git worktree add -b fix/voice-send-session-fix <путь> <заданный SHA>`.
Правки: Python3; прямое форматирование изменённых Rust-файлов через
`rustfmt --edition 2024`. Попытка локального `node_modules/.bin/prettier --write`
не выполнилась — formatter отсутствует; TS-фрагменты форматировались при редактировании.
Существующий translation resources.json согласован вручную, без генератора.
`git diff --check` в обоих worktree проходит.

**Тесты, тестовые цели, сборки, cargo check, typecheck, приложение, линтеры и
генераторы не запускались.** Проверки AGENTS.md отложены по указанию пользователя.
Коммиты, merge, публикация и автоматическая повторная отправка не выполнялись.
Результат передаётся на отдельное статическое ревью; работа на этом этапе остановлена.
