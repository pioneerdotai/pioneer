# Voice Silero: передача на внешнее ревью

Статус: реализация и статическое саморевью готовы; ожидается внешняя приёмка всей
реализации другой моделью. Тесты, сборки, lint/typecheck, приложения, загрузка
модели в runtime и inference **не запускались**. Native совместимость и качество
VAD не объявляются проверенными. Выполнялся только scoped `cargo fmt`.

## Исправления внешнего аудита P2/P3 — повторная приёмка

- P2: staging создаётся через `tempfile::Builder`; на Unix права `0700`
  задаются до создания директории через `Permissions::from_mode`, без последующего
  chmod. Unix imports/configuration ограждены cfg; Windows сохраняет default
  Builder path. Порядок уничтожения native Session → TempDir не изменён.
  В existing ignored smoke добавлена проверка `mode() & 0o077 == 0` у обеих
  фактических staging-директорий. Process-wide umask тест не меняет.
- P3: existing detector cancellation/failure test теперь считает вызовы fake ASR
  через отдельный `Arc<AtomicUsize>`; fake возвращает обычный успешный результат.
  После полного `workers.tasks.wait()` проверяется ноль вызовов для каждой пары
  buffered/incremental × cancellation/failure. Assertions ошибки, lease,
  capacity и завершения worker сохранены; production processor не изменён.
- После каждого исправления проведён статический самоаудит. Сверены исходники
  закреплённого tempfile 3.27.0: Unix implementation передаёт mode в DirBuilder
  до create. Полный текущий diff от base перечитан; остальные исходники/resources
  сохранили digest относительно состояния перед этими исправлениями.
- В этом проходе изменены только `src/voice/silero.rs`, тест в
  `src/voice/incremental.rs` и два существующих отчёта. Существующий diff сохранён.
  Команды добавлены в VOICE_SILERO_COMMANDS.md; scoped `cargo fmt -p pioneer-gateway`
  выполнен успешно. Тесты, сборки, lint/typecheck и inference не запускались.
- Проверка с permissive umask отложена до явной внешней приёмки исправленного
  кода: отдельный внешний процесс, без изменения umask параллельного test runner.
  Native smoke, Windows compilation и все прежние отложенные проверки также
  остаются неподтверждёнными. Требуется повторная внешняя приёмка.

- Branch: `feat/voice-silero-vad` (суффикс не понадобился).
- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/voice-silero-vad`.
- Base = HEAD: `f85e19f7166d0e5484f66beafcf1b3592a196fca`.
- Diff незакоммичен; новые файлы имеют intent-to-add для просмотра через `git diff`.
  Commit/push/PR/merge/rebase/prune/force/fetch/install/deploy не выполнялись.
  Основной checkout и чужие worktree не редактировались; параллельные изменения
  не переносились. Отдельного mobile-worktree нет.

## Карта до правок и итоговое владение

До правок `process` и `process_buffered` независимо создавали Energy detector;
`SmoothedVoiceVad` владел smoothing, а `IncrementalSpeech`/buffered worker — PCM.
Readiness зависела от загрузки ASR; model_catalog/model_install управляли только
выбранной ASR-моделью. Packaging поставляет gateway как executable в gzip/ZIP
(также computer-use package); gateway уже встраивает другие ресурсы в binary.

Теперь `start → start_with_detector → create_voice_detector` создаёт один SDK
`SileroVad` в blocking worker записи, после admission и вне store mutex. Этот
detector передаётся в выбранный processor и живёт между кадрами и паузами.
Session, h/c и приватная временная директория принадлежат одной записи; общие
только встроенные immutable bytes. Пересоздания Session на кадрах и reset на
паузах нет. Energy и его factory доступны исключительно под `cfg(test)`.

Перед `model_operation` и без `inner` mutex supervisor вызывает обязательный
`VoiceEngineLoader::prepare_vad` в `spawn_blocking`: staging, размер/SHA256,
SDK load и один zero-frame compatibility probe. Probe уничтожается до Ready,
его recurrent state не попадает в запись. Ошибки staging/asset/SDK/probe и panic
идут в существующий bounded Failed/readiness path. Отмена проверяется до и
после синхронных native операций, затем перед следующим lifecycle этапом.
Синхронный ONNX принудительно не прерывается.

Buffered сохраняет inbox до Commit, объединяет VAD speech и делает один ASR.
Incremental сохраняет cadence/window/text ownership и `Noise → flush`.
После VAD добавлены cancellation checks; generation fence ASR, terminal claim,
повторная authorization, ingress limits и lease/publication path сохранены.
Lease удерживается до возврата native и далее completion; ошибка detector не
публикует прежний успешный preview как финальный transcript.

## Ресурс и поставка

Оригинальные байты `files/silero_vad.onnx` взяты из upstream Silero v4.0:
[commit 915dd3d639b8333a52e001af095f87c5b7f1e0ac](https://github.com/snakers4/silero-vad/tree/915dd3d639b8333a52e001af095f87c5b7f1e0ac).
Наблюдавшийся annotated tag object: `7a176cc294a2c40615458e50895ed9703782638d`;
tag unsigned, поэтому provenance закреплён immutable commit и digest, не подписью.
Ссылки на точные model/LICENSE/README находятся в [NOTICE](resources/voice/silero-v4/NOTICE.md).
README этого revision явно распространяет VAD под MIT; отдельной лицензии weights
не обнаружено. LICENSE скопирован без изменения, включая отсутствие final newline.

| Файл | Размер / SHA256 фактических байтов |
| --- | --- |
| `silero_vad.onnx` | 1,807,522 bytes; `a35ebf52fd3ce5f1469b2a36158dba761bc47b973ea3382b3186ca15b1f5af28` |
| `LICENSE` | 1,075 bytes; `2e63e9a38b6e8fc0c7bc37ce174caca1862870856c6daf5697cfb785e925520b` |
| `NOTICE.md` | `17b0e08f84b18a8b4495c3508dda87e85bbe1d3375b04e1e96706463db8b8b03` |

Модель и оба notice/license встроены через include_bytes/include_str в gateway.
Это охватывает существующие Linux/macOS/Windows x86_64/aarch64 gateway artifacts,
включая computer-use, без изменения формата архивов/installer. SDK принимает path:
worker материализует приватную временную копию вместе с notices и проверяет её
перед загрузкой. Нужна доступная для записи temp directory; ошибка честно ломает
readiness/запись. Model catalog, выбранная ASR-модель и сеть для VAD не нужны.
Mobile/FFI/UI не изменены и не содержат Silero. Отсутствие исходного embedded файла
не даст собрать gateway; отсутствие/порча staged файла даёт ошибку до native load.

Закреплён `transcribe-rs 0.3.12` (manifest range оставлен 0.3.11); включён только
gateway feature `vad-silero`. ORT/ndarray уже были в lock через onnx. Cargo.lock,
SDK/ORT версии, boundary manifests/seals и native prebuilts не менялись.
Статический protobuf wire-разбор файла без ONNX-библиотек показал:
`input: float[batch,sequence]`, `sr: int64 scalar`, `h/c: float[2,batch,64]`,
`output: float[batch,1]`, `hn/cn: float[2,batch,64]`.
SDK использует 480 samples/16 kHz и sr tensor `[16000]`; runtime совместимость
этой пары должна подтвердиться внешним smoke. Python helper upstream имеет guard
512, но сам graph имеет dynamic sequence, README говорит о training на 30 ms;
guard Python не перенесён на закреплённый Rust wrapper. Handy path отсутствует;
старый Handy код не переносился.

## Tail и самоаудиты

Сохранены 480 samples/30 ms, threshold 0.3 и prefill/hangover/onset 15/15/2.
`finish_incremental_tail` дополняет subframe нулями только на входе classifier;
в downstream попадают реальные tail и ещё не выданный prefill. Пустой tail не
вызывает inference; один оставшийся onset-frame сохраняется при Commit.
Drain сбрасывает onset counter; VAD padding не попадает в pending, prefill,
source/speech counters или ASR. Прежнее отдельное Moonshine native padding до
1600 samples сохранено.

Промежуточно перечитаны: (1) asset/license/packaging и SDK; (2) общий factory и
incremental callers; (3) buffered/Commit/lease; (4) tail и владельцы pending/counters.
Исправлены найденные замечания: неверный относительный include path; VAD probe
под model_operation (вынесен перед gate); случайно удалённый параметр test cleanup
(восстановлен); self-comparison smoke (oracle заменён прямыми SDK instances).

Финальный статический просмотр охватывает весь diff от base, новые resource/binary
и license файлы, тесты и packaging source. Production участок incremental.rs от
`accept_speech` до test module, включая refresh/advance_window, alignment/timing,
flush/decode, Moonshine padding и signal helpers, побайтно совпадает с base:
SHA256 `03d69e2f6c25826ea96abc7c2a3ef734ba78430cb74d3074cb72e515cb9fb545`.
Capture/resampling, UI, mobile/FFI, wire/settings и политика пауз не менялись.

## Тесты написаны, не запущены

- Owner VAD: SDK-sized classification для tails 1/137/479, финальный onset с
  пустым tail, zero/short input, отсутствие повторного prefill и synthetic next frame.
- Pipeline: одинаковый PCM через packets 1/137/479/1600, оба режима, onset после
  hangover с tails 0/1/137/479; exact downstream PCM, source/nonzero/speech counters;
  один buffered ASR. Сохранены existing Commit/cancel/native-Moonshine проверки.
- Blocking detector: отмена во время inference и ошибка без отмены в обоих
  режимах; отсутствие ASR/позднего transcript, lease до реального возврата и publication.
- Readiness: missing/truncated/same-size-corrupt asset; Failed вместо Ready при
  проверенном ASR install; отменённая preparation; новая generation может стать
  Ready, пока старая синхронная preparation ещё возвращается.
- Ignored smoke `silero_v4_bundled_load_inference_and_recording_state_isolation_smoke`:
  настоящее SDK load/inference на 480, две чередующиеся записи, ordinary pause и
  reset; независимые raw SDK sessions как recurrent-state oracle.
  Детерминированный Energy fixture проверяет pipeline, не качество Silero.

## Изменённые файлы

`crates/gateway/Cargo.toml`; `src/voice/{mod,silero,vad,incremental,supervisor}.rs`
в этом crate; `crates/gateway/src/lib.rs` и `src/message/tests.rs` (только test
fixtures prepare_vad/comment); `resources/voice/silero-v4/{silero_vad.onnx,LICENSE,NOTICE.md}`;
этот отчёт и [журнал команд](VOICE_SILERO_COMMANDS.md).

## Команды и отложенная проверка

Полный журнал команд находится в VOICE_SILERO_COMMANDS.md; повторные read-only
просмотры сгруппированы. Единственные Cargo вызовы: первый scoped fmt с неверными
relative file arguments завершился ошибкой «file does not exist», затем
`cargo fmt -p pioneer-gateway` успешно (повторён после последующих правок).
Python использовался только для редактирования, чтения protobuf wire fields и
digest текстовых байтов; upstream Python/модель не исполнялись.

После внешней приёмки другая модель решает, какие compilation/nextest/native smoke
и packaging проверки разрешить, с locked dependencies и соответствующими targets.
До этого ни baseline failures, ни pass новых/существующих тестов не подтверждены.
Отложены все compile/type/lint/test/inference проверки, реальная акустическая
валидация и сборка/инспекция release artifacts всех шести gateway targets.
