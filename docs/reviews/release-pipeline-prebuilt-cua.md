# Release pipeline / prebuilt CUA: передача на статическое ревью

Реализация подготовлена для внешней приёмки координатором. Ниже описан код,
а не результаты испытания релиза.

## Git и границы

- Ветка / итоговый HEAD ref: `fix/release-prebuilt-cua`.
- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/release-prebuilt-cua`.
- Проверенный origin: `https://github.com/pioneerdotai/pioneer.git`.
- Base после `git fetch origin main`: `afe798675a082425b25898bf31d72f9f60d1973d`.
- Commit реализации: `723d924da449eecb07899a113448c2d23512729c` —
  `Refactor releases around verified prebuilt CUA payloads`.
- Следующий commit содержит только этот handoff — `Document prebuilt CUA release review handoff`.
  Его полный SHA (финальный HEAD) передан в финальном ответе; ссылка HEAD ref выше
  обозначает именно его. Файл не содержит SHA собственного будущего commit.
- Точный диапазон кода: `afe798675a082425b25898bf31d72f9f60d1973d..723d924da449eecb07899a113448c2d23512729c`.
  Полный диапазон, включая отчёт: `afe798675a082425b25898bf31d72f9f60d1973d..fix/release-prebuilt-cua`.
- Main и чужие worktree не редактировались. Старый stapler-набросок не использован.
  Push, PR, теги, запуск workflow, подпись и публикация не выполнялись.
  Локальные commit выполнялись с отключением hooks только для данного вызова.

## Матрица требований 0–7: исходная причина → исправление

| № | Исходная причина / результат | Код и consumer / статическое доказательство |
| --- | --- | --- |
| 0 | HEAD SDK/source daemon → проверенный prebuilt 0.38.0 на всех ОС | `cua-release.json`, `cua-driver.py prepare/package/validate`; prepare проверяет trusted archive SHA до extract, package читает только prepared inputs; `Cargo.toml` / девять source entries `Cargo.lock` используют тот же tag/revision, preflight сверяет их; CI/smoke больше не обновляют SDK |
| 1 | Embedded update bridge / 18 Gateway assets → полное удаление / 12 assets | CLI Cargo bin/feature, entrypoint, build binding, три специальные installer-функции, dedicated symbols branch и исключительно bridge-тесты удалены; `build-gateway.py` больше не производит старые CU assets; publication inventory требует только default + full CU для каждого target |
| 2 | Cargo внутри упаковщиков → только готовые входы | `build-gateway.py`, `build-desktop.py` — явные compile producers; `package-gateway.py`, три Desktop packagers, `release-inputs.py`, `desktop-inputs.py` не вызывают Cargo/rustup/compiler/source download; skip-build принят без вложенной компиляции; отсутствующие/чужие входы дают ошибку |
| 3 | Подпись до сохранения результата → compile/runtime и symbols сохранены раньше | `release-platform.yml`: отдельные compile и package jobs; `macos-notary.py`, `desktop-checkpoint.py`, CUA package-work сохраняют подписанный состав, input binding, submission ID и Apple report; ограниченные network/stapler retries; повтор Accepted использует ID без submit |
| 4 | Desktop повторно собирает CU → один окончательный ZIP | Gateway producer выдаёт `assets/` + компактный `runtime/`; Desktop embed копирует ZIP, bootstrap, Intel ONNX, checksums и provenance; root bootstrap/ONNX сравниваются с ZIP побайтно; stage и готовые installers проверяются отдельно; symbol objects не передаются в Desktop packaging |
| 5 | Неподписанные root ONNX / ранний app.zip / поздняя MSI signature → правильный порядок | CUA подписывает root pioneer, root ONNX, daemon, nested libs, затем Gateway.app; submit всего executable payload; Accepted/report → staple app → validate → final ZIP/hashes. Desktop: app/helpers → DMG → Accepted → staple DMG и source app → validate → app.zip. Windows: собственные EXE/helpers → MSI → sign MSI → bundle EXE → sign EXE; upstream UIA не изменяется |
| 6 | Глобальный matrix barrier → target-local цепочки / единая публикация | `release.yml` вызывает один `release-platform.yml` матрицей шести target; default/CU compile jobs разделены, package ждёт только нужные platform jobs; publish проверяет все 12 Gateway + 10 Desktop assets и receipts, затем полный draft upload и remote inventory; symbols workflow независим |
| 7 | Проверки наличия / невызываемый Windows stage → полнота, provenance, регрессии | `release-preflight.py`, runtime/CUA validators проверяют version/source/run/attempt/target/variant/catalog/module/hash; native architecture; companions/licenses/manifest; macOS signatures и tickets; `windows-stage` вызывается для staging и extracted MSI; README содержит breaking manual migration |

Общие install/update, local full asset + SHA256SUMS, managed-by desktop,
schema admission, ONNX bootstrap, служебное управление, rollback/recovery и данные
сохранены. Не менялись ORM/БД, runtime API, CUA allowlist, Pioneer per-call auth,
frozen MCP bindings, images/geometry/tokens, ArtifactService/history/provider/CLI,
delivery receipts и cancellation/no-replay. Desktop/mobile/CLI остаются клиентами;
Gateway владеет daemon/private IPC/readiness/session/cursor/cancel/shutdown.
Default/server собирается отдельно без CUA feature/SDK/daemon.

## Jobs и producer → artifact → consumer

```
preflight → общий model catalog → шесть независимых reusable platform calls
  ├ Gateway default compile → compact runtime + original symbol inputs
  │                         → default package → public asset + packaged mapping
  ├ Gateway CU compile      → compact runtime + original symbol inputs ─┐
  ├ CUA prepare             → pinned prebuilt runtime/companions/legal ─┤
  │                                      CU package/sign/notary ←──────┘
  │                                      → full CU ZIP + mapping
  │                                      → compact Gateway runtime ───┐
  └ Desktop/updater compile → compact own runtime + original symbols ───┤
                                         Desktop package/sign ←───────┘
                                         → installer/update assets + mapping
all platforms → complete provenance/inventory/hash checks → draft upload → publish
              └ independent symbols dispatch → exact original link inputs + mappings
```

| Target | Runner | Default Gateway | CUA asset key | Desktop |
| --- | --- | --- | --- | --- |
| x86_64-unknown-linux-gnu | ubuntu-latest | linux-x86_64.gz | linux-x86_64 | AppImage |
| aarch64-unknown-linux-gnu | ubuntu-24.04-arm | linux-aarch64.gz | linux-arm64 | AppImage |
| x86_64-apple-darwin | macos-15-intel | macos-x86_64.zip | darwin-universal | DMG + app.zip |
| aarch64-apple-darwin | macos-15 | macos-aarch64.gz | darwin-universal | DMG + app.zip |
| x86_64-pc-windows-msvc | windows-latest | windows-x86_64.zip | windows-x86_64 | MSI + EXE |
| aarch64-pc-windows-msvc | windows-11-arm | windows-aarch64.zip | windows-arm64 | MSI + EXE |

Имена default assets сохраняют префикс `pioneer-gateway-`. На каждом target:
Gateway default — 1, Gateway CU — 1, Desktop — 1, updater — 1 собственная
компиляция. Итого 24 platform binaries + один общий catalog binary; daemon CUA —
0; packaging — 0. Зависимости Rust SDK/Swift компилируются в CU Gateway, как прежде.
Несколько тяжёлых компиляторов на одном runner не запускаются.

Artifact role включает target/variant; имена включают run ID и attempt.
Consumer выбирает точный artifact ID из этого run, проверяет содержимое; retry
может использовать предыдущий успешный producer attempt. Symbols mapping хранит
исходный `inputs.json` и выбирает именно его compiler attempt, затем сравнивает
полную metadata и module/hash. Отдельный Gateway mapping включает root/bootstrap
и app main с разными допустимыми signing envelopes. Desktop bootstrap совпадает
с root mapping; оригинальные link inputs Gateway обрабатываются один раз.

macOS recovery сохраняет подписанные payload/app/DMG, checkpoints и Apple state
в private artifacts. После transfer восстанавливаются executable modes Mach-O,
проверяются hashes/signatures. Только помеченный same-input staging до submit
можно собрать заново; подписанный/submitted/uncertain состав не удаляется.

**Явный DAG-компромисс:** CUA prepare параллелен всем тяжёлым Gateway/Desktop
compile jobs, но начинается после единственного общего catalog job. Каталог нужен
до embedded-model compilation; его hash един для всех платформ. Один matrix
reusable call не может запустить внутренний job до внешнего needs другого его
job. Не добавлялись polling/orchestrator, повторная генерация catalog или новый
общий matrix barrier. Время catalog compilation не измерялось; этот компромисс
отдельно предъявляется координатору, без заявления старта CUA до любой компиляции.

## CUA / SDK policy и официальные входы

Одна runtime-декларация: `scripts/packaging/cua-release.json`.
Tag `cua-driver-rs-v0.38.0`, source `85e2b1318bd77ccf3cfa88ccdf875a4bb7eaed19`.
[Официальный релиз](https://github.com/trycua/cua/releases/tag/cua-driver-rs-v0.38.0)
и release-manifest сверены перед внесением значений. Его prerelease-флаг —
monorepo product policy, а не причина отвергать стабильный SemVer.

| Архив (после `cua-driver-rs-0.38.0-`) | SHA256 исходного upstream archive |
| --- | --- |
| darwin-universal-binary.tar.gz | `2bb8dc4a7e69185ffd04c4a9c0eab590bd2c54b72c2c1be13ab675be4f487732` |
| linux-arm64-binary.tar.gz | `80c9ae66bc7322052dd426c6133c96086b33894bf06c972bca8215384400b33d` |
| linux-x86_64-binary.tar.gz | `31ab9709dd91ce73677e49b9f3e75c6f0d3e4d9625c286c9e58262bfbea698dd` |
| windows-arm64-binary.zip | `d25c8ebfffb34e4c095ea170baf47b87c35dc0d1116f42e46b39df2bcbff8700` |
| windows-x86_64-binary.zip | `5edaa74d0afa1b55a9668dbc4f32736507bd2bd4d75b18ca1dc8881bb12402b5` |

Base URL: `https://github.com/trycua/cua/releases/download/cua-driver-rs-v0.38.0/`.
Лицензия/notices берутся по revision и собственным trusted SHA из декларации.
Linux сохраняет четыре GNOME helper-файла из того же binary archive; отсутствует
только дополнительный portal-capture, не все Linux screenshots/input. Не включены
Node/UI/SDK DLL или необязательный cursor-theme authoring compiler. Windows UIA
копируется без изменения байтов. Upstream installer/app/service/updater не запускаются.
Upstream archive hash и final signed daemon/package hashes хранятся раздельно.

Cargo pins сверены статически с workspace и девятью package manifests обоих
revisions, а также upstream lockfiles. Dependency specs/versions совпали;
изменение Linux cfg SDK относится к musl вне матрицы. Разница upstream telemetry
package вне SDK closure отсутствует в Pioneer lock. Изменены только девять Git
source identities; registry версии/checksums не обновлялись. Экспортируемые SDK
host/session/types и используемые Gateway поля просмотрены в versioned source.
Swift/link support сохранён. Cargo resolution не выполнялся.

[Apple workflow](https://developer.apple.com/documentation/security/customizing-the-notarization-workflow)
прочитан в официальной Markdown-версии: nested app получает ticket при submit
DMG; ZIP/standalone Mach-O не являются staple targets. Это обоснование одного
Desktop submit, последующего staple исходного app и создания update ZIP.
Gateway сохраняет только public Apple Events entitlement и purpose string;
restricted upstream provisioning identity не переносится. Desktop audio entitlement сохранён.

## Регрессии и разрешённая проверка

Написаны, НЕ запускались:

- `test_cua_payload.py`: checksum cleanup; ZIP traversal/duplicates/symlink;
  TAR hardlink; version/target/architecture; UIA/GNOME companion; ready packaging
  с пустым PATH и сохранением runtime bytes.
- `test_runtime_inputs.py`: mixed source/version/target/catalog/run; corruption/
  extra file; повтор attempt; связь symbol receipt; restore без compiler;
  producer ZIP → Desktop byte identity и отказ при изменённой доставке.
- `test_macos_notary.py`: Invalid/report error → отказ без staple; retained
  Accepted → ограниченный retry/validate без нового submit, включая исчерпание.
- Сохранены обычные CLI full-package/schema/rollback/recovery regressions и
  существующие symbol-input tests; удалены только исключительно bridge-секции.
  Authoring gates test-audit применены к тестовой фазе, без массового аудита.

`cargo fmt --all -- --check`: exit 0. `git diff --check` и staged diff check:
exit 0 после исправления trailing blank lines новых файлов. Выполнены только
чтение/поиск, сопоставление официальных текстов/metadata, редактирование, Git и fmt.
Поиск всего tracked tree не нашёл действующей реализации bridge/source-build CUA
или автоматического SDK HEAD update. Новые файлы включены в этот поиск через index.

**ТЕСТЫ, СБОРКИ, CARGO CHECK И РЕЛИЗНЫЕ СКРИПТЫ НЕ ЗАПУСКАЛИСЬ.**

После внешней приёмки, только в отдельно разрешённом режиме, планируются:
`python3 -m unittest discover -s scripts/packaging -p 'test_*.py'`;
`cargo nextest run --locked -p pioneer-cli --test-threads 2 --no-fail-fast --retries 0`
и тот же owner с `--features computer-use`; locked target builds/checks всех шести
конфигураций, сохранённые install/update smoke, фактическая упаковка и native
signature/ticket/installer/driver qualification. Сейчас ни одна из этих команд
не исполнялась. Canary/tag/workflow не инициировались.

## Два финальных прохода и ограничения

Авторский проход проследил все шесть target, default/CU, manual/update assets,
provenance, missing/error/retry/cleanup paths, сохранение symbols и final-byte hashes.
Исправлены разделение variant jobs, отсутствие Windows signing env, MSI signing
order, отсутствующий provenance component MSI, повтор strip/sign bootstrap,
app.zip до ticket, root ONNX signatures, потеря executable modes при transfer,
потеря packaged mapping на checkpoint resume и несовпадение compiler attempt.

Критический проход заново проследил реальные команды и пути до пользователя.
Исправлены binding signing identity, purpose string Apple Events, возможность
отравления retry незавершённым pre-submit staging, nonregular input entries,
проверка embedded source/profile/lock, обе signed Gateway копии в symbol mapping,
нормализация существующего base64 key и отдельная проверка готового MSI/AppImage/
app.zip/DMG. Затронутые пути перечитаны после исправлений; known runtime success
не заявляется. Три focused subagent результата прочитаны и приняты; один прошёл
revision. Это внутреннее ревью не заменяет внешнюю приёмку координатором.

Непроверяемые без исполнения предпосылки: Cargo примет вручную согласованный
locked graph; SDK и native toolchains скомпилируются; GitHub Actions оценит reusable
workflow/artifact APIs; hosted runners содержат нужные native tools; macOS
signing/notary/stapler и ticket round trips реально пройдут; UIA Authenticode trust,
Linux shared libraries, upstream archive round trips всех архитектур и daemon
readiness подтвердятся. Статически просмотрены inventories universal macOS,
Linux x86_64 и Windows ARM64; остальные fail-closed guards написаны, но сами assets
не квалифицированы исполнением. Lost submit response требует восстановления ID
из сохранённого лога и не повторяется вслепую. Ускорение не измерялось и не обещается.
