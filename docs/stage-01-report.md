# Отчёт по этапу 01: каркас и историческая схема

Дата: 24 ноября 2026. Задание — `mtl-horizon-stage-01.md`. Выполнены пункты
1–3; пункты 4–10 плана не начинались.

Кратко: исследование upstream Horizon выполнено по исходникам и прогоном его
миграций; собран каркас на Python 3.14; создана первая миграция исторической
базы; **102 теста проходят на настоящем PostgreSQL 17**. Запросов к живому
Horizon не делалось, поэтому всё, что касается REST API, подтверждено чтением
кода, а не ответами сервера.

Содержание: [1. Что сделано](#1-что-сделано) ·
[2. Коммиты upstream](#2-коммиты-upstream) ·
[3. Дерево проекта](#3-дерево-проекта) ·
[4. Результаты прогонов](#4-результаты-прогонов) ·
[5. Тесты и критерии приёмки](#5-тесты-и-критерии-приёмки) ·
[6. Принятые решения и отклонения](#6-принятые-решения-и-отклонения) ·
[7. Найдено во время прогонов](#7-найдено-во-время-прогонов) ·
[8. Что не проверялось и не выполнялось](#8-что-не-проверялось-и-не-выполнялось) ·
[9. Команды воспроизведения](#9-команды-воспроизведения) ·
[10. Открытые вопросы](#10-открытые-вопросы)

## 1. Что сделано

| Пункт задания | Результат |
| --- | --- |
| 1. Проверить историческую модель Horizon | `docs/horizon-schema.md`: действующая схема 17 таблиц `history_*`, идентификаторы и курсоры по типам ресурса, границы курсоров, правила участников, особые случаи, Python SDK |
| 2. Минимальный каркас | `app/` (api, storage, sources, collector, backfill), `Dockerfile`, `compose.yaml`, `.env.example`, `uv.lock`, `GET /health`, `GET /ready` |
| 3. Первая миграция | `alembic/versions/0001_history_schema.py`: 15 таблиц, модели в `app/storage/models.py` |
| `docs/architecture.md` | схема, решения, границы проекта, расхождения с предложенной моделью |
| Тесты | 102 теста в 8 файлах, все на PostgreSQL 17, кроме чистых проверок курсоров и конвертаций |

## 2. Коммиты upstream

| Репозиторий | Commit | Версия |
| --- | --- | --- |
| `stellar/stellar-horizon` | `4e13b1bea4c87e770447814a242384e0eb00c015` | `main`, 2026-11-24T09:23:24Z, после 30.0.1, Protocol 30 |
| `stellar/go-stellar-sdk` (зависимость Horizon) | `35ebe09ed33c24da6df989969f42cbd648f4ce34` | `v0.9.1-0.20261118131054-35ebe09ed33c`, 2026-11-18 |
| `stellar-sdk` (PyPI) | 17.0.1 | — |

Клонирование неглубокое, в одноразовый каталог в `/tmp`; в репозиторий проекта
не входит. Ссылки на файлы и строки — в `docs/horizon-schema.md`.

## 3. Дерево проекта

```
alembic.ini
alembic/
  env.py
  script.py.mako
  versions/0001_history_schema.py
app/
  __init__.py
  config.py
  api/__init__.py
  api/app.py
  backfill/__init__.py          только описание границы
  collector/__init__.py         только описание границы
  sources/__init__.py
  sources/horizon_cursors.py
  sources/horizon_format.py
  storage/__init__.py
  storage/db.py
  storage/models.py
  storage/network.py
  storage/upsert.py
compose.yaml
Dockerfile
.dockerignore
.env.example
.gitignore
pyproject.toml
uv.lock
scripts/sdk_probe.py
tests/
  conftest.py
  factories.py
  test_api.py
  test_history_storage.py
  test_horizon_cursors.py
  test_horizon_format.py
  test_manage_data.py
  test_network_guard.py
  test_schema.py
  test_tracking_and_sync.py
docs/
  architecture.md
  horizon-schema.md
  stage-01-report.md
README.md
```

Файл задания `mtl-horizon-stage-01.md` лежит в корне проекта и в коммит не
включён.

## 4. Результаты прогонов

Окружение: Docker 29.7.2, Compose v5.5.0, образ `python:3.14-slim`
(Python 3.14.10), `postgres:17` (PostgreSQL 17.12). На хосте uv 0.13.1,
CPython 3.14.7. Образ API — 79 МБ, работает от пользователя `mtl`.

Последний прогон выполнен с удалённым томом базы, с нуля:

| Шаг | Результат |
| --- | --- |
| `docker compose build` | образ `mtl-horizon-api` собран |
| `docker compose up -d` | стек поднят |
| `GET /health` | 200 |
| `GET /ready` до миграций | 503, `{"database":"ok","migrations":"not applied","network":"not checked"}` |
| `docker compose run --rm api alembic upgrade head` | применено без вывода и ошибок |
| `GET /ready` после миграций | 200, `{"database":"ok","migrations":"ok","network":"ok"}` |
| `alembic current` | `0001 (head)` |
| `alembic check` | `No new upgrade operations detected.` |
| `docker compose --profile test run --rm tests` | `102 passed, 1 warning in 2.36s` |
| тесты с хоста (`uv run --frozen pytest`) | `102 passed, 1 warning` |
| `uv run --frozen scripts/sdk_probe.py` | 11 из 11 проверок |
| код-карта проекта, `boundaries all` | `boundaries-ok`, 27 импортов, нарушений нет |

Единственное предупреждение тестов — `StarletteDeprecationWarning`: `TestClient`
поверх `httpx` объявлен устаревшим в пользу `httpx2`.

Перезапуск и доступ (проверялось отдельным прогоном):

| Проверка | Результат |
| --- | --- |
| строка, записанная кодом приложения, после `docker compose down` и `up -d` | на месте |
| `docker compose stop db` | `/health` 200, `/ready` 503, `{"database":"unavailable",…}` |
| `docker compose start db` | `/ready` 200, данные на месте |
| опубликованные порты `db` | нет |
| опубликованные порты `api` | `8000/tcp -> 127.0.0.1:8000` |
| healthcheck образа | `healthy` (`curl`) |

## 5. Тесты и критерии приёмки

| Файл | Тестов | Что проверяет |
| --- | --- | --- |
| `test_horizon_cursors.py` | 32 | Total Order ID, разбор курсоров по типам ресурса, отклонение неверных, порядок пар |
| `test_horizon_format.py` | 30 | точность сумм и цен, идентичность активов, разбор времени |
| `test_history_storage.py` | 18 | связанная запись, идемпотентность, конфликты, сделки |
| `test_tracking_and_sync.py` | 9 | покрытие, списки наблюдения, чекпоинты |
| `test_api.py` | 5 | `/health` и `/ready` в пяти состояниях |
| `test_manage_data.py` | 3 | участники, источник, удаление записи |
| `test_schema.py` | 3 | база и модели описывают одну схему |
| `test_network_guard.py` | 2 | привязка базы к сети |
| **Итого** | **102** | |

Соответствие критерию 3 задания:

| Требуется тестом | Где |
| --- | --- |
| вставка минимальной связанной записи | `test_minimal_linked_record` |
| повторная вставка, идемпотентность | `test_rewriting_a_record_neither_duplicates_nor_downgrades_it`, `test_conflicting_content_is_reported_not_overwritten`, `test_raw_json_is_merged_keywise` |
| одна транзакция для двух участников | `test_transaction_found_through_two_participants_is_stored_once` |
| точность amount / price | `test_amounts_prices_and_times_stay_exact_in_the_database` и весь `test_horizon_format.py` |
| неизвестный тип операции, отсутствующий meta XDR | `test_unknown_operation_type_and_missing_meta_are_kept` |
| один code, разные issuer | `test_assets_with_one_code_and_different_issuers_are_distinct`, `test_native_asset_is_one_asset_without_code_and_issuer` |
| сортировка составных курсоров | `test_pair_tokens_are_not_ordered_as_text`, `test_effect_id_sorts_as_text_exactly_like_the_pair_as_numbers`, `test_account_history_is_ordered_chronologically_by_total_order_id` |
| незаполненное и пустое проверенное покрытие | `test_unverified_range_is_not_coverage`, `test_overlapping_ranges_of_one_key_are_refused` |
| фильтр `manage_data` по участнику и источнику | `test_manage_data_is_found_by_participant_and_by_source`, `test_data_entry_owner_is_not_necessarily_the_operation_source` |

Сверх списка: эффект или сделка без операции отвергается внешним ключом;
идентификатор должен соответствовать леджеру и порядку; сделка с
переставленной парой не дублируется; чекпоинт откатывается вместе с данными;
`manage_data`-удаление неотличимо от пустого значения по одной операции.

## 6. Принятые решения и отклонения

Полная таблица — в `docs/architecture.md`, раздел 9. Главное:

- **Python 3.14 вместо 3.13** — по указанию владельца. Все зависимости
  ставятся готовыми wheels, тесты проходят на 3.14.7 и 3.14.10.
- **Естественные ключи** вместо суррогатных id Horizon: G-адрес для аккаунта,
  `native` или `CODE:ISSUER` для актива. Суррогаты чужой базы не переносимы.
- **Внешние ключи включены**; у Horizon их нет.
- **У эффекта два поля**: `id` и `paging_token` — в текущем Horizon они
  различаются. Курсор сделки и эффекта — `<operation id>-<order>`, а не число.
- **Сделка хранится в ориентации источника**, её идентичность от ориентации
  не зависит.
- **Добавлены** `network_identity` (одна сеть на базу), `state` и
  `verified_at` в `sync_coverage`, запрет пересечения диапазонов.
- **`raw` nullable**, не NOT NULL: будущий импорт может не иметь REST JSON.
- Код и комментарии — на английском, документация — на русском.

Главные находки исследования — в `docs/horizon-schema.md`. Три из них меняют
представление «по документации»:

1. `history_trades_60000` — агрегация сделок (свечи за 60 секунд), а не
   таблица сделок.
2. По операции `manage_data` удаление записи неотличимо от записи пустого
   значения; различает только тип эффекта либо `envelope_xdr`.
3. Python SDK отвергает envelope с неизвестным типом операции целиком.

## 7. Найдено во время прогонов

Всё найдено выполнением на настоящей базе или образе, а не рассуждением, и
исправлено до коммита:

| Симптом | Причина |
| --- | --- |
| контейнер не стартует: `uvicorn: executable file not found` | `/app/.venv/bin` не был в `PATH` образа |
| `/ready` отдаёт 500 до миграций | у `ScriptDirectory.get_heads()` результат — строки, а не объекты |
| миграция падает: имя длиннее 63 символов | прежняя конвенция имён внешних ключей включала имя обеих таблиц |
| имена CHECK-ограничений в базе задвоены | Alembic применяет naming convention моделей к именам в миграции; один из них PostgreSQL молча обрезал |
| upsert считает каждую вставку повторной | `rowcount` оператора INSERT через Session равен −1 |
| неполная запись падает по NOT NULL | PostgreSQL проверяет NOT NULL раньше, чем конфликт по ключу; upsert переписан на «прочитать под блокировкой, затем вставить или дополнить» |

Добавлен тест `test_schema.py`, который сверяет имена всех ограничений и
индексов базы с моделями: `alembic check` CHECK-ограничения не сравнивает.
Мои же ошибки в ожиданиях тестов (граница int64 в строопах, неверные ключи)
пойманы самими тестами и исправлены.

## 8. Что не проверялось и не выполнялось

1. **Живой Horizon не запрашивался.** Все утверждения о REST — по коду
   адаптеров. Вопросы для проверки на живом инстансе перечислены в
   `docs/horizon-schema.md`, раздел 9.
2. **Horizon и Stellar Core не запускались.**
3. **Миграции upstream** применялись моим сценарием (раздел «Up» каждого файла
   через `psql`, порядок по числовому префиксу), а не командой Horizon. Её
   собственный учёт применённых миграций не воспроизводился; результат совпал
   со схемой, выведенной чтением.
4. **Веб-источники задействованы частично.** SEP-35 через веб-интерфейс
   отдал 503, текст взят из raw-файла репозитория; первичный источник для
   Total Order ID — реализация `sdk/toid/main.go`. Страница effects на
   developers.stellar.org — только обзор, без списка полей; поля взяты из
   структур Go. Страница accounts не запрашивалась. Страницы Page arguments и
   Before History прочитаны.
5. **operations-notifier** не открывался и не менялся — только ссылка.
6. **Python SDK** проверен на синтетических envelope. Разбор реальных ответов
   Horizon, данных Soroban и meta реальных fee-bump не проверялся. Заявление
   о Protocol 30 взято из README пакета; константы поддерживаемого протокола в
   коде пакета нет.
7. **Загрузка данных не реализована**: клиент Horizon, collector, backfill,
   staging для «сирот».
8. **Реальные списки аккаунтов** не использовались; в тестах — адреса,
   выведенные из малых целых чисел.

Границы задания соблюдены: пуша, публикации и деплоя не было; существующие
сервисы не менялись.

## 9. Команды воспроизведения

```bash
cp .env.example .env                              # задать POSTGRES_PASSWORD
docker compose down -v                            # начать с чистой базы
docker compose build
docker compose up -d
curl -s http://127.0.0.1:8000/ready               # 503
docker compose run --rm api alembic upgrade head
curl -s http://127.0.0.1:8000/ready               # 200
docker compose run --rm api alembic check
docker compose --profile test run --rm tests
uv run --frozen scripts/sdk_probe.py
```

Исследование upstream в репозиторий не входит и воспроизводится так:

```bash
git clone --depth 1 https://github.com/stellar/stellar-horizon
git -C stellar-horizon rev-parse HEAD             # 4e13b1bea4c87e770447814a242384e0eb00c015
ls stellar-horizon/internal/db2/schema/migrations # 72 файла
```

## 10. Открытые вопросы

1. По Horizon REST — девять пунктов в `docs/horizon-schema.md`, раздел 9
   (актуальность версии, `result_meta_xdr`, граница удержания, комплектность
   страниц, лимиты).
2. По архитектуре — шесть решений в `docs/architecture.md`, раздел 10
   (фиксация границы удержания, ориентация сделок при выводе, «сироты»,
   полнота внутри диапазона, замена `httpx` в тестовом клиенте).
3. Какой Horizon-инстанс будет источником и какова его фактическая граница
   истории — без этого нельзя спланировать backfill.
