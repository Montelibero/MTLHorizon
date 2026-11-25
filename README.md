# MTL Horizon

Read-only индексатор истории Stellar для Montelibero: бессрочное хранение
истории выбранных аккаунтов и сделок выбранных активов и рынков. Не Stellar
Core и не форк Horizon.

**Статус: этап 02** — каркас, схема исторической базы, загрузка истории
заданного окна леджеров из Horizon и проверки. Постоянный сбор новых
леджеров (collector), бизнес-API и графики не реализованы.

- [`docs/architecture.md`](docs/architecture.md) — схема, решения, границы
  проекта, расхождения с заданием.
- [`docs/horizon-schema.md`](docs/horizon-schema.md) — что и как проверено в
  upstream Horizon (commit `4e13b1be`), курсоры, участники, Python SDK.
- [`docs/stage-01-report.md`](docs/stage-01-report.md) — отчёт этапа 01.
- [`docs/stage-02-report.md`](docs/stage-02-report.md) — отчёт этапа 02:
  что показал живой сервер, выборка, покрытие, сверка, замеры.

## Стек

Python 3.14, FastAPI, SQLAlchemy 2, Alembic, psycopg 3, HTTPX,
stellar-sdk 17, PostgreSQL 17. Версии зафиксированы в `uv.lock`.

## Локальное окружение

Нужны Docker и Docker Compose v2. Секреты задаются только через файл `.env`
(он не попадает в git).

```bash
cp .env.example .env          # задайте POSTGRES_PASSWORD
docker compose build
docker compose up -d

# API жив, но ещё не готов: схема не применена.
curl -s http://127.0.0.1:8000/health     # 200
curl -s http://127.0.0.1:8000/ready      # 503, "migrations": "not applied"

docker compose run --rm api alembic upgrade head
curl -s http://127.0.0.1:8000/ready      # 200
```

| Адрес | Назначение |
| --- | --- |
| `GET /health` | живость процесса; базу не трогает |
| `GET /ready` | база отвечает, схема на голове цепочки миграций, база принадлежит настроенной сети; иначе 503 с именем сбоя |

PostgreSQL не публикует порт: база доступна только внутри сети Compose. API
публикуется только на `127.0.0.1:8000`.

### Миграции

Цепочка из двух миграций. `0001` — историческая схема целиком (таблица
`network_identity`, 9 таблиц `history_*`, 3 списка наблюдения,
`sync_coverage`, `sync_state`). `0002` — то, что понадобилось для загрузки
истории: признак загруженного контекста у транзакции, направление у
чекпоинта, состояние `unverifiable` у покрытия.

```bash
docker compose run --rm api alembic upgrade head    # применить
docker compose run --rm api alembic current         # 0001 (head)
docker compose run --rm api alembic check           # расхождения моделей со схемой
docker compose run --rm api alembic history
docker compose run --rm api alembic downgrade base  # удалить все таблицы проекта
```

Миграции запускаются отдельной командой, а не при старте API: так момент
изменения схемы всегда явный. Строка соединения берётся из переменной
`MTL_HORIZON_DATABASE_URL`; в `alembic.ini` её нет.

## Загрузка истории (этап 02)

История читается из Horizon **назад**, от нового к старому, отдельными
потоками: у каждого аккаунта в режиме FULL четыре потока (транзакции,
операции, эффекты, сделки), у каждого рынка — сделки пары. Аккаунты других
режимов не читаются. Поток можно остановить в любой момент и продолжить.

Команды выполняются в контейнере `api`; файлы (список наблюдения, отчёты)
попадают в него через примонтированный каталог `local/`:

```bash
mkdir -p local
alias bf='docker compose run --rm --user "$(id -u):$(id -g)" -v "$PWD/local:/app/local" api python -m app.backfill'
```

### Список наблюдения

Формат — [`local-tracking.example.yaml`](local-tracking.example.yaml). Скопируйте
его в `local/tracking.yaml` (каталог `local/` не попадает в git) и заполните.

```bash
bf import-config --file local/tracking.yaml --dry-run   # проверить и показать, ничего не записав
bf import-config --file local/tracking.yaml
bf import-config --file local/tracking.yaml --apply-changes   # сменить режим аккаунтов, у которых он отличается
```

Ошибки в файле перечисляются все сразу, и ничего не импортируется. Повторный
импорт того же файла ничего не меняет. Записи, которых в новом файле нет, не
удаляются. Режим аккаунта меняется только с `--apply-changes`.

### Один взгляд на сервер

```bash
bf probe --output local/reports/probe.json
```

Тридцать с небольшим read-only запросов: версия сервера и его сеть, границы
истории, обе ориентации рынка, цепочка сделка → операция → транзакция →
леджер, поведение курсоров. Никакой записи в базу.

### Оценка и загрузка окна

```bash
bf estimate --from-ledger L1 --to-ledger L0     # сколько страниц и записей в окне, сколько запросов понадобится
bf run       --from-ledger L1 --to-ledger L0 --report local/reports/run.json
bf status
```

Границы можно не указывать: тогда окно — вся история, которую сервер ещё
хранит (от `history_elder_ledger` до `history_latest_ledger` на момент
старта). Леджеры считаются включительно.

`run` читает только то, что ещё не проверено: повторный запуск готового окна
не делает ни одного запроса к потокам. Остановить можно лимитами или
сигналом (`Ctrl-C`, `SIGTERM`); сохранённое остаётся, следующий запуск
продолжает с чекпоинта.

```bash
bf run --max-pages 50 --max-requests 2000 --max-seconds 600   # остановиться, достигнув любого из лимитов
bf run --subject GACKTN5DA                                   # только потоки, в имени которых есть эта строка
bf reimport --from-ledger L1 --to-ledger L0                  # забыть покрытие окна и прочитать заново
```

Коды возврата `run`: `0` — окно проверено; `1` — поток закончился ошибкой;
`2` — остановлен лимитом или сигналом; `3` — закончено, но часть окна
вышла из истории сервера и помечена `unverifiable`.

Темп по умолчанию — 1 запрос в секунду. Публичный Horizon ограничивает
запросы по IP; ответ 429 несёт заголовки `X-RateLimit-*`, и клиент ждёт
столько, сколько сервер просит. Журнал команды можно дублировать в файл —
общие опции ставятся до подкоманды:

```bash
bf --log-level INFO --log-file local/logs/run.log run --from-ledger L1 --to-ledger L0
```

### Проверка загруженного

```bash
bf verify      --from-ledger L1 --to-ledger L0   # ID записей в базе против страниц сервера, поток за потоком
bf fingerprint --from-ledger L1 --to-ledger L0   # число строк и слепок каждой таблицы в окне
```

`fingerprint` двух баз совпадает тогда, когда в них одни и те же строки:
так сравнивают непрерывный запуск с остановленным и возобновлённым.

### Что попадает в базу

- Родителей догружаем всегда: сделка или эффект не хранится без своей
  операции, транзакции и леджера. Если родителя нет (404) — поток
  останавливается с ошибкой, страница не записывается, курсор не движется.
- Контекст транзакции (все её операции, эффекты, сделки) качается один раз;
  при повторной встрече берётся из базы.
- `result_meta_xdr` публичный Horizon не отдаёт — в базе будет `NULL`.
- Что вышло из истории сервера (окно ниже `history_elder_ledger`, ответ 410),
  помечается `unverifiable`, а не `complete`: код возврата `3` у `run`.
- У очень активных аккаунтов публичный Horizon на `/accounts/{id}/trades`
  отвечает 503. Такой поток останавливается с ошибкой (код возврата `1`) и
  остаётся непроверенным; остальные потоки загружаются.
- Участники: для наблюдаемого аккаунта — те, которых назвал сам Horizon
  (он вернул запись в потоке аккаунта); для остальных — то, что выводится из
  тела операции и источников транзакции. Участники по изменениям записей
  (это видно только из meta) не восстанавливаются.

Подробности — в [`docs/architecture.md`](docs/architecture.md).

### Тесты

Тестам нужна настоящая PostgreSQL. Они создают на указанном сервере свою
базу, применяют к ней миграции, прогоняют всё и удаляют базу; база
приложения не затрагивается. Сеть в тестах не используется: Horizon
заменён встроенным фейком (`tests/fake_horizon.py`).

```bash
docker compose --profile test run --rm tests
```

То же с хоста (нужен [uv](https://docs.astral.sh/uv/) и доступ к базе —
порт у неё закрыт, поэтому одноразовый проброс только на localhost):

```bash
docker run -d --rm --name mtl-horizon-db-tunnel --network mtl-horizon_default \
  -p 127.0.0.1:55432:5432 alpine/socat \
  tcp-listen:5432,fork,reuseaddr tcp:db:5432

export MTL_HORIZON_TEST_DATABASE_URL="postgresql+psycopg://mtl_horizon:<пароль из .env>@127.0.0.1:55432/ignored"
uv sync --frozen
uv run --frozen pytest
docker rm -f mtl-horizon-db-tunnel
```

### Проверка Python SDK

```bash
uv run --frozen scripts/sdk_probe.py
```

### Перезапуск и очистка

```bash
docker compose down        # остановить; данные остаются в volume mtl-horizon_pgdata
docker compose up -d       # поднять обратно; схема и данные на месте
docker compose down -v     # УДАЛИТЬ базу вместе с данными
docker compose logs -f api
```

## Переменные окружения

| Переменная | Обязательно | По умолчанию | Назначение |
| --- | --- | --- | --- |
| `POSTGRES_PASSWORD` | да | — | пароль PostgreSQL; для Compose |
| `MTL_HORIZON_DATABASE_URL` | да | — | `postgresql+psycopg://user:password@host:5432/db` |
| `MTL_HORIZON_NETWORK_PASSPHRASE` | нет | public network | сеть, к которой принадлежит база |
| `MTL_HORIZON_API_HOST` | нет | `127.0.0.1` | адрес прослушивания; в контейнере `0.0.0.0` |
| `MTL_HORIZON_API_PORT` | нет | `8000` | порт API |
| `MTL_HORIZON_TEST_DATABASE_URL` | для тестов | — | сервер PostgreSQL для тестов; имя базы игнорируется |
| `MTL_HORIZON_URL` | нет | `https://horizon.stellar.org` | сервер Horizon — источник истории; только чтение |
| `MTL_HORIZON_REQUESTS_PER_SECOND` | нет | `1` | темп запросов к источнику |
| `MTL_HORIZON_CONCURRENCY` | нет | `1` | сколько потоков читаются одновременно |

База создаётся для сети **public**. Если passphrase в настройках отличается,
`/ready` отвечает 503, а запись в базу отказывает: pubnet и testnet в одной
базе не смешиваются.

## Структура

```
app/
  api/          /health и /ready
  storage/      схема (models), правила записи (upsert), покрытие (coverage),
                единственный писатель (writer), привязка к сети
  sources/      Horizon снаружи
    horizon.py             HTTP-клиент: темп, повторы, дедлайн, проверка _links.next
    horizon_records.py     REST-записи → строки базы, участники
    horizon_cursors.py     Total Order ID и курсоры по типам ресурса
    horizon_format.py      точные суммы, цены, время
  collector/    живой сбор — граница описана, код не написан
  backfill/     загрузка окна леджеров
    cli.py                 команды
    config.py              YAML-список наблюдения
    jobs.py                из каких потоков состоит окно
    loader.py              чтение назад, контекст транзакций, чекпоинты
    store.py               страница → строки базы
    probe.py, verify.py    что говорит сервер против того, что в базе
alembic/        миграции
tests/          237 тестов; встроенный Horizon для них — tests/fake_horizon.py
scripts/        sdk_probe.py
docs/           архитектура, схема Horizon, отчёты этапов 01 и 02
```

Направление зависимостей: `collector` / `backfill` → `sources` → `storage`.
Подробности — в [`docs/architecture.md`](docs/architecture.md).

## Языки

Код, комментарии и сообщения коммитов — английские; документация и этот
файл — русские.
