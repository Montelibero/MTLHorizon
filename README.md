# MTL Horizon

Read-only индексатор истории Stellar для Montelibero: бессрочное хранение
истории выбранных аккаунтов и сделок выбранных активов и рынков. Не Stellar
Core и не форк Horizon.

**Статус: этап 01** — каркас, схема исторической базы и проверки. Загрузка
данных (collector, backfill), бизнес-API и графики не реализованы.

- [`docs/architecture.md`](docs/architecture.md) — схема, решения, границы
  проекта, расхождения с заданием.
- [`docs/horizon-schema.md`](docs/horizon-schema.md) — что и как проверено в
  upstream Horizon (commit `4e13b1be`), курсоры, участники, Python SDK.
- [`docs/stage-01-report.md`](docs/stage-01-report.md) — отчёт этапа: что
  сделано, результаты прогонов, что не проверялось.

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

Цепочка состоит из одной миграции, `0001` — историческая схема целиком
(таблица `network_identity`, 9 таблиц `history_*`, 3 списка наблюдения,
`sync_coverage`, `sync_state`).

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

### Тесты

Тестам нужна настоящая PostgreSQL. Они создают на указанном сервере свою
базу, применяют к ней миграции, прогоняют всё и удаляют базу; база
приложения не затрагивается.

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

База создаётся для сети **public**. Если passphrase в настройках отличается,
`/ready` отвечает 503, а запись в базу отказывает: pubnet и testnet в одной
базе не смешиваются.

## Структура

```
app/
  api/          /health и /ready
  storage/      схема (models), правила записи (upsert), привязка к сети
  sources/      курсоры Horizon и точные конвертации величин
  collector/    живой сбор — граница описана, код не написан
  backfill/     догрузка истории — граница описана, код не написан
alembic/        миграции
tests/          102 теста
scripts/        sdk_probe.py
docs/           архитектура, схема Horizon, отчёт этапа
```

Направление зависимостей: `collector` / `backfill` → `sources` → `storage`.
Подробности — в [`docs/architecture.md`](docs/architecture.md).

## Языки

Код, комментарии и сообщения коммитов — английские; документация и этот
файл — русские.
