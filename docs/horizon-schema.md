# Историческая схема Horizon: результаты проверки upstream

Дата проверки: 24 ноября 2026. Всё, что ниже, получено из исходников по
указанным commit'ам, а не из памяти или публикуемой документации.

| Что | Commit | Версия |
| --- | --- | --- |
| [stellar/stellar-horizon](https://github.com/stellar/stellar-horizon) | `4e13b1bea4c87e770447814a242384e0eb00c015` | `main` после 30.0.1 (раздел «Unreleased»), Protocol 30 |
| [stellar/go-stellar-sdk](https://github.com/stellar/go-stellar-sdk) — зависимость Horizon | `35ebe09ed33c24da6df989969f42cbd648f4ce34` | `v0.9.1-0.20261118131054-35ebe09ed33c` |
| [stellar-sdk (Python)](https://pypi.org/project/stellar-sdk/) | — | 17.0.1 |

Номера строк ниже относятся к этим commit'ам. Пути без префикса — внутри
репозитория Horizon; `sdk/` — репозиторий go-stellar-sdk.

Как проверялось:

1. **Чтение исходников** — миграции, модели, запросы, адаптеры REST, правила
   участников.
2. **Прогон миграций** — все 72 файла `internal/db2/schema/migrations/*.sql`
   применены к чистому PostgreSQL 17.12 (одноразовый контейнер, порядок по
   числовому префиксу файла). Ошибок нет; итоговая схема снята с каталога и
   совпала с выведенной чтением. Она приведена в разделе 2.
3. **Python SDK** — установлен в окружение Python 3.14.7 и проверен пробой
   (раздел 7).

Horizon и Stellar Core для проверки не запускались; запросов к публичному
Horizon не делалось — это работа следующего этапа.

Содержание: [1. Где что лежит](#1-где-что-лежит) ·
[2. Действующая схема](#2-действующая-схема-таблиц-history) ·
[3. Идентификаторы и курсоры](#3-идентификаторы-и-курсоры) ·
[4. Границы курсоров](#4-границы-курсоров-по-типу-ресурса) ·
[5. Участники](#5-участники) ·
[6. Особые случаи](#6-особые-случаи) ·
[7. Python SDK](#7-python-stellar-sdk-1701-на-python-314) ·
[8. Что нельзя воспроизвести по REST](#8-что-нельзя-воспроизвести-по-rest-api) ·
[9. Открытые вопросы](#9-открытые-вопросы-по-horizon-rest) ·
[10. Живой сервер](#10-живой-сервер)

Разделы 1–9 написаны в этапе 01 по исходникам; раздел 10 — в этапе 02 по
ответам сервера. Там же ответы на вопросы раздела 9.

## 1. Где что лежит

| Что | Путь |
| --- | --- |
| SQL-миграции | `internal/db2/schema/migrations/` (72 файла: номера 1–70, номера 8 и 39 встречаются дважды) |
| Встраивание и применение миграций | `internal/db2/schema/bindata.go`, `internal/db2/schema/main.go` (`migrate.AssetMigrationSource`, библиотека sql-migrate) |
| Go-модели и запросы | `internal/db2/history/` — `main.go` (структуры), `ledger.go`, `transaction.go`, `transaction_batch_insert_builder.go`, `operation.go`, `effect.go`, `trade.go`, `participants.go`, `account.go`, `asset.go`, `total_order_id.go` |
| REST-адаптеры | `internal/resourceadapter/` — `transaction.go`, `operations.go`, `effects.go`, `trade.go`, `ledger.go`, `account_entry.go` |
| Структуры JSON | `sdk/protocols/horizon/main.go`, `sdk/protocols/horizon/effects/main.go`, `sdk/protocols/horizon/operations/main.go`, `sdk/protocols/horizon/base/main.go` |
| Total Order ID (SEP-35) | `sdk/toid/main.go` |
| Курсоры и пагинация | `internal/db2/page_query.go`, `internal/actions/helpers.go`, `internal/render/problem/problem.go` |
| Участники | `internal/ingest/processors/participants_processor.go`, `operations_processor.go` |
| Эффекты и сделки | `internal/ingest/processors/effects_processor.go`, `trades_processor.go` |

Каталога `migrations/` в корне репозитория больше нет: схема переехала в
`internal/db2/schema/migrations/`. Имена таблиц с тех пор не менялись, но
полагаться на это нельзя — курсоры как раз поменялись (раздел 3).

## 2. Действующая схема таблиц `history_*`

После всех миграций в базе 17 таблиц с префиксом `history_`. **Внешних ключей
между ними нет ни одного** (у сделок они были, миграция 59 их удалила); у
`history_ledgers`, `history_transactions`, `history_operations` и
`history_accounts` нет ограничения PRIMARY KEY — только уникальные индексы.

Обозначения: **NN** — NOT NULL; уникальные индексы помечены «U».

### `history_accounts`

Миграции: `1_initial_schema.sql:23`, `3_use_sequence_in_history_accounts.sql`.

| Колонка | Тип | | |
| --- | --- | --- | --- |
| `id` | bigint | NN | суррогатный id из sequence |
| `address` | varchar(64) | | |

Индексы: U `index_history_accounts_on_address (address)`,
U `index_history_accounts_on_id (id)`.
Запись: `INSERT … ON CONFLICT (address) DO NOTHING` (`account.go:29`).

### `history_assets`

Миграция: `6_create_assets_table.sql`.

| Колонка | Тип | |
| --- | --- | --- |
| `id` | integer (serial) | NN, PK |
| `asset_type` | varchar(64) | NN |
| `asset_code` | varchar(12) | NN |
| `asset_issuer` | varchar(56) | NN |

UNIQUE (`asset_code`, `asset_type`, `asset_issuer`). XLM хранится как
(`native`, `''`, `''`). Индекс по эмитенту удалён миграцией 67.
Запись: `ON CONFLICT (asset_code, asset_type, asset_issuer) DO NOTHING`
(`asset.go:43`). Значение `id` зависит от порядка вставки в конкретной базе.

### `history_ledgers`

Миграции: `1_initial_schema.sql:46`, 4, 9, 15, 37, 38; удаление индексов — 33, 67.

| Колонка | Тип | | |
| --- | --- | --- | --- |
| `sequence` | integer | NN | |
| `ledger_hash` | varchar(64) | NN | |
| `previous_ledger_hash` | varchar(64) | | |
| `transaction_count` | integer | NN, default 0 | фактически число успешных |
| `successful_transaction_count`, `failed_transaction_count` | integer | | |
| `operation_count`, `tx_set_operation_count` | integer | | |
| `closed_at` | timestamp **without** time zone | NN | |
| `created_at`, `updated_at` | timestamp without time zone | | время ингеста |
| `id` | bigint | | TOID леджера (`sequence << 32`) |
| `importer_version` | integer | NN, default 1 | |
| `total_coins`, `fee_pool` | bigint | NN | |
| `base_fee`, `base_reserve`, `max_tx_set_size` | integer | NN | |
| `protocol_version` | integer | NN, default 0 | `LedgerHeader.LedgerVersion` |
| `ledger_header` | text | | base64 XDR заголовка |

Индексы: U `index_history_ledgers_on_sequence`, U `hs_ledger_by_id`.
CHECK (все `NOT VALID`): `sequence > 0` и неотрицательность счётчиков и сумм.

### `history_transactions`

Миграции: `1_initial_schema.sql:143`, 16, 17, 34, 40, 46, 51, 54.

| Колонка | Тип | | |
| --- | --- | --- | --- |
| `transaction_hash` | varchar(64) | NN | хеш внешнего envelope |
| `inner_transaction_hash` | varchar(64) | | только fee-bump |
| `ledger_sequence`, `application_order` | integer | NN | |
| `id` | bigint | | TOID (`ledger << 32 \| tx << 12`) |
| `account`, `fee_account` | varchar(64) | NN / — | source транзакции / плательщик fee-bump |
| `account_muxed`, `fee_account_muxed` | varchar(69) | | M-адрес, если источник был muxed |
| `account_sequence` | bigint | NN | |
| `max_fee` | bigint | NN | для fee-bump — fee внутренней транзакции |
| `new_max_fee` | bigint | | fee fee-bump |
| `fee_charged` | bigint | | NULL только в старых базах до реингеста |
| `operation_count` | integer | NN | |
| `successful` | boolean | | NULL означает true (`transaction.go:272`) |
| `tx_envelope`, `tx_result`, `tx_meta`, `tx_fee_meta` | text | NN | base64 XDR |
| `signatures` | varchar(96)[] | NN | подписи fee-bump |
| `inner_signatures` | varchar(96)[] | | |
| `memo_type`, `memo` | varchar | NN / — | |
| `time_bounds`, `ledger_bounds` | int8range | | |
| `min_account_sequence`, `min_account_sequence_ledger_gap` | bigint | | |
| `min_account_sequence_age` | varchar(20) | | секунды текстом |
| `extra_signers` | text[] | | |
| `created_at`, `updated_at` | timestamp without time zone | | |

Индексы: `by_hash (transaction_hash)`, `by_inner_hash` (частичный),
`by_ledger (ledger_sequence, application_order)`, U `hs_transaction_by_id`.
CHECK: `ledger_sequence > 0`, `max_fee >= 0` (внутренняя транзакция fee-bump
может иметь нулевой fee, миграция 40), `fee_charged > 0`.

Время закрытия леджера транзакция не хранит — оно берётся соединением с
`history_ledgers` (`transaction.go:282`, `289`).

### `history_operations`

Миграции: `1_initial_schema.sql:99`, 46, 64, 65, 67.

| Колонка | Тип | |
| --- | --- | --- |
| `id` | bigint | NN, TOID операции (индекс операции ≥ 1) |
| `transaction_id` | bigint | NN, TOID транзакции |
| `application_order` | integer | NN, номер операции, начиная с 1 |
| `type` | integer | NN, `type_i` |
| `details` | jsonb | тело операции |
| `source_account` | varchar(64) | NN |
| `source_account_muxed` | varchar(69) | |
| `is_payment` | boolean | внутренний признак для `/payments` |

Индексы: U `index_history_operations_on_id`,
`index_history_operations_on_transaction_id`. Индекс по `type` удалён
миграцией 67.

### `history_transaction_participants`, `history_operation_participants`

Миграции: `1_initial_schema.sql:69` и `:113`, 2, 35.

Две колонки: `history_transaction_id` / `history_operation_id` (bigint NN) и
`history_account_id` (bigint NN). Собственный `id` удалён миграцией 35.

Индексы: U `hist_tx_p_id (history_account_id, history_transaction_id)`,
`htp_by_htid (history_transaction_id)`, U
`hist_op_p_id (history_account_id, history_operation_id)`,
`hop_by_hoid (history_operation_id)`.

### `history_effects`

Миграции: `1_initial_schema.sql:33`, 46, 67.

| Колонка | Тип | |
| --- | --- | --- |
| `history_account_id` | bigint | NN |
| `history_operation_id` | bigint | NN |
| `order` | integer | NN, номер эффекта внутри операции |
| `type` | integer | NN, `type_i` |
| `details` | jsonb | |
| `address_muxed` | varchar(69) | |

Индексы: U `hist_e_by_order (history_operation_id, order)`,
U `hist_e_id (history_account_id, history_operation_id, order)`. Индекс по
`type` и частичный индекс по торговым эффектам удалены миграцией 67.

### `history_trades`

Миграции: 5, 7, 10, 11, 13, 21, 39 (`39_history_trades_indices.sql`), 50, 52,
53, 59, 60, 67.

| Колонка | Тип | |
| --- | --- | --- |
| `history_operation_id`, `order` | bigint, integer | NN, идентификатор сделки |
| `ledger_closed_at` | timestamp without time zone | NN |
| `base_account_id`, `counter_account_id` | bigint | NULL на стороне пула ликвидности |
| `base_asset_id`, `counter_asset_id` | bigint | NN |
| `base_amount`, `counter_amount` | bigint | NN, строопы |
| `base_is_seller` | boolean | |
| `price_n`, `price_d` | bigint | NULL в очень старых базах (миграция 10) |
| `base_offer_id`, `counter_offer_id` | bigint | |
| `base_liquidity_pool_id`, `counter_liquidity_pool_id` | bigint | |
| `liquidity_pool_fee` | integer | |
| `trade_type` | smallint | default 1; 1 — оффер, 2 — пул |
| `rounding_slippage`, `base_is_exact` | bigint, boolean | |

Индексы (12): U `htrd_pid (history_operation_id, order)`,
`htrd_pair_pid`, `htrd_pair_time_lookup`,
`htrd_by_base_account`, `htrd_by_counter_account`,
`htrd_by_base_offer`, `htrd_by_counter_offer`,
`htrd_by_base_liquidity_pool_id`, `htrd_by_counter_liquidity_pool_id`,
`htrd_by_counter_asset`, `htrd_by_trade_type`,
`htrd_agg_bucket_lookup` (выражение).
CHECK: `base_asset_id < counter_asset_id` (канонический порядок пары),
суммы ≥ 0 (нули разрешены миграцией 21), `trade_type > 0`.

Порядок пары `base`/`counter` Horizon выбирает по **своим внутренним id
активов**: если `base_asset_id > counter_asset_id`, стороны меняются местами
вместе с суммами, счетами, офферами и ценой
(`trades_processor.go:133-145`). Эти id — insertion order в конкретной базе и
через REST не видны.

### `history_trades_60000` — не сделки

Это **агрегация** сделок (свечи за 60 секунд), а не часть истории сделок:
миграция `47_precompute_trade_aggregations.sql`, PK
(`base_asset_id`, `counter_asset_id`, `timestamp`), колонки `count`,
`base_volume`, `counter_volume`, `avg`, `high_n/d`, `low_n/d`,
`open_ledger_toid`, `open_n/d`, `close_ledger_toid`, `close_n/d`.
Отдельно хранить не нужно: считается из `history_trades`.

### Остальные таблицы с префиксом

`history_claimable_balances`, `history_liquidity_pools`,
`history_operation_claimable_balances`, `history_transaction_claimable_balances`,
`history_operation_liquidity_pools`, `history_transaction_liquidity_pools`,
`history_transactions_filtered_tmp` (служебная, миграция 56). Таблицы
`exp_history_*` существовали с миграций 26–31 и удалены миграцией 32.

## 3. Идентификаторы и курсоры

### Total Order ID (SEP-35)

`sdk/toid/main.go:56-170`, совпадает с
[SEP-35](https://github.com/stellar/stellar-protocol/blob/master/ecosystem/sep-0035.md):

```
id = (ledger << 32) | (transaction << 12) | operation      — int64
ledger       32 бита   — sequence леджера
transaction  20 бит    — порядок применения в леджере, с 0
operation    12 бит    — индекс операции, с 1
```

Одинаковый id у леджера, его первой транзакции и её первой операции —
нормально: id задаёт порядок внутри одного типа объектов.
Вспомогательные: `AfterLedger(seq) = New(seq, 2^20-1, 2^12-1)`,
`LedgerRangeInclusive(from, to)`.

### `paging_token` по типам ресурса

| Ресурс | `id` | `paging_token` | Номер леджера | Где в коде |
| --- | --- | --- | --- | --- |
| accounts | адрес | **адрес аккаунта** `G…` | не выводится | `account_entry.go:25-26` |
| ledgers | хеш леджера | `sequence << 32` | `value >> 32` | `ledger.go:264`, `resourceadapter/ledger.go:16-17` |
| transactions | хеш | TOID (часть операции 0) | `value >> 32` | `transaction_batch_insert_builder.go:209`, `resourceadapter/transaction.go:27-28` |
| operations | TOID текстом | тот же TOID | `value >> 32` | `operations_processor.go:98-104`, `resourceadapter/operations.go:171-172` |
| effects | `%019d-%010d` от (операция, номер) | `<operation id>-<order>` | первый компонент `>> 32` | `effect.go:59-72` |
| trades | `<operation id>-<order>` | тот же текст | первый компонент `>> 32` | `trade.go:17-19`, `resourceadapter/trade.go:23-24` |

Что из этого следует:

- **Курсор — не всегда число.** Для аккаунтов это адрес, для эффектов и сделок
  — пара через дефис. Страница документации *Page arguments* описывает курсор
  как «a number» — для этих ресурсов это неточно.
- **У эффекта `id` и `paging_token` разные**, хотя кодируют одну пару. `id`
  дополнен нулями, чтобы текстовый порядок совпадал с хронологическим;
  `paging_token` не дополнен, и текстовый порядок для него неверный
  (`"10-1"` < `"9-1"`).
- **Сделки:** курсор из `<operation id>-<order>`. Приведено для этого commit;
  в другой версии Horizon формат может быть иным, его сверяют с кодом.
- **Номер леджера для эффекта или сделки** в REST-ответе отсутствует. Он
  выводится из первого компонента курсора: `operation_id >> 32`. Второй
  способ — ссылка `_links.operation.href` (id операции), затем тот же сдвиг.
  Логика Horizon: `toid.Parse(historyOperationID).LedgerSequence`
  (`effect.go:63-67`, `operation.go:19-22`).

### Связь объектов

Эффект и сделка знают только операцию. Транзакция находится по
`operation_id` с обнулёнными 12 битами (`TotalOrderID.transaction_id`,
Horizon: `ForTransaction` — `toid.Parse(tx.ID)`, `operation.go:220-238`).
Хеш транзакции в REST у эффекта и сделки отсутствует, только у операции.

## 4. Границы курсоров по типу ресурса

`internal/db2/page_query.go`, `internal/actions/helpers.go`.

| Правило | Значение | Код |
| --- | --- | --- |
| Лимит страницы | по умолчанию 10, максимум 200 | `page_query.go:16-18` |
| Порядок | `asc` по умолчанию | `page_query.go:210-213` |
| `cursor=now` | заменяется на `AfterLedger(history_latest_ledger)` | `helpers.go:68-71` |
| Отрицательный курсор | ошибка `invalid_field` | `helpers.go:77-86` |
| Валидация курсора | пара `int64-int64`, оба ≥ 0 | `page_query.go:148-198`, `222-228` |
| Один компонент вместо пары | второй считается `MaxInt64` | `page_query.go:173-179` |
| Второй компонент в запросах эффектов и сделок | обрезается до `MaxInt32` | `effect.go:213-216`, `trade.go:188-190` |
| Эффекты и сделки | сравнение пары `(operation id, order)`, не текста | `effect.go:236-258`, `trade.go:242-272` |
| Сделки по аккаунту, офферу или пулу | два подзапроса (сторона base и сторона counter) через `UNION` | `trade.go:192-230` |
| Сделки по паре активов | пара приводится к внутреннему каноническому порядку; при обратном порядке колонки переставляются в SQL | `trade.go:136-139`, `154-155`, `317-350` |
| `before_history` | HTTP 410, `https://stellar.org/horizon-errors/before_history`, заголовок «Data Requested Is Before Recorded History» | `problem.go:96-104` |
| Когда возникает | курсор ≤ `toid(history_elder_ledger, 0, 0)` при порядке `desc` | `helpers.go:572-595`, `effect.go:219-224` |
| При порядке `asc` | ошибки нет: курсор сдвигается на `AfterLedger(elder - 1)` | `helpers.go:550-565` |

Последние два пункта важны для backfill: граница удержания истории
обнаруживается только запросами в сторону `desc`; при `asc` Horizon молча
начинает с самого старого доступного леджера. Иными словами, **отсутствие
ошибки не доказывает, что история получена с самого начала**. Границы
проверяются только в действиях transaction, trade, effects, operation, ledger
(`internal/actions/{transaction,trade,effects,operation,ledger}.go`); у потоков по
аккаунту то же правило, так как они проходят через эти действия.

## 5. Участники

### Участники транзакции (`ParticipantsForTransaction`, `participants_processor.go:176-217`)

Источник транзакции; для fee-bump — ещё и `fee_source`; затем все аккаунты,
чьи записи `LedgerEntryTypeAccount` созданы, изменены или удалены в meta
транзакции (`participants_processor.go:40-83`) и в изменениях fee; затем все
участники всех операций. Повторы убираются.

### Участники операции (`Participants`, `operations_processor.go:983-1095`)

Всегда источник **операции** (свой, если задан, иначе источник транзакции;
`operations_processor.go:117-127`), плюс аккаунты из тела операции:

| Операция | Кто добавляется |
| --- | --- |
| `create_account` | destination |
| `payment`, `path_payment_strict_receive/send`, `account_merge` | destination |
| `allow_trust`, `set_trust_line_flags` | trustor |
| `create_claimable_balance` | все claimants |
| `begin_sponsoring_future_reserves` | sponsored_id |
| `end_sponsoring_future_reserves` | источник парного `begin_sponsoring…` |
| `revoke_sponsorship` | владелец записи; для подписанта — только аккаунт, сам подписант **не** добавляется (защита от спама) |
| `clawback` | `from` |
| `invoke_host_function` | владельцы изменённых записей и адресаты событий Stellar Asset Contract (transfer, mint, clawback, burn); только адреса аккаунтов, не контракты |
| `manage_data`, `set_options`, `change_trust`, оффера, `inflation`, `bump_sequence`, `claim_claimable_balance`, `clawback_claimable_balance`, `liquidity_pool_deposit/withdraw`, `extend_footprint_ttl`, `restore_footprint` | только источник |

Плюс спонсор записи, если он есть (`operations_processor.go:1086-1092`).

Изменения записей в meta **не** добавляют участников операции — только
транзакции. Поэтому «аккаунт встречается в raw JSON» и «аккаунт — участник» —
разные вещи. Неизвестный тип операции — ошибка (`:1082-1083`), а у Horizon
новее это требует актуального SDK.

## 6. Особые случаи

### `manage_data`

- `type_i = 10`, `type = "manage_data"` (`sdk/xdr/xdr_generated.go:27016`).
- `details`: `name` и `value` — base64, либо `null` при удалении записи
  (`operations_processor.go:532-539`). В JSON REST `value` при удалении
  превращается в **пустую строку**: у структуры `ManageData` поле `Value`
  без `omitempty` (`sdk/protocols/horizon/operations/main.go:140-144`).
  Значит, по одной операции «запись удалена» и «записано пустое значение»
  **не различимы**.
- Различает их эффект: `data_removed` (41) против `data_created` (40) или
  `data_updated` (42). Аккаунт эффекта — источник операции
  (`effects_processor.go:966-1004`), значение base64 есть у created/updated.
- Единственный участник — источник операции, который не обязан совпадать с
  источником транзакции.
- Записи `data_*` создаются не только `manage_data`: контракт в Soroban тоже
  может изменить запись аккаунта. Тогда операция — `invoke_host_function`,
  аккаунт эффекта — владелец записи, и он может не быть ни источником, ни
  участником операции. Фильтр «по участнику» и фильтр «по источнику» дают
  разные множества — это проверено тестом.

### Fee-bump

Колонки: `inner_transaction_hash`, `fee_account`, `new_max_fee`,
`inner_signatures`; `transaction_hash` — внешний хеш, `account` — источник
внутренней транзакции, `max_fee` — её fee, `signatures` — подписи fee-bump
(`transaction_batch_insert_builder.go:211-232`). Поиск по хешу находит и по
внешнему, и по внутреннему (`transaction.go:19`).

В REST (`resourceadapter/transaction.go`): `max_fee` — это fee fee-bump
(`new_max_fee`, `:107`), `inner_transaction.max_fee` — fee внутренней
транзакции, `fee_account` — плательщик (`:101`). Особый случай: если транзакцию запросили по
**внутреннему** хешу, поля `id` и `hash` ответа содержат внутренний хеш, а
`signatures` — подписи внутренней транзакции (`:27`, `:30`, `:117-118`). Внешний
хеш всегда в `fee_bump_transaction.hash`. `paging_token` от способа запроса не
зависит.

### Неуспешные транзакции

Хранятся с `successful = false`. Эффектов и сделок у них нет. По умолчанию
конечные точки их скрывают: условие `ht.successful = true OR ht.successful IS
NULL` (`transaction.go:200-204`); нужен `include_failed=true`. Операции
неуспешной транзакции доступны и имеют `transaction_successful: false`.

### Muxed-аккаунты

Базовая колонка всегда содержит G-адрес, M-адрес — в отдельной колонке
`*_muxed` (миграция 46). Участники нормализуются через `ToAccountId()`
(`participants_processor.go:124`, `:143`), то есть M и G одного ключа — один
участник. REST: `account_muxed`, `account_muxed_id`, `source_account_muxed`,
`fee_account_muxed`.

### Пассивные оффера

Сделки извлекаются из результата операции. Для `create_passive_sell_offer` в
Horizon стоит пометка **KNOWN ISSUE**: Stellar Core присваивает результату
офферную ветку `manage_sell_offer` вместо `create_passive_sell_offer`, и
Horizon разбирает обе (`trades_processor.go:350-363`). Сделки пассивного
оффера ничем не отличаются от прочих офферных.

`counter_offer_id`: если у taker не было своего оффера, Horizon подставляет
**синтетический** id `EncodeOfferId(opID, TOIDType)` (`:437-441`). То есть
`counter_offer_id` не всегда существовавший оффер.

Записи результата с нулевыми суммами (сборка мусора офферов ядром) сделками не
считаются и пропускаются (`:376-385`); из-за этого номера `order` внутри
операции могут идти с пропусками.

### Сделки пула ликвидности

Сторона пула: `base_account_id`/`counter_account_id` = NULL,
`base_liquidity_pool_id`/`counter_liquidity_pool_id`, `liquidity_pool_fee`,
`trade_type = 2`; REST: `trade_type: "liquidity_pool"`,
`liquidity_pool_fee_bp`, `*_liquidity_pool_id`. Цена для пула — точное
отношение купленного к проданному (`:162-164`), для оффера — цена оффера
продавца.

### Операции Soroban

`invoke_host_function` (24), `extend_footprint_ttl` (25), `restore_footprint`
(26) хранятся как обычные строки `history_operations` с `details`. Балансовые
эффекты SAC — `contract_credited` (96) и `contract_debited` (97). События
контрактов Horizon в базе не хранит. Для нас это непрозрачные записи: JSON и
`envelope_xdr` сохраняются как есть, интерпретация — позже.

### Неизвестный тип операции

Horizon отвечает базовыми полями с `type: "unknown"` и настоящим `type_i`
(`resourceadapter/operations.go:162-164`, `199-207`; для эффектов так же,
`effects.go:294-299`).

## 7. Python `stellar-sdk` 17.0.1 на Python 3.14

Установлен в чистое окружение CPython 3.14.7 командой
`uv pip install stellar-sdk`. Все зависимости встали готовыми wheels, ничего
не собиралось: PyNaCl 1.6.2, cffi 2.1.1, pydantic 2.14.4, requests 2.34.2,
mnemonic 0.21, xdrlib3 0.1.1, requests-sse 0.5.3.

Что подтверждено пробой (`scripts/sdk_probe.py`, повторяется командой
`uv run --frozen scripts/sdk_probe.py`):

| Проверка | Результат |
| --- | --- |
| 27 типов операций; `MANAGE_DATA = 10`, `INVOKE_HOST_FUNCTION = 24`, `RESTORE_FOOTPRINT = 26` | совпадает с Go |
| `manage_data`: задать значение, задать пустое, удалить | в XDR различимы: `b"hello"`, `b""`, `None` |
| Источник на уровне операции, в том числе muxed (memo id 123) | сохраняется |
| Операции Soroban в envelope | собираются и разбираются |
| Fee-bump envelope (тип 5) | разбирается через `FeeBumpTransactionEnvelope.from_xdr` |
| `TransactionMeta` | есть ветки v, v1, v2, v3, v4 |
| Коды результата | до `txFROZEN_KEY_ACCESSED` (−18) |
| Muxed-контракты CAP-0084 (`W…`) | `StrKey.encode_muxed_contract` / `decode_muxed_contract` |
| Сеть pubnet | `Network.PUBLIC_NETWORK_PASSPHRASE` совпадает с используемой нами |
| **Неизвестный тип операции** | **отвергается**: `ValueError: 99 is not a valid OperationType` |

README пакета заявляет поддержку Protocol 30; константы «максимально
поддерживаемый протокол» в коде пакета нет, проверить это по коду нельзя.

Несовместимости и подводные камни:

1. **Envelope с неизвестным типом операции не разбирается целиком** — не
   только неизвестная операция, но и остальные в той же транзакции. Поэтому
   хранение не должно зависеть от разбора через SDK: JSON и `envelope_xdr`
   сохраняются как есть, SDK используется как «если получилось».
2. Готового признака успешности у `TransactionResult` нет; успешность
   выводится из кода результата.
3. `MuxedAccount.universal_account_id` возвращает M-адрес, если задан memo id.
   Канонический G-адрес — `MuxedAccount.account_id` или
   `MuxedAccount.from_account(M).account_id`.
   `StrKey.decode_med25519_public_key` возвращает 40 сырых байт (32 ключа и
   8 memo id).
4. HTTP-клиент пакета — синхронный `requests` (aiohttp только как extra), а в
   стеке HTTPX. SDK планируется использовать только для XDR и strkey. Пакет
   приносит `pydantic`, `requests`, `requests-sse`, `mnemonic` как
   транзитивные зависимости.
5. Сгенерированные XDR-классы используют обёртки (`Uint64`, `Int64`…) и
   объединения с `None` по умолчанию; после `from_xdr` нужна проверка ветки.
6. Не проверялось: разбор реальных ответов Horizon, глубина разбора данных
   Soroban, meta реальных fee-bump.

## 8. Что нельзя воспроизвести по REST API

| Данные Horizon | Почему недоступны из REST |
| --- | --- |
| `history_trades.rounding_slippage`, `base_is_exact` | в структуре `Trade` таких полей нет (`sdk/protocols/horizon/main.go:407-437`) |
| `history_operations.is_payment` | в ответе не отдаётся; видно только по составу `/payments` |
| Таблицы участников | напрямую не отдаются; только через состав `/accounts/{id}/transactions` и `/operations`. Восстановление требует meta, envelope и правил раздела 5 |
| `history_accounts.id`, `history_assets.id` и порядок пары в сделках | внутренние суррогаты конкретной базы |
| `history_ledgers.importer_version`, `created_at`, `updated_at` | время ингеста конкретной базы |
| `tx_meta` в полном объёме | REST отдаёт `result_meta_xdr`, но опускает его в ответах с `join=transactions` (`resourceadapter/transaction.go:46-50`). `envelope_xdr` и `result_xdr` его не заменяют |
| Удаление записи `manage_data` | по операции неразличимо с пустым значением (раздел 6) |

## 9. Открытые вопросы по Horizon REST

Проверка по коду не заменяет проверку на живом инстансе. К следующему этапу:

1. **Актуальна ли версия.** Какой Horizon стоит на публичном инстансе и
   совпадают ли его курсоры сделок и поле `trade_type` с этим commit.
2. **`result_meta_xdr`.** Всегда ли он присутствует в `/transactions` и
   `/accounts/{id}/transactions`, в том числе для fee-bump и Soroban.
3. **Граница удержания.** Реакция на `before_history` для каждого типа
   ресурса и для запросов `asc`; совпадает ли `history_elder_ledger` из корня
   API с фактической границей по каждому ресурсу.
4. **Полнота.** Нет ли у Horizon «ям» внутри удерживаемого диапазона (у
   Horizon есть команда `db detect-gaps`, но по REST это не видно).
5. **Комплектность страницы.** Достоверна ли связь «страница effects
   получена целиком» с `/operations/{id}/effects`, и наоборот.
6. **Fee-bump по внутреннему хешу.** Поведение `id`/`hash` из раздела 6
   подтверждено только чтением кода.
7. **Сделки по аккаунту через `UNION`.** Сохраняет ли пагинация строгий
   порядок между двумя сторонами.
8. **Лимиты.** Фактические ограничения на количество запросов публичного
   инстанса.
9. **Сортировка и `order` сделок.** Подтвердить пропуски в номерах на
   реальных данных.

## 10. Живой сервер

Проверено 24 ноября 2026 на `https://horizon.stellar.org` машиной, с
которой шла загрузка. Файл проверок — `python -m app.backfill probe`.

### Версия и сеть

| Поле root | Значение |
| --- | --- |
| `horizon_version` | `30.0.1-6f7cff3903be1df4fb980a12910a1313c5d7d8dd` |
| `core_version` | `stellar-core 30.0.0 (0e49751aa9ddf892302cca02cea7f3f5d97df9f1)` |
| `network_passphrase` | `Public Global Stellar Network ; September 2015` |
| `current_protocol_version`, `supported_protocol_version`, `core_supported_protocol_version` | 30, 30, 30 |

Сервер — релиз 30.0.1. Исходники в разделах 1–8 читались на `4e13b1be`, то
есть на `main` после 30.0.1. Во всём, что проверено ниже, ответы сервера
совпадают с прочитанным кодом; расхождений не найдено.

### Границы истории

| | |
| --- | --- |
| `history_elder_ledger` | 59346721, закрыт 2025-10-12T11:20:29Z, `protocol_version` 23 |
| `history_latest_ledger` | 65653961, закрыт 2026-11-24T21:30:59Z, `protocol_version` 30 |
| Хранится | 6 307 241 леджер, 408,4 дня |
| `ingest_latest_ledger`, `history_latest_ledger` | 65653961, одинаковы |
| `core_latest_ledger` | 65653962, на один вперед |

Граница движется: в 20:35 UTC старшим был леджер 59346001, в 21:30 —
59346721. Это 720 леджеров в час, по 5 секунд на леджер. Через час загрузки
снизу уходит около 720 леджеров.

`/ledgers/59346720` (на один ниже границы) отвечает 410. `paging_token`
леджера — `sequence << 32`: у 59346721 это 254892225819836416.

### Курсоры на живом сервере

| Запрос | Результат |
| --- | --- |
| `order=desc`, `cursor=<после леджера L>` | все записи в леджерах ≤ L |
| `order=desc`, `cursor=<TOID леджера L>` | все записи в леджерах < L |
| `order=asc`, `cursor=<TOID леджера L>` | все записи в леджерах ≥ L |
| `cursor=<TOID старшего леджера>` | 410 |
| `cursor=<TOID леджера на один выше старшего>` | 200 |
| `cursor=<ниже старшего>` | 410 |

Проверено на эффектах аккаунта, глобальных транзакциях и сделках рынка.
Курсор пары (эффекты, сделки), заданный одним числом, сервер принимает и
понимает как позицию в общем порядке.

`_links.next` есть у **каждой** страницы, включая последнюю и пустую; у
пустой страницы курсор в ссылке пуст или повторяет запрошенный. Поэтому
«есть ссылка» не значит «есть ещё записи»: страница короче лимита —
последняя.

### Сделки

- Первая страница рынка: 200 сделок, из них 145 `liquidity_pool` и 55
  `orderbook`.
- Обе ориентации пары возвращают **одни и те же 200 id**; во второй
  ориентации переставлены активы, суммы, счета, офферы и `base_is_seller`,
  цена `n`/`d` обменена.
- Цепочка сделка → операция → транзакция → леджер работает: леджер
  транзакции совпадает с леджером операции.
- У транзакции из цепочки: 1 операция, 7 эффектов, 5 сделок. Сделки
  получены глобальным `/trades` от позиции транзакции: маршрутов
  `/transactions/{hash}/trades` и `/operations/{id}/trades` на сервере нет
  (оба отвечают 404).
- Идентификатор оффера на стороне трейдера без своего оффера (path payment) —
  синтетическое число (раздел 6). В операции 281967128423751681 оно одно и то
  же во всех четырёх сделках, 4893653146851139585: в трёх оффербук-сделках это
  `base_offer_id`, в сделке с пулом — `counter_offer_id`. В каком из двух полей
  оно стоит, зависит от ориентации пары; «`counter_offer_id` — всегда оффер
  другой стороны» не верно.

### Сделки по аккаунту: порядок страниц и 503 на активных аккаунтах

`/accounts/{id}/trades` собирается из двух подзапросов (сторона base и
сторона counter) через `UNION`, и объединение сортируется заново:
`ORDER BY history_operation_id desc, "order" desc` (`trade.go:218-229` на
`4e13b1be`). Условие курсора наложено в каждом подзапросе. Отсюда порядок
строгий по курсору и на стыке двух страниц, и на стыке двух сторон — это
ответ на вопрос 7 раздела 9. Живьём проверены первые страницы четырёх аккаунтов
(200, 200, 200 и 118 записей): в каждой записи идут строго по убыванию. Стык
страниц живьём не проверялся.

Тот же endpoint на **очень активных аккаунтах отвечает 503** «Service
Unavailable» через 10,7–11,2 с — при любом размере страницы:

| Аккаунт | Сделок в базе за окно (base / counter) | `limit` | Ответ |
| --- | --- | --- | --- |
| `GD77RS525TLPXBSFTZSHDEVDSCDO3NQBTATLRJQVHBOTKUC2PMGPTQEF` | 233 / 221 | 200, 200, 200 | 503, три раза из трёх |
| он же | | 50 | 503 |
| он же, `trade_type=orderbook` | | 200 | 200 за 9,5 с |
| `GBTB3MJPQKPIWLYX34IKBE7ENT46ZDVQGPAVHGBDOMNA4HP6Q2KKJFLH` | 233 / 307 | 200, 50 | 503, оба раза |
| `GBDHHLYIRZMO6LKWY2SY5GXKR6GAOD5MJFBGMQJQ4OHAHYEKHG7OXQMT` (бот) | 29 / 30 | 200 | 503 |
| `GCPOWDQQDVSAQGJXZW3EWPPJ5JCF4KTTHBYNB4U54AKQVDLZXLLYMXY7` | 9 / 1 | 200 | 200 за 1,7 с, страница полная |
| `GDUMK6YJZ6ZC72CAMVHLUHLIFTNSLD7WFWO75Q3T2EOEW75XWH4PNSOZ` | 0 / 1 | 200 | 200 за 1,7 с, страница полная |
| `GA5U6OV2P77IZIXWNKNHGXSFKCRJX3NVQESVZKMYEKV5WNB6IIJKL4GF` | 26 / 1 | 200 | 200 за 2,2 с, 118 записей |

В те же секунды `operations` и `transactions` аккаунта `GD77RS52…` отвечают
за 1,4 и 2,2 с, а загрузка в 0,8 запроса/с получает одни 200. То есть это не
лимит запросов, а цена самого запроса: по его виду сервер собирает все сделки
аккаунта за удержание до применения `LIMIT`, и она растёт с активностью
аккаунта, а не с размером страницы. Подтвердить план запроса на чужом сервере
нельзя — это объяснение по симптомам.

Следствие для проекта: сделки таких аккаунтов с публичного сервера через этот
endpoint не получить. Клиент после пяти попыток останавливает поток с ошибкой,
его покрытие остаётся `pending`: неполнота видна, а не спрятана.

### Чтение вперёд (этап 03)

Проверено 25 ноября 2026 командой `python -m app.collector check` (10
запросов, аккаунт `GCPOWD…`, рынок, окно в 86 400 леджеров):

| Запрос с `order=asc` | Результат |
| --- | --- |
| от позиции «после всего в леджере L» | все записи в леджерах ≥ L+1: у пяти потоков первая запись не ниже L+1 |
| от курсора последней записи страницы | запись с этим курсором не повторяется; если дальше ничего нет — пустая страница |
| от позиции на 1000 леджеров ниже старейшего | **200**: сервер молча начинает с самой старой записи (первая — в леджере 59359290 при старейшем 59356081) |

Третье — то, из-за чего коллектор обязан сам помечать пропущенное
`unverifiable`: при чтении вперёд никакой ошибки нет, и пустая страница не
отличается от «этого уже нет». Курсор пары (эффекты, сделки), заданный одним
числом, понимается как «эта операция и любой порядок»: читается всё, что
старше по номеру операции.

### Транзакции, которые нельзя восстановить по одному REST

- **`result_meta_xdr` сервер не отдаёт.** В странице аккаунта из 80
  транзакций (вся его история в удержании): с meta — 0, с `fee_meta_xdr` —
  80. То же в каждом другом прочитанном ответе. `envelope_xdr`, `result_xdr`
  и `fee_meta_xdr` приходят всегда.
- **Fee-bump по внутреннему хешу.** Для операции 281967128423751681: при
  запросе транзакции по внутреннему хешу поля `id` и `hash` содержат
  внутренний хеш, а `fee_bump_transaction.hash` — внешний. При запросе по
  внешнему — внешний. `max_fee` = 10 000 000 000 (fee fee-bump),
  `inner_transaction.max_fee` = 10 000, `fee_charged` = 200.
- **Неудачные транзакции.** `/accounts/{id}/operations` без
  `include_failed` их скрывает: у бота 200 операций занимают 32 минуты, с
  `include_failed=true` — 13 минут, и 112 из 200 относятся к неудачным.
  `/transactions/{hash}/operations` возвращает операции неудачной транзакции
  и без флага; `/transactions/{hash}/effects` для неё пуст.
- **Muxed-аккаунты** ни разу не встретились в прочитанных страницах.
- **`/accounts/{id}/trades`** один раз ответил 503 на очень активном
  аккаунте; повторный запрос прошёл.
- Несуществующий хеш транзакции — 404 `not_found`; неверный id операции —
  400.

### `manage_data` на живых аккаунтах

| Аккаунт | Операций `manage_data` в последних 200 операциях | Записей data сейчас |
| --- | --- | --- |
| `GA5U6OV2P77IZIXWNKNHGXSFKCRJX3NVQESVZKMYEKV5WNB6IIJKL4GF` | 3 (из 97 операций) | 2 |
| `GC6CMCHNOVQGX4STELR7W2HN5H6EGY7LG2ED27DJAFYVFR3PTCG47CWP` | 1 | 14 |
| `GDUMK6YJZ6ZC72CAMVHLUHLIFTNSLD7WFWO75Q3T2EOEW75XWH4PNSOZ` | 11 | 382 |

### `/trade_aggregations`

Отвечает корзинами по суткам UTC. **Лимит по умолчанию — 10 корзин**; для
большего нужен `limit` (до 200 за запрос) и `offset`. Поля корзины:
`timestamp`, `trade_count`, `base_volume`, `counter_volume`, `avg`, `high`,
`low`, `open`, `close` и их точные дроби `*_r`.

### Как сервер считает корзины

Формулы — `internal/db2/history/trade_aggregation.go` на `4e13b1be`
(`RebuildTradeAggregationTimes`, та же `aggregate` для корзин длиннее
минуты); подтверждены сверкой с сохранёнными сделками (отчёт этапа 03):

- `trade_count`, `base_volume`, `counter_volume` — счёт и суммы сторон в
  ориентации запроса;
- `avg` — `sum(counter_volume)/sum(base_volume)`, напечатанное семью
  знаками; точного `avg_r` сервер **не отдаёт**;
- `high`, `low` — максимум и минимум **цен сделок** (`price_n/price_d` —
  цена оффера), а не отношений сумм; `open` и `close` — цены первой и
  последней сделки корзины по порядку операций.

Для сделок через пул с комиссией цена сделки и отношение сумм расходятся:
считать крайние цены из сумм нельзя. `high_r`, `low_r`, `open_r`, `close_r`
точные, сравнивать можно до дроби.

### Лимит запросов

Публичный сервер ограничивает запросы по IP. Успешные ответы заголовков
лимита не несут. Ответ 429 содержит:

```
X-RateLimit-Limit: 3000
X-RateLimit-Remaining: 0
X-RateLimit-Reset: 300
```

и **не содержит** `Retry-After`. Код лимитера — `internal/httpx/rate_limiter.go`
(GCRA на адрес клиента); в открытой конфигурации `per-hour-rate-limit` по
умолчанию 3600 и всплеск 100 (`internal/flags.go:464-474`), публичный сервер
настроен иначе: 3000.

Что измерено с одного адреса 24 ноября (подробности — отчёт этапа 02, раздел
об инцидентах):

| Прогон | Темп | Запросов | Ответы |
| --- | --- | --- | --- |
| загрузка 7 дней | 2 в секунду | 1420 за 12 минут | все 200 |
| проверка `verify` с ошибкой в цикле — первая попытка | 1 в секунду | несколько десятков, одна и та же страница | 429 с заголовками выше |
| она же, вторая попытка | 1 в секунду | 639 за 10 минут, одна и та же страница | без 429, затем запросы перестали получать ответ |
| возобновление и повторный импорт того же окна | 1 в секунду | 1226 + 184 + 36 | все 200 |

429 получен не «чистой» загрузкой: командой проверки с моей ошибкой, которая
запрашивала одну страницу снова и снова. По этим числам ёмкость и окно лимита
не установить. Установлено другое: 2 запроса в секунду в течение 12 минут и
1 запрос в секунду в течение 20 минут проходят без единого 429, а после
сотен повторов одного запроса сервер сначала отвечает 429, а затем молча не
отвечает. Для длинных загрузок этого проекта выбрано не выше 1 запроса в
секунду и ожидание 429 столько, сколько просит `X-RateLimit-Reset`.

### Ответы на открытые вопросы раздела 9

1. **Версия.** 30.0.1. Курсоры сделок и поле `trade_type` такие, как в
   разделах 3 и 6.
2. **`result_meta_xdr`.** Отсутствует всегда.
3. **Граница удержания.** При `desc` 410 приходит на курсор, равный TOID
   старшего леджера и ниже. При `asc` ошибки нет: сервер молча начинает со
   старейшего (проверено живьём в этапе 03).
4. **Ямы внутри удержания.** По-прежнему не видны через REST.
5. **Комплектность страниц.** Проверена сверкой сохранённых id с ново
   прочитанными страницами (отчёт этапа 02).
6. **Fee-bump по внутреннему хешу.** Подтверждено.
7. **`UNION` в сделках по аккаунту.** Порядок строгий: объединение
   сортируется заново (`trade.go:218-229`), подтверждено на первых страницах
   четырёх аккаунтов. Новый факт — 503 на очень активных аккаунтах (см. выше).
8. **Лимиты.** Заголовки и код лимитера известны, ёмкость не измерена; см.
   выше.
9. **Пропуски в номерах сделок.** В операции 281967128423751681 номера 0–3
   без пропусков; в общем случае не проверялось.
