# Neo-Hack 2026: MOEX Data Platform (Этап 1)

Репозиторий содержит базовую инфраструктуру и ETL-скрипты для загрузки и подготовки данных MOEX:
- TimescaleDB (PostgreSQL) для DWH
- Apache Airflow (init, webserver, scheduler)
- SQL-инициализация слоев `stg -> core -> mart`
- Скрипты загрузки сырых данных и трансформации в минутные свечи

## Технологический стек
- Docker + Docker Compose
- PostgreSQL 16 + TimescaleDB 2.x
- Apache Airflow 2.10 (Python 3.11)
- Python 3.11 + `requests`, `psycopg2`

## Структура проекта

```text
.
├─ docker-compose.yml
├─ requirements.txt
├─ sql/
│  └─ init/
│     ├─ 01_schemas.sql
│     └─ 02_continuous_aggregates.sql
├─ script/
│  ├─ load_raw_moex_candles.py
│  ├─ transform_raw_to_candles.py
│  └─ calculate_daily_metrics.py
└─ airflow/
   ├─ dags/
   ├─ logs/
   └─ plugins/
```

## Предварительные требования
- Docker Desktop 4.0+
- Docker Compose v2+
- Python 3.11+

## Быстрый старт

1) Поднимите инфраструктуру:

```bash
docker compose up -d
```

2) Сервисы:
- Airflow UI: `http://localhost:8080`
- PostgreSQL: `localhost:5432`

3) Дефолтные доступы (можно переопределить через `.env`):
- Airflow: `admin / admin`
- PostgreSQL: `moex / moex_pass`

## Инструкция по моей части (инфра + ingestion + transform)

Ниже минимальный поток запуска, который можно повторить с нуля:

1) Поднять Docker-инфраструктуру:

```bash
docker compose up -d
```

2) Проверить, что контейнеры живы:

```bash
docker compose ps
```

3) Загрузить сырые данные MOEX в `stg.raw_moex_data`:

```bash
python3 -u ./script/load_raw_moex_candles.py --from-date 2026-03-08 --till-date 2026-04-08
```

4) Преобразовать payload в свечи `core.minute_candles`:

```bash
python3 -u ./script/transform_raw_to_candles.py
```

5) Быстрая проверка в БД:

```sql
SELECT COUNT(*) FROM stg.raw_moex_data;
SELECT COUNT(*) FROM core.minute_candles;
```

6) Airflow UI для мониторинга:
- `http://localhost:8080`
- логин/пароль: `admin / admin` (или значения из `.env`)

## Что инициализируется в БД

При первом старте контейнера БД выполняются скрипты из `sql/init`:

- `sql/init/01_schemas.sql`
  - Схемы: `stg`, `core`, `mart`
  - Гипертаблицы:
    - `stg.raw_moex_data`
    - `core.minute_candles`
    - `mart.dashboard_metrics`
  - Сервисные таблицы:
    - `mart.daily_metrics`
    - `mart.anomaly_events`

- `sql/init/02_continuous_aggregates.sql`
  - Continuous aggregates:
    - `core.hourly_candles`
    - `core.daily_candles`
    - `core.weekly_candles`
  - Политики автообновления агрегатов

Важно: init-скрипты запускаются только при первом создании volume БД.

## Запуск ETL-скриптов

Ниже команды для трех функций:
1) загрузка сырых payload в `stg.raw_moex_data`
2) преобразование payload в `core.minute_candles`

### 1. Загрузка сырых данных (`load_raw_moex_candles.py`)

#### Windows (PowerShell)
```powershell
Set-Location "D:\hackathon_Neoflex_Neo-Hack-2026"
python -u .\script\load_raw_moex_candles.py --from-date 2026-03-08 --till-date 2026-04-08
```

#### macOS (Terminal)
```bash
cd /path/to/hackathon_Neoflex_Neo-Hack-2026
python3 -u ./script/load_raw_moex_candles.py --from-date 2026-03-08 --till-date 2026-04-08
```

Полезные параметры:
- `--tickers` - список тикеров через запятую
- `--from-date` и `--till-date` - диапазон дат (`YYYY-MM-DD`)
- `--interval` - интервал свечей MOEX (`1`, `10`, `60`, `24`)

### 2. Трансформация в минутные свечи (`transform_raw_to_candles.py`)

#### Все тикеры

Windows (PowerShell):
```powershell
Set-Location "D:\hackathon_Neoflex_Neo-Hack-2026"
python -u .\script\transform_raw_to_candles.py
```

macOS (Terminal):
```bash
cd /path/to/hackathon_Neoflex_Neo-Hack-2026
python3 -u ./script/transform_raw_to_candles.py
```

#### Только один тикер

Windows (PowerShell):
```powershell
python -u .\script\transform_raw_to_candles.py --ticker SBER
```

macOS (Terminal):
```bash
python3 -u ./script/transform_raw_to_candles.py --ticker SBER
```

### 3. Загрузка данных из БД в pandas DataFrame

Модуль `analyze/load_data_from_db.py` предоставляет функции для загрузки данных в DataFrame в собственном коде анализа:

```python
from analyze.load_data_from_db import load_candles, load_daily_metrics

# Загрузить минутные свечи для SBER
df_minute = load_candles(interval="minute", ticker="SBER")

# Загрузить часовые свечи с фильтром по времени
df_hourly = load_candles(
    interval="hourly",
    ticker="GAZP",
    from_ts="2026-04-01",
    to_ts="2026-04-08"
)

# Загрузить дневные метрики
df_daily = load_daily_metrics(ticker="LKOH", from_date="2026-04-01")
```

Доступные функции:
- `load_candles(interval, ticker, from_ts, to_ts)` - свечи (minute, hourly, daily, weekly)
- `load_daily_metrics(ticker, from_date, to_date)` - дневные метрики
- `load_dashboard_metrics(ticker, interval_type, from_ts, to_ts)` - с техническими индикаторами
- `load_raw_payloads(ticker, from_ts, to_ts)` - сырые MOEX payload

## Полезные команды Docker

Запуск:

```bash
docker compose up -d
```

Остановка:

```bash
docker compose down
```

Полный reset (с удалением volume БД):

```bash
docker compose down -v
```

Логи контейнеров (stdout/stderr):

```bash
docker compose logs -f db
docker compose logs -f airflow-webserver
docker compose logs -f airflow-scheduler
```

## Установка Python-зависимостей

Windows (PowerShell):
```powershell
python -m pip install -r requirements.txt
```

macOS (Terminal):
```bash
python3 -m pip install -r requirements.txt
```

## Примечания
- Данные MOEX ISS в бесплатном доступе имеют задержку около 15 минут.
- Соблюдайте лимиты API по частоте запросов.
- На этом этапе реализован инфраструктурный bootstrap и базовые ETL-скрипты.

## Преобразованния 

Этот блок описывает мой контур работы с данными: от сырого слоя до агрегатов по часам, дням и неделям.

### Шаг 1. Поднять инфраструктуру

```bash
docker compose up -d
docker compose ps
```

### Шаг 2. Загрузить сырой слой (`stg.raw_moex_data`)

```bash
python3 -u ./script/load_raw_moex_candles.py --from-date 2026-03-08 --till-date 2026-04-08
```

Проверка:

```sql
SELECT COUNT(*) AS raw_rows FROM stg.raw_moex_data;
```

### Шаг 3. Преобразовать в минутные свечи (`core.minute_candles`)

```bash
python3 -u ./script/transform_raw_to_candles.py
```

Проверка:

```sql
SELECT COUNT(*) AS minute_rows FROM core.minute_candles;
```

### Шаг 4. Получить срезы по часам / дням / неделям

После загрузки и трансформации данные доступны через continuous aggregates.

#### По часам (`core.hourly_candles`)

```sql
SELECT *
FROM core.hourly_candles
WHERE ticker = 'SBER'
ORDER BY bucket DESC
LIMIT 24;
```

#### По дням (`core.daily_candles`)

```sql
SELECT *
FROM core.daily_candles
WHERE ticker = 'SBER'
ORDER BY bucket DESC
LIMIT 30;
```

#### По неделям (`core.weekly_candles`)

```sql
SELECT *
FROM core.weekly_candles
WHERE ticker = 'SBER'
ORDER BY bucket DESC
LIMIT 12;
```

### Шаг 5. Если агрегаты не обновились сразу

Можно принудительно обновить их (например, после большого догруза):

```sql
CALL refresh_continuous_aggregate('core.hourly_candles', NULL, NULL);
CALL refresh_continuous_aggregate('core.daily_candles', NULL, NULL);
CALL refresh_continuous_aggregate('core.weekly_candles', NULL, NULL);
```

## Инструкция для коллеги: полная пересборка за 30 дней (только Python-скрипты)

Ниже сценарий пересборки, который выполняется Python-скриптами:
- очистка текущих данных контура,
- загрузка полного minute-исторического диапазона с пагинацией MOEX,
- трансформация в `core.minute_candles`,
- обновление агрегатов `hourly/daily/weekly`,
- проверка количества строк по слоям.

### Что нужно
- Поднятая инфраструктура Docker (`db`, `airflow-*`) — запускается один раз.
- Python и зависимости из `requirements.txt` (лучше через `.venv_run`).
- Доступ к MOEX ISS API.

### Вариант 1: один командный скрипт (рекомендуется)

```bash
.venv_run/bin/python -u ./script/rebuild_history_pipeline.py \
  --tickers SBER,GAZP,LKOH,YDEX,VTBR,ROSN,NVTK,TATN,GMKN,NLMK \
  --from-date 2026-03-10 \
  --till-date 2026-04-08 \
  --page-size 500 \
  --max-pages 1000
```

Что делает `rebuild_history_pipeline.py`:
1. `reset_pipeline_data.py` — очищает `stg.raw_moex_data` и `core.minute_candles`;
2. `load_raw_moex_candles.py` — грузит все страницы MOEX за период;
3. `transform_raw_to_candles.py --refresh-aggregates` — строит minute и обновляет `hourly/daily/weekly`;
4. `verify_pipeline_counts.py` — выводит контрольные объёмы таблиц.

### Вариант 2: пошагово, но тоже только Python

```bash
.venv_run/bin/python -u ./script/reset_pipeline_data.py
.venv_run/bin/python -u ./script/load_raw_moex_candles.py --tickers SBER,GAZP,LKOH,YDEX,VTBR,ROSN,NVTK,TATN,GMKN,NLMK --from-date 2026-03-10 --till-date 2026-04-08 --page-size 500 --max-pages 1000
.venv_run/bin/python -u ./script/transform_raw_to_candles.py --from-date 2026-03-10 --till-date 2026-04-08 --refresh-aggregates
.venv_run/bin/python -u ./script/verify_pipeline_counts.py
```

## Риал тайм

Поминутное дозаполнение работает через Airflow DAG:
- **DAG ID:** `moex_minute_incremental_sync`
- **Файл:** `airflow/dags/moex_minute_incremental_dag.py`
- **Расписание:** каждую минуту (`*/1 * * * *`)

### Как это работает

Для каждого тикера DAG:
1. Читает из `core.minute_candles` последнюю загруженную минуту (`max(bucket)`).
2. Запрашивает MOEX candles с интервалом 1 минута.
3. Сохраняет raw payload в `stg.raw_moex_data`.
4. Пишет в `core.minute_candles` только новые минуты (`bucket > last_bucket`) через upsert.
5. Обновляет continuous aggregates (`core.hourly_candles`, `core.daily_candles`) только по затронутому диапазону.

Это именно **дозаполнение**, а не полная перезагрузка данных.

### Запуск для коллеги

1) Убедиться, что инфраструктура запущена:
```bash
docker compose up -d
```

2) Открыть Airflow UI:
- `http://localhost:8080`
- логин/пароль из `.env` (по умолчанию `admin/admin`)

3) Включить DAG `moex_minute_incremental_sync` и выполнить первый ручной запуск (`Trigger DAG`).

4) Дальше DAG будет работать автоматически каждую минуту.

### Быстрая проверка, что риал тайм идёт

```sql
SELECT max(ingested_at) AS last_raw
FROM stg.raw_moex_data;

SELECT ticker, max(bucket) AS last_bucket
FROM core.minute_candles
GROUP BY ticker
ORDER BY ticker;
```

Если `last_raw` и `last_bucket` обновляются по времени, значит поминутный инкремент работает корректно.

## Запуск DAG'ов в Airflow

Ниже краткая инструкция, как запускать DAG'и из этого репозитория.

### Какие DAG'и есть

- `bootstrap_healthcheck` — технический healthcheck DAG (`schedule=None`).
- `moex_minute_incremental_sync` — дозаполнение минутных свечей (каждую минуту).
- `daily_metrics_dag` — расчет `mart.daily_metrics` (ежедневно в `00:10`).
- `anomaly_on_new_data_dag` — проверка аномалий при появлении новых данных (`каждые 5 минут`).

### 1) Поднять сервисы

```bash
docker compose up -d
docker compose ps
```

### 2) Открыть Airflow UI

- URL: `http://localhost:8080`
- Логин/пароль: из `.env` (по умолчанию `admin/admin`)

### 3) Включить DAG

В списке DAG'ов переведите переключатель нужного DAG в состояние **On**.

### 4) Запустить DAG вручную

В UI:
- откройте страницу DAG,
- нажмите **Trigger DAG**,
- проверьте статус запуска в **Grid**/**Graph**.

### 5) Проверить логи задачи

- откройте конкретный `Task Instance` в Grid/Graph,
- нажмите **Log**.

CLI-альтернатива:

```bash
docker exec neo_hack_airflow_webserver airflow dags list
docker exec neo_hack_airflow_webserver airflow dags trigger moex_minute_incremental_sync
docker exec neo_hack_airflow_webserver airflow tasks list moex_minute_incremental_sync --tree
```

### 6) Остановить DAG

В UI переведите переключатель DAG обратно в **Off**.

### Проверка результата в БД

```sql
SELECT max(ingested_at) AS last_raw FROM stg.raw_moex_data;

SELECT ticker, max(bucket) AS last_bucket
FROM core.minute_candles
GROUP BY ticker
ORDER BY ticker;

SELECT count(*) FROM mart.daily_metrics;
SELECT count(*) FROM mart.anomaly_events;
```

## Отдельный блок запуска (Auth API + Frontend + DAG)

Ниже минимальная последовательность запуска всего контура после `git pull`.

### 1) Запуск инфраструктуры

```bash
docker compose up -d
```

### 2) Подготовка auth-таблицы пользователей в БД

```bash
.venv_run/bin/python ./script/init_auth_schema.py
```

### 3) Запуск Python API авторизации

```bash
.venv_run/bin/python ./script/auth_api.py
```

Проверка:

```bash
curl http://localhost:8001/health
```

### 4) Запуск frontend-react

```bash
cd frontend-react
npm install
npm run dev -- --host 0.0.0.0 --port 5173
```

Открыть в браузере:
- `http://localhost:5173`

Логика авторизации:
- регистрация требуется только один раз;
- если email уже существует, форма выполняет вход по email/паролю.

### 5) Запуск DAG-оркестрации

В Airflow UI (`http://localhost:8080`) включить:
- `pipeline_orchestration_dag`
- `moex_minute_incremental_sync`
- `anomaly_on_new_data_dag`
- `daily_metrics_dag`

Сценарий работы:
- `pipeline_orchestration_dag` каждую минуту запускает `minute -> anomaly`,
- `daily_metrics_dag` выполняется раз в день по расписанию.
