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
├─ scripts/
│  ├─ load_raw_moex_candles.py
│  └─ transform_raw_to_candles.py
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

Ниже команды для двух функций:
1) загрузка сырых payload в `stg.raw_moex_data`
2) преобразование payload в `core.minute_candles`

### 1. Загрузка сырых данных (`load_raw_moex_candles.py`)

#### Windows (PowerShell)
```powershell
Set-Location "D:\hackathon_Neoflex_Neo-Hack-2026"
python -u .\scripts\load_raw_moex_candles.py --from-date 2026-03-08 --till-date 2026-04-08
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
python -u .\scripts\transform_raw_to_candles.py
```

macOS (Terminal):
```bash
cd /path/to/hackathon_Neoflex_Neo-Hack-2026
python3 -u ./script/transform_raw_to_candles.py
```

#### Только один тикер

Windows (PowerShell):
```powershell
python -u .\scripts\transform_raw_to_candles.py --ticker SBER
```

macOS (Terminal):
```bash
python3 -u ./script/transform_raw_to_candles.py --ticker SBER
```

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
