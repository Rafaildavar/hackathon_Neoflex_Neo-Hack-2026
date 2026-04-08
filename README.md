# Neo Invest — загрузка данных MOEX

Проект загружает исторические и оперативные данные с Московской биржи (ISS MOEX) в PostgreSQL с расширением **TimescaleDB**. Оркестрация — **Apache Airflow**, код ingestion — **Python** (SQLAlchemy, pandas).

## Что внутри репозитория

| Путь | Назначение |
|------|------------|
| `docker/docker-compose.yml` | Postgres (TimescaleDB), Airflow (webserver + scheduler + init), опционально контейнер `app` для разового запуска скрипта |
| `docker/airflow/Dockerfile` | Образ Airflow с зависимостями из `requirements.txt` |
| `dags/` | DAG’и Airflow (ручная загрузка истории в `stg`) |
| `src/` | Клиент MOEX, загрузка в БД, репозиторий, настройки |
| `sql/staging`, `sql/core`, `sql/mart` | DDL схем `stg`, `core`, `mart` |
| `sql/migrations/` | Миграции данных/схемы (переименование тикера, смена формата `daily_candles`) |

Слои данных:

- **`stg`** — сырой JSON ответов API (`stg.raw_moex_data`).
- **`core`** — нормализованные дневные свечи и внутридневные котировки (`core.daily_candles`, `core.intraday_quotes`).

## Требования

- **Docker** и **Docker Compose** (plugin `docker compose`).
- Для локального запуска Python без Docker: **Python 3.11+** (рекомендуется).

## Быстрый старт (Docker)

### 1. Клонирование и переменные окружения

```bash
git clone <url-репозитория>
cd "НЕОФЛЕКС ХАКАТОН"
cp .env.example .env
```

Отредактируйте `.env`. Важно:

- **`POSTGRES_DB` / `POSTGRES_USER` / `POSTGRES_PASSWORD`** должны совпадать с тем, что ожидает Airflow в `docker/docker-compose.yml` (строка `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`). В примере используется база `neo_invest` и пользователь `neo`.
- **`POSTGRES_PORT`** на хосте: в `docker-compose` проброшен порт **`5434:5432`**, чтобы не конфликтовать с другим Postgres на `5432`/`5433`. Подключение с вашего Mac: `localhost:5434`.

### 2. Запуск инфраструктуры

Из каталога `docker`:

```bash
cd docker
docker compose up -d postgres
```

Дождитесь статуса `healthy` у контейнера `neo_postgres`:

```bash
docker compose ps postgres
```

Полный стек (Postgres + инициализация Airflow + webserver + scheduler):

```bash
docker compose up -d
```

Первый запуск может занять несколько минут: `airflow-init` выполняет `airflow db migrate` и создаёт пользователя админки.

### 3. Airflow UI

- URL: **http://localhost:8080**
- Логин/пароль: из `.env` — `AIRFLOW_USER` / `AIRFLOW_PASSWORD` (по умолчанию `admin` / `admin`).

Включите DAG **`moex_manual_load`** и запустите задачу вручную (DAG без расписания, `schedule=None`): загрузится история за ~30 дней по тикерам из кода DAG в таблицу **`stg.raw_moex_data`**.

### 4. Подключение к Postgres

С хоста (psql, DBeaver, TablePlus):

| Параметр | Значение (по умолчанию из примера) |
|----------|-------------------------------------|
| Host | `localhost` |
| Port | `5434` |
| Database | `neo_invest` |
| User / Password | из `.env` (`POSTGRES_USER` / `POSTGRES_PASSWORD`) |

Через CLI:

```bash
docker exec -it neo_postgres psql -U neo -d neo_invest
```

Полезные команды в `psql`: `\dn` (схемы), `\dt stg.*`, `\dt core.*`, `\q` (выход).

**Примечание.** Порт `5434` — это не веб-страница; в браузере он не откроется. Для веб-интерфейса к БД используйте pgAdmin/Adminer или клиент с GUI.

### 5. Остановка

```bash
cd docker
docker compose stop
```

Данные Postgres сохраняются в volume `pg_data`. Полное удаление контейнеров без удаления volume:

```bash
docker compose down
```

## Локальный Python (без Docker для кода)

Удобно для отладки `src/` и запуска скриптов с хоста.

```bash
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Убедитесь, что Postgres из Docker запущен и в `.env` указаны **`POSTGRES_HOST=localhost`** и **`POSTGRES_PORT=5434`**.

Запуск из **корня репозитория** (важен `PYTHONPATH`):

```bash
export PYTHONPATH=.
python -m src.ingestion.fetch_history
python -m src.ingestion.load_history_manual
```

При первом использовании загрузчика с SQLAlchemy вызывается `ensure_db_objects()`: создаются схемы и применяются SQL-файлы из `sql/staging`, `sql/core` и `sql/migrations` (если файлы существуют).

## Структура БД (кратко)

- Инициализация контейнера: `docker/postgres/init.sql` — расширение TimescaleDB и схемы `stg`, `core`, `mart`.
- Таблица сырья: `stg.raw_moex_data` (JSONB).
- Таблица дневных свечей: `core.daily_candles` (ключ `(name, date)`; поле объёма названо **`valume`** — опечатка сохранена в схеме).

## Частые проблемы

- **`bind: address already in use` для порта** — на машине уже занят порт. В `docker/docker-compose.yml` смените левую часть в `ports` у `postgres` (например `5435:5432`) и обновите `POSTGRES_PORT` в `.env`.
- **Airflow не видит DAG** — проверьте монтирование `../dags` в `docker-compose` и логи `neo_airflow_scheduler`.
- **Несовпадение учётных данных** — `POSTGRES_*` в `.env`, строка подключения Airflow в `docker-compose.yml` и параметры в приложении должны описывать одну и ту же БД и пользователя.

## Лицензия

Уточните у команды хакатона / владельца репозитория.
