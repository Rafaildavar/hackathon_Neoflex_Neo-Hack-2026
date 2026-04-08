# Neo-Hack 2026: MOEX Data Platform (Stage 1)

This repository contains the first stage of the project:
- TimescaleDB (PostgreSQL) container for DWH
- Apache Airflow containers (init, webserver, scheduler)
- Bootstrap SQL for `stg -> core -> mart` layers
- Base Python 3.11 dependencies for ETL scripts

## Tech stack for this stage
- Docker + Docker Compose
- PostgreSQL 16 + TimescaleDB 2.x
- Apache Airflow 2.10 (Python 3.11 image)

## Repository structure

```text
.
├─ docker-compose.yml
├─ .env.example
├─ requirements.txt
├─ sql/
│  └─ init/
│     ├─ 01_schemas.sql
│     └─ 02_continuous_aggregates.sql
└─ airflow/
   ├─ dags/
   │  └─ bootstrap_healthcheck_dag.py
   ├─ logs/
   └─ plugins/
```

## Prerequisites
- Docker Desktop 4.0+
- Docker Compose v2+

> Works on both macOS and Windows (Docker Desktop).

## Quick start

1. Start infrastructure:

```bash
docker compose up -d
```

2. Open services:
- Airflow UI: `http://localhost:8080`
- PostgreSQL: `localhost:5432`

3. Default credentials (change in `.env`):
- Airflow: `admin / admin`
- PostgreSQL: `moex / moex_pass`

## What is initialized in DB

During the first DB start, SQL scripts in `sql/init` are executed automatically:

- `01_schemas.sql`
  - Creates schemas: `stg`, `core`, `mart`
  - Creates hypertables:
    - `stg.raw_moex_data`
    - `core.minute_candles`
    - `mart.dashboard_metrics`
  - Creates service marts:
    - `mart.daily_metrics`
    - `mart.anomaly_events`

- `02_continuous_aggregates.sql`
  - Creates continuous aggregates:
    - `core.hourly_candles`
    - `core.daily_candles`
    - `core.weekly_candles`
  - Adds refresh policies for each aggregate

This follows the architecture from `db.md`: detailed minute layer + derived intervals via continuous aggregates.

## Common commands

Start containers:

```bash
docker compose up -d
```

Stop containers:

```bash
docker compose down
```

Stop and remove DB volume (full reset):

```bash
docker compose down -v
```

View logs:

```bash
docker compose logs -f db
docker compose logs -f airflow-webserver
docker compose logs -f airflow-scheduler
```

## Python 3.11 local dependencies

Install dependencies for future ETL scripts:

```bash
python3.11 -m pip install -r requirements.txt
```

On Windows (if `python` points to 3.11):

```powershell
python -m pip install -r requirements.txt
```

## Notes
- MOEX ISS free data is delayed (~15 minutes).
- Keep API polling rates inside the documented limits.
- Stage 1 includes infra bootstrap only (no ingestion DAG yet).

