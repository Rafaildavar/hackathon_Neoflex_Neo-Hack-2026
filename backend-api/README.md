# NeoInvest Backend API

Отдельный backend-сервис для фронтенда `frontend-react`.

Что делает сервис:
- `POST /auth/register` и `POST /auth/login` через `auth.users` в PostgreSQL
- `POST /api/v1/dashboard/snapshot` с данными для графиков/карточек/лидеров/аномалий

Источник данных:
- реальные данные из DWH (`core.*`, `mart.*`)
- DWH наполняется из MOEX ISS API через существующие ETL-скрипты проекта

## 1. Поднять инфраструктуру

```bash
docker compose up -d
```

## 2. Наполнить DWH данными MOEX

```bash
python -u ./script/load_raw_moex_candles.py --tickers SBER,GAZP,LKOH,YDEX,VTBR,ROSN,NVTK --from-date 2026-03-10 --till-date 2026-04-08
python -u ./script/transform_raw_to_candles.py --refresh-aggregates
python -u ./analyze/daily_metrics.py
python -u ./analyze/anomaly.py
python -u ./script/init_auth_schema.py
```

## 3. Запустить backend API

```bash
cd backend-api
python -m pip install -r requirements.txt
python run.py
```

По умолчанию API стартует на `http://localhost:8001`.

## 4. Переменные окружения

Скопируйте `.env.example` в `.env` и при необходимости измените значения:

- `API_HOST`, `API_PORT`
- `CORS_ORIGINS`
- `POSTGRES_*`
- `MOEX_TICKERS`
- `MOEX_DELAY_MINUTES`

## 5. Проверка

```bash
curl http://localhost:8001/health
```

```bash
curl -X POST http://localhost:8001/auth/login -H "Content-Type: application/json" -d "{\"email\":\"admin\",\"password\":\"admin\"}"
```

