# NeoInvest React UI

Standalone frontend skeleton for the `neo.hack-2026_NeoInvest` case.

## Implemented

- Email registration page (local demo auth via browser storage)
- Dashboard layout with:
  - KPI cards
  - Filters by ticker, period, and metric focus
  - Price + volume chart
  - Volatility chart
  - Top gainers/decliners table
  - Anomaly events table (`volume > 3 sigma`, `price move > 2%`)
  - Email alert preferences panel

## Run

```bash
cd frontend-react
npm install
npm run dev
```

Перед запуском задайте API URL (опционально, по умолчанию `http://localhost:8001`):

```bash
cp .env.example .env
```

## Notes

- Dashboard и auth работают через backend API.
- Ожидаемый endpoint snapshot: `POST /api/v1/dashboard/snapshot`.
- Current environment in this session does not contain Node.js, so install/start checks were not executed here.
