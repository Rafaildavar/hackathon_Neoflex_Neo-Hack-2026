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

## Notes

- Uses mock market data and localStorage only.
- API integration can be added in `src/services/dashboardService.ts`.
- Current environment in this session does not contain Node.js, so install/start checks were not executed here.
