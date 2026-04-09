import { MOCK_SERIES, MONITORED_TICKERS } from "../data/mockData";
import {
  AlertPreferences,
  AnomalyRow,
  DashboardFilters,
  DashboardSnapshot,
  DailyMetricPoint,
  KpiCardData,
  LeaderRow,
  PriceVolumePoint,
  SeverityLevel,
  VolatilityPoint
} from "../types";

const ALERT_STORAGE_KEY = "neo_invest_alert_preferences";
const MOEX_DELAY_MINUTES = 15;

function rangeToDays(range: DashboardFilters["range"]): number {
  switch (range) {
    case "7d":
      return 7;
    case "30d":
      return 30;
    case "90d":
      return 90;
    default:
      return 30;
  }
}

function formatCompactNumber(value: number): string {
  return new Intl.NumberFormat("ru-RU", {
    notation: "compact",
    maximumFractionDigits: 2
  }).format(value);
}

function formatNumber(value: number, maximumFractionDigits = 2): string {
  return new Intl.NumberFormat("ru-RU", {
    minimumFractionDigits: maximumFractionDigits,
    maximumFractionDigits
  }).format(value);
}

function formatSignedPercent(value: number, maximumFractionDigits = 2): string {
  const formatted = formatNumber(Math.abs(value), maximumFractionDigits);
  if (value > 0) {
    return `+${formatted}%`;
  }
  if (value < 0) {
    return `-${formatted}%`;
  }
  return `${formatted}%`;
}

function standardDeviation(values: number[]): number {
  if (values.length === 0) {
    return 0;
  }

  const mean = values.reduce((sum, value) => sum + value, 0) / values.length;
  const squareDiff = values.map((value) => (value - mean) ** 2);
  return Math.sqrt(squareDiff.reduce((sum, value) => sum + value, 0) / values.length);
}

function formatDateLabel(isoDate: string): string {
  return new Date(isoDate).toLocaleDateString("ru-RU", {
    month: "short",
    day: "2-digit"
  });
}

function createKpis(tickerMap: Record<string, DailyMetricPoint[]>): KpiCardData[] {
  const latestRows = Object.values(tickerMap).map((rows) => rows[rows.length - 1]).filter(Boolean);
  const previousRows = Object.values(tickerMap).map((rows) => rows[rows.length - 2]).filter(Boolean);

  const latestAverageClose =
    latestRows.reduce((sum, row) => sum + row.close, 0) / Math.max(latestRows.length, 1);
  const previousAverageClose =
    previousRows.reduce((sum, row) => sum + row.close, 0) / Math.max(previousRows.length, 1);
  const averageCloseDelta = previousAverageClose
    ? ((latestAverageClose - previousAverageClose) / previousAverageClose) * 100
    : 0;

  const totalVolume = latestRows.reduce((sum, row) => sum + row.volume, 0);
  const maxVolatility = latestRows.reduce((max, row) => Math.max(max, row.volatility), 0);

  const movers = latestRows
    .map((row) => ({ ticker: row.ticker, delta: row.priceChangePct }))
    .sort((a, b) => b.delta - a.delta);

  const topMover = movers[0];

  return [
    {
      title: "Средняя цена закрытия",
      value: formatNumber(latestAverageClose, 2),
      delta: formatSignedPercent(averageCloseDelta, 2),
      trend: averageCloseDelta > 0 ? "up" : averageCloseDelta < 0 ? "down" : "neutral"
    },
    {
      title: "Суммарный объем",
      value: formatCompactNumber(totalVolume),
      delta: `${latestRows.length} тикеров`,
      trend: "neutral"
    },
    {
      title: "Лидер движения",
      value: topMover ? topMover.ticker : "-",
      delta: topMover ? formatSignedPercent(topMover.delta, 2) : "0%",
      trend: topMover && topMover.delta < 0 ? "down" : "up"
    },
    {
      title: "Макс. волатильность",
      value: `${formatNumber(maxVolatility, 2)}%`,
      delta: "дневной диапазон",
      trend: maxVolatility > 4 ? "up" : "neutral"
    }
  ];
}

function createLeaders(tickerMap: Record<string, DailyMetricPoint[]>): {
  gainers: LeaderRow[];
  losers: LeaderRow[];
} {
  const rows: LeaderRow[] = Object.values(tickerMap)
    .map((series) => series[series.length - 1])
    .filter(Boolean)
    .map((row) => ({
      ticker: row.ticker,
      changePct: row.priceChangePct,
      volume: row.volume
    }));

  const gainers = [...rows]
    .sort((a, b) => b.changePct - a.changePct)
    .slice(0, 5);
  const losers = [...rows]
    .sort((a, b) => a.changePct - b.changePct)
    .slice(0, 5);

  return { gainers, losers };
}

function createPriceVolumeSeries(tickerMap: Record<string, DailyMetricPoint[]>): PriceVolumePoint[] {
  const firstTicker = Object.keys(tickerMap)[0];
  if (!firstTicker || tickerMap[firstTicker].length === 0) {
    return [];
  }

  const length = tickerMap[firstTicker].length;
  const tickers = Object.keys(tickerMap);
  const output: PriceVolumePoint[] = [];

  for (let index = 0; index < length; index += 1) {
    const row: PriceVolumePoint = {
      date: formatDateLabel(tickerMap[firstTicker][index].date),
      totalVolume: 0
    };

    tickers.forEach((ticker) => {
      const point = tickerMap[ticker][index];
      row[ticker] = point.close;
      row.totalVolume += point.volume;
    });

    output.push(row);
  }

  return output;
}

function createVolatilitySeries(tickerMap: Record<string, DailyMetricPoint[]>): VolatilityPoint[] {
  const firstTicker = Object.keys(tickerMap)[0];
  if (!firstTicker || tickerMap[firstTicker].length === 0) {
    return [];
  }

  const length = tickerMap[firstTicker].length;
  const tickers = Object.keys(tickerMap);
  const output: VolatilityPoint[] = [];

  for (let index = 0; index < length; index += 1) {
    const row: VolatilityPoint = {
      date: formatDateLabel(tickerMap[firstTicker][index].date)
    };

    tickers.forEach((ticker) => {
      const point = tickerMap[ticker][index];
      row[ticker] = point.volatility;
    });

    output.push(row);
  }

  return output;
}

function createAnomalies(tickerMap: Record<string, DailyMetricPoint[]>): AnomalyRow[] {
  const anomalies: AnomalyRow[] = [];

  Object.values(tickerMap).forEach((series) => {
    if (series.length === 0) {
      return;
    }

    const volumeValues = series.map((point) => point.volume);
    const volumeAverage = volumeValues.reduce((sum, value) => sum + value, 0) / volumeValues.length;
    const volumeStd = standardDeviation(volumeValues);
    const volumeThreshold = volumeAverage + 3 * volumeStd;

    series.forEach((point) => {
      if (point.volume > volumeThreshold) {
        const severity: SeverityLevel = point.volume > volumeAverage + 4 * volumeStd ? "high" : "medium";
        anomalies.push({
          id: `${point.ticker}-${point.date}-volume`,
          eventTs: point.date,
          ticker: point.ticker,
          anomalyType: "volume_spike",
          metricValue: point.volume,
          threshold: Math.round(volumeThreshold),
          severity
        });
      }

      if (Math.abs(point.priceChangePct) > 2) {
        const severity: SeverityLevel = Math.abs(point.priceChangePct) > 4 ? "high" : "medium";
        anomalies.push({
          id: `${point.ticker}-${point.date}-price`,
          eventTs: point.date,
          ticker: point.ticker,
          anomalyType: "price_jump",
          metricValue: point.priceChangePct,
          threshold: 2,
          severity
        });
      }
    });
  });

  return anomalies
    .sort((a, b) => b.eventTs.localeCompare(a.eventTs))
    .slice(0, 30);
}

export function getStoredAlertPreferences(defaultEmail: string): AlertPreferences {
  const raw = localStorage.getItem(ALERT_STORAGE_KEY);
  if (!raw) {
    return {
      email: defaultEmail,
      priceAlerts: true,
      volumeAlerts: true,
      minSeverity: "medium"
    };
  }

  try {
    const parsed = JSON.parse(raw) as AlertPreferences;
    return {
      email: parsed.email || defaultEmail,
      priceAlerts: Boolean(parsed.priceAlerts),
      volumeAlerts: Boolean(parsed.volumeAlerts),
      minSeverity: parsed.minSeverity === "high" ? "high" : "medium"
    };
  } catch {
    return {
      email: defaultEmail,
      priceAlerts: true,
      volumeAlerts: true,
      minSeverity: "medium"
    };
  }
}

export function saveAlertPreferences(preferences: AlertPreferences): void {
  localStorage.setItem(ALERT_STORAGE_KEY, JSON.stringify(preferences));
}

export async function fetchDashboardSnapshot(filters: DashboardFilters): Promise<DashboardSnapshot> {
  await new Promise((resolve) => setTimeout(resolve, 180));

  const requestedTickers = filters.tickers.length > 0 ? filters.tickers : MONITORED_TICKERS;
  const days = rangeToDays(filters.range);
  const tickerMap: Record<string, DailyMetricPoint[]> = {};

  requestedTickers.forEach((ticker) => {
    const fullSeries = MOCK_SERIES[ticker] ?? [];
    tickerMap[ticker] = fullSeries.slice(-days);
  });

  return {
    generatedAt: new Date().toISOString(),
    delayedByMinutes: MOEX_DELAY_MINUTES,
    availableTickers: MONITORED_TICKERS,
    kpis: createKpis(tickerMap),
    priceVolumeSeries: createPriceVolumeSeries(tickerMap),
    volatilitySeries: createVolatilitySeries(tickerMap),
    leaders: createLeaders(tickerMap),
    anomalies: createAnomalies(tickerMap)
  };
}
