import { MOCK_SERIES, MONITORED_TICKERS } from "../data/mockData";
import {
  AlertPreferences,
  AnomalyRow,
  CandlestickPoint,
  DashboardFilters,
  DashboardSnapshot,
  DailyMetricPoint,
  KpiCardData,
  LstmForecastPoint,
  LstmHistoryPoint,
  LeaderRow,
  PriceVolumePoint,
  SeverityLevel,
  TimeRange,
  TimeResolution,
  VolatilityPoint
} from "../types";

const ALERT_STORAGE_KEY = "neo_invest_alert_preferences";
const MOEX_DELAY_MINUTES = 15;
const DASHBOARD_API_BASE_URL =
  import.meta.env.VITE_API_BASE_URL ??
  import.meta.env.VITE_AUTH_API_BASE_URL ??
  "http://localhost:8001";
const TICKER_CACHE_TTL_MS = 60_000;

let cachedAvailableTickers: string[] | null = null;
let cachedAvailableTickersAt = 0;

interface ResolvedTickerPoint {
  timestamp: string;
  label: string;
  close: number;
  volume: number;
  volatility: number;
}

function normalizeTickers(tickers: string[]): string[] {
  return Array.from(
    new Set(
      tickers
        .map((ticker) => ticker.trim().toUpperCase())
        .filter((ticker) => ticker.length > 0)
    )
  );
}

function getMockSeriesForTicker(ticker: string): DailyMetricPoint[] {
  const direct = MOCK_SERIES[ticker];
  if (direct) {
    return direct;
  }

  const fallback = MOCK_SERIES[MONITORED_TICKERS[0]] ?? [];
  return fallback.map((point) => ({
    ...point,
    ticker
  }));
}

async function fetchAvailableTickersFromBackend(): Promise<string[]> {
  const now = Date.now();
  if (cachedAvailableTickers && now - cachedAvailableTickersAt < TICKER_CACHE_TTL_MS) {
    return cachedAvailableTickers;
  }

  try {
    const response = await fetch(`${DASHBOARD_API_BASE_URL}/api/tickers`);
    if (response.ok) {
      const payload = (await response.json()) as { tickers?: string[] };
      const normalized = normalizeTickers(payload.tickers ?? []);
      if (normalized.length > 0) {
        cachedAvailableTickers = normalized;
        cachedAvailableTickersAt = now;
        return normalized;
      }
    }
  } catch {
    // Fall through to static fallback when backend is unavailable.
  }

  const fallback = normalizeTickers(MONITORED_TICKERS);
  cachedAvailableTickers = fallback;
  cachedAvailableTickersAt = now;
  return fallback;
}

function rangeToDays(range: TimeRange): number {
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

function plusOneDayIso(isoDate: string): string {
  const next = new Date(`${isoDate}T00:00:00`);
  next.setDate(next.getDate() + 1);
  return next.toISOString().slice(0, 10);
}

function formatCandleLabel(date: Date, resolution: TimeResolution): string {
  if (resolution === "minute") {
    return date.toLocaleTimeString("ru-RU", {
      hour: "2-digit",
      minute: "2-digit"
    });
  }

  if (resolution === "hour") {
    return date.toLocaleDateString("ru-RU", {
      day: "2-digit",
      month: "2-digit"
    }) +
      " " +
      date.toLocaleTimeString("ru-RU", {
        hour: "2-digit",
        minute: "2-digit"
      });
  }

  return date.toLocaleDateString("ru-RU", {
    day: "2-digit",
    month: "2-digit"
  });
}

function tickerSeed(ticker: string): number {
  return ticker.split("").reduce((sum, char) => sum + char.charCodeAt(0), 0);
}

function roundForTicker(value: number, ticker: string): number {
  const precision = ticker === "VTBR" ? 5 : value < 1 ? 4 : 2;
  const factor = 10 ** precision;
  return Math.round(value * factor) / factor;
}

function buildDailyResolvedPoints(series: DailyMetricPoint[]): ResolvedTickerPoint[] {
  return series.map((point) => {
    const date = new Date(`${point.date}T00:00:00`);
    return {
      timestamp: date.toISOString(),
      label: formatDateLabel(point.date),
      close: point.close,
      volume: point.volume,
      volatility: point.volatility
    };
  });
}

function buildIntradayResolvedPoints(
  series: DailyMetricPoint[],
  ticker: string,
  resolution: Exclude<TimeResolution, "day">,
  days: number
): ResolvedTickerPoint[] {
  const candles = buildIntradayCandles(series, ticker, resolution, days);
  return candles.map((candle) => ({
    timestamp: candle.timestamp,
    label: candle.label,
    close: candle.close,
    volume: candle.volume,
    volatility: Math.abs(((candle.high - candle.low) / Math.max(candle.open, 0.000001)) * 100)
  }));
}

function createResolvedTickerSeriesMap(
  tickerMap: Record<string, DailyMetricPoint[]>,
  range: TimeRange,
  resolution: TimeResolution
): Record<string, ResolvedTickerPoint[]> {
  const days = rangeToDays(range);
  const resolved: Record<string, ResolvedTickerPoint[]> = {};

  Object.entries(tickerMap).forEach(([ticker, series]) => {
    if (resolution === "day") {
      resolved[ticker] = buildDailyResolvedPoints(series.slice(-days));
      return;
    }

    resolved[ticker] = buildIntradayResolvedPoints(
      series.slice(-Math.min(days, 30)),
      ticker,
      resolution,
      days
    );
  });

  return resolved;
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

function createPriceVolumeSeries(tickerMap: Record<string, ResolvedTickerPoint[]>): PriceVolumePoint[] {
  const firstTicker = Object.keys(tickerMap)[0];
  if (!firstTicker || tickerMap[firstTicker].length === 0) {
    return [];
  }

  const length = tickerMap[firstTicker].length;
  const tickers = Object.keys(tickerMap);
  const output: PriceVolumePoint[] = [];

  for (let index = 0; index < length; index += 1) {
    const row: PriceVolumePoint = {
      date: tickerMap[firstTicker][index].label,
      totalVolume: 0
    };

    tickers.forEach((ticker) => {
      const point = tickerMap[ticker][index];
      if (!point) {
        return;
      }
      row[ticker] = point.close;
      row.totalVolume += point.volume;
    });

    output.push(row);
  }

  return output;
}

function createVolatilitySeries(tickerMap: Record<string, ResolvedTickerPoint[]>): VolatilityPoint[] {
  const firstTicker = Object.keys(tickerMap)[0];
  if (!firstTicker || tickerMap[firstTicker].length === 0) {
    return [];
  }

  const length = tickerMap[firstTicker].length;
  const tickers = Object.keys(tickerMap);
  const output: VolatilityPoint[] = [];

  for (let index = 0; index < length; index += 1) {
    const row: VolatilityPoint = {
      date: tickerMap[firstTicker][index].label
    };

    tickers.forEach((ticker) => {
      const point = tickerMap[ticker][index];
      if (!point) {
        return;
      }
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

function buildDailyCandles(series: DailyMetricPoint[], ticker: string): CandlestickPoint[] {
  if (series.length === 0) {
    return [];
  }

  return series.map((point, index) => {
    const previousClose =
      index > 0
        ? series[index - 1].close
        : point.priceChangePct === -100
          ? point.close
          : point.close / (1 + point.priceChangePct / 100);

    const open = roundForTicker(previousClose, ticker);
    const close = roundForTicker(point.close, ticker);

    const body = Math.abs(close - open);
    const volatilitySpread = (point.volatility / 100) * point.close * 0.45;
    const wick = Math.max(body * 0.7, volatilitySpread, point.close * 0.0015);

    const high = roundForTicker(Math.max(open, close) + wick, ticker);
    const low = roundForTicker(Math.min(open, close) - wick, ticker);

    const date = new Date(`${point.date}T00:00:00`);

    return {
      timestamp: date.toISOString(),
      label: formatCandleLabel(date, "day"),
      open,
      high,
      low,
      close,
      volume: point.volume
    };
  });
}

function resolutionBarsCount(resolution: TimeResolution, days: number): number {
  if (resolution === "minute") {
    return Math.min(320, Math.max(70, days * 4));
  }
  if (resolution === "hour") {
    return Math.min(260, Math.max(50, days * 3));
  }
  return days;
}

function buildIntradayCandles(
  series: DailyMetricPoint[],
  ticker: string,
  resolution: Exclude<TimeResolution, "day">,
  days: number
): CandlestickPoint[] {
  const seed = tickerSeed(ticker);
  const bars = resolutionBarsCount(resolution, days);
  const stepMs = resolution === "hour" ? 60 * 60 * 1000 : 5 * 60 * 1000;

  const fallbackPrice = 100 + (seed % 50) * 1.2;
  const latestClose = series.length > 0 ? series[series.length - 1].close : fallbackPrice;
  const averageVolume =
    series.length > 0
      ? series.reduce((sum, point) => sum + point.volume, 0) / Math.max(series.length, 1)
      : 1_000_000;

  const barsPerDay = resolution === "hour" ? 8 : 40;
  const baseVolume = averageVolume / barsPerDay;
  const now = Date.now();
  const start = now - bars * stepMs;

  let currentPrice = latestClose * (1 + Math.sin(seed * 0.17) * 0.01);
  const candles: CandlestickPoint[] = [];

  for (let index = 0; index < bars; index += 1) {
    const timestampDate = new Date(start + index * stepMs);

    const drift = Math.sin((index + seed) / 11) * 0.0014 + Math.cos((index + seed) / 17) * 0.0009;
    const impulse =
      index % (resolution === "minute" ? 37 : 29) === 0
        ? index % 2 === 0
          ? 0.0045
          : -0.004
        : 0;

    const open = currentPrice;
    const close = open * (1 + drift + impulse);

    const wickMultiplier =
      resolution === "minute"
        ? 0.0014 + Math.abs(Math.sin((index + seed) / 6)) * 0.0017
        : 0.0019 + Math.abs(Math.cos((index + seed) / 7)) * 0.0022;

    const high = Math.max(open, close) * (1 + wickMultiplier * (1.15 + Math.abs(Math.sin(index * 0.28))));
    const low = Math.min(open, close) * (1 - wickMultiplier * (1.15 + Math.abs(Math.cos(index * 0.34))));

    const volume = Math.max(
      100,
      Math.round(baseVolume * (1 + Math.abs(Math.sin((index + seed) / 5)) * 0.95 + (impulse !== 0 ? 1.1 : 0)))
    );

    candles.push({
      timestamp: timestampDate.toISOString(),
      label: formatCandleLabel(timestampDate, resolution),
      open: roundForTicker(open, ticker),
      high: roundForTicker(high, ticker),
      low: roundForTicker(low, ticker),
      close: roundForTicker(close, ticker),
      volume
    });

    currentPrice = close;
  }

  return candles;
}

function createCandlestickSeries(
  tickerMap: Record<string, DailyMetricPoint[]>,
  filters: DashboardFilters
): { ticker: string; series: CandlestickPoint[] } {
  const fallbackTicker = Object.keys(tickerMap)[0] ?? MONITORED_TICKERS[0];
  const ticker = filters.candlestickTicker || filters.mainTickers[0] || fallbackTicker;
  const series = tickerMap[ticker] ?? [];
  const rangeDays = rangeToDays(filters.candlestickRange);

  if (filters.candlestickResolution === "day") {
    return {
      ticker,
      series: buildDailyCandles(series.slice(-rangeDays), ticker)
    };
  }

  return {
    ticker,
    series: buildIntradayCandles(
      series.slice(-Math.min(rangeDays, 30)),
      ticker,
      filters.candlestickResolution,
      rangeDays
    )
  };
}

function createLstmPredictionData(
  tickerMap: Record<string, DailyMetricPoint[]>,
  requestedTickers: string[]
): { historySeries: LstmHistoryPoint[]; forecast: LstmForecastPoint | null } {
  const ticker = requestedTickers[0];
  if (!ticker) {
    return { historySeries: [], forecast: null };
  }

  const series = tickerMap[ticker] ?? [];
  if (series.length === 0) {
    return { historySeries: [], forecast: null };
  }

  const source = series[series.length - 1];
  const previous = series.length > 1 ? series[series.length - 2] : source;
  const drift =
    previous.close > 0 ? (source.close - previous.close) / previous.close : source.priceChangePct / 100;

  const openRaw = source.close * (1 + drift * 0.25);
  const closeRaw = source.close * (1 + drift * 0.85);
  const wick = Math.max(
    Math.abs(closeRaw - openRaw) * 0.7,
    source.close * (source.volatility / 100) * 0.42,
    source.close * 0.002
  );

  const highRaw = Math.max(openRaw, closeRaw) + wick;
  const lowRaw = Math.max(0.00001, Math.min(openRaw, closeRaw) - wick);
  const predictedBucket = plusOneDayIso(source.date);

  const forecast: LstmForecastPoint = {
    ticker,
    sourceBucket: source.date,
    predictedBucket,
    predictedOpen: roundForTicker(openRaw, ticker),
    predictedHigh: roundForTicker(highRaw, ticker),
    predictedLow: roundForTicker(lowRaw, ticker),
    predictedClose: roundForTicker(closeRaw, ticker),
    modelVersion: "lstm_v1"
  };

  const historySeries: LstmHistoryPoint[] = series
    .slice(-Math.min(series.length, 20))
    .map((point) => ({
      date: formatDateLabel(point.date),
      close: point.close
    }));

  historySeries.push({
    date: formatDateLabel(predictedBucket),
    predictedClose: forecast.predictedClose
  });

  return { historySeries, forecast };
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

  const availableTickers = await fetchAvailableTickersFromBackend();
  const mainTickersFromFilters = filters.mainTickers.filter((ticker) =>
    availableTickers.includes(ticker)
  );
  const baseTickers =
    mainTickersFromFilters.length > 0
      ? mainTickersFromFilters
      : availableTickers.slice(0, 3);
  const candlestickTicker = availableTickers.includes(filters.candlestickTicker)
    ? filters.candlestickTicker
    : baseTickers[0] ?? availableTickers[0] ?? MONITORED_TICKERS[0];
  const requestedTickers = Array.from(
    new Set([
      ...baseTickers,
      candlestickTicker
    ])
  );
  const days = Math.max(rangeToDays(filters.mainRange), rangeToDays(filters.candlestickRange));
  const tickerMap: Record<string, DailyMetricPoint[]> = {};
  const mainTickerMap: Record<string, DailyMetricPoint[]> = {};

  requestedTickers.forEach((ticker) => {
    const fullSeries = getMockSeriesForTicker(ticker);
    tickerMap[ticker] = fullSeries.slice(-days);
  });

  baseTickers.forEach((ticker) => {
    const series = tickerMap[ticker] ?? [];
    mainTickerMap[ticker] = series.slice(-rangeToDays(filters.mainRange));
  });

  const resolvedTickerMap = createResolvedTickerSeriesMap(
    mainTickerMap,
    filters.mainRange,
    filters.mainResolution
  );
  const candlestickData = createCandlestickSeries(tickerMap, filters);
  const lstmPredictionData = createLstmPredictionData(tickerMap, requestedTickers);

  return {
    generatedAt: new Date().toISOString(),
    delayedByMinutes: MOEX_DELAY_MINUTES,
    availableTickers,
    kpis: createKpis(mainTickerMap),
    priceVolumeSeries: createPriceVolumeSeries(resolvedTickerMap),
    volatilitySeries: createVolatilitySeries(resolvedTickerMap),
    candlestickTicker: candlestickData.ticker,
    candlestickSeries: candlestickData.series,
    lstmHistorySeries: lstmPredictionData.historySeries,
    lstmPrediction: lstmPredictionData.forecast,
    leaders: createLeaders(mainTickerMap),
    anomalies: createAnomalies(mainTickerMap)
  };
}
