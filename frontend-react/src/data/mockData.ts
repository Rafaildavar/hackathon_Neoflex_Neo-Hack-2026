import { DailyMetricPoint } from "../types";

const TICKERS = ["SBER", "GAZP", "LKOH", "YDEX", "VTBR", "ROSN", "NVTK"] as const;

const BASE_PRICE_BY_TICKER: Record<(typeof TICKERS)[number], number> = {
  SBER: 282,
  GAZP: 169,
  LKOH: 7340,
  YDEX: 3890,
  VTBR: 0.024,
  ROSN: 578,
  NVTK: 1112
};

const BASE_VOLUME_BY_TICKER: Record<(typeof TICKERS)[number], number> = {
  SBER: 18500000,
  GAZP: 15100000,
  LKOH: 4200000,
  YDEX: 1900000,
  VTBR: 82000000,
  ROSN: 5100000,
  NVTK: 2800000
};

function toIsoDate(date: Date): string {
  return date.toISOString().slice(0, 10);
}

function round(value: number, precision = 2): number {
  const factor = 10 ** precision;
  return Math.round(value * factor) / factor;
}

function tickerSeed(ticker: string): number {
  return ticker.split("").reduce((sum, char) => sum + char.charCodeAt(0), 0);
}

function buildTickerSeries(ticker: (typeof TICKERS)[number], days = 90): DailyMetricPoint[] {
  const seed = tickerSeed(ticker);
  const basePrice = BASE_PRICE_BY_TICKER[ticker];
  const baseVolume = BASE_VOLUME_BY_TICKER[ticker];
  const output: DailyMetricPoint[] = [];
  let previousClose = basePrice * (1 + Math.sin(seed) * 0.015);

  for (let offset = days - 1; offset >= 0; offset -= 1) {
    const dayIndex = days - offset;
    const date = new Date();
    date.setHours(0, 0, 0, 0);
    date.setDate(date.getDate() - offset);

    const trend = 1 + (dayIndex / days) * ((seed % 9) / 100 - 0.045);
    const seasonal = Math.sin(dayIndex / 5 + seed * 0.03) * 0.018;
    const pulse = Math.cos(dayIndex * 0.7 + seed * 0.05) * 0.009;
    let close = basePrice * trend * (1 + seasonal + pulse);

    if (dayIndex % (17 + (seed % 4)) === 0) {
      close *= dayIndex % 2 === 0 ? 1.028 : 0.971;
    }

    const priceChangePct = ((close - previousClose) / previousClose) * 100;
    const volatility = 1 + Math.abs(seasonal * 100) + Math.abs(pulse * 110);

    let volume =
      baseVolume *
      (1 + Math.abs(Math.sin(dayIndex / 4 + seed * 0.08)) * 0.5 + Math.abs(pulse) * 2.5);

    if (dayIndex % (19 + (seed % 3)) === 0) {
      volume *= 3.7;
    }

    output.push({
      date: toIsoDate(date),
      ticker,
      close: round(close, ticker === "VTBR" ? 5 : 2),
      volume: Math.round(volume),
      volatility: round(volatility, 2),
      priceChangePct: round(priceChangePct, 2)
    });

    previousClose = close;
  }

  return output;
}

export const MONITORED_TICKERS = [...TICKERS];

export const MOCK_SERIES: Record<string, DailyMetricPoint[]> = TICKERS.reduce(
  (accumulator, ticker) => {
    accumulator[ticker] = buildTickerSeries(ticker);
    return accumulator;
  },
  {} as Record<string, DailyMetricPoint[]>
);
