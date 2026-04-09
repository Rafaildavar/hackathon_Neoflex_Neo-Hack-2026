export type TimeRange = "7d" | "30d" | "90d";
export type MetricType = "price" | "volume" | "volatility";
export type TimeResolution = "minute" | "hour" | "day";
export type SeverityLevel = "medium" | "high";

export interface DailyMetricPoint {
  date: string;
  ticker: string;
  close: number;
  volume: number;
  volatility: number;
  priceChangePct: number;
}

export interface DashboardFilters {
  mainTickers: string[];
  mainRange: TimeRange;
  mainResolution: TimeResolution;
  mainMetricType: MetricType;
  candlestickTicker: string;
  candlestickRange: TimeRange;
  candlestickResolution: TimeResolution;
}

export interface KpiCardData {
  title: string;
  value: string;
  delta: string;
  trend: "up" | "down" | "neutral";
}

export interface LeaderRow {
  ticker: string;
  changePct: number;
  volume: number;
}

export interface LeadersData {
  gainers: LeaderRow[];
  losers: LeaderRow[];
}

export interface AnomalyRow {
  id: string;
  eventTs: string;
  ticker: string;
  anomalyType: "volume_spike" | "price_jump";
  metricValue: number;
  threshold: number;
  severity: SeverityLevel;
}

export interface PriceVolumePoint {
  date: string;
  totalVolume: number;
  [key: string]: string | number;
}

export interface VolatilityPoint {
  date: string;
  [key: string]: string | number;
}

export interface DashboardSnapshot {
  generatedAt: string;
  delayedByMinutes: number;
  availableTickers: string[];
  kpis: KpiCardData[];
  priceVolumeSeries: PriceVolumePoint[];
  volatilitySeries: VolatilityPoint[];
  candlestickTicker: string;
  candlestickSeries: CandlestickPoint[];
  leaders: LeadersData;
  anomalies: AnomalyRow[];
}

export interface CandlestickPoint {
  timestamp: string;
  label: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface AlertPreferences {
  email: string;
  priceAlerts: boolean;
  volumeAlerts: boolean;
  minSeverity: SeverityLevel;
}

export interface RegisteredUser {
  email: string;
  loggedInAt: string;
}
