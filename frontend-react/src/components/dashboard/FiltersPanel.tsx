import { DashboardFilters, MetricType, TimeRange, TimeResolution } from "../../types";

interface FiltersPanelProps {
  availableTickers: string[];
  value: DashboardFilters;
  onChange: (next: DashboardFilters) => void;
}

const RANGE_OPTIONS: Array<{ label: string; value: TimeRange }> = [
  { label: "7 дней", value: "7d" },
  { label: "30 дней", value: "30d" },
  { label: "90 дней", value: "90d" }
];

const METRIC_OPTIONS: Array<{ label: string; value: MetricType }> = [
  { label: "Цена", value: "price" },
  { label: "Объем", value: "volume" },
  { label: "Волатильность", value: "volatility" }
];

const RESOLUTION_OPTIONS: Array<{ label: string; value: TimeResolution }> = [
  { label: "Минуты", value: "minute" },
  { label: "Часы", value: "hour" },
  { label: "Дни", value: "day" }
];

function FiltersPanel({ availableTickers, value, onChange }: FiltersPanelProps) {
  const toggleMainTicker = (ticker: string) => {
    const exists = value.mainTickers.includes(ticker);
    const nextTickers = exists
      ? value.mainTickers.filter((item) => item !== ticker)
      : [...value.mainTickers, ticker];

    onChange({
      ...value,
      mainTickers: nextTickers.length > 0 ? nextTickers : [ticker]
    });
  };

  const defaultMainTickers = availableTickers.slice(0, 3);

  return (
    <section className="glass-panel reveal" style={{ animationDelay: "90ms" }}>
      <div className="panel-header">
        <h2 className="panel-title">Настройки графиков</h2>
        <button
          type="button"
          className="ghost-button"
          onClick={() =>
            onChange({
              mainTickers: defaultMainTickers,
              mainRange: "30d",
              mainResolution: "day",
              mainMetricType: "price",
              candlestickTicker: availableTickers[0] ?? "SBER",
              candlestickRange: "30d",
              candlestickResolution: "day"
            })
          }
        >
          Сбросить
        </button>
      </div>

      <div className="filters-grid">
        <div className="filters-group">
          <p className="filters-group-title">Основной график</p>

          <p className="filters-label">Отслеживаемые тикеры</p>
          <div className="chips-wrap">
            {availableTickers.map((ticker) => {
              const selected = value.mainTickers.includes(ticker);
              return (
                <button
                  key={ticker}
                  type="button"
                  className={`chip ${selected ? "selected" : ""}`}
                  onClick={() => toggleMainTicker(ticker)}
                >
                  {ticker}
                </button>
              );
            })}
          </div>

          <p className="filters-label">Тип метрики</p>
          <div className="segmented">
            {METRIC_OPTIONS.map((option) => (
              <button
                key={option.value}
                type="button"
                className={`segment ${value.mainMetricType === option.value ? "selected" : ""}`}
                onClick={() => onChange({ ...value, mainMetricType: option.value })}
              >
                {option.label}
              </button>
            ))}
          </div>

          <p className="filters-label">Период</p>
          <div className="segmented">
            {RANGE_OPTIONS.map((option) => (
              <button
                key={option.value}
                type="button"
                className={`segment ${value.mainRange === option.value ? "selected" : ""}`}
                onClick={() => onChange({ ...value, mainRange: option.value })}
              >
                {option.label}
              </button>
            ))}
          </div>

          <p className="filters-label">Детализация</p>
          <div className="segmented">
            {RESOLUTION_OPTIONS.map((option) => (
              <button
                key={option.value}
                type="button"
                className={`segment ${value.mainResolution === option.value ? "selected" : ""}`}
                onClick={() => onChange({ ...value, mainResolution: option.value })}
              >
                {option.label}
              </button>
            ))}
          </div>
        </div>

        <div className="filters-group">
          <p className="filters-group-title">Свечной график</p>

          <p className="filters-label">Тикер</p>
          <select
            className="field-input"
            value={value.candlestickTicker}
            onChange={(event) => onChange({ ...value, candlestickTicker: event.target.value })}
          >
            {availableTickers.map((ticker) => (
              <option key={ticker} value={ticker}>
                {ticker}
              </option>
            ))}
          </select>

          <p className="filters-label">Период</p>
          <div className="segmented">
            {RANGE_OPTIONS.map((option) => (
              <button
                key={option.value}
                type="button"
                className={`segment ${value.candlestickRange === option.value ? "selected" : ""}`}
                onClick={() => onChange({ ...value, candlestickRange: option.value })}
              >
                {option.label}
              </button>
            ))}
          </div>

          <p className="filters-label">Детализация</p>
          <div className="segmented">
            {RESOLUTION_OPTIONS.map((option) => (
              <button
                key={option.value}
                type="button"
                className={`segment ${value.candlestickResolution === option.value ? "selected" : ""}`}
                onClick={() => onChange({ ...value, candlestickResolution: option.value })}
              >
                {option.label}
              </button>
            ))}
          </div>
        </div>
      </div>
    </section>
  );
}

export default FiltersPanel;
