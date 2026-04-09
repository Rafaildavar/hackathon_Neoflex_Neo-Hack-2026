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
  const toggleTicker = (ticker: string) => {
    const exists = value.tickers.includes(ticker);
    const nextTickers = exists
      ? value.tickers.filter((item) => item !== ticker)
      : [...value.tickers, ticker];

    onChange({
      ...value,
      tickers: nextTickers.length > 0 ? nextTickers : [ticker]
    });
  };

  return (
    <section className="glass-panel reveal" style={{ animationDelay: "90ms" }}>
      <div className="panel-header">
        <h2 className="panel-title">Фильтры</h2>
        <button
          type="button"
          className="ghost-button"
          onClick={() =>
            onChange({
              tickers: availableTickers.slice(0, 3),
              range: "30d",
              resolution: "day",
              metricType: "price"
            })
          }
        >
          Сбросить
        </button>
      </div>

      <div className="filters-grid">
        <div>
          <p className="filters-label">Отслеживаемые тикеры</p>
          <div className="chips-wrap">
            {availableTickers.map((ticker) => {
              const selected = value.tickers.includes(ticker);
              return (
                <button
                  key={ticker}
                  type="button"
                  className={`chip ${selected ? "selected" : ""}`}
                  onClick={() => toggleTicker(ticker)}
                >
                  {ticker}
                </button>
              );
            })}
          </div>
        </div>

        <div>
          <p className="filters-label">Период</p>
          <div className="segmented">
            {RANGE_OPTIONS.map((option) => (
              <button
                key={option.value}
                type="button"
                className={`segment ${value.range === option.value ? "selected" : ""}`}
                onClick={() => onChange({ ...value, range: option.value })}
              >
                {option.label}
              </button>
            ))}
          </div>
        </div>

        <div>
          <p className="filters-label">Тип метрики</p>
          <div className="segmented">
            {METRIC_OPTIONS.map((option) => (
              <button
                key={option.value}
                type="button"
                className={`segment ${value.metricType === option.value ? "selected" : ""}`}
                onClick={() => onChange({ ...value, metricType: option.value })}
              >
                {option.label}
              </button>
            ))}
          </div>
        </div>

        <div>
          <p className="filters-label">Детализация</p>
          <div className="segmented">
            {RESOLUTION_OPTIONS.map((option) => (
              <button
                key={option.value}
                type="button"
                className={`segment ${value.resolution === option.value ? "selected" : ""}`}
                onClick={() => onChange({ ...value, resolution: option.value })}
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
