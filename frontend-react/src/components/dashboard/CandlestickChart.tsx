import { useMemo } from "react";
import { CandlestickPoint, TimeResolution } from "../../types";

interface CandlestickChartProps {
  ticker: string;
  resolution: TimeResolution;
  data: CandlestickPoint[];
}

function resolutionLabel(resolution: TimeResolution): string {
  if (resolution === "minute") {
    return "Минуты";
  }
  if (resolution === "hour") {
    return "Часы";
  }
  return "Дни";
}

function formatPrice(value: number): string {
  return new Intl.NumberFormat("ru-RU", {
    minimumFractionDigits: value < 1 ? 4 : 2,
    maximumFractionDigits: value < 1 ? 4 : 2
  }).format(value);
}

function CandlestickChart({ ticker, resolution, data }: CandlestickChartProps) {
  const prepared = useMemo(() => {
    if (data.length === 0) {
      return null;
    }

    const svgHeight = 320;
    const paddingLeft = 54;
    const paddingRight = 16;
    const paddingTop = 18;
    const paddingBottom = 40;

    const svgWidth = Math.max(760, data.length * 11 + paddingLeft + paddingRight);
    const chartHeight = svgHeight - paddingTop - paddingBottom;
    const plotWidth = svgWidth - paddingLeft - paddingRight;

    const maxHigh = Math.max(...data.map((point) => point.high));
    const minLow = Math.min(...data.map((point) => point.low));
    const range = maxHigh - minLow || 1;
    const step = plotWidth / Math.max(data.length, 1);
    const candleBodyWidth = Math.max(3, Math.min(9, step * 0.62));

    const priceToY = (price: number) =>
      paddingTop + ((maxHigh - price) / range) * chartHeight;

    const xToIndex = (index: number) => paddingLeft + index * step + step / 2;
    const yTicks = [0, 0.25, 0.5, 0.75, 1].map((part) => maxHigh - range * part);

    const labelStep = Math.max(1, Math.ceil(data.length / 9));
    const xLabels = data
      .map((point, index) => ({ point, index }))
      .filter((item, idx) => idx % labelStep === 0 || idx === data.length - 1);

    return {
      svgHeight,
      svgWidth,
      paddingLeft,
      paddingRight,
      paddingBottom,
      step,
      candleBodyWidth,
      yTicks,
      xLabels,
      priceToY,
      xToIndex
    };
  }, [data]);

  const last = data[data.length - 1];

  return (
    <section className="glass-panel chart-panel reveal" style={{ animationDelay: "240ms" }}>
      <div className="panel-header">
        <h2 className="panel-title">Свечной график</h2>
        <span className="badge">
          {ticker} - {resolutionLabel(resolution)}
        </span>
      </div>

      {prepared ? (
        <>
          <div className="candlestick-scroll">
            <svg
              width={prepared.svgWidth}
              height={prepared.svgHeight}
              role="img"
              aria-label={`Свечной график ${ticker}`}
            >
              {prepared.yTicks.map((value) => {
                const y = prepared.priceToY(value);
                return (
                  <g key={`y-${value.toFixed(6)}`}>
                    <line
                      x1={prepared.paddingLeft}
                      y1={y}
                      x2={prepared.svgWidth - prepared.paddingRight}
                      y2={y}
                      className="candle-grid-line"
                    />
                    <text x={8} y={y + 4} className="candle-axis-text">
                      {formatPrice(value)}
                    </text>
                  </g>
                );
              })}

              {data.map((point, index) => {
                const x = prepared.xToIndex(index);
                const openY = prepared.priceToY(point.open);
                const closeY = prepared.priceToY(point.close);
                const highY = prepared.priceToY(point.high);
                const lowY = prepared.priceToY(point.low);
                const bodyTop = Math.min(openY, closeY);
                const bodyHeight = Math.max(1.6, Math.abs(closeY - openY));
                const isUp = point.close >= point.open;

                return (
                  <g key={`${point.timestamp}-${index}`}>
                    <title>
                      {`${point.label} | O:${formatPrice(point.open)} H:${formatPrice(point.high)} L:${formatPrice(
                        point.low
                      )} C:${formatPrice(point.close)}`}
                    </title>
                    <line
                      x1={x}
                      y1={highY}
                      x2={x}
                      y2={lowY}
                      className={isUp ? "candle-wick-up" : "candle-wick-down"}
                    />
                    <rect
                      x={x - prepared.candleBodyWidth / 2}
                      y={bodyTop}
                      width={prepared.candleBodyWidth}
                      height={bodyHeight}
                      rx={1.5}
                      className={isUp ? "candle-body-up" : "candle-body-down"}
                    />
                  </g>
                );
              })}

              {prepared.xLabels.map(({ point, index }) => (
                <text
                  key={`${point.timestamp}-label`}
                  x={prepared.xToIndex(index)}
                  y={prepared.svgHeight - 10}
                  textAnchor="middle"
                  className="candle-axis-text"
                >
                  {point.label}
                </text>
              ))}
            </svg>
          </div>

          {last ? (
            <div className="candle-summary">
              <span>O: {formatPrice(last.open)}</span>
              <span>H: {formatPrice(last.high)}</span>
              <span>L: {formatPrice(last.low)}</span>
              <span>C: {formatPrice(last.close)}</span>
            </div>
          ) : null}
        </>
      ) : (
        <p className="muted">Нет данных для построения свечей.</p>
      )}
    </section>
  );
}

export default CandlestickChart;
