import {
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from "recharts";
import { LstmForecastPoint, LstmHistoryPoint } from "../../types";

interface LstmPredictionChartProps {
  history: LstmHistoryPoint[];
  prediction: LstmForecastPoint | null;
}

function formatPrice(value: number): string {
  return new Intl.NumberFormat("ru-RU", {
    minimumFractionDigits: value < 1 ? 4 : 2,
    maximumFractionDigits: value < 1 ? 4 : 2
  }).format(value);
}

function LstmPredictionChart({ history, prediction }: LstmPredictionChartProps) {
  return (
    <section className="glass-panel chart-panel reveal" style={{ animationDelay: "300ms" }}>
      <div className="panel-header">
        <h2 className="panel-title">Прогноз LSTM (следующий день)</h2>
        {prediction ? (
          <span className="badge">
            {prediction.ticker} / {prediction.modelVersion}
          </span>
        ) : null}
      </div>

      {history.length > 0 ? (
        <>
          <div className="chart-shell">
            <ResponsiveContainer width="100%" height={320}>
              <ComposedChart data={history}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.12)" />
                <XAxis dataKey="date" tick={{ fill: "#d8e4f2", fontSize: 12 }} />
                <YAxis tick={{ fill: "#d8e4f2", fontSize: 12 }} width={74} />
                <Tooltip
                  contentStyle={{
                    backgroundColor: "#0b111d",
                    border: "1px solid rgba(164, 189, 214, 0.4)",
                    borderRadius: "12px"
                  }}
                  formatter={(value: number | string, name: string) => {
                    if (typeof value !== "number") {
                      return [value, name];
                    }
                    if (name === "Прогноз (Close)") {
                      return [formatPrice(value), name];
                    }
                    return [formatPrice(value), name];
                  }}
                />
                <Legend />
                <Line
                  type="monotone"
                  dataKey="close"
                  name="История (Close)"
                  stroke="#1f6feb"
                  strokeWidth={2.1}
                  dot={false}
                  activeDot={{ r: 4 }}
                />
                <Line
                  type="monotone"
                  dataKey="predictedClose"
                  name="Прогноз (Close)"
                  stroke="#ef8f20"
                  strokeWidth={2.2}
                  strokeDasharray="6 4"
                  connectNulls={false}
                  dot={{ r: 5, strokeWidth: 2, fill: "#ef8f20", stroke: "#ffd4a1" }}
                  activeDot={{ r: 6 }}
                />
              </ComposedChart>
            </ResponsiveContainer>
          </div>

          {prediction ? (
            <div className="prediction-summary">
              <span>Источник: {prediction.sourceBucket}</span>
              <span>Прогноз на: {prediction.predictedBucket}</span>
              <span>O: {formatPrice(prediction.predictedOpen)}</span>
              <span>H: {formatPrice(prediction.predictedHigh)}</span>
              <span>L: {formatPrice(prediction.predictedLow)}</span>
              <span>C: {formatPrice(prediction.predictedClose)}</span>
            </div>
          ) : (
            <p className="muted">Данные прогноза модели пока недоступны.</p>
          )}
        </>
      ) : (
        <p className="muted">Недостаточно данных для построения прогноза.</p>
      )}
    </section>
  );
}

export default LstmPredictionChart;

