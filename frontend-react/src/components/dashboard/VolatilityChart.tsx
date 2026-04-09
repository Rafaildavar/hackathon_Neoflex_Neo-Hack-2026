import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from "recharts";
import { VolatilityPoint } from "../../types";

interface VolatilityChartProps {
  data: VolatilityPoint[];
  tickers: string[];
}

const LINE_COLORS = ["#00a878", "#ef8f20", "#d64550", "#1f6feb", "#0085a1", "#6a5acd", "#5c940d"];

function VolatilityChart({ data, tickers }: VolatilityChartProps) {
  return (
    <section className="glass-panel chart-panel reveal" style={{ animationDelay: "270ms" }}>
      <div className="panel-header">
        <h2 className="panel-title">Мониторинг волатильности</h2>
      </div>
      <div className="chart-shell">
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={data}>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.12)" />
            <XAxis dataKey="date" tick={{ fill: "#d8e4f2", fontSize: 12 }} />
            <YAxis tick={{ fill: "#d8e4f2", fontSize: 12 }} width={74} />
            <Tooltip
              contentStyle={{
                backgroundColor: "#0b111d",
                border: "1px solid rgba(164, 189, 214, 0.4)",
                borderRadius: "12px"
              }}
            />
            <Legend />
            {tickers.map((ticker, index) => (
              <Line
                key={ticker}
                type="monotone"
                dataKey={ticker}
                stroke={LINE_COLORS[index % LINE_COLORS.length]}
                strokeWidth={2}
                dot={false}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </section>
  );
}

export default VolatilityChart;
