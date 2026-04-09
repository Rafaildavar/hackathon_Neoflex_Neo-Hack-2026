import {
  Bar,
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from "recharts";
import { PriceVolumePoint } from "../../types";

interface PriceAndVolumeChartProps {
  data: PriceVolumePoint[];
  tickers: string[];
}

const LINE_COLORS = ["#1f6feb", "#00a878", "#ef8f20", "#d64550", "#0085a1", "#6a5acd", "#5c940d"];

function PriceAndVolumeChart({ data, tickers }: PriceAndVolumeChartProps) {
  return (
    <section className="glass-panel chart-panel reveal" style={{ animationDelay: "210ms" }}>
      <div className="panel-header">
        <h2 className="panel-title">Динамика цен и рыночного объема</h2>
      </div>
      <div className="chart-shell">
        <ResponsiveContainer width="100%" height={320}>
          <ComposedChart data={data}>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.12)" />
            <XAxis dataKey="date" tick={{ fill: "#d8e4f2", fontSize: 12 }} />
            <YAxis
              yAxisId="price"
              tick={{ fill: "#d8e4f2", fontSize: 12 }}
              orientation="left"
              width={74}
            />
            <YAxis
              yAxisId="volume"
              tick={{ fill: "#9cb3c8", fontSize: 12 }}
              orientation="right"
              width={84}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: "#0b111d",
                border: "1px solid rgba(164, 189, 214, 0.4)",
                borderRadius: "12px"
              }}
            />
            <Legend />
            <Bar
              yAxisId="volume"
              dataKey="totalVolume"
              name="Суммарный объем"
              fill="rgba(31, 111, 235, 0.2)"
              radius={[6, 6, 0, 0]}
            />
            {tickers.map((ticker, index) => (
              <Line
                key={ticker}
                yAxisId="price"
                type="monotone"
                dataKey={ticker}
                stroke={LINE_COLORS[index % LINE_COLORS.length]}
                strokeWidth={2.25}
                dot={false}
                activeDot={{ r: 4 }}
              />
            ))}
          </ComposedChart>
        </ResponsiveContainer>
      </div>
    </section>
  );
}

export default PriceAndVolumeChart;
