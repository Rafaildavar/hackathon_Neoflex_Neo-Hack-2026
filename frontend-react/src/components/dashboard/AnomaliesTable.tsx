import { AnomalyRow } from "../../types";

interface AnomaliesTableProps {
  rows: AnomalyRow[];
}

function anomalyLabel(type: AnomalyRow["anomalyType"]): string {
  if (type === "volume_spike") {
    return "Объем > 3 сигм";
  }
  return "Изменение цены > 2%";
}

function severityLabel(severity: AnomalyRow["severity"]): string {
  return severity === "high" ? "высокий" : "средний";
}

function AnomaliesTable({ rows }: AnomaliesTableProps) {
  return (
    <section className="glass-panel reveal" style={{ animationDelay: "390ms" }}>
      <div className="panel-header">
        <h2 className="panel-title">Последние аномалии</h2>
        <span className="badge">{rows.length} событий</span>
      </div>
      <div className="table-scroll">
        <table className="market-table">
          <thead>
            <tr>
              <th>Дата</th>
              <th>Тикер</th>
              <th>Тип</th>
              <th>Значение</th>
              <th>Порог</th>
              <th>Критичность</th>
            </tr>
          </thead>
          <tbody>
            {rows.slice(0, 12).map((row) => (
              <tr key={row.id}>
                <td>{row.eventTs}</td>
                <td>{row.ticker}</td>
                <td>{anomalyLabel(row.anomalyType)}</td>
                <td>
                  {row.anomalyType === "price_jump"
                    ? `${row.metricValue > 0 ? "+" : ""}${row.metricValue.toFixed(2)}%`
                    : new Intl.NumberFormat("ru-RU").format(row.metricValue)}
                </td>
                <td>
                  {row.anomalyType === "price_jump"
                    ? `${row.threshold.toFixed(2)}%`
                    : new Intl.NumberFormat("ru-RU").format(row.threshold)}
                </td>
                <td>
                  <span className={`severity-badge ${row.severity}`}>{severityLabel(row.severity)}</span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

export default AnomaliesTable;
