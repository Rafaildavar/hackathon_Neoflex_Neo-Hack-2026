import { LeadersData } from "../../types";

interface LeadersTableProps {
  data: LeadersData;
}

function LeadersTable({ data }: LeadersTableProps) {
  return (
    <section className="glass-panel reveal" style={{ animationDelay: "330ms" }}>
      <div className="panel-header">
        <h2 className="panel-title">Лидеры движения</h2>
      </div>
      <div className="tables-dual">
        <div>
          <p className="table-title">Топ-5 роста</p>
          <table className="market-table">
            <thead>
              <tr>
                <th>Тикер</th>
                <th>Изменение</th>
                <th>Объем</th>
              </tr>
            </thead>
            <tbody>
              {data.gainers.map((row) => (
                <tr key={`gainer-${row.ticker}`}>
                  <td>{row.ticker}</td>
                  <td className="trend-up">+{row.changePct.toFixed(2)}%</td>
                  <td>{new Intl.NumberFormat("ru-RU").format(row.volume)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div>
          <p className="table-title">Топ-5 падения</p>
          <table className="market-table">
            <thead>
              <tr>
                <th>Тикер</th>
                <th>Изменение</th>
                <th>Объем</th>
              </tr>
            </thead>
            <tbody>
              {data.losers.map((row) => (
                <tr key={`loser-${row.ticker}`}>
                  <td>{row.ticker}</td>
                  <td className="trend-down">{row.changePct.toFixed(2)}%</td>
                  <td>{new Intl.NumberFormat("ru-RU").format(row.volume)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  );
}

export default LeadersTable;
