import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import AlertSettings from "../components/dashboard/AlertSettings";
import AnomaliesTable from "../components/dashboard/AnomaliesTable";
import CandlestickChart from "../components/dashboard/CandlestickChart";
import FiltersPanel from "../components/dashboard/FiltersPanel";
import KpiCards from "../components/dashboard/KpiCards";
import LeadersTable from "../components/dashboard/LeadersTable";
import PriceAndVolumeChart from "../components/dashboard/PriceAndVolumeChart";
import VolatilityChart from "../components/dashboard/VolatilityChart";
import { logoutUser } from "../services/authService";
import {
  fetchDashboardSnapshot,
  getStoredAlertPreferences,
  saveAlertPreferences
} from "../services/dashboardService";
import { AlertPreferences, DashboardFilters, DashboardSnapshot } from "../types";

interface DashboardPageProps {
  userEmail: string;
}

function DashboardPage({ userEmail }: DashboardPageProps) {
  const navigate = useNavigate();

  const [filters, setFilters] = useState<DashboardFilters>({
    tickers: ["SBER", "GAZP", "LKOH"],
    range: "30d",
    resolution: "day",
    metricType: "price"
  });
  const [snapshot, setSnapshot] = useState<DashboardSnapshot | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [alertPreferences, setAlertPreferences] = useState<AlertPreferences>(
    getStoredAlertPreferences(userEmail)
  );

  useEffect(() => {
    let isCancelled = false;
    setIsLoading(true);

    fetchDashboardSnapshot(filters)
      .then((result) => {
        if (!isCancelled) {
          setSnapshot(result);
        }
      })
      .finally(() => {
        if (!isCancelled) {
          setIsLoading(false);
        }
      });

    return () => {
      isCancelled = true;
    };
  }, [filters]);

  const selectedTickers = useMemo(() => {
    if (!snapshot?.availableTickers) {
      return filters.tickers;
    }
    return filters.tickers.filter((ticker) => snapshot.availableTickers.includes(ticker));
  }, [filters.tickers, snapshot?.availableTickers]);

  const handleLogout = () => {
    logoutUser();
    navigate("/register", { replace: true });
  };

  const handleSaveAlerts = (value: AlertPreferences) => {
    setAlertPreferences(value);
    saveAlertPreferences(value);
  };

  const showPriceVolumeChart = filters.metricType === "price" || filters.metricType === "volume";
  const showVolatilityChart = filters.metricType === "volatility";

  return (
    <main className="page-shell dashboard-layout">
      <header className="dashboard-header reveal">
        <div>
          <p className="eyebrow">NeoInvest Центр Мониторинга</p>
          <h1 className="dashboard-title">Дашборд рынка MOEX</h1>
          <p className="muted">
            Задержка данных: {snapshot?.delayedByMinutes ?? 15} минут (лента MOEX ISS с задержкой)
          </p>
        </div>
        <div className="header-actions">
          <span className="badge">Пользователь: {userEmail}</span>
          <button type="button" className="ghost-button" onClick={handleLogout}>
            Выйти
          </button>
        </div>
      </header>

      {snapshot ? (
        <>
          <FiltersPanel availableTickers={snapshot.availableTickers} value={filters} onChange={setFilters} />
          <KpiCards cards={snapshot.kpis} />

          <CandlestickChart
            ticker={snapshot.candlestickTicker}
            resolution={filters.resolution}
            data={snapshot.candlestickSeries}
          />

          {showPriceVolumeChart ? (
            <PriceAndVolumeChart data={snapshot.priceVolumeSeries} tickers={selectedTickers} />
          ) : null}
          {showVolatilityChart ? (
            <VolatilityChart data={snapshot.volatilitySeries} tickers={selectedTickers} />
          ) : null}

          <LeadersTable data={snapshot.leaders} />
          <AnomaliesTable rows={snapshot.anomalies} />
          <AlertSettings initialValue={alertPreferences} onSave={handleSaveAlerts} />
        </>
      ) : (
        <section className="glass-panel">{isLoading ? "Загрузка дашборда..." : "Данные не найдены."}</section>
      )}
    </main>
  );
}

export default DashboardPage;
