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
    mainTickers: ["SBER", "GAZP", "LKOH"],
    mainRange: "30d",
    mainResolution: "day",
    mainMetricType: "price",
    candlestickTicker: "SBER",
    candlestickRange: "30d",
    candlestickResolution: "day"
  });
  const [snapshot, setSnapshot] = useState<DashboardSnapshot | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [alertPreferences, setAlertPreferences] = useState<AlertPreferences>(
    getStoredAlertPreferences(userEmail)
  );

  useEffect(() => {
    setFilters((previous) => {
      if (snapshot?.availableTickers && !snapshot.availableTickers.includes(previous.candlestickTicker)) {
        return { ...previous, candlestickTicker: snapshot.availableTickers[0] ?? "SBER" };
      }
      return previous;
    });
  }, [snapshot?.availableTickers]);

  useEffect(() => {
    let isCancelled = false;
    setIsLoading(true);
    setLoadError("");

    fetchDashboardSnapshot(filters)
      .then((result) => {
        if (!isCancelled) {
          setSnapshot(result);
        }
      })
      .catch((error) => {
        if (!isCancelled) {
          setSnapshot(null);
          setLoadError(error instanceof Error ? error.message : "Ошибка загрузки дашборда.");
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
      return filters.mainTickers;
    }
    return filters.mainTickers.filter((ticker) => snapshot.availableTickers.includes(ticker));
  }, [filters.mainTickers, snapshot?.availableTickers]);

  const handleLogout = () => {
    logoutUser();
    navigate("/register", { replace: true });
  };

  const handleSaveAlerts = (value: AlertPreferences) => {
    setAlertPreferences(value);
    saveAlertPreferences(value);
  };

  const showPriceVolumeChart =
    filters.mainMetricType === "price" || filters.mainMetricType === "volume";
  const showVolatilityChart = filters.mainMetricType === "volatility";

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
        <div className="dashboard-content">
          <aside className="dashboard-sidebar">
            <FiltersPanel availableTickers={snapshot.availableTickers} value={filters} onChange={setFilters} />
            <AlertSettings initialValue={alertPreferences} onSave={handleSaveAlerts} />
          </aside>

          <section className="dashboard-main">
            <KpiCards cards={snapshot.kpis} />

            {showPriceVolumeChart ? (
              <PriceAndVolumeChart data={snapshot.priceVolumeSeries} tickers={selectedTickers} />
            ) : null}
            {showVolatilityChart ? (
              <VolatilityChart data={snapshot.volatilitySeries} tickers={selectedTickers} />
            ) : null}

            <CandlestickChart
              ticker={snapshot.candlestickTicker}
              resolution={filters.candlestickResolution}
              data={snapshot.candlestickSeries}
            />

            <LeadersTable data={snapshot.leaders} />
            <AnomaliesTable rows={snapshot.anomalies} />
          </section>
        </div>
      ) : (
        <section className="glass-panel">
          {isLoading ? "Загрузка дашборда..." : loadError || "Данные не найдены."}
        </section>
      )}
    </main>
  );
}

export default DashboardPage;
