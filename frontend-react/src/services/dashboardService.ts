import { AlertPreferences, DashboardFilters, DashboardSnapshot } from "../types";

const ALERT_STORAGE_KEY = "neo_invest_alert_preferences";
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8001";

interface ApiErrorPayload {
  error?: string;
  detail?: string;
}

export function getStoredAlertPreferences(defaultEmail: string): AlertPreferences {
  const raw = localStorage.getItem(ALERT_STORAGE_KEY);
  if (!raw) {
    return {
      email: defaultEmail,
      priceAlerts: true,
      volumeAlerts: true,
      minSeverity: "medium"
    };
  }

  try {
    const parsed = JSON.parse(raw) as AlertPreferences;
    return {
      email: parsed.email || defaultEmail,
      priceAlerts: Boolean(parsed.priceAlerts),
      volumeAlerts: Boolean(parsed.volumeAlerts),
      minSeverity: parsed.minSeverity === "high" ? "high" : "medium"
    };
  } catch {
    return {
      email: defaultEmail,
      priceAlerts: true,
      volumeAlerts: true,
      minSeverity: "medium"
    };
  }
}

export function saveAlertPreferences(preferences: AlertPreferences): void {
  localStorage.setItem(ALERT_STORAGE_KEY, JSON.stringify(preferences));
}

export async function fetchDashboardSnapshot(filters: DashboardFilters): Promise<DashboardSnapshot> {
  let response: Response;

  try {
    response = await fetch(`${API_BASE_URL}/api/v1/dashboard/snapshot`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(filters)
    });
  } catch {
    throw new Error(
      "Backend дашборда недоступен. Убедитесь, что backend-api запущен и подключен к DWH."
    );
  }

  const data = (await response.json().catch(() => ({}))) as DashboardSnapshot | ApiErrorPayload;
  if (!response.ok) {
    const message =
      "error" in data && data.error
        ? data.error
        : "detail" in data && data.detail
          ? data.detail
          : "Не удалось загрузить данные дашборда.";
    throw new Error(message);
  }

  return data as DashboardSnapshot;
}

