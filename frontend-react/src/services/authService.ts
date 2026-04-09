import { RegisteredUser } from "../types";

const USER_STORAGE_KEY = "neo_invest_registered_user";
const AUTH_API_BASE_URL = import.meta.env.VITE_AUTH_API_BASE_URL ?? "http://localhost:8001";

async function callAuthApi(
  path: string,
  email: string,
  password: string
): Promise<{ email: string }> {
  const response = await fetch(`${AUTH_API_BASE_URL}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      email: email.trim().toLowerCase(),
      password
    })
  });

  const data = (await response.json().catch(() => ({}))) as { email?: string; error?: string };
  if (!response.ok) {
    throw new Error(data.error ?? "Ошибка авторизации.");
  }
  if (!data.email) {
    throw new Error("Пустой ответ сервера авторизации.");
  }
  return { email: data.email };
}

function saveSession(email: string): RegisteredUser {
  const payload: RegisteredUser = {
    email: email.trim().toLowerCase(),
    loggedInAt: new Date().toISOString()
  };
  localStorage.setItem(USER_STORAGE_KEY, JSON.stringify(payload));
  return payload;
}

export async function registerUser(email: string, password: string): Promise<RegisteredUser> {
  const result = await callAuthApi("/auth/register", email, password);
  return saveSession(result.email);
}

export async function loginUser(email: string, password: string): Promise<RegisteredUser> {
  const result = await callAuthApi("/auth/login", email, password);
  return saveSession(result.email);
}

export function getCurrentUser(): RegisteredUser | null {
  const raw = localStorage.getItem(USER_STORAGE_KEY);
  if (!raw) {
    return null;
  }

  try {
    const parsed = JSON.parse(raw) as RegisteredUser;
    if (!parsed.email || !parsed.loggedInAt) {
      return null;
    }
    return parsed;
  } catch {
    return null;
  }
}

export function logoutUser(): void {
  localStorage.removeItem(USER_STORAGE_KEY);
}
