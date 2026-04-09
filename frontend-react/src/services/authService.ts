import { RegisteredUser } from "../types";

interface LocalAuthUser {
  email: string;
  password: string;
  registeredAt: string;
}

const DEFAULT_TEST_USER: LocalAuthUser = {
  email: "admin",
  password: "admin",
  registeredAt: "2026-01-01T00:00:00.000Z"
};

const SESSION_STORAGE_KEY = "neo_invest_session_user";
const LEGACY_SESSION_STORAGE_KEY = "neo_invest_registered_user";
const LOCAL_USERS_STORAGE_KEY = "neo_invest_local_users";
const AUTH_API_BASE_URL =
  import.meta.env.VITE_API_BASE_URL ??
  import.meta.env.VITE_AUTH_API_BASE_URL ??
  "http://localhost:8001";
const AUTH_STATE_EVENT = "neo_invest_auth_state_changed";

class AuthApiUnavailableError extends Error {
  constructor(message = "Сервис авторизации недоступен.") {
    super(message);
    this.name = "AuthApiUnavailableError";
  }
}

function normalizeEmail(email: string): string {
  return email.trim().toLowerCase();
}

function withDefaultTestUser(users: LocalAuthUser[]): LocalAuthUser[] {
  const hasAdmin = users.some((user) => user.email === DEFAULT_TEST_USER.email);
  return hasAdmin ? users : [...users, DEFAULT_TEST_USER];
}

function readLocalUsers(): LocalAuthUser[] {
  const raw = localStorage.getItem(LOCAL_USERS_STORAGE_KEY);
  if (!raw) {
    return [DEFAULT_TEST_USER];
  }

  try {
    const parsed = JSON.parse(raw) as LocalAuthUser[];
    const users = Array.isArray(parsed)
      ? parsed.filter((user) => Boolean(user?.email && user?.password))
      : [];
    return withDefaultTestUser(users);
  } catch {
    return [DEFAULT_TEST_USER];
  }
}

function writeLocalUsers(users: LocalAuthUser[]): void {
  localStorage.setItem(LOCAL_USERS_STORAGE_KEY, JSON.stringify(users));
}

function emitAuthStateChanged(): void {
  window.dispatchEvent(new Event(AUTH_STATE_EVENT));
}

function saveSession(email: string): RegisteredUser {
  const payload: RegisteredUser = {
    email: normalizeEmail(email),
    loggedInAt: new Date().toISOString()
  };
  localStorage.setItem(SESSION_STORAGE_KEY, JSON.stringify(payload));
  emitAuthStateChanged();
  return payload;
}

async function callAuthApi(
  path: string,
  email: string,
  password: string
): Promise<{ email: string }> {
  let response: Response;

  try {
    response = await fetch(`${AUTH_API_BASE_URL}${path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        email: normalizeEmail(email),
        password
      })
    });
  } catch {
    throw new AuthApiUnavailableError();
  }

  if (response.status === 404 || response.status === 405) {
    throw new AuthApiUnavailableError();
  }

  const data = (await response.json().catch(() => ({}))) as {
    email?: string;
    error?: string;
    detail?: string;
  };
  if (!response.ok) {
    throw new Error(data.error ?? data.detail ?? "Ошибка авторизации.");
  }

  if (!data.email) {
    throw new Error("Пустой ответ сервера авторизации.");
  }

  return { email: normalizeEmail(data.email) };
}

function registerLocalUser(email: string, password: string): RegisteredUser {
  const normalizedEmail = normalizeEmail(email);
  const users = readLocalUsers();

  const userExists = users.some((user) => user.email === normalizedEmail);
  if (userExists) {
    throw new Error("Пользователь с таким email уже зарегистрирован.");
  }

  users.push({
    email: normalizedEmail,
    password,
    registeredAt: new Date().toISOString()
  });
  writeLocalUsers(users);

  return saveSession(normalizedEmail);
}

function loginLocalUser(email: string, password: string): RegisteredUser {
  const normalizedEmail = normalizeEmail(email);
  const users = readLocalUsers();

  const user = users.find((item) => item.email === normalizedEmail);
  if (!user) {
    throw new Error("Пользователь не найден. Сначала зарегистрируйтесь.");
  }

  if (user.password !== password) {
    throw new Error("Неверный пароль.");
  }

  return saveSession(normalizedEmail);
}

export async function registerUser(email: string, password: string): Promise<RegisteredUser> {
  try {
    const result = await callAuthApi("/auth/register", email, password);
    return saveSession(result.email);
  } catch (error) {
    if (error instanceof AuthApiUnavailableError) {
      return registerLocalUser(email, password);
    }
    throw error;
  }
}

export async function loginUser(email: string, password: string): Promise<RegisteredUser> {
  const normalizedEmail = normalizeEmail(email);
  if (normalizedEmail === DEFAULT_TEST_USER.email && password === DEFAULT_TEST_USER.password) {
    return saveSession(DEFAULT_TEST_USER.email);
  }

  try {
    const result = await callAuthApi("/auth/login", email, password);
    return saveSession(result.email);
  } catch (error) {
    if (error instanceof AuthApiUnavailableError) {
      return loginLocalUser(email, password);
    }
    throw error;
  }
}

export function getCurrentUser(): RegisteredUser | null {
  const raw = localStorage.getItem(SESSION_STORAGE_KEY);
  const legacyRaw = localStorage.getItem(LEGACY_SESSION_STORAGE_KEY);
  const source = raw ?? legacyRaw;
  if (!source) {
    return null;
  }

  try {
    const parsed = JSON.parse(source) as Partial<RegisteredUser> & { registeredAt?: string };
    const loggedInAt = parsed.loggedInAt ?? parsed.registeredAt;
    if (!parsed.email || !loggedInAt) {
      return null;
    }
    const normalized: RegisteredUser = {
      email: normalizeEmail(parsed.email),
      loggedInAt
    };
    localStorage.setItem(SESSION_STORAGE_KEY, JSON.stringify(normalized));
    return normalized;
  } catch {
    return null;
  }
}

export function logoutUser(): void {
  localStorage.removeItem(SESSION_STORAGE_KEY);
  localStorage.removeItem(LEGACY_SESSION_STORAGE_KEY);
  emitAuthStateChanged();
}

export function subscribeAuthChanges(listener: () => void): () => void {
  window.addEventListener(AUTH_STATE_EVENT, listener);
  return () => {
    window.removeEventListener(AUTH_STATE_EVENT, listener);
  };
}
