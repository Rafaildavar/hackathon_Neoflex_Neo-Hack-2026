import { RegisteredUser } from "../types";

const USER_STORAGE_KEY = "neo_invest_registered_user";

export function registerUser(email: string, password: string): RegisteredUser {
  const payload: RegisteredUser = {
    email: email.trim().toLowerCase(),
    password,
    registeredAt: new Date().toISOString()
  };
  localStorage.setItem(USER_STORAGE_KEY, JSON.stringify(payload));
  return payload;
}

export function getCurrentUser(): RegisteredUser | null {
  const raw = localStorage.getItem(USER_STORAGE_KEY);
  if (!raw) {
    return null;
  }

  try {
    const parsed = JSON.parse(raw) as RegisteredUser;
    if (!parsed.email || !parsed.password) {
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
