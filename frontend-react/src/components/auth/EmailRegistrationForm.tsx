import { FormEvent, useMemo, useState } from "react";
import { registerUser } from "../../services/authService";

interface EmailRegistrationFormProps {
  onRegistered: (email: string) => void;
}

function isValidEmail(email: string): boolean {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email.trim());
}

function EmailRegistrationForm({ onRegistered }: EmailRegistrationFormProps) {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [passwordConfirm, setPasswordConfirm] = useState("");
  const [error, setError] = useState("");

  const isFormValid = useMemo(() => {
    return isValidEmail(email) && password.length >= 8 && password === passwordConfirm;
  }, [email, password, passwordConfirm]);

  const handleSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setError("");

    if (!isValidEmail(email)) {
      setError("Введите корректный email-адрес.");
      return;
    }

    if (password.length < 8) {
      setError("Пароль должен содержать минимум 8 символов.");
      return;
    }

    if (password !== passwordConfirm) {
      setError("Пароли не совпадают.");
      return;
    }

    registerUser(email, password);
    onRegistered(email);
  };

  return (
    <form className="glass-panel registration-form reveal" onSubmit={handleSubmit}>
      <div>
        <p className="eyebrow">NeoInvest Доступ</p>
        <h1 className="form-title">Создайте аккаунт по email</h1>
        <p className="muted">
          В демо-режиме хакатона регистрация хранится локально. Позже можно подключить реальный API
          авторизации.
        </p>
      </div>

      <label className="field-label" htmlFor="email">
        Электронная почта
      </label>
      <input
        id="email"
        className="field-input"
        type="email"
        autoComplete="email"
        placeholder="analitik@company.ru"
        value={email}
        onChange={(event) => setEmail(event.target.value)}
      />

      <label className="field-label" htmlFor="password">
        Пароль
      </label>
      <input
        id="password"
        className="field-input"
        type="password"
        autoComplete="new-password"
        placeholder="Минимум 8 символов"
        value={password}
        onChange={(event) => setPassword(event.target.value)}
      />

      <label className="field-label" htmlFor="password-confirm">
        Подтвердите пароль
      </label>
      <input
        id="password-confirm"
        className="field-input"
        type="password"
        autoComplete="new-password"
        placeholder="Повторите пароль"
        value={passwordConfirm}
        onChange={(event) => setPasswordConfirm(event.target.value)}
      />

      {error ? <p className="form-error">{error}</p> : null}

      <button type="submit" className="primary-button" disabled={!isFormValid}>
        Зарегистрироваться и открыть дашборд
      </button>
    </form>
  );
}

export default EmailRegistrationForm;
