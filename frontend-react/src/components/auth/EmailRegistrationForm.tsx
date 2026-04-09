import { FormEvent, useMemo, useState } from "react";
import { loginUser, registerUser } from "../../services/authService";

interface EmailRegistrationFormProps {
  onRegistered: (email: string) => void;
}

function isValidEmail(email: string): boolean {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email.trim());
}

function EmailRegistrationForm({ onRegistered }: EmailRegistrationFormProps) {
  const [mode, setMode] = useState<"register" | "login">("register");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [passwordConfirm, setPasswordConfirm] = useState("");
  const [error, setError] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);

  const isFormValid = useMemo(() => {
    const base = isValidEmail(email) && password.length >= 8;
    if (mode === "login") {
      return base;
    }
    return base && password === passwordConfirm;
  }, [email, password, passwordConfirm, mode]);

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
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

    if (mode === "register" && password !== passwordConfirm) {
      setError("Пароли не совпадают.");
      return;
    }

    try {
      setIsSubmitting(true);
      if (mode === "register") {
        await registerUser(email, password);
      } else {
        await loginUser(email, password);
      }
      onRegistered(email);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Ошибка авторизации.");
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <form className="glass-panel registration-form reveal" onSubmit={handleSubmit}>
      <div>
        <p className="eyebrow">NeoInvest Доступ</p>
        <h1 className="form-title">
          {mode === "register" ? "Создайте аккаунт по email" : "Вход в NeoInvest"}
        </h1>
        <p className="muted">
          Данные регистрации и входа проверяются через Python API и сохраняются в БД.
        </p>
      </div>

      <div className="form-mode-toggle">
        <button
          type="button"
          className={`secondary-button ${mode === "register" ? "active" : ""}`}
          onClick={() => setMode("register")}
        >
          Регистрация
        </button>
        <button
          type="button"
          className={`secondary-button ${mode === "login" ? "active" : ""}`}
          onClick={() => setMode("login")}
        >
          Вход
        </button>
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

      {mode === "register" ? (
        <>
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
        </>
      ) : null}

      {error ? <p className="form-error">{error}</p> : null}

      <button type="submit" className="primary-button" disabled={!isFormValid || isSubmitting}>
        {isSubmitting
          ? "Проверка..."
          : mode === "register"
            ? "Зарегистрироваться и открыть дашборд"
            : "Войти и открыть дашборд"}
      </button>
    </form>
  );
}

export default EmailRegistrationForm;
