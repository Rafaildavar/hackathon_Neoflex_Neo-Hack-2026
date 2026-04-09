import { FormEvent, useMemo, useState } from "react";
import { loginUser, registerUser } from "../../services/authService";

interface EmailRegistrationFormProps {
  onAuthorized: (email: string) => void;
}

function isValidEmail(email: string): boolean {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email.trim());
}

function isAdminLogin(email: string): boolean {
  return email.trim().toLowerCase() === "admin";
}

function isValidLoginIdentifier(email: string): boolean {
  return isValidEmail(email) || isAdminLogin(email);
}

function EmailRegistrationForm({ onAuthorized }: EmailRegistrationFormProps) {
  const [mode, setMode] = useState<"register" | "login">("register");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [passwordConfirm, setPasswordConfirm] = useState("");
  const [error, setError] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);

  const isFormValid = useMemo(() => {
    const hasValidIdentifier = mode === "login" ? isValidLoginIdentifier(email) : isValidEmail(email);
    if (!hasValidIdentifier || password.length === 0) {
      return false;
    }
    if (mode === "login") {
      return true;
    }
    return password.length >= 8 && password === passwordConfirm;
  }, [mode, email, password, passwordConfirm]);

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setError("");

    if (mode === "login" && !isValidLoginIdentifier(email)) {
      setError("Введите корректный email или тестовый логин admin.");
      return;
    }

    if (mode === "register" && !isValidEmail(email)) {
      setError("Введите корректный email-адрес.");
      return;
    }

    if (password.length === 0) {
      setError("Введите пароль.");
      return;
    }

    if (mode === "register") {
      if (password.length < 8) {
        setError("Пароль должен содержать минимум 8 символов.");
        return;
      }

      if (password !== passwordConfirm) {
        setError("Пароли не совпадают.");
        return;
      }
    }

    try {
      setIsSubmitting(true);
      if (mode === "register") {
        await registerUser(email, password);
      } else {
        await loginUser(email, password);
      }
      onAuthorized(email);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Ошибка авторизации.");
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <form className="glass-panel registration-form reveal" onSubmit={handleSubmit}>
      <div>
        <div className="segmented" style={{ marginBottom: "0.7rem" }}>
          <button
            type="button"
            className={`segment ${mode === "register" ? "selected" : ""}`}
            onClick={() => {
              setMode("register");
              setError("");
            }}
          >
            Регистрация
          </button>
          <button
            type="button"
            className={`segment ${mode === "login" ? "selected" : ""}`}
            onClick={() => {
              setMode("login");
              setError("");
            }}
          >
            Вход
          </button>
        </div>

        <p className="eyebrow">NeoInvest Доступ</p>
        <h1 className="form-title">
          {mode === "register" ? "Создайте аккаунт по email" : "Войдите в аккаунт"}
        </h1>
        <p className="muted">
          {mode === "register"
            ? "Зарегистрируйтесь, чтобы получить доступ к дашборду с графиками и аномалиями."
            : "Введите ваш email и пароль, чтобы продолжить анализ рынка. Тестовый вход: admin / admin."}
        </p>
      </div>

      <label className="field-label" htmlFor="email">
        {mode === "login" ? "Электронная почта или логин" : "Электронная почта"}
      </label>
      <input
        id="email"
        className="field-input"
        type={mode === "login" ? "text" : "email"}
        autoComplete={mode === "login" ? "username" : "email"}
        placeholder={mode === "login" ? "analitik@company.ru или admin" : "analitik@company.ru"}
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
        autoComplete={mode === "register" ? "new-password" : "current-password"}
        placeholder={mode === "register" ? "Минимум 8 символов" : "Введите пароль"}
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
          ? mode === "register"
            ? "Регистрация..."
            : "Вход..."
          : mode === "register"
            ? "Зарегистрироваться"
            : "Войти"}
      </button>
    </form>
  );
}

export default EmailRegistrationForm;
