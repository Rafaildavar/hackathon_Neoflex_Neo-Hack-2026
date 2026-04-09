import { FormEvent, useMemo, useState } from "react";
import { loginUser, registerUser } from "../../services/authService";

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
  const [isSubmitting, setIsSubmitting] = useState(false);

  const isFormValid = useMemo(() => {
    return isValidEmail(email) && password.length >= 8 && password === passwordConfirm;
  }, [email, password, passwordConfirm]);

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

    if (password !== passwordConfirm) {
      setError("Пароли не совпадают.");
      return;
    }

    try {
      setIsSubmitting(true);
      try {
        await registerUser(email, password);
      } catch (registerError) {
        if (
          registerError instanceof Error &&
          registerError.message.includes("уже существует")
        ) {
          await loginUser(email, password);
        } else {
          throw registerError;
        }
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
        <h1 className="form-title">Создайте аккаунт по email</h1>
        <p className="muted">
          Регистрация нужна только один раз. Если email уже существует, будет выполнен вход
          с указанным паролем.
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

      <button type="submit" className="primary-button" disabled={!isFormValid || isSubmitting}>
        {isSubmitting ? "Проверка..." : "Продолжить"}
      </button>
    </form>
  );
}

export default EmailRegistrationForm;
