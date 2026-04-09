import { useNavigate } from "react-router-dom";
import EmailRegistrationForm from "../components/auth/EmailRegistrationForm";

function RegisterPage() {
  const navigate = useNavigate();

  return (
    <main className="page-shell register-layout">
      <section className="intro-copy reveal">
        <p className="eyebrow">NEO.HACK 2026</p>
        <h1 className="hero-title">NeoInvest аналитическая платформа</h1>
        <p className="hero-text">
          React-интерфейс соответствует требованиям кейса: обзорный дашборд, мониторинг рынка,
          фильтры по тикеру/дате/метрике, отслеживание аномалий и email-уведомления.
        </p>
        <ul className="hero-list">
          <li>Интерактивные графики цен, объемов и волатильности</li>
          <li>Лидеры роста и падения за выбранный период</li>
          <li>Регистрация и вход по email для аналитиков</li>
        </ul>
      </section>

      <EmailRegistrationForm onAuthorized={() => navigate("/dashboard")} />
    </main>
  );
}

export default RegisterPage;
