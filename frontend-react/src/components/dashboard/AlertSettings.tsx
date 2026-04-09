import { FormEvent, useEffect, useState } from "react";
import { AlertPreferences } from "../../types";

interface AlertSettingsProps {
  initialValue: AlertPreferences;
  onSave: (value: AlertPreferences) => void;
}

function AlertSettings({ initialValue, onSave }: AlertSettingsProps) {
  const [form, setForm] = useState(initialValue);
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    setForm(initialValue);
  }, [initialValue]);

  const handleSubmit = (event: FormEvent) => {
    event.preventDefault();
    onSave(form);
    setSaved(true);
    window.setTimeout(() => setSaved(false), 1800);
  };

  return (
    <section className="glass-panel reveal" style={{ animationDelay: "450ms" }}>
      <div className="panel-header">
        <h2 className="panel-title">Настройки уведомлений по почте</h2>
      </div>

      <form className="alert-form" onSubmit={handleSubmit}>
        <label className="field-label" htmlFor="alert-email">
          Почта для уведомлений
        </label>
        <input
          id="alert-email"
          className="field-input"
          type="email"
          value={form.email}
          onChange={(event) => setForm({ ...form, email: event.target.value })}
          required
        />

        <label className="checkbox-row">
          <input
            type="checkbox"
            checked={form.priceAlerts}
            onChange={(event) => setForm({ ...form, priceAlerts: event.target.checked })}
          />
          Уведомления о скачках цены ({"> 2%"})
        </label>

        <label className="checkbox-row">
          <input
            type="checkbox"
            checked={form.volumeAlerts}
            onChange={(event) => setForm({ ...form, volumeAlerts: event.target.checked })}
          />
          Уведомления об аномальном объеме ({"> 3 сигм"})
        </label>

        <label className="field-label" htmlFor="severity">
          Минимальная критичность
        </label>
        <select
          id="severity"
          className="field-input"
          value={form.minSeverity}
          onChange={(event) =>
            setForm({ ...form, minSeverity: event.target.value === "high" ? "high" : "medium" })
          }
        >
          <option value="medium">средняя и высокая</option>
          <option value="high">только высокая</option>
        </select>

        <button type="submit" className="primary-button">
          Сохранить профиль уведомлений
        </button>
        {saved ? <p className="muted">Сохранено в локальном профиле.</p> : null}
      </form>
    </section>
  );
}

export default AlertSettings;
