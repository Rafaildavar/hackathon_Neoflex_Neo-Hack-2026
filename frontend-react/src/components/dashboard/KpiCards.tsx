import { KpiCardData } from "../../types";

interface KpiCardsProps {
  cards: KpiCardData[];
}

function KpiCards({ cards }: KpiCardsProps) {
  return (
    <section className="kpi-grid">
      {cards.map((card, index) => (
        <article
          key={card.title}
          className="glass-panel kpi-card reveal"
          style={{ animationDelay: `${index * 70 + 120}ms` }}
        >
          <p className="kpi-title">{card.title}</p>
          <p className="kpi-value">{card.value}</p>
          <p className={`kpi-delta ${card.trend}`}>{card.delta}</p>
        </article>
      ))}
    </section>
  );
}

export default KpiCards;
