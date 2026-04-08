from src.ingestion.loader import load_history
from src.utils.logger import get_logger

logger = get_logger(__name__)


def main() -> None:
    logger.info("Start manual history load for configured MOEX tickers")
    load_history(days_back=30)
    logger.info("Manual history load completed")


if __name__ == "__main__":
    main()
