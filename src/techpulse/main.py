import logging
import sys

from techpulse.config import setup_logging
from techpulse.extract import fetch_jobs
from techpulse.load import load_data
from techpulse.transform import transform_jobs

logger = logging.getLogger("techpulse.main")


def run_pipeline() -> None:
    """Esegue l'intero flusso ETL end-to-end."""
    setup_logging()
    logger.info("=== Avvio Pipeline TechPulse ETL ===")

    try:
        raw_jobs = fetch_jobs()
        clean_df = transform_jobs(raw_jobs)
        new_records = load_data(clean_df)
        logger.info(
            f"=== Pipeline completata con successo ({new_records} nuovi record) ==="
        )
    except Exception as exc:
        logger.critical(
            f"Esecuzione pipeline interrotta per errore critico: {exc}", exc_info=True
        )
        sys.exit(1)


if __name__ == "__main__":
    run_pipeline()
