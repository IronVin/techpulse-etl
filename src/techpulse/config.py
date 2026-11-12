import logging
from pathlib import Path

# Percorsi principali del progetto calcolati dinamicamente
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "jobs.db"
PARQUET_PATH = DATA_DIR / "latest_jobs.parquet"

# Endpoint API pubblico (Arbeitnow offre annunci tech europei/remoti senza API key)
API_URL = "https://www.arbeitnow.com/api/job-board-api"
REQUEST_TIMEOUT = 15.0


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    """Configura e restituisce il logger standard dell'applicazione."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger("techpulse")
