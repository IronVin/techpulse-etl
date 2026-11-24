"""
Modulo di Configurazione Centralizzata della pipeline TechPulse ETL.

Raccoglie in un unico punto percorsi su filesystem, parametri di rete e regole
di logging, evitando valori "hardcoded" sparsi nei moduli operativi e garantendo
portabilità cross-platform (Windows, Linux, macOS e runner CI/CD).
"""

import logging
from pathlib import Path

# Uso pathlib.Path con .resolve() invece di stringhe relative ('../data'):
# la radice del progetto viene calcolata dinamicamente a partire dalla posizione
# fisica di questo file, evitando FileNotFoundError indipendentemente dalla directory
# corrente (CWD) da cui viene lanciato il comando Python o Pytest
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "jobs.db"
PARQUET_PATH = DATA_DIR / "latest_jobs.parquet"

# Endpoint API pubblico (Arbeitnow offre annunci tech europei/remoti senza API key)
API_URL = "https://www.arbeitnow.com/api/job-board-api"

# Soglia esplicita di timeout (in secondi) per le richieste HTTP:
# previene che la pipeline resti appesa all'infinito in caso di server non responsivo
REQUEST_TIMEOUT = 15.0


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    """Configura e restituisce il logger standard dell'applicazione."""
    # Uso il modulo standard `logging` al posto di `print()` per avere:
    # - timestamp ISO strutturati e livelli di severità filtrabili (INFO, ERROR, CRITICAL)
    # - il campo `%(name)s` che identifica subito quale modulo (extract/transform/load)
    #   ha generato l'evento durante il debugging o nei log di GitHub Actions
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger("techpulse")