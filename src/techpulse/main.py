"""
Modulo Orchestratore (Entry Point) della pipeline TechPulse ETL.

Coordina l'esecuzione sequenziale delle tre fasi (Extract, Transform, Load),
centralizza la configurazione del logging e gestisce la terminazione sicura
del processo in caso di errori critici.
"""

import logging
import sys

# Import modulari dal package 'techpulse': separo le responsabilità (Separation of Concerns)
# così ogni fase è testabile in isolamento e facilmente manutenibile
from techpulse.config import setup_logging
from techpulse.extract import fetch_jobs
from techpulse.load import load_data
from techpulse.transform import transform_jobs

# Uso un logger nominativo legato al modulo anziché il root logger,
# per tracciare con precisione da quale componente proviene ogni evento nei log
logger = logging.getLogger("techpulse.main")


def run_pipeline() -> None:
    """
    Esegue l'intero flusso ETL end-to-end.

    Flusso dati:
    1. Extract: recupera i dati grezzi (lista di dizionari JSON) dall'API Arbeitnow.
    2. Transform: normalizza i campi, gestisce i valori nulli e valida lo schema con Pandera.
    3. Load: persiste i dati validati su SQLite (con upsert idempotente) e Parquet.
    """
    # Inizializzo il sistema di logging (console + file) prima di qualsiasi operazione
    # per garantire la piena osservabilità dell'esecuzione fin dal primo step
    setup_logging()
    logger.info("=== Avvio Pipeline TechPulse ETL ===")

    try:
        # Passo 1 - Extract: ottengo i dati grezzi disaccoppiati dalla logica di pulizia
        raw_jobs = fetch_jobs()

        # Passo 2 - Transform: converto in DataFrame e applico il Data Contract (Pandera);
        # se lo schema non è rispettato, il flusso si blocca qui prima di sporcare il DB
        clean_df = transform_jobs(raw_jobs)

        # Passo 3 - Load: salvo i dati puliti e recupero il conteggio dei soli record nuovi
        new_records = load_data(clean_df)

        logger.info(
            f"=== Pipeline completata con successo ({new_records} nuovi record) ==="
        )
    except Exception as exc:
        # Intercetto qualsiasi eccezione non gestita a valle (es. rete assente, API down,
        # violazione dello schema Pandera) loggando l'intero stack trace (exc_info=True)
        logger.critical(
            f"Esecuzione pipeline interrotta per errore critico: {exc}", exc_info=True
        )
        # Esco con exit code 1 (non-zero): fondamentale in produzione e nei job CI/CD
        # (es. GitHub Actions o cron) per segnalare al sistema che il run è fallito
        sys.exit(1)


# Proteggo l'esecuzione con il guardiano standard di Python per permettere
# di importare `run_pipeline` nei test (Pytest) senza avviare subito il flusso
if __name__ == "__main__":
    run_pipeline()