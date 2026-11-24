"""
Modulo di Caricamento e Persistenza (Fase Load) della pipeline TechPulse ETL.

Implementa una strategia di Dual Persistence:
1. SQLite: funge da Single Source of Truth relazionale e idempotente (tramite Primary Key
   e INSERT OR IGNORE), permettendo esecuzioni ripetute senza duplicare lo storico.
2. Apache Parquet: esporta lo storico consolidato in formato colonnare compresso (PyArrow),
   ottimizzato per letture analitiche veloci nella dashboard Streamlit o in tool BI.
"""

import logging
import sqlite3
from pathlib import Path

import pandas as pd

from techpulse.config import DB_PATH, PARQUET_PATH

logger = logging.getLogger("techpulse.load")

# DDL idempotente (CREATE TABLE IF NOT EXISTS): garantisce l'auto-provisioning
# dello schema al primo avvio senza richiedere script di migrazione manuali.
# SQLite non ha un tipo BOOLEAN nativo: uso INTEGER (0/1) con vincoli NOT NULL
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS job_postings (
    job_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    company_name TEXT NOT NULL,
    location TEXT NOT NULL,
    remote INTEGER NOT NULL,
    is_python_role INTEGER NOT NULL,
    matched_skills TEXT NOT NULL,
    url TEXT NOT NULL,
    published_date TEXT NOT NULL,
    extracted_at TEXT NOT NULL
);
"""

# Query DML parametrizzata (placeholder '?') per prevenire SQL Injection.
# INSERT OR IGNORE sfrutta il vincolo PRIMARY KEY su `job_id` per rendere la pipeline
# idempotente: se un annuncio è già presente in archivio, viene saltato senza errori
INSERT_JOB_SQL = """
INSERT OR IGNORE INTO job_postings (
    job_id, title, company_name, location, remote,
    is_python_role, matched_skills, url, published_date, extracted_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""


# Parametrizzo db_path con default da config per isolare i test Pytest su database temporanei
def save_to_sqlite(df: pd.DataFrame, db_path: Path = DB_PATH) -> int:
    """
    Inserisce i nuovi annunci nel database SQLite ignorando quelli già presenti.

    Returns:
        Numero effettivo di nuovi record inseriti.
    """
    # Creo automaticamente le directory padre se non esistono per evitare OperationalError
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Il context manager `with sqlite3.connect` gestisce l'apertura della connessione
    # e il rollback automatico in caso di eccezioni durante la transazione
    with sqlite3.connect(db_path) as conn:
        conn.execute(CREATE_TABLE_SQL)
        # Misuro il conteggio prima e dopo l'inserimento per calcolare con esattezza
        # quanti record nuovi (non ignorati) sono entrati in questo run
        before_count = conn.execute("SELECT COUNT(*) FROM job_postings").fetchone()[0]

        # Uso itertuples(index=False) anziché iterrows(): è ordini di grandezza più veloce
        # perché restituisce namedtuple leggeri senza creare una Series Pandas per ogni riga
        rows = [
            (
                row.job_id,
                row.title,
                row.company_name,
                row.location,
                int(row.remote),
                int(row.is_python_role),
                row.matched_skills,
                row.url,
                row.published_date,
                row.extracted_at,
            )
            for row in df.itertuples(index=False)
        ]

        # Eseguo un'unica operazione batch transazionale anziché N insert singole
        conn.executemany(INSERT_JOB_SQL, rows)
        conn.commit()

        after_count = conn.execute("SELECT COUNT(*) FROM job_postings").fetchone()[0]

    inserted = after_count - before_count
    logger.info(
        f"Database SQLite aggiornato ({db_path.name}): "
        f"{inserted} nuovi annunci inseriti (totale storico: {after_count})"
    )
    return inserted


def export_to_parquet(
    db_path: Path = DB_PATH, parquet_path: Path = PARQUET_PATH
) -> None:
    """Esporta l'intero storico da SQLite a un file Parquet ottimizzato."""
    parquet_path.parent.mkdir(parents=True, exist_ok=True)

    # Leggo l'intero storico ordinato direttamente da SQLite (Single Source of Truth)
    # così il file Parquet riflette sempre lo stato completo e deduplicato del database
    with sqlite3.connect(db_path) as conn:
        full_df = pd.read_sql_query(
            "SELECT * FROM job_postings ORDER BY published_date DESC, extracted_at DESC",
            conn,
        )

    # Ripristino i tipi booleani nativi prima della serializzazione Parquet:
    # evita che i tool analitici interpretino 0/1 come variabili numeriche continue
    full_df["remote"] = full_df["remote"].astype(bool)
    full_df["is_python_role"] = full_df["is_python_role"].astype(bool)

    # Uso il formato colonnare Parquet con motore PyArrow per compressione efficiente,
    # preservazione rigorosa dello schema e letture rapide nella dashboard
    full_df.to_parquet(parquet_path, index=False, engine="pyarrow")
    logger.info(
        f"Esportazione Parquet completata: {len(full_df)} record salvati in {parquet_path.name}"
    )


def load_data(
    df: pd.DataFrame, db_path: Path = DB_PATH, parquet_path: Path = PARQUET_PATH
) -> int:
    """Orchestra il salvataggio su SQLite e l'aggiornamento del file Parquet."""
    # Guard clause: se il DataFrame in ingresso è vuoto, evito connessioni al DB
    # e riscritture inutili del file Parquet su disco
    if df.empty:
        logger.warning("DataFrame vuoto: nessuna operazione di salvataggio eseguita.")
        return 0

    inserted_count = save_to_sqlite(df, db_path=db_path)
    export_to_parquet(db_path=db_path, parquet_path=parquet_path)
    return inserted_count