import logging
import sqlite3
from pathlib import Path

import pandas as pd

from techpulse.config import DB_PATH, PARQUET_PATH

logger = logging.getLogger("techpulse.load")

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

INSERT_JOB_SQL = """
INSERT OR IGNORE INTO job_postings (
    job_id, title, company_name, location, remote,
    is_python_role, matched_skills, url, published_date, extracted_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""


def save_to_sqlite(df: pd.DataFrame, db_path: Path = DB_PATH) -> int:
    """
    Inserisce i nuovi annunci nel database SQLite ignorando quelli già presenti.

    Returns:
        Numero effettivo di nuovi record inseriti.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        conn.execute(CREATE_TABLE_SQL)
        before_count = conn.execute("SELECT COUNT(*) FROM job_postings").fetchone()[0]

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

    with sqlite3.connect(db_path) as conn:
        full_df = pd.read_sql_query(
            "SELECT * FROM job_postings ORDER BY published_date DESC, extracted_at DESC",
            conn,
        )

    # Riconvertiamo gli interi SQLite (0/1) nei booleani originali per il dataset analitico
    full_df["remote"] = full_df["remote"].astype(bool)
    full_df["is_python_role"] = full_df["is_python_role"].astype(bool)

    full_df.to_parquet(parquet_path, index=False, engine="pyarrow")
    logger.info(
        f"Esportazione Parquet completata: {len(full_df)} record salvati in {parquet_path.name}"
    )


def load_data(
    df: pd.DataFrame, db_path: Path = DB_PATH, parquet_path: Path = PARQUET_PATH
) -> int:
    """Orchestra il salvataggio su SQLite e l'aggiornamento del file Parquet."""
    if df.empty:
        logger.warning("DataFrame vuoto: nessuna operazione di salvataggio eseguita.")
        return 0

    inserted_count = save_to_sqlite(df, db_path=db_path)
    export_to_parquet(db_path=db_path, parquet_path=parquet_path)
    return inserted_count
