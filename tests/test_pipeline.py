"""
Suite di Test Automatici (Pytest) per la pipeline TechPulse ETL.

Verifica in completo isolamento (senza chiamate di rete reali e senza sporcare
i database di produzione) la resilienza agli errori HTTP/timeout e l'idempotenza
del caricamento end-to-end su SQLite e Parquet.
"""

from pathlib import Path
from unittest.mock import patch

import httpx
import pandas as pd
import pytest

from techpulse.extract import fetch_jobs
from techpulse.load import load_data
from techpulse.transform import transform_jobs


def test_fetch_jobs_handles_timeout_error():
    """Verifica che un timeout di rete venga intercettato e propagato correttamente."""
    # Uso unittest.mock.patch per simulare il fallimento della chiamata HTTP senza
    # dipendere dalla rete o dover attendere realmente 15 secondi (test istantaneo e deterministico)
    with patch("techpulse.extract.httpx.Client.get") as mock_get:
        mock_get.side_effect = httpx.TimeoutException("Connection timed out")

        # Verifico con pytest.raises che fetch_jobs propaghi l'eccezione attesa
        # verso l'orchestratore (main.py) anziché inghiottirla silenziosamente
        with pytest.raises(httpx.TimeoutException):
            fetch_jobs(url="https://fake-api.local/jobs")


# Uso la fixture built-in `tmp_path` di Pytest: crea una cartella temporanea isolata
# sul filesystem per questo singolo test e la ripulisce automaticamente al termine
def test_load_data_is_idempotent_and_creates_parquet(tmp_path: Path):
    """Test di integrazione Transform -> Load: verifica idempotenza e export Parquet."""
    test_db = tmp_path / "test_jobs.db"
    test_parquet = tmp_path / "test_jobs.parquet"

    # Payload minimale ma realistico che simula la risposta grezza dell'API
    # (inclusi tag HTML nella description e timestamp Unix)
    raw_batch = [
        {
            "slug": "data-eng-01",
            "title": "Data Engineer",
            "company_name": "CloudSystems",
            "location": "Italy",
            "remote": True,
            "description": "<p>Python and PySpark pipeline</p>",
            "url": "https://example.com/data-eng-01",
            "created_at": 1700000000,
        }
    ]

    # Passo i dati grezzi attraverso la vera logica di trasformazione e validazione Pandera
    df = transform_jobs(raw_batch)

    # Prima esecuzione: il database è vuoto, quindi deve inserire esattamente 1 record
    # e generare sia il file .db che il file .parquet
    inserted_first = load_data(df, db_path=test_db, parquet_path=test_parquet)
    assert inserted_first == 1
    assert test_db.exists()
    assert test_parquet.exists()

    # Seconda esecuzione con lo stesso identico batch: verifica il comportamento idempotente
    # dell'INSERT OR IGNORE (0 nuovi record inseriti, nessuna eccezione di Primary Key)
    inserted_second = load_data(df, db_path=test_db, parquet_path=test_parquet)
    assert inserted_second == 0

    # Verifica end-to-end dell'artefatto finale: rileggo il Parquet su disco
    # per assicurarmi che lo schema sia integro e non ci siano righe duplicate
    loaded_df = pd.read_parquet(test_parquet)
    assert len(loaded_df) == 1
    assert loaded_df.loc[0, "job_id"] == "data-eng-01"