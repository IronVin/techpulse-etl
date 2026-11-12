from pathlib import Path
from unittest.mock import patch

import httpx
import pandas as pd
import pytest

from techpulse.extract import fetch_jobs
from techpulse.load import load_data
from techpulse.transform import transform_jobs


def test_fetch_jobs_handles_timeout_error():
    with patch("techpulse.extract.httpx.Client.get") as mock_get:
        mock_get.side_effect = httpx.TimeoutException("Connection timed out")

        with pytest.raises(httpx.TimeoutException):
            fetch_jobs(url="https://fake-api.local/jobs")


def test_load_data_is_idempotent_and_creates_parquet(tmp_path: Path):
    test_db = tmp_path / "test_jobs.db"
    test_parquet = tmp_path / "test_jobs.parquet"

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

    df = transform_jobs(raw_batch)

    # Prima esecuzione: deve inserire 1 record
    inserted_first = load_data(df, db_path=test_db, parquet_path=test_parquet)
    assert inserted_first == 1
    assert test_db.exists()
    assert test_parquet.exists()

    # Seconda esecuzione con gli stessi dati: deve inserire 0 record
    inserted_second = load_data(df, db_path=test_db, parquet_path=test_parquet)
    assert inserted_second == 0

    # Verifica che il file Parquet contenga esattamente 1 riga leggibile
    loaded_df = pd.read_parquet(test_parquet)
    assert len(loaded_df) == 1
    assert loaded_df.loc[0, "job_id"] == "data-eng-01"
