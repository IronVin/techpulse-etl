"""
Suite di Unit Test (Pytest) per il modulo `techpulse.transform`.

Copre sia le singole funzioni pure di pulizia ed estrazione (unit test atomici)
sia il flusso completo `transform_jobs`, verificando deduplicazione, conversioni
temporali UTC e il blocco esplicito tramite `pandera.errors.SchemaError` in caso
di violazione del Data Contract (negative testing).
"""

import pandera.errors
import pytest

from techpulse.transform import clean_html_text, extract_skills, transform_jobs


def test_clean_html_text_removes_tags_and_normalizes_spaces():
    """Verifica la pulizia HTML, la decodifica delle entità e il caso limite None."""
    raw = "<p>Cerchiamo   uno <strong>sviluppatore Python</strong> &amp; SQL.</p>"
    assert clean_html_text(raw) == "Cerchiamo uno sviluppatore Python & SQL."
    # Edge-case: garantisce che un campo description mancante (None) non sollevi TypeError
    assert clean_html_text(None) == ""


def test_extract_skills_detects_technologies_case_insensitive():
    """Verifica il matching case-insensitive delle regex e i sinonimi (es. PostgreSQL -> SQL)."""
    text = "Experience with python, FastAPI, PostgreSQL and docker is required."
    skills = extract_skills(text)
    assert "Python" in skills
    assert "FastAPI" in skills
    # Verifica la normalizzazione: "PostgreSQL" nel testo viene mappato sulla categoria "SQL"
    assert "SQL" in skills
    assert "Docker" in skills
    # Negative assertion: assicura assenza di falsi positivi per skill non menzionate
    assert "AWS" not in skills


def test_transform_jobs_valid_and_deduplicates():
    """Verifica la deduplicazione intra-batch, il feature engineering e il parsing date UTC."""
    raw_jobs = [
        {
            "slug": "python-dev-123",
            "title": "Senior Python Backend Engineer",
            "company_name": "TechCorp",
            "location": "Remote",
            "remote": True,
            "description": "<p>Stack: Python, Docker, Git</p>",
            "url": "https://example.com/jobs/123",
            "created_at": 1700000000,
        },
        # Duplicato intenzionale con lo stesso slug per testare drop_duplicates()
        # prima della validazione di unicità imposta da Pandera
        {
            "slug": "python-dev-123",
            "title": "Senior Python Backend Engineer",
            "company_name": "TechCorp",
            "location": "Remote",
            "remote": True,
            "description": "<p>Stack: Python, Docker, Git</p>",
            "url": "https://example.com/jobs/123",
            "created_at": 1700000000,
        },
    ]

    df = transform_jobs(raw_jobs)

    assert len(df) == 1
    assert df.loc[0, "job_id"] == "python-dev-123"
    assert bool(df.loc[0, "is_python_role"]) is True
    assert "Python" in df.loc[0, "matched_skills"]
    # Verifica deterministica della conversione epoch Unix -> data ISO UTC (1700000000 = 2023-11-14)
    assert df.loc[0, "published_date"] == "2023-11-14"


def test_transform_jobs_fails_pandera_on_invalid_url():
    """Negative test: verifica che Pandera blocchi la pipeline su dati non conformi allo schema."""
    invalid_jobs = [
        {
            "slug": "bad-job-999",
            "title": "Data Analyst",
            "company_name": "DataInc",
            "location": "Milan",
            "remote": False,
            "description": "SQL and Pandas",
            "url": "not-a-valid-url",  # Viola la regola str_startswith="http"
            "created_at": 1700000000,
        }
    ]

    # Mi aspetto esplicitamente SchemaError: dimostra che il Data Contract protegge
    # il database impedendo il salvataggio di record con URL malformati
    with pytest.raises(pandera.errors.SchemaError):
        transform_jobs(invalid_jobs)