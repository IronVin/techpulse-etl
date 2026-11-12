import html
import logging
import re
from datetime import UTC, datetime
from typing import Any

import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series

logger = logging.getLogger("techpulse.transform")

# Dizionario di skill tech da cercare negli annunci (chiave: nome pulito, valore: pattern regex)
TECH_SKILLS_PATTERNS: dict[str, str] = {
    "Python": r"\bpython\b",
    "SQL": r"\bsql\b|\bpostgresql\b|\bmysql\b|\bsqlite\b",
    "Docker": r"\bdocker\b|\bkubernetes\b",
    "AWS": r"\baws\b|\bamazon web services\b",
    "FastAPI": r"\bfastapi\b",
    "Django": r"\bdjango\b",
    "Pandas": r"\bpandas\b",
    "PySpark": r"\bpyspark\b|\bspark\b",
    "Git": r"\bgit\b|\bgithub\b|\bgitlab\b",
    "Linux": r"\blinux\b",
}


class JobPostingSchema(pa.DataFrameModel):
    """Contratto di qualità dei dati: valida struttura e tipi prima del salvataggio."""

    job_id: Series[str] = pa.Field(
        unique=True, nullable=False, str_length={"min_value": 1}
    )
    title: Series[str] = pa.Field(nullable=False, str_length={"min_value": 1})
    company_name: Series[str] = pa.Field(nullable=False)
    location: Series[str] = pa.Field(nullable=False)
    remote: Series[bool] = pa.Field(nullable=False)
    is_python_role: Series[bool] = pa.Field(nullable=False)
    matched_skills: Series[str] = pa.Field(nullable=False)
    url: Series[str] = pa.Field(nullable=False, str_startswith="http")
    published_date: Series[str] = pa.Field(nullable=False)
    extracted_at: Series[str] = pa.Field(nullable=False)

    class Config:
        strict = True  # Blocca il DataFrame se ci sono colonne extra non previste
        coerce = True  # Converte automaticamente i tipi compatibili


def clean_html_text(raw_html: str | None) -> str:
    """Rimuove i tag HTML, decodifica le entità e normalizza gli spazi."""
    if not raw_html:
        return ""
    text = html.unescape(raw_html)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_skills(text: str) -> list[str]:
    """Estrae l'elenco delle skill tecnologiche presenti nel testo."""
    found: list[str] = []
    for skill_name, pattern in TECH_SKILLS_PATTERNS.items():
        if re.search(pattern, text, flags=re.IGNORECASE):
            found.append(skill_name)
    return found


def transform_jobs(raw_jobs: list[dict[str, Any]]) -> pd.DataFrame:
    """
    Pulisce, arricchisce e valida la lista grezza di annunci di lavoro.

    Args:
        raw_jobs: Lista di dizionari proveniente dal modulo extract.

    Returns:
        DataFrame Pandas validato secondo JobPostingSchema.
    """
    if not raw_jobs:
        logger.warning("Nessun annuncio ricevuto in ingresso per la trasformazione.")
        return pd.DataFrame(columns=list(JobPostingSchema.to_schema().columns.keys()))

    logger.info(f"Avvio trasformazione su {len(raw_jobs)} annunci grezzi")
    extraction_ts = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    records: list[dict[str, Any]] = []
    for item in raw_jobs:
        clean_desc = clean_html_text(item.get("description", ""))
        title = str(item.get("title", "")).strip()
        full_text = f"{title} {clean_desc}"

        skills = extract_skills(full_text)
        is_python = "Python" in skills

        # Conversione timestamp Unix (secondi) in data YYYY-MM-DD
        created_at_ts = item.get("created_at")
        if isinstance(created_at_ts, (int, float)):
            pub_date = datetime.fromtimestamp(created_at_ts, tz=UTC).strftime(
                "%Y-%m-%d"
            )
        else:
            pub_date = datetime.now(UTC).strftime("%Y-%m-%d")

        records.append(
            {
                "job_id": str(item.get("slug", "")).strip(),
                "title": title,
                "company_name": str(item.get("company_name", "Unknown")).strip(),
                "location": str(item.get("location", "Remote")).strip(),
                "remote": bool(item.get("remote", False)),
                "is_python_role": is_python,
                "matched_skills": ", ".join(skills) if skills else "None",
                "url": str(item.get("url", "")).strip(),
                "published_date": pub_date,
                "extracted_at": extraction_ts,
            }
        )

    df = pd.DataFrame(records)

    # Rimozione di eventuali duplicati sullo stesso batch
    initial_count = len(df)
    df = df.drop_duplicates(subset=["job_id"], keep="first").reset_index(drop=True)
    if len(df) < initial_count:
        logger.info(f"Rimossi {initial_count - len(df)} annunci duplicati")

    # Validazione finale tramite schema Pandera
    logger.info("Esecuzione validazione Data Quality con Pandera...")
    validated_df = JobPostingSchema.validate(df)
    logger.info(
        f"Trasformazione completata: {len(validated_df)} record validati "
        f"({validated_df['is_python_role'].sum()} ruoli Python individuati)"
    )
    return validated_df
