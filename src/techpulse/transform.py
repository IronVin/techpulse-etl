"""
Modulo di Trasformazione e Validazione (Fase Transform) della pipeline TechPulse ETL.

Converte i payload JSON grezzi in un DataFrame strutturato, estrae feature
analitiche dal testo non strutturato (skill tecnologiche e flag ruoli Python),
rimuove eventuali duplicati e impone un Data Contract rigoroso tramite Pandera
prima della persistenza.
"""

import html
import logging
import re
from datetime import UTC, datetime
from typing import Any

import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series

logger = logging.getLogger("techpulse.transform")

# Dizionario di skill tech da cercare negli annunci (chiave: nome pulito, valore: pattern regex).
# Uso i word boundary (\b) per evitare falsi positivi (es. evitare che "Git" faccia match
# dentro parole come "digital" o che "SQL" scatti su sottostringhe casuali)
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


# Definisco lo schema come classe declarativa (DataFrameModel) anziché scrivere decine di 'if':
# funge sia da validatore a runtime sia da documentazione viva della struttura dati
class JobPostingSchema(pa.DataFrameModel):
    """Contratto di qualità dei dati: valida struttura e tipi prima del salvataggio."""

    # Vincolo di unicità e lunghezza minima sull'ID: previene violazioni di Primary Key in SQLite
    job_id: Series[str] = pa.Field(
        unique=True, nullable=False, str_length={"min_value": 1}
    )
    title: Series[str] = pa.Field(nullable=False, str_length={"min_value": 1})
    company_name: Series[str] = pa.Field(nullable=False)
    location: Series[str] = pa.Field(nullable=False)
    remote: Series[bool] = pa.Field(nullable=False)
    is_python_role: Series[bool] = pa.Field(nullable=False)
    matched_skills: Series[str] = pa.Field(nullable=False)
    # Controllo semantico sull'URL: garantisce che i link salvati siano indirizzi web validi
    url: Series[str] = pa.Field(nullable=False, str_startswith="http")
    published_date: Series[str] = pa.Field(nullable=False)
    extracted_at: Series[str] = pa.Field(nullable=False)

    class Config:
        # strict=True impedisce lo "schema drift": se l'API aggiunge colonne impreviste,
        # la pipeline fallisce esplicitamente anziché propagare dati non mappati
        strict = True
        # coerce=True forza il casting sicuro dei tipi Pandas (es. object -> string/bool)
        coerce = True


def clean_html_text(raw_html: str | None) -> str:
    """Rimuove i tag HTML, decodifica le entità e normalizza gli spazi."""
    if not raw_html:
        return ""
    # Prima decodifico le entità HTML (es. &amp; -> &, &lt; -> <) per evitare residui nel testo
    text = html.unescape(raw_html)
    # Rimuovo i tag HTML sostituendoli con uno spazio (non stringa vuota) per evitare
    # di incollare due parole separate solo da un tag (es. "<p>Python</p><p>SQL</p>")
    text = re.sub(r"<[^>]+>", " ", text)
    # Comprimo spazi multipli, tab e newline in un singolo spazio pulito
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_skills(text: str) -> list[str]:
    """Estrae l'elenco delle skill tecnologiche presenti nel testo."""
    found: list[str] = []
    for skill_name, pattern in TECH_SKILLS_PATTERNS.items():
        # Ricerca case-insensitive per intercettare varianti come "Python", "PYTHON", "python"
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
    # Gestione esplicita del caso limite (empty batch): se l'API restituisce lista vuota,
    # ritorno un DataFrame vuoto ma con le colonne esatte dello schema per non rompere il Load
    if not raw_jobs:
        logger.warning("Nessun annuncio ricevuto in ingresso per la trasformazione.")
        return pd.DataFrame(columns=list(JobPostingSchema.to_schema().columns.keys()))

    logger.info(f"Avvio trasformazione su {len(raw_jobs)} annunci grezzi")
    # Uso UTC come standard per tutti i timestamp della pipeline per evitare ambiguità
    # di fuso orario o ora legale tra macchine locali, server cloud e database
    extraction_ts = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    records: list[dict[str, Any]] = []
    for item in raw_jobs:
        clean_desc = clean_html_text(item.get("description", ""))
        title = str(item.get("title", "")).strip()
        # Unisco titolo e descrizione pulita per cercare le skill sull'intero contesto dell'annuncio
        full_text = f"{title} {clean_desc}"

        skills = extract_skills(full_text)
        # Feature engineering: creo un flag booleano dedicato per filtrare rapidamente
        # i ruoli Python in SQL e nella dashboard Streamlit senza dover fare LIKE sul testo
        is_python = "Python" in skills

        # Conversione timestamp Unix (secondi) in data ISO YYYY-MM-DD con fallback sicuro
        # alla data corrente se il campo sorgente è mancante o malformato
        created_at_ts = item.get("created_at")
        if isinstance(created_at_ts, (int, float)):
            pub_date = datetime.fromtimestamp(created_at_ts, tz=UTC).strftime(
                "%Y-%m-%d"
            )
        else:
            pub_date = datetime.now(UTC).strftime("%Y-%m-%d")

        # Costruisco una lista di dizionari prima di istanziare il DataFrame una sola volta:
        # molto più veloce e memory-efficient rispetto ad accodare righe in ciclo con pd.concat
        records.append(
            {
                "job_id": str(item.get("slug", "")).strip(),
                "title": title,
                "company_name": str(item.get("company_name", "Unknown")).strip(),
                "location": str(item.get("location", "Remote")).strip(),
                "remote": bool(item.get("remote", False)),
                "is_python_role": is_python,
                # Serializzo la lista in stringa separata da virgole per compatibilità
                # diretta con colonne scalari SQLite (evitando tipi complessi non nativi)
                "matched_skills": ", ".join(skills) if skills else "None",
                "url": str(item.get("url", "")).strip(),
                "published_date": pub_date,
                "extracted_at": extraction_ts,
            }
        )

    df = pd.DataFrame(records)

    # Deduplicazione preventiva sullo stesso batch prima di Pandera e dell'UPSERT su SQLite:
    # evita errori di violazione del vincolo `unique=True` nello schema se l'API restituisce doppioni
    initial_count = len(df)
    df = df.drop_duplicates(subset=["job_id"], keep="first").reset_index(drop=True)
    if len(df) < initial_count:
        logger.info(f"Rimossi {initial_count - len(df)} annunci duplicati")

    # Data Quality Gate: validazione finale tramite schema Pandera prima della persistenza
    logger.info("Esecuzione validazione Data Quality con Pandera...")
    validated_df = JobPostingSchema.validate(df)
    logger.info(
        f"Trasformazione completata: {len(validated_df)} record validati "
        f"({validated_df['is_python_role'].sum()} ruoli Python individuati)"
    )
    return validated_df
