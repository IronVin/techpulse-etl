# TechPulse ETL

Pipeline ETL automatizzata in Python per l'estrazione, la pulizia, la validazione e la storicizzazione giornaliera delle offerte di lavoro in ambito tecnologico.

## Architettura e Flusso Dati

```text
[Arbeitnow Public API]
         │
         ▼  (httpx + error handling & logging)
   1. EXTRACT  ──► Dati grezzi JSON
         │
         ▼  (Pandas + Regex Skill Extraction + Pandera Schema)
   2. TRANSFORM ──► Pulizia HTML, feature engineering & Data Quality check
         │
         ▼  (SQLite Upsert + PyArrow)
   3. LOAD      ──► data/jobs.db (storico idempotente) + data/latest_jobs.parquet
```

## Caratteristiche Principali
* **Ingestion resiliente (`extract.py`):** Gestione esplicita di timeout, errori HTTP e payload malformati con logging strutturato.
* **Feature Engineering (`transform.py`):** Rimozione tag HTML, normalizzazione date UTC ed estrazione automatica delle tecnologie richieste (`Python`, `SQL`, `Docker`, `AWS`, `FastAPI`, `Django`, `Pandas`, `PySpark`, `Git`, `Linux`).
* **Data Quality Contract (`Pandera`):** Validazione rigorosa di tipi, unicità degli ID e formato degli URL prima del salvataggio per prevenire la corruzione dello storico.
* **Storage Idempotente (`load.py`):** Storicizzazione su **SQLite** tramite `INSERT OR IGNORE` (nessun duplicato anche in caso di esecuzioni multiple) ed esportazione in formato **Parquet** per analisi ad alte prestazioni.
* **Automazione CI/CD (`GitHub Actions`):** Esecuzione automatica di linting (`Ruff`) e test isolati (`Pytest` con mock di rete) ad ogni push, più schedulazione giornaliera della pipeline.

## Struttura del Progetto

```text
techpulse-etl/
├── .github/workflows/   # Pipeline CI (test) e CD (esecuzione giornaliera ETL)
├── data/                # Database SQLite storicizzato e dataset Parquet
├── src/techpulse/       # Codice sorgente modulare (config, extract, transform, load, main)
├── tests/               # Test unitari e di integrazione con Pytest
└── pyproject.toml       # Configurazione pacchetto, dipendenze, Ruff e Pytest
```

## Installazione ed Esecuzione Locale

1. **Clona il repository e attiva l'ambiente virtuale:**
   ```bash
   git clone https://github.com/IronVin/techpulse-etl.git
   cd techpulse-etl
   python -m venv .venv
   # Su Windows:
   .venv\Scripts\Activate.ps1
   # Su Linux/macOS:
   source .venv/bin/activate
   ```

2. **Installa il pacchetto e le dipendenze di sviluppo:**
   ```bash
   pip install -e ".[dev]"
   ```

3. **Esegui la pipeline ETL:**
   ```bash
   python -m techpulse.main
   ```

4. **Lancia i test e il linter:**
   ```bash
   pytest -v
   ruff check .
   ```