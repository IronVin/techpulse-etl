from pathlib import Path

import pandas as pd
import streamlit as st

from techpulse.config import PARQUET_PATH

st.set_page_config(
    page_title="TechPulse ETL | Job Market Dashboard",
    page_icon="📊",
    layout="wide",
)


@st.cache_data(ttl=3600)
def load_jobs_data(parquet_path: Path) -> pd.DataFrame:
    """Carica il dataset Parquet storicizzato."""
    if not parquet_path.exists():
        return pd.DataFrame()
    return pd.read_parquet(parquet_path)


def main() -> None:
    st.title("📊 TechPulse ETL – Tech Job Market Dashboard")
    st.markdown(
        "Analisi interattiva delle offerte di lavoro tech raccolte e validate "
        "automaticamente dalla pipeline **TechPulse ETL**."
    )

    df = load_jobs_data(PARQUET_PATH)

    if df.empty:
        st.warning(
            "Nessun dato trovato in `data/latest_jobs.parquet`. "
            "Esegui prima `python -m techpulse.main` per popolare il dataset."
        )
        return

    # --- Sidebar: Filtri ---
    st.sidebar.header("🔍 Filtri di Ricerca")

    only_python = st.sidebar.checkbox("Solo ruoli Python", value=False)
    only_remote = st.sidebar.checkbox("Solo lavoro da remoto", value=False)
    search_query = st.sidebar.text_input(
        "Cerca nel titolo o azienda", placeholder="es. Data, Backend..."
    )

    filtered_df = df.copy()
    if only_python:
        filtered_df = filtered_df[filtered_df["is_python_role"]]
    if only_remote:
        filtered_df = filtered_df[filtered_df["remote"]]
    if search_query:
        mask = filtered_df["title"].str.contains(search_query, case=False, na=False) | (
            filtered_df["company_name"].str.contains(search_query, case=False, na=False)
        )
        filtered_df = filtered_df[mask]

    # --- KPI Principali ---
    col1, col2, col3, col4 = st.columns(4)
    total_jobs = len(filtered_df)
    python_jobs = int(filtered_df["is_python_role"].sum()) if total_jobs else 0
    remote_pct = (filtered_df["remote"].mean() * 100) if total_jobs else 0.0
    last_update = df["extracted_at"].max() if "extracted_at" in df.columns else "N/D"

    col1.metric("Annunci Filtrati", f"{total_jobs}")
    col2.metric("Ruoli Python", f"{python_jobs}")
    col3.metric("Offerte Remote", f"{remote_pct:.1f}%")
    col4.metric("Ultimo Aggiornamento (UTC)", last_update.split(" ")[0])

    st.divider()

    # --- Grafici ---
    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        st.subheader("🛠️ Top Tecnologie Richieste")
        skills_series = (
            filtered_df["matched_skills"]
            .loc[filtered_df["matched_skills"] != "None"]
            .str.split(", ")
            .explode()
        )
        if not skills_series.empty:
            skill_counts = skills_series.value_counts().sort_values(ascending=True)
            st.bar_chart(skill_counts, horizontal=True)
        else:
            st.info("Nessuna skill estratta per i filtri selezionati.")

    with chart_col2:
        st.subheader("🌍 Top Località")
        if not filtered_df.empty:
            top_locations = (
                filtered_df["location"]
                .value_counts()
                .head(8)
                .sort_values(ascending=True)
            )
            st.bar_chart(top_locations, horizontal=True)
        else:
            st.info("Nessuna località da mostrare.")

    st.divider()

    # --- Tabella Interattiva ---
    st.subheader("📋 Elenco Annunci Validati")
    display_cols = [
        "published_date",
        "title",
        "company_name",
        "location",
        "remote",
        "matched_skills",
        "url",
    ]
    st.dataframe(
        filtered_df[display_cols],
        use_container_width=True,
        hide_index=True,
        column_config={
            "published_date": "Data",
            "title": "Ruolo",
            "company_name": "Azienda",
            "location": "Sede",
            "remote": st.column_config.CheckboxColumn("Remoto"),
            "matched_skills": "Skill Rilevate",
            "url": st.column_config.LinkColumn(
                "Link Annuncio", display_text="Apri offerta"
            ),
        },
    )


if __name__ == "__main__":
    main()
