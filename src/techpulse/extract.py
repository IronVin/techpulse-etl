import logging
from typing import Any

import httpx

from techpulse.config import API_URL, REQUEST_TIMEOUT

logger = logging.getLogger("techpulse.extract")


def fetch_jobs(
    url: str = API_URL, timeout: float = REQUEST_TIMEOUT
) -> list[dict[str, Any]]:
    """
    Scarica gli annunci di lavoro dall'API pubblica.

    Args:
        url: Endpoint dell'API da interrogare.
        timeout: Tempo massimo di attesa in secondi.

    Returns:
        Lista di dizionari rappresentanti i singoli annunci grezzi.

    Raises:
        httpx.HTTPError: Se la chiamata di rete fallisce o restituisce errore HTTP.
        ValueError: Se il payload JSON non contiene la chiave attesa 'data'.
    """
    logger.info(f"Avvio estrazione dati da {url}")

    headers = {
        "User-Agent": "TechPulse-ETL/0.1 (Portfolio Project)",
        "Accept": "application/json",
    }

    try:
        with httpx.Client(timeout=timeout, headers=headers) as client:
            response = client.get(url)
            response.raise_for_status()  # Solleva eccezione se status code >= 400
    except httpx.TimeoutException:
        logger.error(f"Timeout superato ({timeout}s) durante la connessione a {url}")
        raise
    except httpx.HTTPStatusError as exc:
        logger.error(f"Errore HTTP {exc.response.status_code} ricevuto dall'API")
        raise
    except httpx.RequestError as exc:
        logger.error(f"Errore di rete durante la richiesta: {exc}")
        raise

    payload = response.json()

    if "data" not in payload or not isinstance(payload["data"], list):
        logger.error(
            "Formato risposta API inatteso: chiave 'data' mancante o non valida"
        )
        raise ValueError("Struttura JSON dell'API non valida")

    jobs = payload["data"]
    logger.info(f"Estrazione completata con successo: {len(jobs)} annunci recuperati")
    return jobs
