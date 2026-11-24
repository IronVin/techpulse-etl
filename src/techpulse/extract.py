"""
Modulo di Estrazione (Fase Extract) della pipeline TechPulse ETL.

Si occupa esclusivamente dell'interazione con la sorgente dati esterna (API REST Arbeitnow):
gestisce la connessione HTTP, i timeout, gli errori di rete/stato e verifica la struttura
minima del payload JSON prima di passarlo alla fase di trasformazione.
"""

import logging
from typing import Any

# Uso httpx anziché requests perché supporta nativamente HTTP/2, offre una gestione
# granulare dei timeout ed è lo standard moderno nell'ecosistema Python (anche per FastAPI)
import httpx

from techpulse.config import API_URL, REQUEST_TIMEOUT

logger = logging.getLogger("techpulse.extract")


# Parametrizzo url e timeout con valori di default da config: rende la funzione
# deterministica e facile da testare in Pytest passando URL mock o timeout ridotti
def fetch_jobs(
    url: str = API_URL, timeout: float = REQUEST_TIMEOUT
) -> list[dict[str, Any]]:
    """
    Scarica gli annunci di lavoro dall'API pubblica.

    Args:
        url: Endpoint dell'API da interrogare.
        timeout: Tempo massimo di attesa in secondi.

    Returns:
        Lista di dizionari rappresentanti i singoli annunci grezzi (JSON deserializzato).

    Raises:
        httpx.HTTPError: Se la chiamata di rete fallisce o restituisce errore HTTP.
        ValueError: Se il payload JSON non contiene la chiave attesa 'data'.
    """
    logger.info(f"Avvio estrazione dati da {url}")

    # Imposto un User-Agent identificativo e dichiaro esplicitamente Accept: application/json
    # per rispettare le best practice delle API REST ed evitare blocchi lato server
    headers = {
        "User-Agent": "TechPulse-ETL/0.1 (Portfolio Project)",
        "Accept": "application/json",
    }

    try:
        # Uso il context manager (`with httpx.Client`) per garantire la chiusura pulita
        # del pool di connessioni/socket anche in caso di eccezione, evitando resource leak
        with httpx.Client(timeout=timeout, headers=headers) as client:
            response = client.get(url)
            # Converto subito gli status code 4xx/5xx in eccezioni Python esplicite
            # anziché proseguire con il parsing di una pagina HTML di errore
            response.raise_for_status()
    except httpx.TimeoutException:
        # Distinguo le eccezioni di rete per avere diagnosi chiare nei log:
        # qui il server è lento o irraggiungibile entro la soglia impostata
        logger.error(f"Timeout superato ({timeout}s) durante la connessione a {url}")
        raise
    except httpx.HTTPStatusError as exc:
        # Errore applicativo lato API (es. 404 Not Found, 429 Rate Limit, 500 Server Error)
        logger.error(f"Errore HTTP {exc.response.status_code} ricevuto dall'API")
        raise
    except httpx.RequestError as exc:
        # Catch-all per errori di trasporto a basso livello (es. DNS fallito, assenza di rete)
        logger.error(f"Errore di rete durante la richiesta: {exc}")
        raise

    # Deserializzo il corpo della risposta in strutture native Python (dict/list),
    # mantenendo questo modulo indipendente da Pandas (Extract disaccoppiato da Transform)
    payload = response.json()

    # Controllo difensivo (Defensive Programming) sulla struttura radice del JSON:
    # verifico che esista la chiave 'data' e che sia una lista prima di toccare i singoli record
    if "data" not in payload or not isinstance(payload["data"], list):
        logger.error(
            "Formato risposta API inatteso: chiave 'data' mancante o non valida"
        )
        raise ValueError("Struttura JSON dell'API non valida")

    jobs = payload["data"]
    logger.info(f"Estrazione completata con successo: {len(jobs)} annunci recuperati")
    return jobs
    return jobs
