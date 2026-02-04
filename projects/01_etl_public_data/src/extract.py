import requests
import pandas as pd
from logger import get_logger

logger = get_logger(__name__)

def extract_weather():
    try:
        logger.info("Starting extract from Open-Meteo API")

        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": -23.55,   # São Paulo
            "longitude": -46.63,
            "hourly": "temperature_2m",
            "timezone": "America/Sao_Paulo"
        }

        response = requests.get(url, params=params, timeout=20)
        response.raise_for_status()

        data = response.json()

        df = pd.DataFrame({
            "datetime": data["hourly"]["time"],
            "temperature": data["hourly"]["temperature_2m"]
        })

        logger.info(f"Extract finished successfully. Rows: {len(df)}")
        return df

    except requests.exceptions.RequestException as e:
        logger.exception("API request failed (network/timeout/http).")
        raise RuntimeError("Falha ao consultar a API Open-Meteo. Verifique sua conexão e tente novamente.") from e
    except KeyError as e:
        logger.exception("Unexpected API response format.")
        raise RuntimeError("A API retornou um formato inesperado. Ajuste o parser do extract.") from e
