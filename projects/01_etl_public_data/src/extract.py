import json
import requests
import pandas as pd
from pathlib import Path
from logger import get_logger

logger = get_logger(__name__)

STATE_PATH = Path("projects/01_etl_public_data/state/state.json")

def _load_state():
    if STATE_PATH.exists():
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"last_datetime": None}

def _save_state(last_datetime):
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump({"last_datetime": last_datetime}, f)

def extract_weather():
    try:
        logger.info("Starting incremental extract")

        state = _load_state()
        last_dt = state.get("last_datetime")

        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": -23.55,
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

        df["datetime"] = pd.to_datetime(df["datetime"])

        if last_dt:
            last_dt = pd.to_datetime(last_dt)
            df = df[df["datetime"] > last_dt]
            logger.info(f"Filtered incremental data. Rows after filter: {len(df)}")

        if not df.empty:
            _save_state(df["datetime"].max().isoformat())

        logger.info("Incremental extract finished")
        return df

    except Exception as e:
        logger.exception("Incremental extract failed")
        raise
