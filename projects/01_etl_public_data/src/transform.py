import pandas as pd
from logger import get_logger

logger = get_logger(__name__)

def transform_weather(df: pd.DataFrame) -> pd.DataFrame:
    try:
        logger.info("Starting transform")

        df = df.copy()
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
        df["temperature"] = pd.to_numeric(df["temperature"], errors="coerce")

        before = len(df)
        df = df.dropna(subset=["datetime", "temperature"])
        after = len(df)

        logger.info(f"Transform finished. Dropped rows: {before - after}. Final rows: {after}")
        return df

    except Exception as e:
        logger.exception("Transform step failed.")
        raise RuntimeError("Falha na transformação dos dados. Verifique o formato das colunas.") from e
