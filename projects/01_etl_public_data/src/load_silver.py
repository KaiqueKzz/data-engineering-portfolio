from pathlib import Path
from logger import get_logger

logger = get_logger(__name__)

def load_silver(df):
    path = Path("projects/01_etl_public_data/lake/silver/weather_silver.parquet")
    path.parent.mkdir(parents=True, exist_ok=True)

    logger.info("Saving SILVER data")
    df.to_parquet(path, index=False)
