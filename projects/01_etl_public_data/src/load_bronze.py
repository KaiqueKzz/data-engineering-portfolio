from pathlib import Path
from logger import get_logger

logger = get_logger(__name__)

def load_bronze(df):
    path = Path("projects/01_etl_public_data/lake/bronze/weather_bronze.parquet")
    path.parent.mkdir(parents=True, exist_ok=True)

    logger.info("Saving BRONZE data")
    df.to_parquet(path, index=False)
