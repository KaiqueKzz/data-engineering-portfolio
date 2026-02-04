from pathlib import Path
from logger import get_logger

logger = get_logger(__name__)

def load_weather(df, output_path="projects/01_etl_public_data/data/processed/weather.parquet"):
    try:
        logger.info("Starting load to Parquet")

        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        df.to_parquet(path, index=False)

        logger.info(f"Load finished. File saved at: {path.resolve()}")
    except Exception as e:
        logger.exception("Load step failed.")
        raise RuntimeError("Falha ao salvar o Parquet. Verifique permissões e dependências (pyarrow).") from e
