from logger import get_logger
from extract import extract_weather
from transform import transform_weather
from load import load_weather
from duckdb_queries import run_queries

logger = get_logger("pipeline")

def run():
    logger.info("=== PIPELINE START ===")

    df = extract_weather()
    df = transform_weather(df)
    load_weather(df)
    run_queries()

    logger.info("=== PIPELINE END (SUCCESS) ===")

if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        logger.exception("=== PIPELINE END (FAILED) ===")
        raise
