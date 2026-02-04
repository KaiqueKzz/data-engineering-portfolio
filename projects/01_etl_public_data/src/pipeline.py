from logger import get_logger
from extract import extract_weather
from transform import transform_weather
from load_bronze import load_bronze
from load_silver import load_silver
from load_gold import load_gold
from duckdb_queries import run_queries

logger = get_logger("pipeline")

def run():
    logger.info("=== PIPELINE START ===")

    # BRONZE
    df_bronze = extract_weather()
    load_bronze(df_bronze)

    # SILVER
    df_silver = transform_weather(df_bronze)
    load_silver(df_silver)

    # GOLD (camada analítica)
    load_gold(df_silver)

    # SQL Analytics
    run_queries()

    logger.info("=== PIPELINE END (SUCCESS) ===")

if __name__ == "__main__":
    try:
        run()
    except Exception:
        logger.exception("=== PIPELINE END (FAILED) ===")
        raise
