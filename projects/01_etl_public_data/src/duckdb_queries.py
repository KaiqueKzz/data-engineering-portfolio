import duckdb
from pathlib import Path
from logger import get_logger

logger = get_logger(__name__)

DB_PATH = "projects/01_etl_public_data/data/processed/weather.duckdb"
PARQUET_PATH = "projects/01_etl_public_data/data/processed/weather.parquet"

def run_queries():
    try:
        logger.info("Starting DuckDB analytics")

        Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(DB_PATH)

        con.execute(f"""
            CREATE OR REPLACE TABLE weather AS
            SELECT * FROM read_parquet('{PARQUET_PATH}')
        """)

        result = con.execute("""
            SELECT
                DATE(datetime) AS date,
                ROUND(AVG(temperature), 2) AS avg_temperature
            FROM weather
            GROUP BY date
            ORDER BY date
        """).fetchdf()

        logger.info("DuckDB query executed successfully")
        logger.info(f"Query result rows: {len(result)}")
        print("\n📊 Média de temperatura por dia:")
        print(result)

        con.close()
    except Exception as e:
        logger.exception("DuckDB step failed.")
        raise RuntimeError("Falha ao executar análises no DuckDB. Verifique o arquivo Parquet e o caminho.") from e
