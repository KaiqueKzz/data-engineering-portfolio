import great_expectations as ge
from logger import get_logger

logger = get_logger(__name__)

SILVER_PATH = "projects/01_etl_public_data/lake/silver/weather_silver.parquet"

def run_quality_checks():
    try:
        logger.info("Starting data quality checks (Great Expectations)")

        df = ge.read_parquet(SILVER_PATH)

        # Expectations
        results = []
        results.append(df.expect_table_row_count_to_be_between(min_value=1))
        results.append(df.expect_column_values_to_not_be_null("datetime"))
        results.append(df.expect_column_values_to_not_be_null("temperature"))
        results.append(
            df.expect_column_values_to_be_between(
                "temperature", min_value=-10, max_value=50
            )
        )

        if not all(r.success for r in results):
            raise RuntimeError("One or more data quality checks failed")

        logger.info("All data quality checks passed")

    except Exception:
        logger.exception("Data quality validation failed")
        raise


if __name__ == "__main__":
    run_quality_checks()
