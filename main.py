from src.etl_pipeline import ETLPipeline
from pyspark.sql import SparkSession
from datetime import datetime
import logging
import sys
import argparse

def main(logging_level="INFO"):
    start_time = datetime.now()
    logging.basicConfig(
        filename="logs/etl_pipeline.log",
        level=getattr(logging, logging_level.upper(), logging.INFO), 
        format="%(asctime)s - %(levelname)s - %(message)s"
        )
    logging.info("Starting the ETL pipeline.")
    
    try:  
        etl1 = ETLPipeline()
        spark = etl1.create_spark_session()
        extracted_df = etl1.extract_all_data(spark)
        transformed_df = etl1.transform_data(extracted_df)
        etl1.load_to_csv(transformed_df, "data/processed/orders")
        etl1.sanity_check_data(spark, "data/processed/orders")
        etl1.create_summary_report(spark, "data/processed/orders")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        sys.exit(1)
    finally:
        spark.stop()
        end_time = datetime.now()
        logging.info(f"ETL pipeline completed in {end_time - start_time}.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the ETL pipeline for UrbanFresh Data.")
    parser.add_argument("--log_level", type=str, default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)")
    args = parser.parse_args()

    main(logging_level=args.log_level)