from etl_pipeline import create_spark_session, extract_all_data, transform_data, load_to_csv, sanity_check_data
from pyspark.sql import SparkSession
import logging
import sys

def main():
    start_time = datetime.now()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logging.info("Starting the ETL pipeline.")
    
    try:  
        spark = create_spark_session()
        extracted_df = extract_all_data(spark)
        transformed_df = transform_data(extracted_df)
        load_to_csv(transformed_df, "data/processed/orders")
        sanity_check_data(spark, "data/processed/orders")
        create_summary_report(spark, "data/processed/orders")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        sys.exit(1)
    finally:
        spark.stop()
        end_time = datetime.now()
        logging.info(f"ETL pipeline completed in {end_time - start_time}.")
if __name__ == "__main__":
    main()