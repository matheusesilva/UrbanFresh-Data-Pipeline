from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import logging
from pathlib import Path
from functools import reduce

logger = logging.getLogger(__name__)

def create_spark_session():
    spark = SparkSession.builder \
        .appName("UrbanFresh_Data_Pipeline") \
        .config("spark.sql.adaptative.enabled", "true") \
        .getOrCreate()
    return spark

def extract_sales_data(spark, file_path):
    logger.info(f"Extracting sales data from {file_path}")
    expected_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("price", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("region", StringType(), True)
    ])
    try:
        sales_df = spark.read.schema(expected_schema) \
            .csv(file_path, header=True, mode="PERMISSIVE")
        logger.info("Sales data extracted successfully.")
        return sales_df
    except Exception as e:
        logger.error(f"Error extracting sales data from {file_path}: {e}")
        raise

def extract_all_data(spark):
    data_dir = Path("data/raw")
    
    try:
        files = [
            file_path
            for file_path in data_dir.glob("*.csv")
            if "sales" in file_path.name.lower()
        ]
        if not files:
            logger.warning(f"No CSV files found in {data_dir}")

            # Return empty DataFrame with no schema if no files are found
            return spark.createDataFrame([], StructType([]))
        
        logger.info(f"Found {len(files)} CSV files in {data_dir}")
    
        # Use a generator expression to create DataFrames for each sales file and then union them together
        dataframes = (
            extract_sales_data(spark, str(file_path))
            for file_path in files
            if "sales" in file_path.name.lower()
        )
    except Exception as e:
        logger.error(f"Error during data extraction: {e}")
        raise

    return reduce(DataFrame.unionByName, dataframes)