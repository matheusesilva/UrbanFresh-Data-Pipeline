from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import logging
from pathlib import Path
from functools import reduce
import os
from datetime import datetime

class ETLPipeline:

    logger = logging.getLogger(__name__)

    def create_spark_session(self):
        spark = SparkSession.builder \
            .appName("UrbanFresh_Data_Pipeline") \
            .config("spark.sql.adaptative.enabled", "true") \
            .getOrCreate()
        return spark

    def extract_sales_data(self, spark, file_path):
        self.logger.info(f"Extracting sales data from {file_path}")

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

            self.logger.info("Sales data extracted successfully.")

            return sales_df

        except Exception as e:
            self.logger.error(f"Error extracting sales data from {file_path}: {e}")
            raise

    def extract_all_data(self, spark):
        data_dir = Path("data/raw")

        try:
            files = [
                file_path
                for file_path in data_dir.glob("*.csv")
                if "orders" in file_path.name.lower()
            ]
            if not files:
                self.logger.warning(f"No CSV files found in {data_dir}")

                raise FileNotFoundError(
                    f"No sales CSV files found in {data_dir.resolve()}"
                )

            self.logger.info(f"Found {len(files)} CSV files in {data_dir}")

            # Use a generator expression to create DataFrames for each sales file and then union them together
            dataframes = (
                self.extract_sales_data(spark, str(file_path))
                for file_path in files
            )
            return reduce(DataFrame.unionByName, dataframes)

        except Exception as e:
            self.logger.error(f"Error during data extraction: {e}")
            raise

    def clean_customer_id(self, df):
        df_clean = df.withColumn(
            "customer_id",
            #Select only rows the are not using the standard format for customer_id
            F.when((~F.col("customer_id").startswith("CUST_")) \
                & (F.col("customer_id").rlike("\\d+")), \
                    F.concat(
                        F.lit("CUST_"), 
                        F.regexp_extract(F.col("customer_id"), "\\d+", 0))) \
            .otherwise(F.col("customer_id"))
        )
        self.logger.info(f"{df.count() - df_clean.count()} records had their customer_id cleaned.")
        return df_clean

    def clean_price_column(self, df):
        df = df.withColumns({
            "unit_price": 
            F.when(
                F.col("price").isNull(),
                F.lit(0.0).cast("double")
            ).otherwise(
                F.regexp_replace(F.col("price"), "[^0-9.]", "")
                .cast("double")
            )
        })

        df = df.withColumns({
            "price_quality_flag":
                F.when(F.col("unit_price") < 0, "CHECK_NEGATIVE_PRICE")
                .when(F.col("unit_price") == 0, "CHECK_ZERO_PRICE")
                .when(F.col("unit_price") > 1000, "CHECK_HIGH_PRICE")
                .otherwise("OK")
        })

        self.logger.info(f"Price column cleaned. {df.filter(F.col('price_quality_flag') != 'OK').count()} records flagged for review.")

        return df.drop("price")

    def standardize_date_column(self, df):
        dt1 = F.to_date(F.col("order_date"), "yyyy/MM/dd")
        dt2 = F.to_date(F.col("order_date"), "MM-dd-yyyy")
        dt3 = F.to_date(F.col("order_date"), "dd-MM-yyyy")
        dt4 = F.to_date(F.col("order_date"), "yyyy-MM-dd")
        dt5 = F.to_date(F.col("order_date"), "MM/dd/yyyy")

        df = df.withColumn(
            "order_date",
            F.coalesce(dt1, dt2, dt3, dt4, dt5)
        )

        self.logger.info(f"{df.filter(F.col('order_date').isNull()).count()} records with unparseable dates.")

        return df

    def remove_test_data(self, df):
        df_cleaned = df.filter(
                ~(
                    F.lower(F.col("customer_id")).contains("test") |
                    F.lower(F.col("product_name")).contains("test") |
                    F.col("order_id").isNull() |
                    F.col("customer_id").isNull()
                )
        )
        self.logger.info(f"{df.count()-df_cleaned.count()} records removed after filtering.")
        return df_cleaned

    def handle_duplicates(self, df):
        df_deduped = df.dropDuplicates(["order_id"])
        self.logger.info(f"{df.count() - df_deduped.count()} duplicate records removed based on order_id.")
        return df_deduped

    def transform_data(self, df):
        return (
            df
            .transform(lambda d: self.clean_customer_id(d))
            .transform(lambda d: self.clean_price_column(d))
            .transform(lambda d: self.standardize_date_column(d))
            .transform(lambda d: self.remove_test_data(d))
            .withColumns({
                "quantity": F.col("quantity").cast(IntegerType()),
                "total_amount": F.format_number(
                        (F.col("unit_price") * F.col("quantity")), 2
                    ).cast(DoubleType()),
                "processing_date": F.current_date(),
                "year": F.year(F.col("order_date")),
                "month": F.month(F.col("order_date"))
            })
        )

    def load_to_csv(self, df, output_path):
        if not os.path.exists(output_path):
            self.logger.info(f"Output directory {output_path} does not exist. Using default path /data/processed/orders.")
            output_path = "data/processed/orders"
            os.makedirs(output_path, exist_ok=True)

        pd_df = df.toPandas()

        try:
            pd_df.to_csv(f"{output_path}/orders.csv", index=False)
            self.logger.info(f"{len(pd_df)} records successfully loaded to {output_path}")
        except Exception as e:
            self.logger.error(f"Error loading data to {output_path}: {e}")
            raise

    def sanity_check_data(self, spark, output_path):
        df = spark.read.csv(f"{output_path}/orders.csv", header=True, inferSchema=True)
        df.createOrReplaceTempView("orders")

        total_records = spark.sql("SELECT COUNT(*) AS total_records FROM orders").collect()[0]["total_records"]
        self.logger.info(f"Sanity check: {total_records} records in the output CSV.")

        zero_price_count = spark.sql("""
                                    SELECT COUNT(*) AS zero_price_count 
                                    FROM orders 
                                    WHERE unit_price = 0
                                    """).collect()[0]["zero_price_count"]
        if zero_price_count > 0:
            self.logger.warning(f"Sanity check: {zero_price_count} records with zero unit price in the output CSV.")

        data_range = spark.sql("""
                                SELECT MIN(order_date) AS min_date, MAX(order_date) AS max_date
                                FROM orders
                                """).collect()[0]
        self.logger.info(f"Sanity check: order date range from {data_range['min_date']} to {data_range['max_date']}.")
        return True

    def create_summary_report(self, spark, output_path):
        df = spark.read.csv(f"{output_path}/orders.csv", header=True, inferSchema=True)
        df.createOrReplaceTempView("orders")

        summary_df = spark.sql("""
            SELECT 
                region,
                year,
                month,
                COUNT(*) AS total_orders,
                SUM(quantity) AS total_quantity,
                ROUND(SUM(total_amount),2) AS total_revenue
            FROM orders
            GROUP BY region, year, month
            ORDER BY year, month, region
        """)
        summary_output_path = f"{output_path}/{datetime.now().strftime('%Y%m%d_%H%M%S')}-summary_report.csv"
        summary_pd_df = summary_df.toPandas()
        try:
            summary_pd_df.to_csv(summary_output_path, index=False)
            self.logger.info(f"Summary report successfully created at {summary_output_path}")
        except Exception as e:
            self.logger.error(f"Error creating summary report at {summary_output_path}: {e}")
            raise

