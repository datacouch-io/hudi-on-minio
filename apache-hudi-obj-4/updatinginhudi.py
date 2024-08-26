try:
    import os  # Operating system interface
    import sys  # System-specific parameters and functions
    import uuid  # Universal Unique Identifier
    import pyspark  # Apache Spark Python API
    import datetime  # Date and time manipulation
    from pyspark.sql import SparkSession  # Spark SQL session
    from pyspark import SparkConf, SparkContext  # Spark configuration and context
    from faker import Faker  # Data generation library
    import datetime  # (Duplicate) Date and time manipulation
    from datetime import datetime  # Date and time class
    import random  # Generate pseudo-random numbers
    import pandas as pd  # Pandas library for data manipulation
    from pyspark.sql import SparkSession, Row

    print("Imports loaded ")

except Exception as e:
    print("error", e)  # Print any errors that occur during import

# Constants for Hudi and Spark versions
HUDI_VERSION = '0.14.0'
SPARK_VERSION = '3.4'

# Setting up PySpark environment variables
SUBMIT_ARGS = f"--packages org.apache.hudi:hudi-spark{SPARK_VERSION}-bundle_2.12:{HUDI_VERSION},org.apache.hadoop:hadoop-aws:3.3.2 pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
os.environ['PYSPARK_PYTHON'] = sys.executable

# Initialize Spark session with specific configurations
spark = SparkSession.builder \
    .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
    .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
    .config('className', 'org.apache.hudi') \
    .config('spark.sql.hive.convertMetastoreParquet', 'false') \
    .getOrCreate()

# Configure Spark session to connect to a local S3-compatible service
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://localhost:9000/")
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "admin")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "password")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
spark._jsc.hadoopConfiguration().set(
    "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider",
                                     "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("DataSets/Inventory.csv", header=True, inferSchema=True)


# Load the existing Hudi table
existing_df = spark.read.format("hudi").load(
    "s3a://global-emart/hudi/database=default/table_name=Inventory")

# Create a DataFrame with the updated rows
updated_rows = [
    Row(inventory_id=1, product_id=285, quantity_available=41,
        last_update_date='2023-04-16 04:13:31'),
    Row(inventory_id=2, product_id=4467, quantity_available=60,
        last_update_date='2023-02-04 11:13:24'),
    Row(inventory_id=3, product_id=1533, quantity_available=31,
        last_update_date='2023-03-28 16:11:23')
]

updated_df = spark.createDataFrame(updated_rows)

# Define Hudi options for upsert
hudi_options = {
    "hoodie.table.name": "inventory_table",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",  # or "MERGE_ON_READ"
    "hoodie.datasource.write.recordkey.field": "inventory_id",
    "hoodie.datasource.write.precombine.field": "last_update_date",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.upsert.shuffle.parallelism": 2,
    "hoodie.insert.shuffle.parallelism": 2
}

# Upsert the updated DataFrame
updated_df.write.format("hudi") \
    .options(**hudi_options) \
    .mode("append") \
    .save("path/to/hudi/table")
