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


hudi_options = {
    "hoodie.table.name": "inventory_table",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",  # or "MERGE_ON_READ"
    "hoodie.datasource.write.recordkey.field": "inventory_id",
    # No partitioning for this example
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.write.precombine.field": "last_update_date",
    "hoodie.datasource.write.operation": "upsert",  # Use "insert" for initial load
    "hoodie.upsert.shuffle.parallelism": 2,
    "hoodie.insert.shuffle.parallelism": 2,
    "hoodie.datasource.hive_sync.database": "default",
    "hoodie.datasource.hive_sync.table": "Inventory",
    "hoodie.datasource.hive_sync.metastore.uris": "thrift://localhost:9083",
    "hoodie.datasource.hive_sync.mode": "hms",
    "hoodie.datasource.hive_sync.enable": "true",
}

df.write.format("hudi") \
    .options(**hudi_options) \
    .mode("overwrite") \
    .save("s3a://global-emart/hudi/database=default/table_name=Inventory")
