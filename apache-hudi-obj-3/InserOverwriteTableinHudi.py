try:
    from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType
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

db_name = "default"
InventoryPath = f"s3a://global-emart/hudi/database={db_name}/table_name=Inventory"
Order_ItemPath = f"s3a://global-emart/hudi/database={db_name}/table_name=Order_Item"
OrderPath = f"s3a://global-emart/hudi/database={db_name}/table_name=Order"
PaymentPath = f"s3a://global-emart/hudi/database={db_name}/table_name=Payment"
ProductPath = f"s3a://global-emart/hudi/database={db_name}/table_name=Product"
ShipmentPath = f"s3a://global-emart/hudi/database={db_name}/table_name=Shipment"
UserPath = f"s3a://global-emart/hudi/database={db_name}/table_name=User"

# New data for products
data = [
    ('1', 'Laptop', 'High-performance laptop',
     'Electronics', '1200.00', '1001', '2024-05-28'),
    ('2', 'Smartphone', 'Latest model smartphone',
     'Electronics', '800.00', '1002', '2024-05-28'),
    # Add more products as needed
]

columns = ["product_id", "name", "description",
           "category", "price", "seller_id", "listing_date"]
product_df = spark.createDataFrame(data, columns)

# Hudi configuration
hudi_options = {
    'hoodie.table.name': 'product_table',
    'hoodie.datasource.write.recordkey.field': 'product_id',
    'hoodie.datasource.write.precombine.field': 'listing_date',
    'hoodie.datasource.write.operation': 'insert_overwrite_table',
}

# Write to Hudi table
product_df.write.format("hudi").options(
    **hudi_options).mode("overwrite").save(ProductPath)
