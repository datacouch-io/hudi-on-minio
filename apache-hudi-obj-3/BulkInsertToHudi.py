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
    .config('spark.kryo.registrator', 'org.apache.spark.HoodieSparkKryoRegistrar ') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.hudi.catalog.HoodieCatalog') \
    .config('className', 'org.apache.hudi') \
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


schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("order_time", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("total_amount", StringType(), True)
])

# Create DataFrame with the explicit schema
order_data = [
    ('5001', '2024-05-29', '11:00:00', '1002', 200.00),
    ('5002', '2024-05-29', '11:05:00', '1003', 350.00),
    # Add more orders as needed
]
order_df = spark.createDataFrame(order_data, schema)
order_df.printSchema()


def bulk_insert_to_hudi(spark_df, table_name, db_name, method='bulk_insert', table_type='COPY_ON_WRITE'):
    path = f"s3a://global-emart/hudi/database={db_name}/table_name={table_name}"

    hudi_options = {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.table.type': table_type,
        'hoodie.datasource.write.operation': method,
        'hoodie.datasource.write.recordkey.field': 'order_id',
        'hoodie.datasource.write.precombine.field': 'order_date',
        'hoodie.datasource.write.partitionpath.field': '',  # Add partition field if needed

        'hoodie.datasource.hive_sync.database': db_name,
        'hoodie.datasource.hive_sync.metastore.uris': "thrift://localhost:9083",
        'hoodie.datasource.hive_sync.mode': "hms",
        'hoodie.datasource.hive_sync.enable': "true",

        'hoodie.bulkinsert.sort.mode': 'None'
    }

    print("\n")
    print(path)
    print("\n")

    spark_df.write.format("hudi"). \
        options(**hudi_options). \
        mode("append"). \
        save(path)


# Call the bulk_insert_to_hudi function with the correct DataFrame
try:
    bulk_insert_to_hudi(
        spark_df=order_df,
        db_name="default",
        table_name="Order"
    )
    print("bulk Inserted")
except Exception as e:
    print("error", e)
