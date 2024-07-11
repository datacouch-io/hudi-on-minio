import os
import sys
from pyspark.sql import SparkSession

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

# Define Hudi-specific options for controlling data file sizing
hudi_options_sizing = {
    'hoodie.table.name': 'Inventory',
    'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
    'hoodie.datasource.write.table.name': 'Inventory',
    'hoodie.datasource.write.operation': 'insert',
    'hoodie.parquet.small.file.limit': '67108864',  # 64 MB
    'hoodie.parquet.max.file.size': '1073741824'    # 1 GB
}

# Write to Hudi with data file sizing configuration


def write_to_hudi_sizing():
    # Load your DataFrame
    # df = ...
    df = spark.read.option(
        "header", "true").csv('DataSets/Payment.csv')
    # Write the DataFrame to Hudi with sizing configuration
    df.write.format("hudi").options(
        **hudi_options_sizing).mode("append").save("s3://global-emart/hudi/database=default/table_name=PaymentFileSized")

    # Print success message
    print("Data written to Hudi with data file sizing configuration.")


# Call function to write to Hudi
write_to_hudi_sizing()

# Close the Spark session
spark.stop()
