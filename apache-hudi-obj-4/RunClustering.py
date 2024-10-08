try:
    import os  # Operating system interface
    import sys  # System-specific parameters and functions
    import pyspark  # Apache Spark Python API
    from pyspark.sql import SparkSession  # Spark SQL session

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

# Function to write a Spark DataFrame to Hudi with optional clustering


def write_to_hudi(spark_df, table_name, db_name, method='insert', table_type='COPY_ON_WRITE'):
    # Define the path for the Hudi table in S3
    path = f"s3a://global-emart/hudi/database={db_name}/table_name={table_name}"

    # Define Hudi-specific options for writing the data
    hudi_options = {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.table.type': table_type,
        'hoodie.datasource.write.operation': method,
        "hoodie.datasource.hive_sync.database": db_name,
        "hoodie.datasource.hive_sync.metastore.uris": "thrift://localhost:9083",
        "hoodie.datasource.hive_sync.mode": "hms",
        "hoodie.datasource.hive_sync.enable": "true",

        # clustering options
        "hoodie.clustering.inline": "true",
        "hoodie.clustering.inline.max.commits": "4",
        "hoodie.clustering.plan.strategy.target.file.max.bytes": "1073741824",
        "hoodie.clustering.plan.strategy.small.file.limit": "629145600",
        "hoodie.clustering.plan.strategy.sort.columns": "payment_method",
    }

    # Print the path for debugging purposes
    print("\n")
    print(path)
    print("\n")

    # Write the DataFrame to Hudi
    spark_df.write.format("hudi").options(
        **hudi_options).mode("append").save(path)


# Read the Payments.csv file into a DataFrame
Payment_spark_df = spark.read.option(
    "header", "true").csv('DataSets/Payment.csv')

# Write Payments.csv to Hudi with clustering on 'payment_method'
write_to_hudi(spark_df=Payment_spark_df,
              db_name="default", table_name="PaymentClustered")

print("\n Payments data has been uploaded to the Minio bucket as a Hudi table with clustering")
