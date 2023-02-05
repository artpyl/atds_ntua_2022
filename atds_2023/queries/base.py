from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, row_number, asc, max, month, dayofmonth, hour
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ["SPARK_HOME"] = "/home/atds/spark-3.1.3-bin-hadoop2.7"

spark = SparkSession.builder.master("spark://10.0.2.15:7077").getOrCreate()
print("spark session created")

# Read dataframes
df_taxi = spark.read.option("header", "true").option("inferSchema", "true").parquet("hdfs://master:9000/user/atds/taxi_files/tripdata/")
df_zone = spark.read.option("header", "true").option("inferSchema", "true").csv("hdfs://master:9000/user/atds/taxi_files/taxi+_zone_lookup.csv")

# Convert to RDD
rdd_taxi = df_taxi.rdd
rdd_zone = df_zone.rdd
