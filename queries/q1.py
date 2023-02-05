from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, row_number, asc, max, month, dayofmonth, hour
import os
import sys
import datetime
import time

from base import df_taxi, df_zone

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ["SPARK_HOME"] = "/home/atds/spark-3.1.3-bin-hadoop2.7"

spark = SparkSession.builder.master("spark://10.0.2.15:7077").getOrCreate()

t1 = time.time()

# Join the two dataframes into one based on (Destination) LocationID
df_taxi_zone = df_taxi.join(df_zone.select("LocationID","Zone"), df_taxi.PULocationID == df_zone.LocationID, how = "inner")
df_taxi_zone = df_taxi_zone.withColumnRenamed("Zone","Start").drop("LocationID")

df_taxi_zone = df_taxi_zone.join(df_zone.select("LocationID","Zone"), df_taxi_zone.DOLocationID == df_zone.LocationID, how = "inner")
df_taxi_zone = df_taxi_zone.withColumnRenamed("Zone","Destination").drop("LocationID")

# Filter trips based on Destination
df_taxi_zone = df_taxi_zone.filter(df_taxi_zone.Destination == "Battery Park")

# Filter trips based on Date
df_taxi_zone = df_taxi_zone.filter(df_taxi_zone.tpep_pickup_datetime >= datetime.datetime(2022,3,1)) \
					.filter(df_taxi_zone.tpep_pickup_datetime < datetime.datetime(2022,4,1,0,0))


# Find max tip
max_tip = df_taxi_zone.select(max("Tip_amount")).collect()[0][0]

# Get trip with max tip
df_taxi_zone = df_taxi_zone.filter(df_taxi_zone["Tip_amount"] == max_tip)

# Write result as csv
df_taxi_zone.write.mode('overwrite').option('header','true').csv('hdfs://master:9000/user/atds/taxi_files/results/q1')

t2 = time.time()

# Print time difference
print(f"Time elapsed {t2-t1}s")
