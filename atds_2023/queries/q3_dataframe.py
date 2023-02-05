from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, desc, row_number, asc, max, month, dayofmonth, hour,window
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

# We have noticed that some entries are from 2002 and 2009 so we keep only 2022 records
df_taxi_zone = df_taxi_zone.filter(df_taxi_zone["tpep_pickup_datetime"] >= datetime.datetime(2022,1,1)) \
                 .filter(df_taxi_zone["tpep_pickup_datetime"] < datetime.datetime(2023,1,1))

# Filter trips based on Destination being different than Start
df_taxi_zone = df_taxi_zone.filter(df_taxi_zone.Destination != df_taxi_zone.Start)

# Groupby Window of 15 days and calculate average trip distance and total amount 
result = df_taxi_zone.groupBy(window("tpep_pickup_datetime", "15 days", startTime = "3 days").alias("Interval_Start")) \
		     .agg((sum("Trip_distance") / count("Trip_distance")).alias("Average_Trip_Distance") \
		     ,(sum("Total_amount") / count("Total_amount")).alias("Average_Total_Amount"))

result = result.orderBy(col("Interval_Start").asc()).select(col("Interval_Start.start").alias("Interval_Start"),"Average_Trip_Distance","Average_Total_Amount")
result.show()
#Save results
result.write.mode('overwrite').option('header','true').csv('hdfs://master:9000/user/atds/taxi_files/results/q3')

t2 = time.time()

# Print time difference
print(f"Time elapsed {t2-t1}s")
