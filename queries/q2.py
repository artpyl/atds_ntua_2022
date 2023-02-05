from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, row_number, asc, max, month, dayofmonth, hour, struct
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

# Filtering 0 costs
df_taxi = df_taxi.filter(df_taxi["Tolls_amount"] != 0)

# We have noticed that some entries are from 2002 and 2009 so we keep only 2022 records
df_taxi = df_taxi.filter(df_taxi["tpep_pickup_datetime"] >= datetime.datetime(2022,1,1)) \
		 .filter(df_taxi["tpep_pickup_datetime"] < datetime.datetime(2023,1,1))
# Group by month and find max toll amount
max_tolls = df_taxi.groupBy(month("tpep_pickup_datetime")).max("Tolls_amount").collect()
max_tolls = [(max_tolls[i][0],max_tolls[i][1]) for i in range(len(max_tolls))]

df_taxi = df_taxi.withColumn("month", month(df_taxi["tpep_pickup_datetime"]))

# Get all columns except for month
taxi_cols = df_taxi.columns
taxi_cols.remove("month")


# Append each result to result list
result = []
for tuple in max_tolls:
	result.append(df_taxi.filter(df_taxi["month"] == tuple[0]).filter(df_taxi["Tolls_amount"] == tuple[1]).select(taxi_cols).collect())
result = [result[i][0] for i in range(len(result))]

# Create a dataframe from result
res = spark.createDataFrame(result)

# Represent LocationIDs by true names
df_taxi_zone = res.join(df_zone.select("LocationID","Zone"), res.PULocationID == df_zone.LocationID, how = "inner")
df_taxi_zone = df_taxi_zone.withColumnRenamed("Zone","Start").drop("LocationID")

df_taxi_zone = df_taxi_zone.join(df_zone.select("LocationID","Zone"), df_taxi_zone.DOLocationID == df_zone.LocationID, how = "inner")
df_taxi_zone = df_taxi_zone.withColumnRenamed("Zone","Destination").drop("LocationID")

# Write result as csv
df_taxi_zone.write.mode('overwrite').option('header','true').csv('hdfs://master:9000/user/atds/taxi_files/results/q2')

t2 = time.time()

# Print time difference
print(f"Time elapsed {t2-t1}s")
