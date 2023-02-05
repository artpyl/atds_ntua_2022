from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, desc, row_number, asc, max, month, dayofmonth, hour, window, dayofweek, avg
import os
import sys
import datetime
import time

from base import df_taxi, df_zone

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ["SPARK_HOME"] = "/home/atds/spark-3.1.3-bin-hadoop2.7"

spark = SparkSession.builder.master("spark://10.0.2.15:7077").getOrCreate()

def find_hours(df_taxi,i):
	""" Find 3 top hours with highest mean passengers in a day"""

	# Filter by day
	temp = df_taxi.filter(df_taxi.Day == i).select("Hour","Mean_Passengers")
	# Order by Mean Passengers and collect
	temp.orderBy(col("Mean_Passengers").desc()).show()
	temp = temp.orderBy(col("Mean_Passengers").desc()).collect()
	# Store peak hours
	hours = []
	for j in range(3):
		hour = temp[j][0]
		hour = str(hour) + "-" + str((hour + 1) % 24)
		hours.append(hour)
	return (i, hours[0],hours[1],hours[2])

t1 = time.time()

# We have noticed that some entries are from 2002 and 2009 so we keep only 2022 records
df_taxi = df_taxi.filter(df_taxi["tpep_pickup_datetime"] >= datetime.datetime(2022,1,1)) \
                 .filter(df_taxi["tpep_pickup_datetime"] < datetime.datetime(2023,1,1))

# Create a new column representing day and hour of week
df_taxi = df_taxi.withColumn("Day",dayofweek("tpep_pickup_datetime")).withColumn("Hour",hour("tpep_pickup_datetime"))

# Group by day of week and hour and find Mean Passengers
df_taxi = df_taxi.groupBy("Day","Hour").agg(avg("Passenger_count").alias("Mean_Passengers"))

# Order dataframe
df_taxi = df_taxi.orderBy(col("Day").asc(),col("Hour").asc())

# Calculate peak hours
hours = []
for i in range(1,8):
	hours.append(find_hours(df_taxi,i))

# Create final dataframe
columns = ["Day_Of_Week","Highest_Peak","Second_Highest_Peak","Third_Highest_Peak"]
result = spark.createDataFrame(data=hours, schema = columns)
result.show()
# Write result as csv
result.write.mode('overwrite').option('header','true').csv('hdfs://master:9000/user/atds/taxi_files/results/q4')

t2 = time.time()

# Print time difference
print(f"Time elapsed {t2-t1}s")
