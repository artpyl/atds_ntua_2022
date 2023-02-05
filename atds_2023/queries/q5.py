from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, desc, row_number, asc, max, month, dayofmonth, hour, window, dayofweek,avg
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

def find_days(df_taxi,i):
        """ Find 3 top days with highest mean tip percentage"""

        # Filter by day
        temp = df_taxi.filter(df_taxi.Month == i).select("Day","Mean_Tip_Percentage")
        # Order by Mean Tip Percentage and collect
        temp = temp.orderBy(col("Mean_Tip_Percentage").desc()).collect()
        # Store best days
        days = []
        for j in range(5):
                day = temp[j][0]
                days.append(day)
        return (i, days[0],days[1],days[2],days[3],days[4])

# We have noticed that some entries are from 2002 and 2009 so we keep only 2022 records
df_taxi = df_taxi.filter(df_taxi["tpep_pickup_datetime"] >= datetime.datetime(2022,1,1)) \
                 .filter(df_taxi["tpep_pickup_datetime"] < datetime.datetime(2023,1,1))

# Create a new column for Month, Day of month and one for tip ratio
df_taxi = df_taxi.withColumn("Month",month(df_taxi.tpep_pickup_datetime)) \
		 .withColumn("Day",dayofmonth(df_taxi.tpep_pickup_datetime)) \
		 .withColumn("Tip_Percentage", df_taxi["Tip_amount"] / df_taxi["Fare_amount"] * 100)

# Group by month and day and find Mean Tip Percentage
df_taxi = df_taxi.groupBy("Month","Day").agg(avg("Tip_Percentage").alias("Mean_Tip_Percentage"))


# Order Dataframe
df_taxi = df_taxi.orderBy(col("Month").asc(),col("Day").asc())

# Calculate top days
days = []
for i in range(1,7):
        days.append(find_days(df_taxi,i))

# Create final dataframe
columns = ["Month","Top1","Top2","Top3","Top4","Top5"]
result = spark.createDataFrame(data=days, schema = columns)

result.write.mode('overwrite').option('header','true').csv('hdfs://master:9000/user/atds/taxi_files/results/q5')

t2 = time.time()

# Print time difference
print(f"Time elapsed {t2-t1}s")
