from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, desc, row_number, asc, max, month, dayofmonth, hour,window
import os
import sys
import datetime
import time

from base import rdd_taxi, rdd_zone

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ["SPARK_HOME"] = "/home/atds/spark-3.1.3-bin-hadoop2.7"

spark = SparkSession.builder.master("spark://10.0.2.15:7077").getOrCreate()

t1 = time.time()

def map_to_intervals(x):
	"""Maps date to respective 15-day interval"""

	dates = [datetime.datetime(2022,1,1),datetime.datetime(2022,1,16),datetime.datetime(2022,1,31) \
		,datetime.datetime(2022,2,15),datetime.datetime(2022,3,2),datetime.datetime(2022,3,17) \
		,datetime.datetime(2022,4,1),datetime.datetime(2022,4,16),datetime.datetime(2022,5,1) \
		,datetime.datetime(2022,5,16),datetime.datetime(2022,5,31),datetime.datetime(2022,6,15) \
		,datetime.datetime(2022,6,30),datetime.datetime(2023,1,1)]
	for i in range(len(dates)):
		if (x[1] < dates[i]):
			to_return = dates[i-1]
			break
	return str(to_return),x[4],x[16] # Interval, Trip_distance, Total_amount

# Joining is costly in case of rdd so we make the assumption that different region = PULocationID != DOLocationID
# Indexes of requested values are found by printing a snapshot of the RDD!


# Filter out same region
rdd_taxi = rdd_taxi.filter(lambda x: x.PULocationID != x.DOLocationID) # PULocationID != DOLocationID
# Filter out dates before or after 2022
rdd_taxi = rdd_taxi.filter(lambda x: x[1] >= datetime.datetime(2022,1,1) and x[1] < datetime.datetime(2023,1,1)) # x[1] = tpep_pickup_datetime

# Map dates to 15 date intervals and return only useful attributes
rdd_taxi = rdd_taxi.map(lambda x: map_to_intervals(x))

# Group by 15 day intervals and reduce over Trip_distance and Total_cost
rdd_distance = rdd_taxi.map(lambda x: (x[0],(x[1],1))).reduceByKey(lambda x,y: ((x[0] + y[0]),(x[1] + y[1]))).map(lambda x: (x[0], x[1][0] / x[1][1]))
rdd_amount = rdd_taxi.map(lambda x: (x[0],(x[2],1))).reduceByKey(lambda x,y: ((x[0] + y[0]),(x[1] + y[1]))).map(lambda x: (x[0], x[1][0] / x[1][1]))

# Join 2 rdds into one
result = rdd_distance.join(rdd_amount).map(lambda x: (x[0],x[1][0],x[1][1]))

# Create Dataframe from result

result = result.toDF(["Interval_Start","Average_Trip_Distance","Average_Total_Amount"]).orderBy(col("Interval_Start").asc())
result.show()
result.write.mode('overwrite').option('header','true').csv('hdfs://master:9000/user/atds/taxi_files/results/q3')

t2 = time.time()

# Print time difference
print(f"Time elapsed {t2-t1}s")
