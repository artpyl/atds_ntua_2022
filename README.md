# Atds_ntua_2022

Our submission for the Project of 9th Semester for the class Advanced Topics in Database Systems.

# Team members

Evangelou Spyridon Michail - mixlhs@gmail.com
<br>
Pylarinou Artemis - artemispylarinou@gmail.com

# Repository structure

- <b>queries</b>: Code for tasks Q1-Q5
- <b>results</b>: Results of the queries when run on spark
- <b>upload_hdfs.sh</b>: Helper script that uploads files from local storage to HDFS

# Installation Instructions

In order to be able to run the scripts of the queries, an installation of Apache Spark 3.1.3 and Apache Hadoop 3.3.4 is needed. You will also have to upload the files
regarding Yellow Taxis from the period between January and June 2022 from the website:
<br>
<link>https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page</link>
<br>
as well as the file:
<br>
<link>https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv</link>
<br>
to the HDFS. The final step involves changing the paths used in the scripts to the ones that follow your Spark/HDFS configuration.
