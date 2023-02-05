#!/bin/sh

hdfs dfs -mkdir -p taxi_files
hdfs dfs -mkdir -p taxi_files/results

for file in ~/taxi-files/*
do
	hdfs dfs -put $file taxi_files/ 
done
