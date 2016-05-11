cd data

#
# Prepare for Exercise 1
#
wget https://snap.stanford.edu/data/facebook_combined.txt.gz
gunzip facebook_combined.txt.gz
hdfs dfs -put facebook_combined.txt

#
# Prepare for Exercise 2
#
#
# => we need a Hive table which wraps around the Parquet file with crawl results
# 
# In our exercise we load the Parquet file via SQL-Context and register it as
# a temporary table.
#
# First we upload it into HDFS
#
#
hdfs dfs -put yarn_ds1_run_3_webpage_parquet_1k



