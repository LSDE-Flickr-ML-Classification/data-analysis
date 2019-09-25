# Databricks notebook source
from pyspark.sql.types import StructField, StringType, IntegerType, StructType

# Initial Data Analysis - STEP 01
# Load raw CSV of only a portion (can later be extended to full dataset)

# LOAD CSV
csv_field_list = [
  StructField('id', StringType(), True),
  StructField('user_nsid', StringType(), True),
  StructField('user_nickname', StringType(), True),
  StructField('date_taken', StringType(), True),
  StructField('date_uploaded', StringType(), True),
  StructField('capture_device', StringType(), True),
  StructField('title', StringType(), True),
  StructField('description', StringType(), True),
  StructField('user_tags', StringType(), True),
  StructField('machine_tags', StringType(), True),
  StructField('longitude', StringType(), True),
  StructField('latitude', StringType(), True),
  StructField('accuracy', StringType(), True),
  StructField('photo_video_page_url', StringType(), True),
  StructField('photo_video_download_url', StringType(), True),
  StructField('license_name', StringType(), True),
  StructField('license_url', StringType(), True),
  StructField('photo_video_server_id', StringType(), True),
  StructField('photo_video_farm_id', StringType(), True),
  StructField('photo_video_secret', StringType(), True),
  StructField('photo_video_original_secret', StringType(), True),
  StructField('photo_video_extension_original', StringType(), True),
  StructField('photo_video_marker', StringType(), True)
]
csv_data_scheme = StructType(fields=csv_field_list)
df_full = spark.read.format("CSV").option("delimiter", "\t").schema(csv_data_scheme).load("/mnt/data/flickr/yfcc100m_dataset-1.bz2")

# COMMAND ----------

###
# Sampels the flicker dataset
###
def sample_flickr_dataset(full_sample = False):
  files_list = dbutils.fs.ls("/mnt/data/flickr")
  DF_SAMPLE_SIZE_FRAC = 0.0001
  BUCKET_COUNT = (len(files_list) if full_sample else 1) 

  df_sampled_buckets = spark.createDataFrame([], csv_data_scheme)
  print("file | sample size")
  for file in files_list[0:BUCKET_COUNT]:
    df_bucket = spark.read.format("CSV").option("delimiter", "\t").schema(csv_data_scheme).load(file.path[5:]).sample(DF_SAMPLE_SIZE_FRAC)
    print("{0} | {1}".format(file.name, df_bucket.count()))
    df_sampled_buckets = df_sampled_buckets.union(df_bucket)
  
  return df_sampled_buckets

# COMMAND ----------

def col_analysis(col_name, df):
  df_col = df.select(col_name)
  col_total_cnt = df_col.count()
  null_values_cnt = df_col.filter(df_col[col_name].isNull()).count()
  
  print("Analysis for column: {0}".format(col_name))
  print("\t Total NULL Value : {0}".format(null_values_cnt))
  print("\t NULL Value Ratio column longitude: {0}".format(null_values_cnt / col_total_cnt))

# COMMAND ----------

# Sample dataset (set to True if full dataset - 34gb of data!) and write to parquet
df_sampled = sample_flickr_dataset(False)
df_sampled.write.mode("overwrite").format("parquet").save("/mnt/group07/flicker_sampled.parquet")

# show folder structure (check)
dbutils.fs.ls("/mnt/group07/")

# COMMAND ----------

df_raw_flickr = spark.read.parquet("/mnt/group07/flicker_sampled.parquet")

# Analysis by column
col_analysis("id", df_raw_flickr)
col_analysis("longitude", df_raw_flickr)

# TODO:
# check possible null values
# data violations defined by the schema
# date fields: date_taken, date_uploaded
# url fields: photo/video page url / photo/video download url, liecense url
# 
# is data plausible:
# -> is picture really, picture, video really video?

# check if picture exists - only request head

# COMMAND ----------

