# Databricks notebook source
from pyspark.sql.types import StructField, StringType, IntegerType, StructType

# Initial Data Analysis - STEP 01
#
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
# df_full.printSchema()

# COMMAND ----------

# Show main metrics on the full_df (possibly sample a subset of the data)
df_full_count = df_full.count()

print("Amount of rows read: {}".format(df_full_count))

# COMMAND ----------

# Optional STEP - Reduce amount of rows with sampling

# One Portion of the flickr file consists of 10,000,000 records, we sample:
DF_SAMPLE_SIZE = 1000

df_raw_flickr = df_full.sample(DF_SAMPLE_SIZE / df_full_count)
df_raw_flickr_count = df_raw_flickr.count() # for later reference

# COMMAND ----------

def col_analysis(col_name, df):
  df_col = df.select(col_name)
  col_total_cnt = df_col.count()
  null_values_cnt = df_col.filter(df_col[col_name].isNull()).count()
  
  print("Analysis for column: {0}".format(col_name))
  print("\t Total NULL Value : {0}".format(null_values_cnt))
  print("\t NULL Value Ratio column longitude: {0}".format(null_values_cnt / df_raw_flickr_count))
  

# COMMAND ----------

# Analysis by column
col_analysis("id", df_raw_flickr)
col_analysis("longitude", df_raw_flickr)

# COMMAND ----------

