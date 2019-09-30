# Databricks notebook source
from pyspark.sql.types import StructField, StringType, IntegerType, LongType, StructType, DateType

# DataFrame definitions

# CSV file
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

# Download History
struct_fields_download_history = [
  StructField('id', LongType(), False),
  StructField('status_code', IntegerType(), True),
  StructField('download_duration', FloatType(), True),
  StructField('image_size', LongType(), True),
  StructField('image_path', StringType(), True),
]
download_history_schema = StructType(fields=struct_fields_download_history)

# Analysis for column:
col_analysis = [
  StructField('column_name', StringType(), False),
  StructField('total_value_count', IntegerType(), False),
  StructField('null_value_count', IntegerType(), False)
]
col_analysis_schema = StructType(fields=col_analysis)


# COMMAND ----------

###
# Sampels the flicker dataset with default seed lsde in ascii numbers
###
def sample_flickr_dataset(full_sample = False, prefix = "", seed = 108115100101):
  files_list = dbutils.fs.ls("/mnt/data/flickr")
  DF_SAMPLE_SIZE_FRAC = 0.0001
  BUCKET_COUNT = (len(files_list) if full_sample else 1) 

  df_sampled_buckets = spark.createDataFrame([], csv_data_scheme)
  print("file | sample size")
  for i, file in enumerate(files_list[0:BUCKET_COUNT]):
    df_bucket = spark.read.format("CSV").option("delimiter", "\t").schema(csv_data_scheme).load(file.path[5:]).sample(True, DF_SAMPLE_SIZE_FRAC, seed=seed)
    df_bucket.write.format("parquet").save("/mnt/group07/{0}flickr_bucket_{1}.parquet".format(prefix, i))
    df_sampled_buckets = df_sampled_buckets.union(df_bucket)
    
    print("{0} | {1}".format(file.name, df_bucket.count()))
  df_sampled_buckets.write.format("parquet").save("/mnt/group07/{0}flickr.parquet".format(prefix))
  return df_sampled_buckets

# COMMAND ----------

def col_basic_analysis(col_name, df):
  df_col = df.select(col_name)
  col_total_cnt = df_col.count()
  null_values_cnt = df_col.filter(df_col[col_name].isNull()).count()  
  return [col_name, col_total_cnt, null_values_cnt]

# COMMAND ----------

from urllib.parse import urlparse

def is_flickr_image_download_url(url):
  try:
    result = urlparse(url)
    if (result.scheme != "http") :
      return False
    if (not result.path.endswith(".jpg")):
      return False
    return all([result.scheme, result.netloc, result.path])
  except ValueError:
    return False

# COMMAND ----------

import requests
from timeit import default_timer as timer
from datetime import timedelta

def download_and_save_image(row):
  try:
    # return (row.id, None, None, None, None)
    # initiate download:
    start_download = timer()
    req = requests.get(row.photo_video_download_url)
    end_download = timer()
    download_in_seconds = timedelta(seconds=end_download-start_download).total_seconds()

    status_code = req.status_code
    # process the result
    if req.status_code == 200:
      image_in_bytes = len(req.content)
      image_file_path = "/dbfs/mnt/group07/images_test/{0}.jpg".format(row.id)
      f = open(image_file_path,'wb')
      f.write(req.content)
      f.close()
    else:
        image_in_bytes = None
        image_file_path = None
  except Exception as e:
    return (row.id, None, None, None, str(e))
  return (row.id, status_code, download_in_seconds, image_in_bytes, image_file_path)

# COMMAND ----------

# Triggers the flickr download based on the delta of images
def batch_download_images(df_download_links, df_download_hist):
  batch_map = df_download_links.join(df_download_hist, "id", "leftanti").rdd.map(download_and_save_image)
  return spark.createDataFrame(batch_map, download_history_schema)

# Returns a download history dataframe (either from disk or new one)
def load_download_history():
  # open a new download history
  if "download_history.parquet/" in [fi.name for fi in dbutils.fs.ls("/mnt/group07")]:
    return spark.read.parquet("/mnt/group07/download_history.parquet")
  else:
    return spark.createDataFrame([], download_history_schema)

# COMMAND ----------

# Sample dataset (set to True if full dataset - 10gb of data!) and write to parquet
# df_sampled = sample_flickr_dataset(False, "single/")

# COMMAND ----------

df_raw_flickr = spark.read.parquet("/mnt/group07/full/flickr.parquet")
print(df_raw_flickr.count())

# COMMAND ----------

# Basic Col Analysis
basic_analysis = []
# for col in df_raw_flickr.columns:
#  basic_analysis.append(col_basic_analysis(col, df_raw_flickr))

# df_col_analysis = spark.createDataFrame(basic_analysis, col_analysis_schema)

# COMMAND ----------

from pyspark.sql.functions import udf

valid_img_download_url_udf = udf(lambda str_url: str_url if is_flickr_image_download_url(str_url) else None, StringType())

df_flickr = df_raw_flickr \
            .withColumn("id", df_raw_flickr["id"].cast(LongType())) \
            .withColumn("date_taken", df_raw_flickr["date_taken"].cast(DateType())) \
            .withColumn("date_uploaded", df_raw_flickr["date_uploaded"].cast(IntegerType())) \
            .withColumn("longitude", df_raw_flickr["longitude"].cast(IntegerType())) \
            .withColumn("latitude", df_raw_flickr["latitude"].cast(IntegerType())) \
            .withColumn("accuracy", df_raw_flickr["accuracy"].cast(IntegerType())) \
            .withColumn("photo_video_download_url", valid_img_download_url_udf(df_raw_flickr["photo_video_download_url"])) \
            .withColumn("photo_video_server_id", df_raw_flickr["photo_video_server_id"].cast(IntegerType())) \
            .withColumn("photo_video_farm_id", df_raw_flickr["photo_video_farm_id"].cast(IntegerType())) \
            .withColumn("photo_video_marker", df_raw_flickr["photo_video_marker"].cast(IntegerType()))

#cast_analysis = []
#for col in df_flickr.columns:
#  cast_analysis.append(col_basic_analysis(col, df_flickr))

#df_cast_col_analysis = spark.createDataFrame(cast_analysis, col_analysis_schema)

# check if anaylsis output differs:
#df_diff = df_cast_col_analysis.subtract(df_col_analysis)
#df_diff.show(5, False)

#df_cast_col_analysis.show(40, False)
#df_col_analysis.show(40, False)

df_flickr.printSchema()
#df_raw_flickr.select("photo_video_download_url", "photo_video_marker").where(df_raw_flickr["photo_video_marker"] == 1).show(10000, False)
#df_flickr.select("photo_video_download_url", "photo_video_marker").filter(df_flickr["photo_video_download_url"].isNull()).show(10000, False)

# TODO columns:
# .withColumn("photo_video_page_url", valid_url_udf(df_raw_flickr["photo_video_page_url"])) \

# TODO:
# data violations defined by the schema
# date fields: date_taken, date_uploaded
# url fields: photo/video page url / photo/video download url, liecense url
# is data plausible:
# -> is picture really, picture, video really video?

# check if picture exists - only request head

# COMMAND ----------

df_id_download_link = df_flickr.where(df_flickr.photo_video_marker == 0).select(df_flickr.id, df_flickr.photo_video_download_url)

# Store the full list for temp access (TODO: remove later):
df_full_download_link = df_id_download_link
# TODO: remove later - this takes only a chuck of the data
df_id_download_link = df_full_download_link
# Create required dirs
dbutils.fs.mkdirs("/mnt/group07/images_test/")

# Load the download history (to avoid downloading unecessary stuff)
df_download_history = load_download_history()

# Execute the Batch download images function, that pulls the delta history from flickr
df_downloads = batch_download_images(df_id_download_link, df_download_history)
df_download_history = df_download_history.union(df_downloads)

# Show Downloads:
#df_downloads.show(100, False)

# write the latest download history
df_download_history.write.mode("overwrite").format("parquet").save("/mnt/group07/download_history.parquet")

# COMMAND ----------

df_download_history = spark.read.parquet("/mnt/group07/download_history.parquet")

#df_download_history.show()
df_download_history.count()

# COMMAND ----------

f1 = dbutils.fs.ls("/mnt/group07/images/")
f2 = dbutils.fs.ls("/mnt/group07/images_test/")

print(len(f1))
print(len(f2))

# COMMAND ----------

