# Databricks notebook source
# MAGIC %run /group07/shared

# COMMAND ----------

###
# Get the total count of the dataset
###
def get_total_count():
  files_list = dbutils.fs.ls("/mnt/data/flickr")
  for i, file in enumerate(files_list):
    df_bucket = spark.read.format("CSV").option("delimiter", "\t").schema(get_csv_data_scheme()).load(file.path[5:])
    total_count += df_bucket.count()
    print("{0} | {1}".format(file.name, df_bucket.count()))
  print("{0} | {1}".format("Total", total_count))
# uncomment and use
# get_total_count()

# COMMAND ----------

from functools import reduce

###
# Get the file sizes count of the dataset
###
files = dbutils.fs.ls("/mnt/data/flickr")
total_bytes = reduce(lambda acc, f: acc + f.size, files, 0)
print("total bytes: {0}".format(total_bytes))


# COMMAND ----------

# Get total row count of sample
df_raw_flickr = spark.read.parquet("/mnt/group07/full/flickr.parquet")
print("total rows: {0}".format(df_raw_flickr.count()))

# Get total byte size
df_raw_flickr.write.format("CSV").mode("overwrite").option("delimiter", "\t").save("/mnt/group07/analysis/sample.csv")
files = dbutils.fs.ls("/mnt/group07/analysis/sample.csv")
total_bytes = reduce(lambda acc, f: acc + f.size, files, 0)
print("total bytes: {0}".format(total_bytes))


# COMMAND ----------

print(len(df_raw_flickr.columns))
print(df_raw_flickr.columns)

# COMMAND ----------

# Get Byte Size of Relevant Columns:
df_raw_flickr_subset = df_raw_flickr.select("id", "user_tags", "machine_tags", "photo_video_page_url", "photo_video_download_url", "photo_video_marker", "title")
df_raw_flickr_subset.write.format("CSV").mode("overwrite").option("delimiter", "\t").save("/mnt/group07/analysis/sample_subset.csv")
files = dbutils.fs.ls("/mnt/group07/analysis/sample_subset.csv")
total_bytes_subset = reduce(lambda acc, f: acc + f.size, files, 0)
print("total subset bytes: {0}".format(total_bytes_subset))
print("ratio subset bytes: {0}".format(total_bytes_subset / total_bytes))

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np



df_download_history = spark.read.parquet("/mnt/group07/download_history.parquet")
df_download_history.show()

# COMMAND ----------

df_status_code_count = df_download_history.orderBy("status_code").groupBy("status_code").count()
df_status_code_count.show()
df_scc_pandas = df_status_code_count.toPandas()

x = range(3)
x_labels = df_scc_pandas.status_code

# y_pos = np.array(str(df_scc_pandas.status_code))
#plt.bar(y_pos, height, color=(0.2, 0.4, 0.6, 0.6))

fig = plt.figure(figsize=(6,4))
bars = plt.bar(x, list(df_scc_pandas["count"]), color="#3A81BA")
plt.title('HTTP Return Codes Freq.')
plt.xticks(x, x_labels, ha='left')

# access the bar attributes to place the text in the appropriate location
for bar in bars:
    yval = bar.get_height()
    plt.text(bar.get_x(), yval + 100, yval)

# COMMAND ----------

display(fig)

# COMMAND ----------

from pyspark.sql.functions import col

df_sc_200_pd = df_download_history.select("status_code", col("download_duration").alias("Duration in seconds")).toPandas()

fig, ax = plt.subplots(figsize=(7,3))
bxp = df_sc_200_pd.boxplot(column=['Duration in seconds'], by='status_code', ax=ax)
plt.suptitle('')
plt.subplots_adjust(left=0.2, right=0.7, top=0.9, bottom=0.2)
bxp.set_xlabel("HTTP Status Code")
 

# COMMAND ----------

display(fig)