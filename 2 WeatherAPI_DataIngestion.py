# Databricks notebook source
df_weather_api = spark.read.table("weather_db.bronze_weather_api").dropDuplicates(["time_epoch"]).sort("time_epoch")
display(df_weather_api)

# COMMAND ----------

df_weather_api = df_weather_api.withColumnRenamed("time_epoch", "time_unix")

# COMMAND ----------

columns_to_drop = [
    "temp_c", "condition_icon","wind_kph", "pressure_mb", "precip_mm", "feelslike_c", 
    "windchill_c", "heatindex_c", "dewpoint_c", "vis_km", "gust_kph"
]

df_weather_api = df_weather_api.drop(*columns_to_drop)

# COMMAND ----------

from pyspark.sql.functions import from_unixtime, date_format
from pyspark.sql.functions import col, round
df_weather_api = df_weather_api.withColumn("Date_Hour", from_unixtime(col("time_unix"), "yyyy-MM-dd HH"))
df_weather_api = df_weather_api.withColumn("Date", date_format(col("Date_Hour"), "yyyy-MM-dd"))
df_weather_api = df_weather_api.withColumn("Hour", date_format(col("Date_Hour"), "HH"))

display(df_weather_api)

# COMMAND ----------

df_weather_api.write.format("delta").mode("overwrite").saveAsTable("weather_db.silver_weather_api")

# COMMAND ----------

