# Databricks notebook source
df_openweather_api = spark.read.format("delta").table("weather_db.silver_openweather_api")
display(df_openweather_api.limit(5))

# COMMAND ----------

df_weather_api = spark.read.format("delta").table("weather_db.silver_weather_api")
display(df_weather_api.limit(5))

# COMMAND ----------

from pyspark.sql.functions import expr, col, from_unixtime

df_joined = df_openweather_api.join(df_weather_api, ["time_unix"], "outer")

# COMMAND ----------

columns_to_drop = ["time_unix", "time_unix_ts","openweather.time_unix_ts", "weather.time_unix_ts","openweather.time_unix","weather.time_unix"]
df_joined = df_joined.drop(*columns_to_drop)
display(df_joined)

# COMMAND ----------

df_joined = df_joined.dropna()
display(df_joined)

# COMMAND ----------

from pyspark.sql.functions import col, when

df_joined = df_joined.withColumn("Temp", when(col("main_temp").isNull(), col("temp_f")).when(col("temp_f").isNull(), col("main_temp")).otherwise((col("main_temp") + col("temp_f")) / 2))
df_joined = df_joined.withColumn("Feels_Like", when(col("main_feels_like").isNull(), col("feelslike_f")).when(col("feelslike_f").isNull(), col("main_feels_like")).otherwise((col("main_feels_like") + col("feelslike_f")) / 2))
df_joined = df_joined.withColumn("Pressure", when(col("main_pressure").isNull(), col("pressure_in")).when(col("pressure_in").isNull(), col("main_pressure")).otherwise((col("main_pressure") + col("pressure_in")) / 2))
df_joined = df_joined.withColumn("Wind_Speed", when(col("wind_speed").isNull(), col("wind_mph")).when(col("wind_mph").isNull(), col("wind_speed")).otherwise((col("wind_speed") + col("wind_mph")) / 2))
df_joined = df_joined.withColumn("Wind_Degree", when(col("wind_deg").isNull(), col("wind_degree")).when(col("wind_degree").isNull(), col("wind_deg")).otherwise((col("wind_deg") + col("wind_degree")) / 2))
df_joined = df_joined.withColumn("Humidity", when(col("main_humidity").isNull(), col("humidity")).when(col("humidity").isNull(), col("main_humidity")).otherwise((col("main_humidity") + col("humidity")) / 2))

columns_to_drop = ["date","main_temp", "temp_f", "main_feels_like", "feelslike_f", "main_pressure", "pressure_in", "wind_speed", "wind_mph", "wind_deg", "wind_degree", "main_humidity", "humidity"]
df_joined = df_joined.drop(*columns_to_drop)

display(df_joined.limit(5))

# COMMAND ----------

columns_to_drop = ["temp_c","condition_icon","wind_kph","pressure_mb","precip_mm","feelslike_c","windchill_c","heatindex_c","dewpoint_c","vis_km","gust_kph","main_temp_min","main_temp_max","clouds_all","weather_id","weather_main","weather_description"]
df_joined = df_joined.drop(*columns_to_drop)

# COMMAND ----------

df_joined.createOrReplaceTempView("temp_weather_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE weather_db.silver_table_joined AS
# MAGIC SELECT * FROM temp_weather_view

# COMMAND ----------

