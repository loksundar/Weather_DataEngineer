# Databricks notebook source
df_openweather_api = spark.read.format("delta").table("weather_db.bronze_openweather_api")
df_openweather_api = df_openweather_api.dropDuplicates(["dt"]).sort("dt")
display(df_openweather_api.limit(5))

# COMMAND ----------

df_openweather_api = df_openweather_api.withColumnRenamed("dt", "time_unix")

# COMMAND ----------

from pyspark.sql.functions import col, expr, round

kelvin_to_f = lambda k: (k - 273.15) * 9/5 + 32

temp_columns = ["main_temp", "main_feels_like", "main_temp_min", "main_temp_max"]

for col_name in temp_columns:
    df_openweather_api = df_openweather_api.withColumn(col_name, round(kelvin_to_f(col(col_name)), 2))

display(df_openweather_api.limit(5))

# COMMAND ----------

from pyspark.sql.functions import col, round

# Conversion factor from meters per second to miles per hour
mps_to_mph = lambda mps: mps * 2.23694

# Convert wind speed from meters per second to miles per hour
df_openweather_api = df_openweather_api.withColumn("wind_speed", round(mps_to_mph(col("wind_speed")), 2))

# COMMAND ----------

# Conversion factor from hectoPascal (hPa) to Inch of Mercury (inHg)
hpa_to_inhg = lambda hpa: hpa * 0.02953

# Convert main_pressure from hPa to inHg
df_openweather_api = df_openweather_api.withColumn("main_pressure", round(hpa_to_inhg(col("main_pressure")), 2))

# COMMAND ----------

from pyspark.sql.functions import col, sum
def count_nulls(df):
    return df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])
null_counts_openweather_api = count_nulls(df_openweather_api)
display(null_counts_openweather_api)

# COMMAND ----------

df_openweather_api = df_openweather_api.drop("wind_gust", "snow_1h", "rain_1h","weather_icon")

# COMMAND ----------

df_openweather_api.columns

# COMMAND ----------

display(df_openweather_api)

# COMMAND ----------

df_openweather_api.write.mode("overwrite").saveAsTable("weather_db.silver_openweather_api")

# COMMAND ----------

