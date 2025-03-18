# Databricks notebook source
df = spark.read.format("delta").table("weather_db.silver_table_joined")
display(df)

# COMMAND ----------

from pyspark.sql.functions import col, sum

null_counts = df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])
display(null_counts)

# COMMAND ----------

df_dropped_nulls = df.dropna()
display(df_dropped_nulls)

# COMMAND ----------

from pyspark.sql.functions import col
from functools import reduce

df_with_nulls = df.filter(
    reduce(
        lambda a, b: a | b,
        [col(c).isNull() for c in df.columns]
    )
)
display(df_with_nulls)

# COMMAND ----------



# COMMAND ----------

df_openweather_api = spark.read.format("delta").table("weather_db.bronze_weather_api")
display(df_openweather_api)

# COMMAND ----------

df_openweather_api = spark.read.format("delta").table("weather_db.silver_weather_api")
display(df_openweather_api)

# COMMAND ----------

