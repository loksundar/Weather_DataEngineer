# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE weather_db.gold_daily_aggregates AS
# MAGIC SELECT 
# MAGIC     CAST(Date_Hour AS DATE) AS date,
# MAGIC     MIN(Temp) AS min_temp,
# MAGIC     MAX(Temp) AS max_temp,
# MAGIC     AVG(Temp) AS avg_temp,
# MAGIC     SUM(precip_in) AS total_precip_in,
# MAGIC     SUM(snow_cm) AS total_snow_cm,
# MAGIC     AVG(uv) AS avg_uv_index,
# MAGIC     AVG(Feels_Like) AS avg_feels_like,
# MAGIC     AVG(Pressure) AS avg_pressure,
# MAGIC     MAX(gust_mph) AS max_gust_mph
# MAGIC FROM weather_db.silver_table_joined
# MAGIC GROUP BY CAST(Date_Hour AS DATE);
# MAGIC

# COMMAND ----------

