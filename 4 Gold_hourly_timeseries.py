# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE weather_db.gold_hourly_timeseries AS
# MAGIC SELECT
# MAGIC     CAST(Date_Hour AS DATE) AS date,
# MAGIC     HOUR(TO_TIMESTAMP(Date_Hour, 'yyyy-MM-dd HH')) AS hour_of_day,
# MAGIC     Temp AS temp,
# MAGIC     Feels_Like AS feels_like,
# MAGIC     precip_in,
# MAGIC     wind_dir,
# MAGIC     gust_mph,
# MAGIC     cloud,
# MAGIC     condition_text
# MAGIC FROM weather_db.silver_table_joined;

# COMMAND ----------

