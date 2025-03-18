# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE weather_db.gold_condition_stats AS
# MAGIC SELECT
# MAGIC     CAST(Date_Hour AS DATE) AS date,
# MAGIC     condition_text,
# MAGIC     COUNT(*) AS condition_count,
# MAGIC     AVG(chance_of_rain) AS avg_chance_of_rain,
# MAGIC     AVG(chance_of_snow) AS avg_chance_of_snow,
# MAGIC     MAX(CASE WHEN gust_mph > 50 OR Temp < 32 THEN 1 ELSE 0 END) AS extreme_event_flag
# MAGIC FROM weather_db.silver_table_joined m
# MAGIC GROUP BY 
# MAGIC     CAST(Date_Hour AS DATE),
# MAGIC     condition_text;