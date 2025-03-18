# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE weather_db.gold_wind_vis_metrics AS
# MAGIC SELECT
# MAGIC     TO_TIMESTAMP(Date_Hour, 'yyyy-MM-dd HH') AS date_hour,
# MAGIC     AVG(cloud) AS avg_cloud_cover,
# MAGIC     wind_dir AS primary_wind_dir, -- or do a mode() if needed
# MAGIC     AVG(vis_miles) AS avg_vis_miles,
# MAGIC     MAX(gust_mph) AS max_gust_mph
# MAGIC FROM weather_db.silver_table_joined
# MAGIC GROUP BY 
# MAGIC     TO_TIMESTAMP(Date_Hour, 'yyyy-MM-dd HH'),
# MAGIC     wind_dir;

# COMMAND ----------

