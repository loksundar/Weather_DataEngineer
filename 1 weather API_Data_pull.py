# Databricks notebook source
import requests
import pprint
import time
from datetime import datetime, timedelta
import pickle

# COMMAND ----------

df_weather_api = spark.read.format("delta").table("weather_db.bronze_weather_api")
display(df_weather_api)

# COMMAND ----------

from pyspark.sql.functions import from_unixtime, col
max_date_available = datetime.utcfromtimestamp(df_weather_api.agg({"time_epoch": "max"}).first()[0])
max_date_available

# COMMAND ----------

import requests
from datetime import datetime, timedelta
import json
from dateutil.relativedelta import relativedelta

api_key = "Your_API_Key"
location = "75080"        # Using a zip code as the location
def get_historical_weather(api_key, location, start_dt, end_dt):
    base_url = "http://api.weatherapi.com/v1/history.json"
    params = {
        "key": api_key,
        "q": location,
        "dt": start_dt,
        "end_dt": end_dt
    }

    response = requests.get(base_url, params=params)
    response.raise_for_status()  # Raises an error for HTTP errors
    return response.json()

def fetch_data_in_chunks(start_date, end_date, max_days=30):
    current_start = start_date
    all_results = []

    # Loop over each chunk (maximum 30 days per request)
    while current_start <= end_date:
        # Compute the tentative end of the current chunk (30 days)
        tentative_end = current_start + timedelta(days=max_days-1)

        # Ensure we do not exceed our overall end_date
        current_end = min(tentative_end, end_date)

        dt_str = current_start.strftime("%Y-%m-%d")
        end_dt_str = current_end.strftime("%Y-%m-%d")
        print(f"Fetching data from {dt_str} to {end_dt_str}")

        try:
            # Fetch the data for the current chunk
            data = get_historical_weather(api_key, location, dt_str, end_dt=end_dt_str)
            all_results.append(data)
        except requests.HTTPError as http_err:
            print(f"HTTP error for {dt_str} to {end_dt_str}: {http_err}")

        # Move to the next chunk (the day after the current_end)
        current_start = current_end + timedelta(days=1)

    return all_results

# COMMAND ----------

start_date = max_date_available
start_date = max_date_available - timedelta(days=5) 
end_date = datetime.today()    

all_weather_data = fetch_data_in_chunks(start_date, end_date)

# Print all results
print(json.dumps(all_weather_data, indent=2))

# COMMAND ----------

data = all_weather_data
# Silver 
import pandas as pd
# Create a list to hold rows of hourly data
rows = []
# If the JSON is a list, iterate over each item.
for item in data:
    forecast = item.get("forecast", {})
    for forecastday in forecast.get("forecastday", []):
        # Save the date for each forecast day.
        day_date = forecastday.get("date")
        # Loop over each hourly observation.
        for hour in forecastday.get("hour", []):
            # Create a new dictionary for the row and include the forecast date.
            row = {}
            # Add all top-level fields from the hourly data.
            for key, value in hour.items():
                if key != "condition":
                    row[key] = value
                else:
                    # Flatten the nested 'condition' dictionary
                    row["condition_text"] = value.get("text")
                    row["condition_icon"] = value.get("icon")
                    row["condition_code"] = value.get("code")
            rows.append(row)

# Create a DataFrame from the rows list.
df = pd.DataFrame(rows)
df.drop(columns=["time"], inplace=True)
df.head()

# COMMAND ----------

new_df = spark.createDataFrame(df)
df.shape

# COMMAND ----------

new_df.createOrReplaceTempView("new_df_temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO weather_db.bronze_weather_api
# MAGIC SELECT * FROM new_df_temp

# COMMAND ----------

