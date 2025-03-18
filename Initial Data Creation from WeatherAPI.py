# Databricks notebook source
import requests
from datetime import datetime, timedelta
import json
from dateutil.relativedelta import relativedelta

api_key = "8cc95cb5d24f4729940102245250903"
location = "75080"        # Using a zip code as the location

def get_historical_weather(api_key, location, dt, end_dt=None):
    base_url = "http://api.weatherapi.com/v1/history.json"
    params = {
        "key": api_key,
        "q": location,
        "dt": dt
    }
    if end_dt:
        params["end_dt"] = end_dt

    response = requests.get(base_url, params=params)
    response.raise_for_status()  # Raises an error for HTTP errors
    return response.json()

if __name__ == "__main__":
    # Define the 365-day period ending on March 8, 2024
    end_date = datetime.today() 
    start_date = datetime(2024, 3, 12)  # 365 days total (inclusive)

    current_start = start_date
    all_results = []

    # Loop over each monthly chunk within the 365-day period
    while current_start <= end_date:
        # Compute the tentative end of the month for the current chunk
        tentative_end = current_start + relativedelta(months=1) - timedelta(days=1)
        # Ensure we do not exceed our overall end_date
        current_end = min(tentative_end, end_date)

        dt_str = current_start.strftime("%Y-%m-%d")
        end_dt_str = current_end.strftime("%Y-%m-%d")
        print(f"Fetching data from {dt_str} to {end_dt_str}")

        try:
            data = get_historical_weather(api_key, location, dt_str, end_dt=end_dt_str)
            all_results.append(data)
        except requests.HTTPError as http_err:
            print(f"HTTP error for {dt_str} to {end_dt_str}: {http_err}")

        # Move to the next chunk (the day after the current_end)
        current_start = current_end + timedelta(days=1)


# COMMAND ----------

import pickle
# Define the folder path
folder_path = "/dbfs/FileStore/Weather_Databricks/"

# Define the pickle file path
pickle_filename = f"{folder_path}WeatherApi_historical_data.pkl"

# Save the data to the pickle file using DBFS utilities
with open(f"/dbfs{pickle_filename}", "wb") as f:
    pickle.dump(all_results, f)

# COMMAND ----------

import pickle
# Define the folder path
folder_path = "/dbfs/FileStore/Weather_Databricks/"

# Define the pickle file path
pickle_filename = f"{folder_path}WeatherApi_historical_data.pkl"

with open(f"/dbfs{pickle_filename}", "rb") as f:
    data = pickle.load(f)

# COMMAND ----------

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

# Convert a Pandas DataFrame to a Spark DataFrame
spark_df = spark.createDataFrame(df)
print("Row count:", spark_df.count())

# COMMAND ----------

spark_df = spark_df.sort("time_epoch")

# COMMAND ----------

spark_df = spark_df.filter(spark_df.time_epoch >= 1710028800)
display(spark_df)

# COMMAND ----------

spark_df.write.format("delta").mode("overwrite").saveAsTable("weather_db.bronze_weather_api")

# COMMAND ----------

