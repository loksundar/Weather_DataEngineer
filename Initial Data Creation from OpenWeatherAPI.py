# Databricks notebook source
#%pip install azure-identity azure-keyvault-secrets
#%restart_python

# COMMAND ----------

import requests
import pprint
import time
from datetime import datetime, timedelta
import pickle

# Your OpenWeatherMap API key and location coordinates
api_key = "87a9ca0fbcc5950231bbb11c02f78777"
zip_code = "75080"
# Step 1: Fetch latitude and longitude using the zip code
geo_url = f"http://api.openweathermap.org/data/2.5/weather?zip={zip_code},us&appid={api_key}"

geo_response = requests.get(geo_url)

if geo_response.status_code == 200:
    geo_data = geo_response.json()
    lat = geo_data['coord']['lat']
    lon = geo_data['coord']['lon']
    print(f"Latitude: {lat}, Longitude: {lon}")
else:
    print(f"Failed to get coordinates for zip code: {zip_code}. Error: {geo_response.status_code}")
    exit()

# Define the date range: from March 1st, 2024 to today
start_date = datetime(2024, 3, 12)
end_date = datetime.today()  # or datetime.now() if preferred

# Maximum allowed days per request (7 days based on your observation)
max_days = 7

# List to hold aggregated data
all_data = []

current_date = start_date
while current_date < end_date:
    # Define the end of this chunk (ensuring we don't pass the end_date)
    next_date = current_date + timedelta(days=max_days)
    if next_date > end_date:
        next_date = end_date

    # Convert to Unix timestamps
    start_timestamp = int(current_date.timestamp())
    end_timestamp = int(next_date.timestamp())

    # Construct the URL with the current time range
    url = (
        f"https://history.openweathermap.org/data/2.5/history/city?"
        f"lat={lat}&lon={lon}&type=hour&start={start_timestamp}&end={end_timestamp}&appid={api_key}"
    )
    
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        # Assuming the API returns data under the 'list' key:
        if 'list' in data:
            all_data.extend(data['list'])
            print(f"Fetched data from {current_date} to {next_date} Successfully")
        else:
            print(f"No 'list' key in response for period {current_date} to {next_date}")
    else:
        print(f"Failed to fetch data from {current_date} to {next_date}: {response.status_code}")
    
    # Pause between requests to respect rate limits
    time.sleep(1)
    
    # Move to the next chunk
    current_date = next_date

# COMMAND ----------

# Define the folder path
folder_path = "/dbfs/FileStore/Weather_Databricks/"

# Ensure the folder exists
dbutils.fs.mkdirs(folder_path)

# Define the pickle file path
pickle_filename = f"{folder_path}OPenweatherApi_historical_data.pkl"

# Save the data to the pickle file using DBFS utilities
with open(f"/dbfs{pickle_filename}", "wb") as f:
    pickle.dump(all_data, f)

# COMMAND ----------

# Define the folder path
folder_path = "/dbfs/FileStore/Weather_Databricks/"

# Define the pickle file path
pickle_filename = f"{folder_path}OPenweatherApi_historical_data.pkl"
import pickle
with open(f"/dbfs{pickle_filename}", "rb") as f:
    data = pickle.load(f)

# COMMAND ----------

# silver Table Function

import pandas as pd

# First, flatten the main nested dictionaries using json_normalize.
df = pd.json_normalize(data, sep='_')
# The 'weather' column remains a list of dict(s). Extract the first element from each list.
df['weather'] = df['weather'].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else {})

# COMMAND ----------

# Normalize the weather dictionary into its own DataFrame.
weather_df = pd.json_normalize(df['weather'])
# Rename the columns to indicate these are weather-related values.
weather_df.columns = [f'weather_{col}' for col in weather_df.columns]

# Remove the original weather column and join the normalized weather DataFrame.
df = df.drop(columns=['weather',"rain_1h","snow_1h"]).join(weather_df)
df.head()

# COMMAND ----------

# Convert a Pandas DataFrame to a Spark DataFrame
spark_df = spark.createDataFrame(df)
print("Row count:", spark_df.count())

# COMMAND ----------

spark_df = spark_df.filter(spark_df.dt >= 1710133200)
display(spark_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS weather_db;
# MAGIC USE weather_db;

# COMMAND ----------

spark_df.write.format("delta").mode("overwrite").saveAsTable("weather_db.bronze_openweather_api")