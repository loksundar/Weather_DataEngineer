# Databricks notebook source
import requests
import pprint
import time
from datetime import datetime, timedelta
import pickle

# COMMAND ----------

df_openweather_api = spark.read.format("delta").table("weather_db.bronze_openweather_api")
display(df_openweather_api)

# COMMAND ----------

from pyspark.sql.functions import from_unixtime, col
max_date_available = datetime.utcfromtimestamp(df_openweather_api.agg({"dt": "max"}).first()[0])
max_date_available

# COMMAND ----------

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

# Define the start and end dates for the range (can be changed as per requirement)
end_date = datetime.today() 
 # Example start date (replace as needed)
start_date = max_date_available  # Example end date (replace as needed)
start_date = max_date_available - timedelta(days=5) 
# Maximum allowed days per request (7 days based on your observation)
max_days = 7

# List to hold aggregated data
all_data = []

# Loop to fetch data in chunks
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
    
    # Make the request to the API
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        # Assuming the API returns data under the 'list' key:
        if 'list' in data:
            all_data.extend(data['list'])
            print(f"Fetched data from {current_date} to {next_date} successfully")
        else:
            print(f"No 'list' key in response for period {current_date} to {next_date}")
    else:
        print(f"Failed to fetch data from {current_date} to {next_date}: {response.status_code}")
    
    # Pause between requests to respect rate limits
    time.sleep(1)
    
    # Move to the next chunk
    current_date = next_date

# Now `all_data` contains the aggregated data for the required range
print(f"Fetched {len(all_data)} records in total.")


# COMMAND ----------

# silver Table Function

import pandas as pd

# First, flatten the main nested dictionaries using json_normalize.
df = pd.json_normalize(all_data, sep='_')
# The 'weather' column remains a list of dict(s). Extract the first element from each list.
df['weather'] = df['weather'].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else {})

# Normalize the weather dictionary into its own DataFrame.
weather_df = pd.json_normalize(df['weather'])
# Rename the columns to indicate these are weather-related values.
weather_df.columns = [f'weather_{col}' for col in weather_df.columns]

# Remove the original weather column and join the normalized weather DataFrame.
try:
    df = df.drop(columns=['weather',"rain_1h"]).join(weather_df)
except:
    df = df.drop(columns=['weather']).join(weather_df)
df.head()

# COMMAND ----------

df.head()

# COMMAND ----------

new_df = spark.createDataFrame(df)
df.shape

# COMMAND ----------

new_df.createOrReplaceTempView("new_df_temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO weather_db.bronze_openweather_api
# MAGIC SELECT * FROM new_df_temp