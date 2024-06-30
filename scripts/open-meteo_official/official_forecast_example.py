from pathlib import Path


import openmeteo_requests

import requests_cache
import pandas as pd
from retry_requests import retry

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://api.open-meteo.com/v1/forecast"
params = {
	"latitude": 52.52,
	"longitude": 13.41,
	"hourly": "visibility"
}
responses = openmeteo.weather_api(url, params=params)

# Process first location. Add a for-loop for multiple locations or weather models
response = responses[0]
print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
print(f"Elevation {response.Elevation()} m asl")
print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

# Process hourly data. The order of variables needs to be the same as requested.
hourly = response.Hourly()
hourly_visibility = hourly.Variables(0).ValuesAsNumpy()

hourly_data = {"date": pd.date_range(
	start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
	end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
	freq = pd.Timedelta(seconds = hourly.Interval()),
	inclusive = "left"
)}
hourly_data["visibility"] = hourly_visibility

hourly_dataframe = pd.DataFrame(data = hourly_data)
print(hourly_dataframe)


if __name__ == '__main__':

    # config parameters, change this accordingly
    config_path = 'config/config.yaml'    # relative local filepath to get the list of airport ICAO codes
    end_date = '2024-01-01'         # end date to query the flight data
    number_of_weeks = 53            # default, 53 weeks for a year of data

    # Getting corresponding start date
    start_date = dt.datetime.fromisoformat(end_date) - relativedelta(weeks=number_of_weeks)

    # load config setting file(s)
    with open(directory / config_path, 'r') as f:
        config = yaml.load(f, Loader=SafeLoader)