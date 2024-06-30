import datetime as dt
import logging
import os
from pathlib import Path
from typing import List

import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry
import yaml
from yaml.loader import SafeLoader

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

directory = Path(__file__).parent
config_directory = directory.parent.parent


class InvalidParameters(Exception):
    """
    Raise if any of the given API input parameter is invalid,
    e.g. latitude/longitude outside allowed range

    """

def get_historical_weather_at_given_coordinates(
        latitude: float,
        longitude: float,
        start_date: str,
        end_date: str,
        fields: List[str] = ["snowfall", "wind_speed_10m", "wind_gusts_10m"]
) -> pd.core.frame.DataFrame:
    """
    Get historical weather data at a given (lat, long) from Open-Meteo
    open-source API for a given historical period

    Parameters
    ----------
    latitude
        latitude coordinate of the given location
    longitude
        longitude coordinate of the given location
    start_date
        start date of the historical period of interest
    end_date
        end date of the historical period of interest
    fields
        The weather data fields to be queried from the API.
        Default = ["snowfall", "wind_speed_10m", "wind_gusts_10m"]

    Returns
    -------
        historical weather data of the given period at the given location

    Raises
    ------
    InvalidParameters
        if any of the API input parameters are invalid

    """

    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://archive-api.open-meteo.com/v1/archive"

    if latitude < -90 or latitude > 90:
        raise InvalidParameters(
            'Invalid latitude given! Latitude must be within -90 to 90'
        )

    if longitude < 0 or longitude > 180:
        raise InvalidParameters(
            'Invalid longitude given! Longitude must be within 0 to 180'
        )

    try:
        start_date_parsed = dt.date.fromisoformat(start_date)
        end_date_parsed = dt.date.fromisoformat(end_date)
    except ValueError:
        raise InvalidParameters(
            f'Invalid start_date {start_date} or end_date {end_date} found. Please given the start and end dates in ISO format, i.e. YYYY-MM-DD'
        )

    if end_date_parsed < start_date_parsed:
        raise InvalidParameters(
            'End date cannot be before start date!'
        )

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": fields
    }

    logger.info('Calling the Open-Meteo API ...')
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]

    logger.info("Corresponding input query parameters are:")
    logger.info(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
    logger.info(f"Elevation {response.Elevation()} m asl")
    logger.info(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
    logger.info(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

    # Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    hourly_snowfall = hourly.Variables(0).ValuesAsNumpy()
    hourly_wind_speed_10m = hourly.Variables(1).ValuesAsNumpy()
    hourly_wind_gusts_10m = hourly.Variables(2).ValuesAsNumpy()

    # print(hourly_wind_speed_10m)

    hourly_data = {"date": pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    )}

    hourly_data["snowfall"] = hourly_snowfall
    hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
    hourly_data["wind_gusts_10m"] = hourly_wind_gusts_10m

    return pd.DataFrame(data=hourly_data)


if __name__ == '__main__':

    logger.info(f"Loading the config file from local ...")

    # config parameters, change this accordingly
    config_path = 'config/get_historical_weather.yaml'  # relative local config filepath which include the given historical period and locations

    # load config setting file(s)
    with open(config_directory / config_path, 'r') as f:
        config = yaml.load(f, Loader=SafeLoader)

    # local parent export folder (relative to repo)
    export_parent_folder = config['export_parent_path']

    start_date = config['start_date']
    end_date = config['end_date']

    selected_airports = config['selected_airports']

    for selected_airport, location in selected_airports.items():

        logger.info(f"Start making API call for the airport {selected_airport} ...")

        latitude = location['latitude']
        longitude = location['longitude']

        df = get_historical_weather_at_given_coordinates(
            latitude,
            longitude,
            start_date,
            end_date
        )

        save_dir = directory / f'{export_parent_folder}/{selected_airport}'
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)

        df.to_csv(
            str(save_dir / f'airport_{selected_airport}_weather_{start_date}_to_{end_date}.csv'),
            index=False
        )

