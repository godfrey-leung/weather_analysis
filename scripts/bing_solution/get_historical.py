import requests

# Define your API endpoint
API_URL = "https://api.open-meteo.com/v1/archive"

# Set your location (latitude and longitude)
latitude = 51.5074  # Example: London, UK
longitude = -0.1278

# Set the time period (start and end dates)
start_date = "2023-01-01"
end_date = "2023-01-31"

# Specify the weather variables you're interested in (e.g., snowfall and windspeed)
weather_variables = ["Snowfall", "Wind Speed (10 m)"]

# Construct the API request
params = {
    "lat": latitude,
    "lon": longitude,
    "start": start_date,
    "end": end_date,
    "vars": ",".join(weather_variables),
}

# Send the request
response = requests.get(API_URL, params=params)

# Process the response (you can customize this part based on your needs)
if response.status_code == 200:
    data = response.json()
    # Extract and work with the historical weather data
    # (e.g., snowfall and windspeed for each day)
    for day in data["data"]:
        date = day["date"]
        snowfall = day["Snowfall"]
        windspeed = day["Wind Speed (10 m)"]
        print(f"Date: {date}, Snowfall: {snowfall}, Windspeed: {windspeed}")
else:
    print(f"Error fetching data. Status code: {response.status_code}")