import requests
import snowflake.connector
import json
import uuid

def API_Call(lat='14.447562', lon='75.904795', api_key='4057e6afe2e0c6f80ce15f3772ac4e19'):
    # Define the API endpoint and the parameters
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        'lat': lat,
        'lon': lon,
        'appid': api_key
    }

    # Make the API request
    response = requests.get(url, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the response JSON data
        weather_data = str(response.json())
        print("Making API Call...")
        print(weather_data)
        store_to_snowflake(weather_data)
    else:
        print(f"Failed to get weather data. HTTP Status code: {response.status_code}")
        return None

def store_to_snowflake(weather_data):
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user='PRAJWAL',
        password='Internship@123',
account='ng48387.ap-southeast-1',
        warehouse='COMPUTE_WH',
        database='PRAJWAL_ELT',
        schema='WEATHER_API',
            role='ACCOUNTADMIN'
    )

    # Create a cursor
    cur = conn.cursor()

    try:
        # Insert the data into the Snowflake table
        query = "INSERT INTO weather_raw_data (id, data) VALUES (%s, %s)"
        print("Inserting data...")
        # Use UUID as an identifier for each entry
        id = str(uuid.uuid4())
        cur.execute(query, (id, weather_data))
        print("Data inserted successfully.")
    except Exception as e:
        print("Error inserting data:", e)
    finally:
        cur.close()
        conn.close()

# Call the function
API_Call()
