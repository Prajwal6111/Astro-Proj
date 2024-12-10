import requests
import snowflake.connector
import json
import uuid
from dotenv import load_dotenv
import os

# Load .env variables
load_dotenv()

def API_Call(lat='14.447562', lon='75.904795', api_key=os.getenv("API_KEY")):
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
        user=os.getenv("user_name"),
        password=os.getenv("pass"),
account=os.getenv("account_name"),
        warehouse=os.getenv("warehouse_name"),
        database=os.getenv("database_name"),
        schema=os.getenv("schema_name"),
            role=os.getenv("role_name")
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
