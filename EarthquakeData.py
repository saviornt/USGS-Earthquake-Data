import requests
import subprocess
import pandas as pd
from pymongo import MongoClient as mc
from pymongo import errors as pye
from time import sleep
from datetime import datetime

# Get MongoDB connection details
mongo_uri = "mongodb://localhost:27017/"
mongo_db = "geographical_data"
mongo_collection = "earthquakes"

def get_earthquake_data():
    url = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson"
    response = requests.get(url)
    return response.json()

def create_database(mongo_uri=mongo_uri, db_name=mongo_db, collection_name=mongo_collection):
    # Connect to MongoDB
    client = mc(mongo_uri)

    # Get the database object or create it if it doesn't exist
    db = client[db_name] # Get the existing database

    # Check if the collection exists
    collection_names = db.list_collection_names()
    if collection_name not in collection_names:
        db.create_collection(collection_name)  # Create collection if it doesn't exist

    client.close()
    return db

def update_database(df, collection):
    collection.insert_many(df.to_dict(orient="records"), ordered=False)

def is_mongodb_running():
    command = ["sc", "query", "MongoDB"]
    try:
        output = subprocess.check_output(command, stderr=subprocess.STDOUT)
        return "RUNNING" in output.decode()
    except subprocess.CalledProcessError as e:
        return False

def start_mongodb_service():
    command = ["net", "start", "MongoDB"]
    try:
        subprocess.check_output(command, stderr=subprocess.STDOUT)
        print("Successfully started MongoDB service.")
    except subprocess.CalledProcessError as e:
        print(f"Error starting MongoDB service: {e}")

def create_dataframe(earthquake_data):
    # Create a dataframe of the JSON data
    data = []
    for feature in earthquake_data["features"]:
        # Split the Epoch time into normal date and time
        properties = feature["properties"]
        timestamp_ms = properties["time"]
        timestamp_s = timestamp_ms / 1000  # Convert milliseconds to seconds (if needed)
        date_time = datetime.utcfromtimestamp(timestamp_s)
        data.append(
            {
                "usgs_id": feature["id"],
                "date": date_time.strftime("%Y-%m-%d"),
                "time": date_time.strftime("%H:%M:%S"),
                "longitude": feature["geometry"]["coordinates"][0],
                "latitude": feature["geometry"]["coordinates"][1],
                "magnitude": properties["mag"],
                "place": properties.get("place"),  # Get place if available
                "event_url": properties["url"]
            }
        )
    df = pd.DataFrame(data)
    return df

if __name__ == '__main__':
    # Set the update interval
    update_interval = 60

    # Check to see if MongoDB is running, if it is not, then start it.
    if not is_mongodb_running():
        start_mongodb_service()

    # Create the MongoDB database
    db = create_database()

    # Connect to the MongoDB database and collection
    client = mc(mongo_uri)
    db = client[mongo_db]
    collection = db[mongo_collection]
    collection.create_index('usgs_id', unique=True)

    while True:
        data = get_earthquake_data()
        df = create_dataframe(data)
        try:
            update_database(df, collection)
        except pye.BulkWriteError as e:
            pass
        sleep(update_interval)