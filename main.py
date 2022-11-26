import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime, timedelta
import sqlite3

DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
USER_ID = "Dac_Monalds"
TOKEN = "BQAuZ_6dFz92leNifzCxxjBCZqgjJ66iQjUxfwoeCihkFc_MF7UPS5tHXTReDl1ct0Dm79coySF-_qYfnuY_4gWn45KOYWmu-gb-wYe08fdZy_I_7GiccgFYzjLXIi-YCclcpSMeuDz61v07emBmLIjetTe-WD7hAbogrux0GTAX7ldPDgPng37EVzoFPbeV4YKSe0mW"
# Token generate Link: https://developer.spotify.com/console/get-recently-played/

# We need to valide that the data being recieved is correct
def check_if_valid_data(df: pd.DataFrame) ->bool:
    # Check if the dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False

    # Check if there are duplicates in the data received using the time played as the primary key
    if pd.Series(df["played_at"]).is_unique:
        pass
    else:
        raise Exception("Primary Key Check violated. Duplicates found in the data recieved.")

    # Check if some of the data is missing
    if df.isnull().values.any():
        raise Exception("Null values found")

    # Check if all timestamps are of 3 days ago
    daysAgo = datetime.now() - timedelta(days = 60)
    daysAgo = daysAgo.replace(hour=0, minute=0, second=0, microsecond=0)
    
    timestamps = df["timestamp"].to_list()
    for timestamp in timestamps:
        if datetime.strptime(timestamp, "%Y-%m-%d") < daysAgo:
            raise Exception("At least one of the received songs was not played within the last 72 hours")
    
    return True


if __name__ == "__main__":
    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }

    today = datetime.now()
    daysAgo = today - timedelta(days = 60)
    daysAgo_unix_timestamp = int(daysAgo.timestamp()) * 1000

    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time = daysAgo_unix_timestamp), headers = headers)

    data = r.json()

    song_name = []
    artist_name = []
    played_at_list = []
    timestamp = []

    # Extracting relevant data from the json file received
    for song in data["items"]:
        song_name.append(song["track"]["name"])
        artist_name.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamp.append(song["played_at"][0:10])

    # Prepare a dictionary then convert it into a pandas dataframe
    song_dict = {
        "song_name": song_name,
        "artist_name": artist_name,
        "played_at": played_at_list,
        "timestamp": timestamp
    }

    song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])
    print(song_df)
    # Validate the data
    if check_if_valid_data(song_df):
        print("Data is valid, proceed to the load stage")

    # Load the data
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    connection = sqlite3.connect("my_played_tracks.sqlite")
    cursor = connection.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """
    cursor.execute(sql_query)
    print("Opened database successfully")

    try:
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists="append")
    except:
        print("Data already exists in the database")

    connection.close()
    print("Closed database successfully")


#The end