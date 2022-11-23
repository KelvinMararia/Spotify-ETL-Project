import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime, timedelta
import sqlite3

DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
USER_ID = "Dac_Monalds"
TOKEN = ""


if __name__ == "__main__":
    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }

    today = datetime.now()
    yesterday = today - timedelta(days = 3)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time = yesterday_unix_timestamp), headers = headers)

    data = r.json()

    song_name = []
    artist_name = []
    played_at_list = []
    timestamp = []

    for song in data["items"]:
        song_name.append(song["track"]["name"])
        artist_name.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamp.append(song["played_at"][0:10])

    song_dict = {
        "song_name": song_name,
        "artist_name": artist_name,
        "played_at_list": played_at_list,
        "timestamp": timestamp
    }

    song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at_list", "timestamp"])

    print(song_df)