from functions.data_func import get_all_time_and_latest_month_results, get_actions, get_weapons, get_names, create_side_peak_cols
from functions.google_func import write_to_bucket, read_from_bucket, write_to_bigquery
from datetime import datetime
import pandas as pd
import pytz


# Function to create 4 different tables from players json file
def transform_players_data(data):
    df = pd.DataFrame(data)

    # Clean simple columns
    df["player_id"] = df["player_id"].astype(int)
    df["player_name"] = df["player_name"].str.strip()
    df["steam_id"] = df["steam_id"].str.split().str[-1]
    df["rank"] = df["rank"].str.split("\n").str[-1].astype(int)
    df["experience"] = df["experience"].str.split("\n").str[-1].str.replace(",", "").astype(int)
    df["frags_per_minute"] = df["frags_per_minute"].str.split("\t").str[-1].astype(float)
    df["kill_streak"] = df["kill_streak"].str.split("\t").str[-1].astype(int)
    df["death_streak"] = df["death_streak"].str.split("\t").str[-1].astype(int)
    df["suicides"] = df["suicides"].str.split("\t").str[-1].astype(int)

    # Frags, deaths, headshots and side peaks columns
    df = get_all_time_and_latest_month_results(df, "frags", "all_time_frags", "last_month_frags")
    df = get_all_time_and_latest_month_results(df, "deaths", "all_time_deaths", "last_month_deaths")
    df = get_all_time_and_latest_month_results(df, "headshots", "all_time_headshots", "last_month_headshots")
    df = create_side_peak_cols(df)

    # Create seperate tables for names, actions, weapons and sessions
    names = get_names(df)
    actions = get_actions(df)
    weapons = get_weapons(df)

    # Remove raw columns
    cols_to_remove = ["frags", "deaths", "headshots", "actions", "side_peak", "weapons_stats", "used_names"]
    df.drop(columns=cols_to_remove, inplace=True)

    return df, actions, weapons, names


# Create frags table
def transform_frags_data(data):
    # Tricky reshaping because dictionary lists have unequal lengths
    dct = dict([(key, pd.Series(value)) for key, value in data.items()])
    df = pd.DataFrame(dct)
    df = df.melt(var_name="player_id", value_name="value").dropna()

    # Extract values from strings
    values = df["value"].str.split(";").str[1].str.split("\n")
    df["player_id"] = df["player_id"].astype(int)
    df["killed_player_id"] = df["value"].str.split(";").str[0].str.strip().astype(int)
    df["kills"] = values.str[2].str.replace(",", "").astype(int)
    df["deaths"] = values.str[5].str.replace(",", "").astype(int)
    df["headshots"] = values.str[9].str.replace(",", "").astype(int)

    df.drop(columns=["value"], inplace=True)
    return df


# Create events table for my profile
def transform_events_data(data):
    df = pd.DataFrame(data, columns=["value"])
    values = df["value"].str.split("\n")

    # Convert to Moscow time since server located in this region
    df["timestamp"] = pd.to_datetime(values.str[0].str.strip())
    moscow_tz = pytz.timezone("Europe/Moscow")
    df["timestamp"] = df["timestamp"].dt.tz_localize(moscow_tz)

    df["event"] = values.str[1].str.strip()
    df["description"] = values.str[2].str.strip().str.replace(".", "")

    # Filter out unwanted events
    df = df[~df["event"].isin(["Team Bonus", "Action"])]
    df.drop(columns=["value"], inplace=True)

    return df


# Create my sessions table
def transform_sessions_data(data):
    df = pd.DataFrame(data, columns=["value"])

    values = df["value"].str.split("\n")
    df["date"] = values.str[0].astype("datetime64[ns]")
    df["experience_change"] = values.str[1].astype(int)
    df["experience"] = values.str[2].str.replace(",", "").astype(int)
    df["frags"] = values.str[4].astype(int)
    df["deaths"] = values.str[5].astype(int)
    df["headshots"] = values.str[7].astype(int)

    # Retrive time played in minutes
    time_played = values.str[3].str.split().str[-1].str[:-1]
    df["time_played_in_minutes"] = round(pd.to_timedelta(time_played).dt.seconds / 60, 0).astype(int)
    df.drop(columns=["value"], inplace=True)

    return df


# Create a timestamp to know at what time the data is valid
def get_timestamp_table():
    warsaw_tz = pytz.timezone("Europe/Warsaw")
    warsaw_time = datetime.now(warsaw_tz)
    df = pd.DataFrame({"valid_at": [warsaw_time]})
    return df


# Wrapper function to combine all together
def transform_data():
    print("03. STARTING DATA TRANSFORMATION ...")

    # Load json files from the bucket
    players_data = read_from_bucket("raw/players_data")
    frags_data = read_from_bucket("raw/frags_data")
    events_data = read_from_bucket("raw/game_events")
    sessions_data = read_from_bucket("raw/my_sessions")
    print("Raw json files successfully uploaded")

    # Final tables
    players, actions, weapons, names = transform_players_data(players_data)
    events = transform_events_data(events_data)
    frags = transform_frags_data(frags_data)
    sessions = transform_sessions_data(sessions_data)
    timestamp = get_timestamp_table()
    print("Data transformation completed, created 8 tables")

    # Write dataframes as csv files to the bucket
    write_to_bucket("clean/players", players, file_type="csv")
    write_to_bucket("clean/actions", actions, file_type="csv")
    write_to_bucket("clean/weapons", weapons, file_type="csv")
    write_to_bucket("clean/names", names, file_type="csv")
    write_to_bucket("clean/sessions", sessions, file_type="csv")
    write_to_bucket("clean/frags", frags, file_type="csv")
    write_to_bucket("clean/events", events, file_type="csv")
    write_to_bucket("clean/timestamp", timestamp, file_type="csv")
    print("Data successfully written to the bucket")


# Final step in python script, load everything to BigQuery
def load_tables_to_bigquery():
    print("04. LOADING DATA TO BIGQUERY ...")
    tables = ["actions", "events", "frags", "players", "sessions", "weapons", "names", "timestamp"]
    for table in tables:
        write_to_bigquery(f"clean/{table}", table)
    print("Data successfully loaded to BigQuery")
