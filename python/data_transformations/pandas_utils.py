import pandas as pd
import numpy as np
import pytz


server_timezone = pytz.timezone("Europe/Moscow")
pd.options.mode.chained_assignment = None


# Functions to transform players data


def transform_players_data(df):
    # Clean simple columns
    df["player_id"] = df["player_id"].astype(int)
    df["player_name"] = df["player_name"].str.strip()
    df["steam_id"] = df["steam_id"].str.split().str[-1]
    df["rank"] = df["rank"].str.split("\n").str[-1].astype(int)
    df["experience"] = df["experience"].str.split("\n").str[-1].str.replace(",", "").astype(int)
    df["frags_per_minute"] = df["frags_per_minute"].str.split("\t").str[-1].astype(float)
    df["CT_side_peaks"] = df["CT_side_peaks"].fillna("").str.split("\n").str[2].replace("", None).astype("Int64")
    df["T_side_peaks"] = df["T_side_peaks"].fillna("").str.split("\n").str[2].replace("", None).astype("Int64")

    # Helper function to apply for several columns
    def get_all_time_and_last_30_days_stats(col, col1_name, col2_name):
        # Split source column and extract 2 values
        values = df[col].str.split("\t").str[-1].str.split()
        all_time_value = values.str[0].str.replace(",", "").astype(int)
        last_30_days_value = values.str[1].str[1:-2].str.replace(",", "").astype(int)

        # Assign new columns
        df[col1_name] = all_time_value
        df[col2_name] = last_30_days_value

    get_all_time_and_last_30_days_stats("frags", "kills", "last_30_days_kills")
    get_all_time_and_last_30_days_stats("deaths", "deaths", "last_30_days_deaths")
    get_all_time_and_last_30_days_stats("headshots", "headshots", "last_30_days_headshots")

    df = df.drop(columns=["frags"])
    return df


def transform_player_names(df):
    values = df["value"].str.split("\n")
    df["name"] = values.str[1].str.strip()

    # Add timezone aware datetime column to store proper UTC time in BigQuery
    df["last_used"] = pd.to_datetime(values.str[3].str.strip())
    df["last_used"] = df["last_used"].dt.tz_localize(server_timezone)

    df = df.drop(columns=["value"])
    return df


def transform_player_actions(df):
    values = df["action"].str.split("\n")
    df["action_name"] = values.str[1].str.strip()
    df["value"] = values.str[2].str.replace(",", "").astype(int)

    # Remove unwanted actions
    actions_to_remove = ["Touch a Hostage", "Rescue a Hostage", "Headshot"]
    df = df[~df["action_name"].isin(actions_to_remove)]

    # Rename actions names
    translation_map = {
        "Double Kill (2 фрага)": "Double Kill - 2 kills",
        "Triple Kill (3 фрага)": "Triple Kill - 3 kills",
        "Domination (4 фрага)": "Domination - 4 kills",
        "Rampage (5 фрагов)": "Rampage - 5 kills",
        "Mega Kill (6 фрагов)": "Mega Kill - 6 kills",
        "Ownage (7 фрагов)": "Ownage - 7 kills",
        "Ultra Kill (8 фрагов)": "Ultra Kill - 8 kills",
        "Killing Spree (9 фрагов)": "Killing Spree - 9 kills",
        "Monster Kill (10 фрагов)": "Monster Kill - 10 kills",
        "Unstoppable (11 фрагов)": "Unstoppable - 11 kills",
        "God Like (12+ фрагов)": "God Like - 12+ kills",
    }
    df["action_name"] = df["action_name"].replace(translation_map)
    df = df.drop(columns=["action"])
    return df


def transform_player_weapons(df):
    values = df["value"].str.split("\n")
    df["frags"] = values.str[3].str.replace(",", "").astype(int)
    df["headshots"] = values.str[6].str.replace(",", "").astype(int)
    df = df.drop(columns=["value"])
    return df


def transform_player_frags(df):
    values = df["value"].str.split("\n")
    df["kills"] = values.str[2].str.replace(",", "").astype(int)
    df["deaths"] = values.str[5].str.replace(",", "").astype(int)
    df["headshots"] = values.str[9].str.replace(",", "").astype(int)
    df = df.drop(columns=["value"])
    return df


def transform_sessions(df):
    values = df["value"].str.split("\n")

    # Get simple columns
    df["date"] = values.str[0].astype("datetime64[ns]")
    df["experience_change"] = values.str[1].astype(int)
    df["experience"] = values.str[2].str.replace(",", "").astype(int)
    df["kills"] = values.str[4].astype(int)
    df["deaths"] = values.str[5].astype(int)
    df["headshots"] = values.str[7].astype(int)

    # Retrive time played in minutes
    time_played = values.str[3].str.split().str[-1].str[:-1]
    df["time_played_in_minutes"] = round(pd.to_timedelta(time_played).dt.seconds / 60, 0).astype(int)

    df = df.drop(columns=["value"])
    return df


def transform_events(df):
    df["event_index"] = df["event_index"].astype(int)
    values = df["value"].str.split("\n")

    # Timezone aware datetime
    df["event_timestamp"] = pd.to_datetime(values.str[0].str.strip())
    df["event_timestamp"] = df["event_timestamp"].dt.tz_localize(server_timezone)

    df["event"] = values.str[1].str.strip()
    df["description"] = values.str[2].str.strip().str.replace(".", "")

    # Filter out unwanted events
    df = df[~df["event"].isin(["Team Bonus", "Action"])]
    df = df.drop(columns=["value"])
    return df
