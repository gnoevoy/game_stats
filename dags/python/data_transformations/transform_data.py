from python.gcp_utils import write_to_bucket, read_from_bucket
from airflow.sdk import task
import pandas as pd
import numpy as np
import logging
import pytz


logger = logging.getLogger(__name__)
server_timezone = pytz.timezone("Europe/Moscow")


@task()
def transform_players_data():
    df = read_from_bucket("raw/players.csv", "csv")

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

    df.drop(columns=["frags"], inplace=True)
    logger.info(">>> Players data transformed successfully")
    write_to_bucket("clean/players.csv", df, "csv")


@task()
def transform_player_names():
    df = read_from_bucket("raw/names.csv", "csv")
    values = df["value"].str.split("\n")
    df["name"] = values.str[1].str.strip()

    # Timezone aware datetime
    df["last_used"] = pd.to_datetime(values.str[3].str.strip())
    df["last_used"] = df["last_used"].dt.tz_localize(server_timezone)

    df.drop(columns=["value"], inplace=True)
    logger.info(">>> Player names data transformed successfully")
    write_to_bucket("clean/names.csv", df, "csv")


@task()
def transform_player_actions():
    df = read_from_bucket("raw/actions.csv", "csv")
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
    df["action_name"] = df["action_name"].map(translation_map).fillna(df["action_name"])

    df.drop(columns=["action"], inplace=True)
    logger.info(">>> Player actions data transformed successfully")
    write_to_bucket("clean/actions.csv", df, "csv")


@task()
def transform_player_weapons():
    df = read_from_bucket("raw/weapons.csv", "csv")
    values = df["value"].str.split("\n")
    df["frags"] = values.str[3].str.replace(",", "").astype(int)
    df["headshots"] = values.str[6].str.replace(",", "").astype(int)
    df.drop(columns=["value"], inplace=True)
    logger.info(">>> Player weapons data transformed successfully")
    write_to_bucket("clean/weapons.csv", df, "csv")


@task()
def transform_player_frags():
    df = read_from_bucket("raw/frags.csv", "csv")
    values = df["value"].str.split("\n")
    df["kills"] = values.str[2].str.replace(",", "").astype(int)
    df["deaths"] = values.str[5].str.replace(",", "").astype(int)
    df["headshots"] = values.str[9].str.replace(",", "").astype(int)
    df.drop(columns=["value"], inplace=True)
    logger.info(">>> Player frags data transformed successfully")
    write_to_bucket("clean/frags.csv", df, "csv")


@task()
def transform_sessions():
    df = read_from_bucket("raw/sessions.csv", "csv")
    values = df["value"].str.split("\n")

    # Get simle columns
    df["date"] = values.str[0].astype("datetime64[ns]")
    df["experience_change"] = values.str[1].astype(int)
    df["experience"] = values.str[2].str.replace(",", "").astype(int)
    df["kills"] = values.str[4].astype(int)
    df["deaths"] = values.str[5].astype(int)
    df["headshots"] = values.str[7].astype(int)

    # Retrive time played in minutes
    time_played = values.str[3].str.split().str[-1].str[:-1]
    df["time_played_in_minutes"] = round(pd.to_timedelta(time_played).dt.seconds / 60, 0).astype(int)

    df.drop(columns=["value"], inplace=True)
    logger.info("Sessions data transformed successfully")
    write_to_bucket("clean/sessions.csv", df, "csv")


@task()
def transform_events():
    df = read_from_bucket("raw/events.csv", "csv")
    values = df["value"].str.split("\n")

    # Timezone aware datetime
    df["timestamp"] = pd.to_datetime(values.str[0].str.strip())
    df["timestamp"] = df["timestamp"].dt.tz_localize(server_timezone)

    df["event"] = values.str[1].str.strip()
    df["description"] = values.str[2].str.strip().str.replace(".", "")

    # Column that helps to keep correct order of events if there're multiple events occurred at the same time
    # Highest index means the latest event
    df["event_index"] = range(len(df), 0, -1)

    # Filter out unwanted events
    df = df[~df["event"].isin(["Team Bonus", "Action"])]
    df.drop(columns=["value"], inplace=True)
    logger.info("Events data transformed successfully")
    write_to_bucket("clean/events.csv", df, "csv")
