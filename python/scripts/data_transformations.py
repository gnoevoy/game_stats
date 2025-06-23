from functions.data_func import get_all_time_and_latest_month_results, get_actions, get_weapons, get_sessions, create_side_peak_cols
from functions.google_func import write_to_bucket, read_from_bucket
import pandas as pd
import numpy as np


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

    # Frags, deaths, headshots
    df = get_all_time_and_latest_month_results(df, "frags", "all_time_frags", "last_month_frags")
    df = get_all_time_and_latest_month_results(df, "deaths", "all_time_deaths", "last_month_deaths")
    df = get_all_time_and_latest_month_results(df, "headshots", "all_time_headshots", "last_month_headshots")

    # Side peaks columns
    df = create_side_peak_cols(df)

    # Actions, weapons and sessions tables
    actions = get_actions(df)
    weapons = get_weapons(df)
    sessions = get_sessions(df)

    cols_to_remove = ["frags", "deaths", "headshots", "actions", "side_peak", "weapons_stats", "sessions"]
    df.drop(columns=cols_to_remove, inplace=True)

    return df, actions, weapons, sessions


def transform_frags_data(data):
    dct = dict([(key, pd.Series(value)) for key, value in data.items()])
    df = pd.DataFrame(dct)

    df = df.melt(var_name="player_id", value_name="frags_data").dropna()
    df["player_id"] = df["player_id"].astype(int)
    df["killed_player_id"] = df["frags_data"].str.split(";").str[0].str.strip().astype(int)

    values = df["frags_data"].str.split(";").str[1].str.split("\n")
    df["kills"] = values.str[2].str.replace(",", "").astype(int)
    df["deaths"] = values.str[5].str.replace(",", "").astype(int)
    df["headshots"] = values.str[9].str.replace(",", "").astype(int)

    df.drop(columns=["frags_data"], inplace=True)
    return df


def transform_events_data(data):
    pass


def transform_data():
    players_data = read_from_bucket("raw/players_data")
    frags_data = read_from_bucket("raw/frags_data")
    events_data = read_from_bucket("raw/game_events")

    players, actions, weapons, sessions = transform_players_data(players_data)

    # players, actions, weapons, sessions = transform_players_data(players_data)
    # frags_table = transform_frags_data(frags_data)

    # players, actions, weapons, sessions = transform_players_data(players_data)
