from scripts.google_functions import write_to_bucket, read_from_bucket
import pandas as pd
import numpy as np


# 3 files
# from players data -> sessions, weapons, actions, primary table


def transform_players_data(data):
    df = pd.DataFrame(data)

    def get_all_time_and_latest_month_results(df, col, col1_name, col2_name):
        values = df[col].str.split("\t").str[-1].str.split()
        all_time_value = values.str[0].str.replace(",", "").astype(int)
        last_month_value = values.str[1].str[1:-2].str.replace(",", "").astype(int)

        df[col1_name] = all_time_value
        df[col2_name] = last_month_value
        return df

    def get_actions(df):
        actions = df[["player_id", "actions"]]
        actions = actions.explode("actions").reset_index(drop=True)

        values = actions["actions"].str.split("\n")
        actions["action_name"] = values.str[1]
        actions["value"] = values.str[2].str.replace(",", "").astype(int)

        actions.drop(columns=["actions"], inplace=True)
        return actions

    def get_weapons(df):
        weapons_cols = pd.json_normalize(df["weapons_stats"])
        weapons = pd.merge(df["player_id"], weapons_cols, left_index=True, right_index=True, how="left")
        weapons = weapons.melt(id_vars=["player_id"], var_name="weapon", value_name="value").dropna()

        values = weapons["value"].str.split("\n")
        weapons["frags"] = values.str[3].str.replace(",", "").astype(int)
        weapons["headshots"] = values.str[6].str.replace(",", "").astype(int)

        weapons.drop(columns=["value"], inplace=True)
        return weapons

    def get_sessions(df):
        sessions = pd.DataFrame(df["sessions"].dropna()[0], columns=["values"])
        values = sessions["values"].str.split("\n")

        sessions["date"] = values.str[0].astype("datetime64[ns]")
        sessions["experience_change"] = values.str[1].astype(int)
        sessions["experience"] = values.str[2].str.replace(",", "").astype(int)

        time_played = values.str[3].str.split().str[-1].str[:-1]
        sessions["time_played_minutes"] = round(pd.to_timedelta(time_played).dt.seconds / 60, 0).astype(int)

        sessions["frags"] = values.str[4].astype(int)
        sessions["deaths"] = values.str[5].astype(int)
        sessions["headshots"] = values.str[7].astype(int)

        sessions.drop(columns=["values"], inplace=True)
        return sessions

    # Clean simple columns
    df["player_name"] = df["player_name"].str.strip()
    df["steam_id"] = df["steam_id"].str.split().str[-1]
    df["rank"] = df["rank"].str.split("\n").str[-1].astype(int)
    df["experience"] = df["experience"].str.split("\n").str[-1].str.replace(",", "").astype(int)
    df["frags_per_minute"] = df["frags_per_minute"].str.split("\t").str[-1].astype(float)
    df["kill_streak"] = df["kill_streak"].str.split("\t").str[-1].astype(int)
    df["death_streak"] = df["death_streak"].str.split("\t").str[-1].astype(int)
    df["suicides"] = df["suicides"].str.split("\t").str[-1].astype(int)

    df = get_all_time_and_latest_month_results(df, "frags", "all_time_frags", "last_month_frags")
    df = get_all_time_and_latest_month_results(df, "deaths", "all_time_deaths", "last_month_deaths")
    df = get_all_time_and_latest_month_results(df, "headshots", "all_time_headshots", "last_month_headshots")

    actions = get_actions(df)
    weapons = get_weapons(df)
    sessions = get_sessions(df)

    # df.drop
    return df, actions, weapons, sessions


def transform_data():
    players_data = read_from_bucket("raw/players_data")

    transform_players_data(players_data)
