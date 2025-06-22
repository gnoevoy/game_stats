from scripts.google_functions import write_to_bucket, read_from_bucket
import pandas as pd
import numpy as np


def transform_players_data(data):
    df = pd.DataFrame(data)

    # Apply the same transformations logic to 3 columns
    def get_all_time_and_latest_month_results(df, col, col1_name, col2_name):
        values = df[col].str.split("\t").str[-1].str.split()
        all_time_value = values.str[0].str.replace(",", "").astype(int)
        last_month_value = values.str[1].str[1:-2].str.replace(",", "").astype(int)

        df[col1_name] = all_time_value
        df[col2_name] = last_month_value
        return df

    # Create an actions table
    def get_actions(df):
        # Extract actions, convert lits to rows, and get values
        actions = df[["player_id", "actions"]]
        actions = actions.explode("actions").dropna().reset_index(drop=True)
        values = actions["actions"].str.split("\n")
        actions["action_name"] = values.str[1]
        actions["value"] = values.str[2].str.replace(",", "").astype(int)
        actions.drop(columns=["actions"], inplace=True)

        # Remove unwanted actions, filter out dataframe
        actions_to_remove = ["Touch a Hostage", "Rescue a Hostage", "Headshot"]
        actions = actions[~actions["action_name"].isin(actions_to_remove)]

        # Rename actions to English
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
        actions["action_name"] = actions["action_name"].map(translation_map).fillna(actions["action_name"])
        return actions

    # Create side peak columns
    def side_peak_cols(df):
        # Convert dct to 2 columns
        sides = pd.json_normalize(df["side_peak"]).rename(columns={"CT_side": "ct_side_peaks", "T_side": "t_side_peaks"})
        df = df.merge(sides, left_index=True, right_index=True, how="left")

        # Clean columns
        df["ct_side_peaks"] = df["ct_side_peaks"].apply(lambda x: int(x.split("\n")[2]) if isinstance(x, str) else x)
        df["t_side_peaks"] = df["t_side_peaks"].apply(lambda x: int(x.split("\n")[2]) if isinstance(x, str) else x)
        return df

    # Create a weapons table
    def get_weapons(df):
        # Flatten dct to columns -> reshape to long format
        weapons_cols = pd.json_normalize(df["weapons_stats"])
        weapons = pd.merge(df["player_id"], weapons_cols, left_index=True, right_index=True, how="left")
        weapons = weapons.melt(id_vars=["player_id"], var_name="weapon", value_name="value").dropna()

        # Extract metrics from raw values
        values = weapons["value"].str.split("\n")
        weapons["frags"] = values.str[3].str.replace(",", "").astype(int)
        weapons["headshots"] = values.str[6].str.replace(",", "").astype(int)
        weapons.drop(columns=["value"], inplace=True)
        return weapons

    # Create a sessions table
    def get_sessions(df):
        sessions = pd.DataFrame(df["sessions"].dropna()[0], columns=["values"])
        values = sessions["values"].str.split("\n")

        # Clean values
        sessions["date"] = values.str[0].astype("datetime64[ns]")
        sessions["experience_change"] = values.str[1].astype(int)
        sessions["experience"] = values.str[2].str.replace(",", "").astype(int)

        # Extract time played in minutes
        time_played = values.str[3].str.split().str[-1].str[:-1]
        sessions["time_played_in_minutes"] = round(pd.to_timedelta(time_played).dt.seconds / 60, 0).astype(int)

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

    # Frags, deaths, headshots
    df = get_all_time_and_latest_month_results(df, "frags", "all_time_frags", "last_month_frags")
    df = get_all_time_and_latest_month_results(df, "deaths", "all_time_deaths", "last_month_deaths")
    df = get_all_time_and_latest_month_results(df, "headshots", "all_time_headshots", "last_month_headshots")

    # Actions, side peaks, weapons and sessions
    actions = get_actions(df)
    weapons = get_weapons(df)
    sessions = get_sessions(df)
    df = side_peak_cols(df)

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


def transform_data():
    players_data = read_from_bucket("raw/players_data")
    frags_data = read_from_bucket("raw/frags_data")

    players, actions, weapons, sessions = transform_players_data(players_data)
    frags_table = transform_frags_data(frags_data)

    # players, actions, weapons, sessions = transform_players_data(players_data)
