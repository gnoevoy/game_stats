from functions.data_func import get_all_time_and_latest_month_results, get_actions, get_weapons, get_sessions, create_side_peak_cols
from functions.google_func import write_to_bucket, read_from_bucket
import pandas as pd


# Main function to create 4 different tables
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

    # Create seperate tables for actions, weapons and sessions
    actions = get_actions(df)
    weapons = get_weapons(df)
    sessions = get_sessions(df)

    # Remove raw columns
    cols_to_remove = ["frags", "deaths", "headshots", "actions", "side_peak", "weapons_stats", "sessions"]
    df.drop(columns=cols_to_remove, inplace=True)

    return df, actions, weapons, sessions


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
    df["timestamp"] = pd.to_datetime(values.str[0].str.strip())
    df["event"] = values.str[1].str.strip()
    df["description"] = values.str[2].str.strip().str.replace(".", "")

    df.drop(columns=["value"], inplace=True)
    return df


# Wrapper function to combine all together
def transform_data():
    print("03. STARTING DATA TRANSFORMATION ...")
    # Load json files from the bucket
    players_data = read_from_bucket("raw/players_data")
    frags_data = read_from_bucket("raw/frags_data")
    events_data = read_from_bucket("raw/game_events")
    print("Raw json files successfully uploaded")

    # Final tables
    players, actions, weapons, sessions = transform_players_data(players_data)
    frags = transform_frags_data(frags_data)
    events = transform_events_data(events_data)
    print("Data transformation completed, created 6 tables")

    # Write dataframes as csv files to the bucket
    write_to_bucket("clean/players", players, file_type="csv")
    write_to_bucket("clean/actions", actions, file_type="csv")
    write_to_bucket("clean/weapons", weapons, file_type="csv")
    write_to_bucket("clean/sessions", sessions, file_type="csv")
    write_to_bucket("clean/frags", frags, file_type="csv")
    write_to_bucket("clean/events", events, file_type="csv")
    print("Data successfully written to the bucket, 6 csv files")
