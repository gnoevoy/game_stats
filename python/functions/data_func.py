from datetime import timedelta
import pandas as pd


# Reusable function to extract frags, deaths, and headshots data
def get_all_time_and_latest_month_results(df, col, col1_name, col2_name):
    values = df[col].str.split("\t").str[-1].str.split()
    all_time_value = values.str[0].str.replace(",", "").astype(int)
    last_month_value = values.str[1].str[1:-2].str.replace(",", "").astype(int)

    df[col1_name] = all_time_value
    df[col2_name] = last_month_value
    return df


def create_side_peak_cols(df):
    # Convert dct with values to columns
    cols = {"CT_side": "ct_side_peaks", "T_side": "t_side_peaks"}
    sides = pd.json_normalize(df["side_peak"]).rename(columns=cols)
    df = df.merge(sides, left_index=True, right_index=True, how="left")

    df["ct_side_peaks"] = df["ct_side_peaks"].apply(lambda x: int(x.split("\n")[2]) if isinstance(x, str) else x)
    df["t_side_peaks"] = df["t_side_peaks"].apply(lambda x: int(x.split("\n")[2]) if isinstance(x, str) else x)
    return df


# Get seperated table for players names
def get_names(df):
    names = df[["player_id", "used_names"]]
    names = names.explode("used_names").dropna()

    values = names["used_names"].str.split("\n")
    names["name"] = values.str[1].str.strip()
    # Convert to Warszaw time
    names["last_used"] = pd.to_datetime(values.str[3].str.strip())
    names["last_used"] = names["last_used"] - timedelta(hours=1)

    names.drop(columns=["used_names"], inplace=True)
    return names


# Get separate table for player actions
def get_actions(df):
    # Reshape actions data, from lists to long format table
    actions = df[["player_id", "actions"]]
    actions = actions.explode("actions").dropna().reset_index(drop=True)

    # Extract values
    values = actions["actions"].str.split("\n")
    actions["action_name"] = values.str[1]
    actions["value"] = values.str[2].str.replace(",", "").astype(int)
    actions.drop(columns=["actions"], inplace=True)

    # Remove unwanted actions and rename names
    actions_to_remove = ["Touch a Hostage", "Rescue a Hostage", "Headshot"]
    actions = actions[~actions["action_name"].isin(actions_to_remove)]
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


# Get separate table for weapons stats
def get_weapons(df):
    # Reshape dct to columns and pivot table
    weapons_cols = pd.json_normalize(df["weapons_stats"])
    weapons = pd.merge(df["player_id"], weapons_cols, left_index=True, right_index=True, how="left")
    weapons = weapons.melt(id_vars=["player_id"], var_name="weapon", value_name="value").dropna()

    values = weapons["value"].str.split("\n")
    weapons["frags"] = values.str[3].str.replace(",", "").astype(int)
    weapons["headshots"] = values.str[6].str.replace(",", "").astype(int)
    weapons.drop(columns=["value"], inplace=True)

    return weapons
