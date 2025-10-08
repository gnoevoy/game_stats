from pandas_utils import (
    transform_players_data,
    transform_player_names,
    transform_player_actions,
    transform_player_weapons,
    transform_player_frags,
    transform_sessions,
    transform_events,
)
from gcp_utils import write_to_bucket, read_from_bucket, write_to_bigquery
import logging


logger = logging.getLogger(__name__)


def transform_tables():
    logger.info("DATA TRANSFORMATIONS")

    # Read csv files from the bucket
    players_df = read_from_bucket("raw/players.csv", "csv")
    names_df = read_from_bucket("raw/names.csv", "csv")
    actions_df = read_from_bucket("raw/actions.csv", "csv")
    weapons_df = read_from_bucket("raw/weapons.csv", "csv")
    frags_df = read_from_bucket("raw/frags.csv", "csv")
    sessions_df = read_from_bucket("raw/sessions.csv", "csv")
    events_df = read_from_bucket("raw/events.csv", "csv")

    # Transform dataframes
    players = transform_players_data(players_df)
    names = transform_player_names(names_df)
    actions = transform_player_actions(actions_df)
    weapons = transform_player_weapons(weapons_df)
    frags = transform_player_frags(frags_df)
    sessions = transform_sessions(sessions_df)
    events = transform_events(events_df)

    # Write dataframes to the bucket
    write_to_bucket("clean/players.csv", players, "csv")
    write_to_bucket("clean/names.csv", names, "csv")
    write_to_bucket("clean/actions.csv", actions, "csv")
    write_to_bucket("clean/weapons.csv", weapons, "csv")
    write_to_bucket("clean/frags.csv", frags, "csv")
    write_to_bucket("clean/sessions.csv", sessions, "csv")
    write_to_bucket("clean/events.csv", events, "csv")


# Load cleaned csv files to BigQuery
def load_to_bigquery():
    logger.info("LOADING DATA TO BIGQUERY")
    tables = ["players", "names", "actions", "weapons", "frags", "sessions", "events"]

    for table in tables:
        write_to_bigquery(f"clean/{table}.csv", table)
