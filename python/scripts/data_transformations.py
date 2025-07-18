from functions.data_func import (
    transform_players_data,
    transform_player_names,
    transform_player_actions,
    transform_player_weapons,
    transform_player_frags,
    transform_sessions,
    transform_events,
    create_timestamp_table,
)
from functions.google_func import write_to_bucket, read_from_bucket, write_to_bigquery
import logging


logger = logging.getLogger(__name__)


def transform_data():
    logger.info("---------- DATA TRANSFORMATIONS ----------")

    # Read csv files from the bucket
    players_df = read_from_bucket("raw/players", file_type="csv")
    names_df = read_from_bucket("raw/names", file_type="csv")
    actions_df = read_from_bucket("raw/actions", file_type="csv")
    weapons_df = read_from_bucket("raw/weapons", file_type="csv")
    frags_df = read_from_bucket("raw/frags", file_type="csv")
    sessions_df = read_from_bucket("raw/sessions", file_type="csv")
    events_df = read_from_bucket("raw/events", file_type="csv")
    logger.info("Data successfully uploaded from the bucket")

    # Transform dataframes
    players = transform_players_data(players_df)
    names = transform_player_names(names_df)
    actions = transform_player_actions(actions_df)
    weapons = transform_player_weapons(weapons_df)
    frags = transform_player_frags(frags_df)
    sessions = transform_sessions(sessions_df)
    events = transform_events(events_df)
    timestamp = create_timestamp_table()
    logger.info("Data successfully transformed")

    # Write dataframes to the bucket
    write_to_bucket("clean/players", players, file_type="csv")
    write_to_bucket("clean/names", names, file_type="csv")
    write_to_bucket("clean/actions", actions, file_type="csv")
    write_to_bucket("clean/weapons", weapons, file_type="csv")
    write_to_bucket("clean/frags", frags, file_type="csv")
    write_to_bucket("clean/sessions", sessions, file_type="csv")
    write_to_bucket("clean/events", events, file_type="csv")
    write_to_bucket("clean/timestamp", timestamp, file_type="csv")
    logger.info("Data successfully written to the bucket")


# Final step in a python script
def load_data_to_bigquery():
    logger.info("---------- LOADING DATA TO BIGQUERY ----------")
    tables = ["players", "names", "actions", "weapons", "frags", "sessions", "events", "timestamp"]

    for table in tables:
        write_to_bigquery(f"clean/{table}", table)
    logger.info("Data successfully loaded to BigQuery")
