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

    data = [
        ("raw/players.csv", transform_players_data, "clean/players.csv"),
        ("raw/names.csv", transform_player_names, "clean/names.csv"),
        ("raw/actions.csv", transform_player_actions, "clean/actions.csv"),
        ("raw/weapons.csv", transform_player_weapons, "clean/weapons.csv"),
        ("raw/frags.csv", transform_player_frags, "clean/frags.csv"),
        ("raw/sessions.csv", transform_sessions, "clean/sessions.csv"),
        ("raw/events.csv", transform_events, "clean/events.csv"),
    ]

    # For each file apply transformation logic and write back to the bucket
    for raw_path, func, clean_path in data:
        df = read_from_bucket(raw_path, "csv")

        # Check if dataframe has records
        if df.empty:
            logger.warning(f"{raw_path} is empty, skipping transformation and upload")
            continue

        cleaned_df = func(df)
        write_to_bucket(clean_path, cleaned_df, "csv")


# Load cleaned csv files to BigQuery
def load_to_bigquery():
    logger.info("LOADING DATA TO BIGQUERY")
    tables = ["players", "names", "actions", "weapons", "frags", "sessions", "events"]

    for table in tables:
        write_to_bigquery(f"clean/{table}.csv", table)
