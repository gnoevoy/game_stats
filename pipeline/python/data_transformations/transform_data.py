from pandas_utils import (
    transform_players_data,
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
        ["players", transform_players_data],
        ["actions", transform_player_actions],
        ["weapons", transform_player_weapons],
        ["frags", transform_player_frags],
        ["sessions", transform_sessions],
        ["events", transform_events],
    ]

    # Loop through all raw csv files
    for file, func in data:
        raw_path = f"raw/{file}.csv"
        clean_path = f"clean/{file}.csv"
        df = read_from_bucket(raw_path, "csv")

        # Check if dataframe has records
        if df.empty:
            logger.warning(f"{raw_path} is empty, skipping transformation and upload")
            continue

        cleaned_df = func(df)
        logger.info(f"{file} csv file transformed successfully")
        write_to_bucket(clean_path, cleaned_df, "csv")


# Load cleaned csv files to BigQuery
def load_to_bigquery():
    logger.info("LOADING DATA TO BIGQUERY")
    tables = ["players", "actions", "weapons", "frags", "sessions", "events"]

    for table in tables:
        write_to_bigquery(f"clean/{table}.csv", table)
