from dotenv import load_dotenv
from pathlib import Path
import logging

# Import env variable for local execution
env_file = Path(".env")
if env_file.exists():
    load_dotenv(env_file)

# Import logger and scripts
from functions.logger import setup_logging
from scripts.get_top_100_players import get_players_links
from scripts.get_players_data import get_players_data
from scripts.data_transformations import transform_data, load_data_to_bigquery


logger = logging.getLogger(__name__)


def main():
    try:
        setup_logging()
        get_players_links()
        get_players_data()
        transform_data()
        load_data_to_bigquery()
    except KeyboardInterrupt:
        raise
    except:
        logger.error("", exc_info=True)
        raise


if __name__ == "__main__":
    main()
