from dotenv import load_dotenv
from pathlib import Path
import logging

# Import env variable for local execution
env_file = Path(".env")
if env_file.exists():
    load_dotenv(env_file)

from scripts.get_top_100_players import get_players_links
from scripts.get_players_data import get_players_data
from scripts.data_transformations import transform_data, load_tables_to_bigquery


# Set up logging configuration
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main():
    logger = logging.getLogger(__name__)

    try:
        get_players_links()
        get_players_data()
        transform_data()
        load_tables_to_bigquery()
    except KeyboardInterrupt:
        raise
    except:
        logger.error("", exc_info=True)


if __name__ == "__main__":
    setup_logging()
    main()
