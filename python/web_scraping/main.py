from dotenv import load_dotenv
from pathlib import Path
import logging
import sys


# Add path to access gcp_utils and .env file
ROOT_DIR = Path(__file__).parents[1]
sys.path.append(str(ROOT_DIR))

# Load env variables for local execution
ENV_FILE = ROOT_DIR / ".env"
if ENV_FILE.exists():
    load_dotenv(ENV_FILE)


# Set up a logger
def logger_setup():
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_format, datefmt="%Y-%m-%d %H:%M:%S")


# Import scripts
from get_top_players import get_players_links
from get_players_data import get_players_stats


def main():
    logger_setup()
    # get_players_links()
    get_players_stats()


if __name__ == "__main__":
    main()
