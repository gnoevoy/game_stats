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
from transform_data import transform_tables, load_to_bigquery


def main():
    logger_setup()
    transform_tables()
    load_to_bigquery()


if __name__ == "__main__":
    main()
