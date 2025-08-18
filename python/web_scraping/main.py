from dotenv import load_dotenv
from pathlib import Path
import sys

# Append path
ROOT_DIR = Path(__file__).parents[1]
sys.path.append(str(ROOT_DIR))

# Load environment variables for local execution
ENV_FILE = ROOT_DIR / ".env"
if ENV_FILE.exists():
    load_dotenv(ENV_FILE)

# Import scripts
from logger import setup_logger
from get_top_players import get_players_links
from get_players_data import get_players_stats


def main():
    try:
        setup_logger()
        get_players_links()
        get_players_stats()
    except:
        raise


if __name__ == "__main__":
    main()
