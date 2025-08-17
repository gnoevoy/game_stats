from dotenv import load_dotenv
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).parents[1]
ENV_FILE = ROOT_DIR / ".env"

sys.path.append(str(ROOT_DIR))
if ENV_FILE.exists():
    load_dotenv(ENV_FILE)

from logger import setup_logger
from get_top_players import get_players_links


def main():
    try:
        setup_logger()
        get_players_links()
    except:
        raise


if __name__ == "__main__":
    main()
