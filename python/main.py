from dotenv import load_dotenv
import traceback

load_dotenv(".env")

from scripts.get_top_100_players import get_players_links
from scripts.get_players_data import get_players_data
from scripts.data_transformations import transform_data


def main():
    try:
        get_players_links()
        get_players_data()
        transform_data()
    except:
        traceback.print_exc()


if __name__ == "__main__":
    main()
