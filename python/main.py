from dotenv import load_dotenv
import traceback

load_dotenv(".env")

from get_players_list import get_players_list
from get_players_data import get_players_data

def main():
    try:
        # get_players_list()
        get_players_data()
    except:
        traceback.print_exc()

if __name__ == "__main__":
    main()