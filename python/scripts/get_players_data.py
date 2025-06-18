from scripts.google_functions import write_to_bucket, read_from_bucket
from bs4 import BeautifulSoup
import requests
import os


def get_general_info(player_id, home_page):
    url = f"{home_page}hlstats.php?mode=playerinfo&type=ajax&game=css&tab=general_aliases&player={player_id}"
    response = requests.get(url)
    content = response.content
    soup = BeautifulSoup(content, "html.parser")

    tables = soup.find_all("table", class_="data-table")
    left_table = tables[0].find_all("tr")
    right_table = tables[1].find_all("tr")

    dct = {
        "player_id": player_id,
        "player_name": left_table[1].text.strip(),
        "steam_id": left_table[3].text.strip(),
        "activity": right_table[1].text.strip(),
        "rank": right_table[3].text.strip(),
        "experience": right_table[2].text.strip(),
        "frags": right_table[10].text.strip(),
        "deaths": right_table[11].text.strip(),
        "headshots": right_table[9].text.strip(),
        "frags_per_minute": right_table[4].text.strip(),
        "kill_streak": right_table[12].text.strip(),
        "death_streak": right_table[13].text.strip(),
        "suicides": right_table[14].text.strip(),
    }

    return dct


def get_achievements(player_id, home_page):
    url = f"{home_page}hlstats.php?mode=playerinfo&type=ajax&game=css&tab=playeractions_teams&player={player_id}"
    response = requests.get(url)
    content = response.content
    soup = BeautifulSoup(content, "html.parser")

    tables = soup.find_all("table", class_="data-table")
    achievements = 1


def get_players_data():
    # Get players profile links from bucket
    links = read_from_bucket("players_list")
    home_page = os.getenv("BASE_URL")
    players_data = []

    for link in links[:1]:
        player_id = link.split("=")[-1]
        player_info = {}

        general_info = get_general_info(player_id, home_page)

        # players_data.append(player_info)
