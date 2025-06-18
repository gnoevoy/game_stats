from scripts.google_functions import write_to_bucket, read_from_bucket
from bs4 import BeautifulSoup
import requests
import os


# There are 4 helper functions to extract data from different pages of player profile
def get_general_info(player_id, home_page):
    # Make request and parse the HTML content
    url = f"{home_page}hlstats.php?mode=playerinfo&type=ajax&game=css&tab=general_aliases&player={player_id}"
    response = requests.get(url)
    content = response.content
    soup = BeautifulSoup(content, "html.parser")

    # Find appropriate elements and extract values
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


def get_player_actions(player_id, home_page):
    sort_order = "&obj_sort=obj_count&obj_sortorder=desc&teams_sort=name&teams_sortorder=asc"
    url = f"{home_page}hlstats.php?mode=playerinfo&type=ajax&game=css&tab=playeractions_teams&player={player_id}{sort_order}"
    response = requests.get(url)
    content = response.content
    soup = BeautifulSoup(content, "html.parser")

    tables = soup.find_all("table", class_="data-table")
    actions_table = tables[0].find_all("tr")
    side_peak_table = tables[1].find_all("tr")

    # Extract players actions stats with side peaks
    actions = [action.text.strip() for action in actions_table[2:]]
    side_peak = {"CT_side": side_peak_table[1].text.strip(), "T_side": side_peak_table[2].text.strip()}
    dct = {"actions": actions, "side_peak": side_peak}
    return dct


def get_weapons_stats(player_id, home_page):
    url = f"{home_page}hlstats.php?mode=playerinfo&type=ajax&game=css&tab=weapons&player={player_id}&weap_sort=kills"
    response = requests.get(url)
    content = response.content
    soup = BeautifulSoup(content, "html.parser")

    table = soup.find("table", class_="data-table")
    dct = {}

    for row in table.find_all("tr")[1:]:
        # Get weapon name from the image
        weapon_name = row.find("img")["src"].split("/")[-1][:-4]
        dct[weapon_name] = row.text.strip()
    return dct


def get_frags_stats(player_id, home_page):
    sort_order = "&killLimit=5&playerkills_sort=kills&playerkills_sortorder=desc"
    num = 1
    lst = []

    # Iterate through the pages with most kills
    while num <= 4:
        url = f"{home_page}hlstats.php?mode=playerinfo&type=ajax&game=css&tab=killstats&player={player_id}{sort_order}&playerkills_page={num}"
        response = requests.get(url)

        # Check if the page exists
        if response.status_code != 200:
            break

        content = response.content
        soup = BeautifulSoup(content, "html.parser")
        table = soup.find("table", class_="data-table")

        frags = []
        for row in table.find_all("tr")[1:]:
            # Get the killed player ID for identification
            killed_player_id = row.find("a")["href"].split("=")[-1]
            values = [killed_player_id, row.text.strip()]
            frags.append(values)

        lst.extend(frags)
        num += 1

    return {"frags_stats": lst}


# Cover function to get all players data
def get_players_data():
    # Get players profile links from bucket
    links = read_from_bucket("players_list")
    home_page = os.getenv("BASE_URL")
    players_data = []

    # Iterate through the player links and fetch their data
    for link in links[:1]:
        player_id = link.split("=")[-1]

        general_info = get_general_info(player_id, home_page)
        player_actions = get_player_actions(player_id, home_page)
        weapons_stats = get_weapons_stats(player_id, home_page)
        player_frags = get_frags_stats(player_id, home_page)

        player_info = {**general_info, **player_actions, **weapons_stats, **player_frags}
        players_data.append(player_info)

    # Write data to bucket
    write_to_bucket("players_data", players_data)
