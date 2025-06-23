from functions.google_func import write_to_bucket, read_from_bucket
from bs4 import BeautifulSoup
import traceback
import requests
import os


# There are 6 helper functions to extract data from different pages of player profile
# All functions have almost the same logic: make request -> render HTML content -> find elements ->  scrape data -> save in variable


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

    # Check if the element exists, if player doesnt played last 28 days, there will be no records
    if tables:
        actions_table = tables[0].find_all("tr")
        side_peak_table = tables[1].find_all("tr")

        # Extract players actions stats with side peaks and check if they exist
        actions = [action.text.strip() for action in actions_table[2:]] if len(actions_table) > 2 else []
        side_peak = {
            "CT_side": side_peak_table[1].text.strip() if len(side_peak_table) > 1 else 0,
            "T_side": side_peak_table[2].text.strip() if len(side_peak_table) > 2 else 0,
        }
        dct = {"actions": actions, "side_peak": side_peak}
        return dct

    return {"actions": [], "side_peak": {"CT_side": 0, "T_side": 0}}


def get_weapons_stats(player_id, home_page):
    url = f"{home_page}hlstats.php?mode=playerinfo&type=ajax&game=css&tab=weapons&player={player_id}&weap_sort=kills"
    response = requests.get(url)
    content = response.content
    soup = BeautifulSoup(content, "html.parser")

    table = soup.find("table", class_="data-table")

    if table:
        weapons = {}
        for row in table.find_all("tr")[1:]:
            # Get weapon name from the image
            weapon_name = row.find("img")["src"].split("/")[-1][:-4]
            weapons[weapon_name] = row.text.strip()
        return {"weapons_stats": weapons}

    return {"weapons_stats": {}}


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

        if table:
            frags = []
            for row in table.find_all("tr")[1:]:
                # Get the killed player ID for identification
                killed_player_id = row.find("a")["href"].split("=")[-1]
                value = " ; ".join([killed_player_id, row.text.strip()])
                frags.append(value)

            lst.extend(frags)
            num += 1
        else:
            break

    return lst


# Get my profile sessions
def get_my_sessions(player_id, home_page):
    url = f"{home_page}hlstats.php?mode=playersessions&player={player_id}"
    response = requests.get(url)
    content = response.content
    soup = BeautifulSoup(content, "html.parser")

    table = soup.find("table", class_="data-table")

    if table:
        sessions = []
        for row in table.find_all("tr")[1:]:
            session = row.text.strip()
            sessions.append(session)
        return {"sessions": sessions}

    return {"sessions": []}


# Extract events history with timestamps for my profile
def get_my_games_events(player_id, home_page):
    num = 1
    lst = []

    # Use infinite loop because there are many pages, breaking condition is when page does not exist or table is empty
    while True:
        url = f"{home_page}hlstats.php?mode=playerhistory&player={player_id}&page={num}"
        response = requests.get(url)

        # Check if the page exists
        if response.status_code != 200:
            break

        content = response.content
        soup = BeautifulSoup(content, "html.parser")
        table = soup.find("table", class_="data-table")

        # Check if table has records
        if table and len(table.find_all("tr")) > 1:
            events = []
            for row in table.find_all("tr")[1:]:
                event = row.text.strip()
                events.append(event)

            lst.extend(events)
            num += 1
        else:
            break

    return lst


# Final function to put everything together
def get_players_data():
    print("02. SCRAPING PLAYERS DATA ...")
    # Get players profile links from bucket
    links = read_from_bucket("top_100_players")
    home_page = os.getenv("BASE_URL")
    players_data, frags_data = [], {}

    # Extract my own stats firstly, since I could not be present in the top 100 players
    # Dont use error handling because this data is essential
    my_id = 4720
    my_general_info = get_general_info(my_id, home_page)
    my_actions = get_player_actions(my_id, home_page)
    my_weapons_stats = get_weapons_stats(my_id, home_page)
    my_frags = get_frags_stats(my_id, home_page)
    my_session = get_my_sessions(my_id, home_page)
    my_game_events = get_my_games_events(my_id, home_page)

    my_info = {**my_general_info, **my_actions, **my_weapons_stats, **my_session}
    players_data.append(my_info)
    frags_data[my_id] = my_frags
    print("My stats successfully scraped")

    # Iterate through the player links and fetch their data
    for i, link in enumerate(links, start=1):
        # If something goes wrong, skip this player and display an error message
        try:
            player_id = link.split("=")[-1]

            # Skip my own profile to avoid duplication
            if int(player_id) == my_id:
                print(f"{i}/100, Already get my own stats")
                continue

            general_info = get_general_info(player_id, home_page)
            player_actions = get_player_actions(player_id, home_page)
            weapons_stats = get_weapons_stats(player_id, home_page)
            player_frags = get_frags_stats(player_id, home_page)

            player_info = {**general_info, **player_actions, **weapons_stats}
            players_data.append(player_info)
            frags_data[player_id] = player_frags
            print(f"{i}/100, Player {player_id} scraped")

        except:
            print(f"Failed to scrape data for player {player_id}, {link}")
            traceback.print_exc()
            continue

    # Write data to bucket
    write_to_bucket("raw/players_data", players_data)
    write_to_bucket("raw/frags_data", frags_data)
    write_to_bucket("raw/game_events", my_game_events)
    print("Data successfully written to bucket, 3 json files")
