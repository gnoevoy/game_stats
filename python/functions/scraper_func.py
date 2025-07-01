from bs4 import BeautifulSoup
import requests


# There are 6 helper functions to extract data from different pages of player profile
# All functions have almost the same logic: make request -> render HTML content -> find elements ->  scrape data -> save in variable


def get_general_info(player_id, home_page):
    # Make request and parse the HTML content
    url = f"{home_page}hlstats.php?mode=playerinfo&type=ajax&game=css&tab=general_aliases&player={player_id}"
    response = requests.get(url)
    content = response.content
    soup = BeautifulSoup(content, "html.parser")

    # Extract basic stats + used names in the server
    tables = soup.find_all("table", class_="data-table")
    left_table = tables[0].find_all("tr")
    right_table = tables[1].find_all("tr")

    # Check if the names table exists
    names = []
    if len(tables) > 8:
        names_table = tables[8].find_all("tr")
        for row in names_table[1:]:
            name = row.text.strip()
            names.append(name)

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
        "used_names": names,
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
    # Tricky part where may be 2 or 1 table on a web page

    if len(tables) == 1:
        side_peak_table = tables[0].find_all("tr")
        side_peak = {
            "CT_side": side_peak_table[1].text.strip() if len(side_peak_table) > 1 else 0,
            "T_side": side_peak_table[2].text.strip() if len(side_peak_table) > 2 else 0,
        }
        return {"actions": [], "side_peak": side_peak}

    elif tables:
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
    sessions = []

    if table:
        for row in table.find_all("tr")[1:]:
            session = row.text.strip()
            sessions.append(session)

    return sessions


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
