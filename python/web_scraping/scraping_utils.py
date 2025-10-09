from bs4 import BeautifulSoup
import requests


# Web scraping functions to get players data across website
# All functions have almost the same logic: make request -> render HTML content -> find elements ->  scrape data -> save it as a variable


# Helper function to get parsed HTML content
def get_parsed_html(url):
    response = requests.get(url)
    content = response.content
    soup = BeautifulSoup(content, "html.parser")
    return soup


# Get current player stats
def get_general_info(player_id, home_page, timestamp):
    # Make request and parse the HTML content
    url = f"{home_page}hlstats.php?mode=playerinfo&type=ajax&game=css&tab=general_aliases&player={player_id}"
    soup = get_parsed_html(url)

    # Extract tables with content
    tables = soup.find_all("table", class_="data-table")
    left_table = tables[0].find_all("tr")
    right_table = tables[1].find_all("tr")

    # Get player used names, if they exist
    names = []
    if len(tables) > 8:
        names_table = tables[8].find_all("tr")
        for row in names_table[1:]:
            value = row.text.strip()
            name = {"player_id": player_id, "value": value, "timestamp": timestamp}
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
        "timestamp": timestamp,
    }
    return dct, names


# Get player actions for the last 30
def get_player_actions(player_id, home_page, timestamp):
    sort_order = "&obj_sort=obj_count&obj_sortorder=desc&teams_sort=name&teams_sortorder=asc"
    url = f"{home_page}hlstats.php?mode=playerinfo&type=ajax&game=css&tab=playeractions_teams&player={player_id}{sort_order}"
    soup = get_parsed_html(url)
    tables = soup.find_all("table", class_="data-table")
    actions = []
    ct_side, t_side = None, None

    # Check if element exists
    if not tables:
        return actions, ct_side, t_side

    # Check if only side peaks table exists
    elif len(tables) == 1:
        side_peak_table = tables[0].find_all("tr")
        ct_side = side_peak_table[1].text.strip() if len(side_peak_table) > 1 else None
        t_side = side_peak_table[2].text.strip() if len(side_peak_table) > 2 else None
        return actions, ct_side, t_side

    # Otherwise, scrape everything
    else:
        actions_table = tables[0].find_all("tr")
        side_peak_table = tables[1].find_all("tr")

        ct_side = side_peak_table[1].text.strip() if len(side_peak_table) > 1 else None
        t_side = side_peak_table[2].text.strip() if len(side_peak_table) > 2 else None

        # Get actions
        if len(actions_table) > 2:
            for row in actions_table[2:]:
                value = row.text.strip()
                action = {"player_id": player_id, "action": value, "timestamp": timestamp}
                actions.append(action)

        return actions, ct_side, t_side


# Get player weapons stats for the last 30 days
def get_weapons_stats(player_id, home_page, timestamp):
    url = f"{home_page}hlstats.php?mode=playerinfo&type=ajax&game=css&tab=weapons&player={player_id}&weap_sort=kills"
    soup = get_parsed_html(url)
    table = soup.find("table", class_="data-table")

    weapons = []

    if table:
        for row in table.find_all("tr")[1:]:
            # Get weapon name from the image + stats
            weapon_name = row.find("img")["src"].split("/")[-1][:-4]
            value = row.text.strip()
            weapon_stats = {"player_id": player_id, "weapon_name": weapon_name, "value": value, "timestamp": timestamp}
            weapons.append(weapon_stats)
        return weapons

    return weapons


# Get player frags stats for the last 30 days
def get_frags_stats(player_id, home_page, timestamp):
    sort_order = "killLimit=5&playerkills_sort=kills&playerkills_sortorder=desc"
    num = 1
    lst = []

    # Iterate through all pages with frags
    while True:
        url = f"{home_page}hlstats.php?mode=playerinfo&type=ajax&game=css&tab=killstats&player={player_id}&{sort_order}&playerkills_page={num}"
        response = requests.get(url)

        # Check if the page exists
        if response.status_code != 200:
            break

        content = response.content
        soup = BeautifulSoup(content, "html.parser")
        table = soup.find("table", class_="data-table")

        # Check if the element exists
        if not table:
            break

        frags = []
        records = table.find_all("tr")[1:]

        # Check if there are records in the table
        if not records:
            break

        for row in records:
            # Get the killed player ID for identification
            killed_player_id = row.find("a")["href"].split("=")[-1]
            value = row.text.strip()
            frag = {"player_id": player_id, "killed_player_id": killed_player_id, "value": value, "timestamp": timestamp}
            frags.append(frag)

        lst.extend(frags)
        num += 1

    return lst


# Get my profile sessions for the last 30 days
def get_my_sessions(player_id, home_page, timestamp):
    url = f"{home_page}hlstats.php?mode=playersessions&player={player_id}"
    soup = get_parsed_html(url)
    table = soup.find("table", class_="data-table")

    sessions = []
    if table:
        for row in table.find_all("tr")[1:]:
            session = row.text.strip()
            dct = {"value": session, "timestamp": timestamp}
            sessions.append(dct)
    return sessions


# Extract logs for my profile for the last 30 days
def get_my_games_events(player_id, home_page, timestamp):
    num, event_index = 1, 1
    lst = []

    # Loop over unknown amount of pages and extract logs
    while True:
        url = f"{home_page}hlstats.php?mode=playerhistory&sortorder=asc&player={player_id}&page={num}"
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
                # Add incremental event index to correctly order events that happened at the same time
                dct = {"value": event, "event_index": event_index, "timestamp": timestamp}
                events.append(dct)
                event_index += 1

            lst.extend(events)
            num += 1
        else:
            break

    return lst


# Combine all helper functions to scrape my profile stats
def get_my_profile_data(player_id, home_page, timestamp):
    general_info, names = get_general_info(player_id, home_page, timestamp)
    actions, ct_side_peaks, t_side_peaks = get_player_actions(player_id, home_page, timestamp)
    weapons = get_weapons_stats(player_id, home_page, timestamp)
    frags = get_frags_stats(player_id, home_page, timestamp)
    sessions = get_my_sessions(player_id, home_page, timestamp)
    events = get_my_games_events(player_id, home_page, timestamp)

    # Append team side peaks to general player info
    general_info["CT_side_peaks"] = ct_side_peaks
    general_info["T_side_peaks"] = t_side_peaks

    return general_info, names, actions, weapons, frags, sessions, events
