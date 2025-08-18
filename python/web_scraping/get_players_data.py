from scraping_utils import get_general_info, get_player_actions, get_weapons_stats, get_frags_stats, get_my_profile_data
from gcp_utils import write_to_bucket, read_from_bucket
import pandas as pd
import logging
import os


logger = logging.getLogger(__name__)


def get_players_stats():
    logger.info("SCRAPING PLAYERS DATA")

    # Get players links from the bucket
    links = read_from_bucket("top_100_players.json")
    home_page = os.getenv("BASE_URL")
    players, names, actions, weapons, frags = [], [], [], [], []

    # Extract my own stats first, since I could not be present in the top 100 players
    my_id = 4720
    general_info, my_names, my_actions, my_weapons, my_frags, sessions, events = get_my_profile_data(my_id, home_page)
    players.append(general_info)
    names.extend(my_names)
    actions.extend(my_actions)
    weapons.extend(my_weapons)
    frags.extend(my_frags)
    logger.info("Scraped my profile stats")

    # Iterate through the list and scrape players data
    for i, link in enumerate(links, start=1):
        # If something goes wrong, skip this player and display an error message
        try:
            player_id = link.split("=")[-1]

            # If my profile in top 100 -> skip it to avoid duplication
            if int(player_id) == my_id:
                logger.info(f"{i}/100, skipping my profile")
                continue

            player_info, player_names = get_general_info(player_id, home_page)
            player_actions, ct_side_peaks, t_side_peaks = get_player_actions(player_id, home_page)
            player_weapons = get_weapons_stats(player_id, home_page)
            player_frags = get_frags_stats(player_id, home_page)

            # Append team side peaks to general player info
            player_info["CT_side_peaks"] = ct_side_peaks
            player_info["T_side_peaks"] = t_side_peaks

            # Append values
            players.append(player_info)
            names.extend(player_names)
            actions.extend(player_actions)
            weapons.extend(player_weapons)
            frags.extend(player_frags)
            logger.info(f"{i}/100, player {player_id} scraped")

        except:
            logger.warning(f"Failed to scrape data for player {player_id}, {link}", exc_info=True)
            continue

    # Write data to the bucket
    write_to_bucket("raw/players.csv", pd.DataFrame(players), "csv")
    write_to_bucket("raw/names.csv", pd.DataFrame(names), "csv")
    write_to_bucket("raw/actions.csv", pd.DataFrame(actions), "csv")
    write_to_bucket("raw/weapons.csv", pd.DataFrame(weapons), "csv")
    write_to_bucket("raw/frags.csv", pd.DataFrame(frags), "csv")
    write_to_bucket("raw/sessions.csv", pd.DataFrame(sessions), "csv")
    write_to_bucket("raw/events.csv", pd.DataFrame(events), "csv")
