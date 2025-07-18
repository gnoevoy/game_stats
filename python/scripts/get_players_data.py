from functions.scraper_func import get_general_info, get_player_actions, get_weapons_stats, get_frags_stats, get_my_profile_data
from functions.google_func import write_to_bucket, read_from_bucket
import pandas as pd
import logging
import os


logger = logging.getLogger(__name__)


def get_players_data():
    logger.info("---------- SCRAPING PLAYERS DATA ----------")

    # Get players links from the bucket
    links = read_from_bucket("top_100_players")
    home_page = os.getenv("BASE_URL")
    players, names, actions, weapons, frags = [], [], [], [], []

    # Extract my own stats firstly, since I could not be present in the top 100 players
    my_id = 4720
    general_info, my_names, my_actions, my_weapons, my_frags, sessions, events = get_my_profile_data(my_id, home_page)
    players.append(general_info)
    names.extend(my_names)
    actions.extend(my_actions)
    weapons.extend(my_weapons)
    frags.extend(my_frags)
    logger.info("My stats successfully scraped")

    # Iterate through the list and scrape players data
    for i, link in enumerate(links, start=1):
        # If something goes wrong, skip this player and display an error message
        try:
            player_id = link.split("=")[-1]

            # If my profile in top 100 -> skip it to avoid duplication
            if int(player_id) == my_id:
                logger.info(f"{i}/100, Already get my own stats")
                continue

            player_info, player_names = get_general_info(player_id, home_page)
            player_actions, ct_side_peaks, t_side_peaks = get_player_actions(player_id, home_page)
            player_weapons = get_weapons_stats(player_id, home_page)
            player_frags = get_frags_stats(player_id, home_page)

            # Append team side peaks to general player info
            player_info["CT_side_peaks"] = ct_side_peaks
            player_info["T_side_peaks"] = t_side_peaks

            # Append values to final lists
            players.append(player_info)
            names.extend(player_names)
            actions.extend(player_actions)
            weapons.extend(player_weapons)
            frags.extend(player_frags)

            logger.info(f"{i}/100, Player {player_id} scraped")

        except KeyboardInterrupt:
            raise
        except:
            logger.warning(f"Failed to scrape data for player {player_id}, {link}", exc_info=True)
            continue

    # Convert lists to pandas DataFrames
    players_df = pd.DataFrame(players)
    names_df = pd.DataFrame(names)
    actions_df = pd.DataFrame(actions)
    weapons_df = pd.DataFrame(weapons)
    frags_df = pd.DataFrame(frags)
    sessions_df = pd.DataFrame(sessions)
    events_df = pd.DataFrame(events)

    # Write dataframes to the bucket
    write_to_bucket("raw/players", players_df, file_type="csv")
    write_to_bucket("raw/names", names_df, file_type="csv")
    write_to_bucket("raw/actions", actions_df, file_type="csv")
    write_to_bucket("raw/weapons", weapons_df, file_type="csv")
    write_to_bucket("raw/frags", frags_df, file_type="csv")
    write_to_bucket("raw/sessions", sessions_df, file_type="csv")
    write_to_bucket("raw/events", events_df, file_type="csv")
    logger.info("Data successfully written to bucket")
