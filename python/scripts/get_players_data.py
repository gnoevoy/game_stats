from functions.scraper_func import get_general_info, get_player_actions, get_weapons_stats, get_frags_stats, get_my_sessions, get_my_games_events
from functions.google_func import write_to_bucket, read_from_bucket
import traceback
import os


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
    my_sessions = get_my_sessions(my_id, home_page)
    my_game_events = get_my_games_events(my_id, home_page)

    my_info = {**my_general_info, **my_actions, **my_weapons_stats}
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

            # Append values to final lst and dct
            player_info = {**general_info, **player_actions, **weapons_stats}
            players_data.append(player_info)
            frags_data[player_id] = player_frags
            print(f"{i}/100, Player {player_id} scraped")

        except KeyboardInterrupt:
            traceback.print_exc()
            exit(0)
        except:
            print(f"Failed to scrape data for player {player_id}, {link}")
            traceback.print_exc()
            continue

    # Write data to bucket
    write_to_bucket("raw/players_data", players_data)
    write_to_bucket("raw/frags_data", frags_data)
    write_to_bucket("raw/game_events", my_game_events)
    write_to_bucket("raw/my_sessions", my_sessions)
    print("Data successfully written to bucket, 4 json files")
