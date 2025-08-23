from game_stats.defs.web_scraping.gcs_utils import write_to_bucket
from bs4 import BeautifulSoup
import dagster as dg
import requests
import os


def get_links(content, home_page):
    # Parse page content and extract profile links
    soup = BeautifulSoup(content, "html.parser")
    players_table = soup.find("table", class_="data-table")
    links = players_table.find_all("a")
    lst = []

    for link in links:
        value = link.get("href")
        if "playerinfo&player" in value:
            lst.append(f"{home_page}{value}")

    return lst


@dg.asset
def get_players_links(context, gcs):
    context.log.info("GET TOP 100 PLAYERS LINKS")

    # Set up base URL
    home_page = os.getenv("BASE_URL")
    players, page_num = [], 1

    # Loop through the first two pages and get top 100 players
    while page_num <= 2:
        url = f"{home_page}hlstats.php?mode=players&game=css&sort=skill&sortorder=desc&page={page_num}"
        response = requests.get(url)

        # Check if the request was successful
        if response.status_code == 200:
            content = response.content
            links = get_links(content, home_page)
            players.extend(links)
        else:
            context.log.error(f"Failed to retrieve page {page_num}: {response.status_code}")
            break

        page_num += 1

    context.log.info(f"Scraped {len(players)} / 100 links")
    write_to_bucket(gcs, "top_100_players.json", players)
