from playwright.sync_api import sync_playwright, Playwright
from google_functions import write_to_bucket
from bs4 import BeautifulSoup
import traceback
import requests
import os


def get_links(page, home_page):
    # Render the page and extract all player links
    html = page.content()
    soup = BeautifulSoup(html, "html.parser")
    players_table = soup.find("table", class_="data-table")
    links = players_table.find_all("a")
    lst = []

    for link in links:
        try:
            # Check if the link is valid
            value = link.get("href")
            if "playerinfo&player" in value:
                lst.append(f"{home_page}{value}")
        except:
            traceback.print_exc()
            continue

    return lst


def run(playwright: Playwright):
    # Open a browser and block images
    browser = playwright.chromium.launch(headless=False)
    page = browser.new_page()
    page.route("**/*.{webp,png,jpeg,jpg}", lambda route: route.abort())

    # Initialize list to store links and set up base URL
    players_links = []
    home_page = os.getenv("HOME_PAGE")
    base_url = f"{home_page}hlstats.php?mode=players&game=css&sort=skill&sortorder=desc&page="
    page_num = 1

    # Loop through the first two pages and get top 100 players
    while page_num <= 2:
        url = f"{base_url}{page_num}"
        response = requests.get(url)

        # Check if the request was successful
        if response.status_code == 200:
            page.goto(url)
            links = get_links(page, home_page)
            players_links.extend(links)

        page_num += 1

    print(f"Scraped {len(players_links)} / 100 links successfully")
    browser.close()
    return players_links


def get_players_list():
    print("01. Extracting players links")
    # Run scraper
    with sync_playwright() as pw:
        data = run(pw)

    # Write data to bucket
    write_to_bucket("players_list", data)
    print("Data successfully written to bucket")
