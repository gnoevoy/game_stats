from playwright.sync_api import sync_playwright, Playwright
from google_functions import write_to_bucket
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import os

load_dotenv(".env")

def get_links(page, home_page):
    # render the page and extract all player links
    html = page.content()
    soup = BeautifulSoup(html, "html.parser")
    players_table = soup.find("table", class_="data-table")
    links = players_table.find_all("a")

    # try except include
    lst = []
    for link in links:
        value = link.get("href")
        # check if the link is correct
        if "playerinfo&player" in value:
            lst.append(f"{home_page}{value}")

    return lst


def run(playwright: Playwright):
    # Open a browser and block images
    browser = playwright.chromium.launch(headless=False)
    page = browser.new_page()
    page.route("**/*.{webp,png,jpeg,jpg}", lambda route: route.abort())

    players_links = [] 
    home_page = os.getenv("HOME_PAGE")
    base_url = f"{home_page}hlstats.php?mode=players&game=css&sort=skill&sortorder=desc&page="
    page_num = 1

    while page_num <= 2:
        url = f"{base_url}{page_num}"
        page.goto(url)
        links = get_links(page, home_page)
        players_links.extend(links)
        page_num += 1

    browser.close()
    return players_links

def get_players_list():
    with sync_playwright() as pw:
        data = run(pw)

    write_to_bucket("players_list", data)

get_players_list()