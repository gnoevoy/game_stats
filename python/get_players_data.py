from playwright.sync_api import sync_playwright, Playwright
from google_functions import write_to_bucket, read_from_bucket


def run(playwright: Playwright):
    # Open a browser and block images
    browser = playwright.chromium.launch(headless=False)
    page = browser.new_page()
    page.route("**/*.{webp,png,jpeg,jpg}", lambda route: route.abort())



    links = read_from_bucket("players_list") 

    for link in links:
        pass





def get_players_data():
    print("02. Extracting players data")
    # Run scraper
    with sync_playwright() as pw:
        data = run(pw)

    # Write data to bucket
    # write_to_bucket("players_list", data)
    print("Data successfully written to bucket")
