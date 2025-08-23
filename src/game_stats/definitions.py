from game_stats.defs.web_scraping.get_top_players import get_players_links
from game_stats.defs.web_scraping.get_players_data import get_players_stats
from dagster_gcp.gcs import GCSResource
import dagster as dg
import os

GOOGLE_CLOUD_PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT_ID")

defs = dg.Definitions(
    assets=[get_players_links, get_players_stats],
    resources={"gcs": GCSResource(project=GOOGLE_CLOUD_PROJECT_ID)},
)
