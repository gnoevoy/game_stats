from airflow.decorators import task_group
from airflow.sdk import dag
import pendulum
import sys
import os

# Add path to import scripts
HOME_DIR = os.getenv("AIRFLOW_HOME")
sys.path.append(f"{HOME_DIR}/dags")

from python.web_scraping.get_top_players import get_players_links
from python.web_scraping.get_players_data import get_players_stats
from python.data_transformations.transform_data import (
    transform_players_data,
    transform_player_names,
    transform_player_actions,
    transform_player_weapons,
    transform_player_frags,
    transform_sessions,
    transform_events,
)
from python.data_transformations.load_to_bigquery import load_data
from dbt.dbt_script import dbt_group


@dag(schedule="@weekly", start_date=pendulum.datetime(2025, 1, 1, tz="UTC"), catchup=False)
def game_stats():

    # Web scraping
    @task_group(group_id="web_scraping")
    def web_scraping():
        t_get_players_links = get_players_links()
        t_get_players_stats = get_players_stats()
        t_get_players_links >> t_get_players_stats

    # Data transformation
    @task_group(group_id="data_transformations")
    def data_transformations():
        # Process csv files with pandas
        t_transform_players_data = transform_players_data()
        t_transform_player_names = transform_player_names()
        t_transform_player_actions = transform_player_actions()
        t_transform_player_weapons = transform_player_weapons()
        t_transform_player_frags = transform_player_frags()
        t_transform_sessions = transform_sessions()
        t_transform_events = transform_events()

        # Move cleaned data to BigQuery
        t_load_players = load_data(task_id="load_players", source_object="clean/players.csv", table="players")
        t_load_names = load_data(task_id="load_names", source_object="clean/names.csv", table="names")
        t_load_action = load_data(task_id="load_actions", source_object="clean/actions.csv", table="actions")
        t_load_weapons = load_data(task_id="load_weapons", source_object="clean/weapons.csv", table="weapons")
        t_load_frags = load_data(task_id="load_frags", source_object="clean/frags.csv", table="frags")
        t_load_sessions = load_data(task_id="load_sessions", source_object="clean/sessions.csv", table="sessions")
        t_load_events = load_data(task_id="load_events", source_object="clean/events.csv", table="events")

        # Chain tasks in a group
        t_transform_players_data >> t_load_players
        t_transform_player_names >> t_load_names
        t_transform_player_actions >> t_load_action
        t_transform_player_weapons >> t_load_weapons
        t_transform_player_frags >> t_load_frags
        t_transform_sessions >> t_load_sessions
        t_transform_events >> t_load_events

    g_web_scraping = web_scraping()
    g_data_transformations = data_transformations()
    g_dbt = dbt_group(group_id="dbt_group")

    g_web_scraping >> g_data_transformations >> g_dbt


game_stats()
