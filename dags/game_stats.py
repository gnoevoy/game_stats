from airflow.decorators import task_group
from airflow.sdk import dag, task
import pendulum
import sys
import os

# Add path to import scripts
HOME_DIR = os.getenv("AIRFLOW_HOME")
sys.path.append(f"{HOME_DIR}/dags")

from python.web_scraping.get_top_players import get_players_links
from python.web_scraping.get_players_data import get_players_stats
from dbt.dbt_script import dbt_group


@dag(schedule="@weekly", start_date=pendulum.datetime(2025, 1, 1, tz="UTC"), catchup=False)
def game_stats():

    # Web scraping
    @task_group(group_id="web_scraping")
    def web_scraping():
        t_get_players_links = get_players_links()
        t_get_players_stats = get_players_stats()
        t_get_players_links >> t_get_players_stats

    g_web_scraping = web_scraping()

    # dbt
    g_dbt = dbt_group(group_id="dbt_group")

    # Empty task to chain python and dbt groups
    # @task(trigger_rule="none_failed")
    # def empty_task():
    #     print("")

    # g_python = python_group()
    # t_empty_task = empty_task()
    # g_dbt = dbt_group(group_id="dbt_group")

    # # Chain scripts together
    # g_python >> t_empty_task >> g_dbt


game_stats()
