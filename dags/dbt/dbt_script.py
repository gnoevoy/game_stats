from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping
import os


# Construct paths
HOME_DIR = os.getenv("AIRFLOW_HOME")
DBT_PROJECT_PATH = f"{HOME_DIR}/dags/dbt"
DBT_EXECUTABLE_PATH = f"{HOME_DIR}/dbt_venv/bin/dbt"

# Import env variables
PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT_ID")
DATASET = os.getenv("DATASET_NAME")

# Establish connection to BigQuery + specify dbt profile
profile_config = ProfileConfig(
    profile_name="game_stats",
    target_name="airflow",
    profile_mapping=GoogleCloudServiceAccountDictProfileMapping(
        conn_id="google_cloud",
        profile_args={"project": PROJECT, "dataset": DATASET, "threads": 64},
    ),
)

# Specify path where code is located + provide variables
project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
    env_vars={"GOOGLE_CLOUD_PROJECT_ID": PROJECT, "BIGQUERY_DATASET": DATASET},
)

# Provide path to dbt virtual environment inside Airflow container
execution_config = ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE_PATH)


# Dbt task group to visualize script execution order in the Airflow UI
def dbt_group(group_id):
    return DbtTaskGroup(
        group_id=group_id,
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        default_args={"retries": 1},
    )
