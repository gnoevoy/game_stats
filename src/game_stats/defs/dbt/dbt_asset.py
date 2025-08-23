from dagster_dbt import DbtCliResource, DbtProject, dbt_assets, get_asset_key_for_model
from pathlib import Path
import dagster as dg


# Points to the dbt project path
dbt_project_directory = Path(__file__).absolute().parent
dbt_project = DbtProject(project_dir=dbt_project_directory)

# References the dbt project object
dbt_resource = DbtCliResource(project_dir=dbt_project)

# Compiles the dbt project & allow Dagster to build an asset graph
dbt_project.prepare_if_dev()


# Yields Dagster events streamed from the dbt CLI
@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
