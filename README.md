# Project Overview

**Subject:** Player statistics and leaderboard data for an FPS game server.

**Needs:** Track players data over time to monitor performance and identify trends.

**Problem:** The server’s website only shows recent activity, with no historical records — making long-term analysis and comparisons impossible.

**Solution:** Build a data pipeline that regularly collects leaderboard data and stores it in a warehouse, creating a complete historical dataset for analytics and reporting.

**Used Tools:** `Python`, `SQL`, `dbt`, `BigQuery`, `Google Cloud`, `Excel`, `Power BI`, `Docker`, `Git`


<br>


# Data Pipeline

The pipeline runs on a scheduled interval  in the cloud, processing roughly 3–5 MB of data per execution. It collects raw leaderboard data from the website, cleans and transforms it, and loads the final results into `BigQuery` as the central data warehouse. Below is the schema of the final analytics-ready tables produced by the pipeline.

![Database Schema](images/schema.jpg)

### Challenges during Development

| Challenge | Description |
|-----------|-------------|
| **Understanding the data source** | Identifying how the website generates player stats, determining which metrics matter, and selecting only the most relevant data using domain knowledge. |
| **Web scraping** | Since there’s no API, data had to be scraped directly from the website — a clumsy, quire iterative and error-prone process. |
| **Time zone handling** | Converting timestamps correctly across different time zones to ensure accurate data. |
| **Data modeling** | Building a well-structured and efficient `dbt` models to organize and store historical player records. |

<br>

## Pipeline Logic

![Pipeline Logic](images/pipeline.jpg)

The ETL pipeline is divided into three independent stages, each running in its own Docker container with separated logic: one handles web scraping, one processes the data and loads it into BigQuery, and one runs dbt transformations to produce the final analytics tables.

### Python  
- Web scraping handled with `requests` and `BeautifulSoup`.
- Used `Google Cloud` libraries to read and write data to `Cloud Storage` and `BigQuery`.  
- Data cleaning and transformation performed with `pandas`.

### dbt  
- `Macros` provide reusable SQL logic across models.
- `Seeds` store small, static reference datasets.
- Used `incremental models` to append/update data efficiently.
- Used `snapshots` to preserve historical changes.


<br>

## Orchestration and Scheduling

For orchestration and scheduling was chosen Google Cloud services over Airflow because:
- **Lower cost** — Serverless tools like `Cloud Run Jobs` and `Workflows` cost far less than Composer or Astronomer.  
- **Easy deployment** — Running Docker containers in the cloud is simpler than managing Airflow setups.  
- **Native integration** — Works seamlessly with `BigQuery`, `Cloud Storage`, and other Google services.  
- **No maintenance** — Fully managed, scalable, and requires zero infrastructure management.

![Google Cloud](images/cloud.jpg)

| Tool / Service            | Description                                                                                           | Role in the Project                                                                                 |
|----------------------------|-------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------|
| `Git`                      | Version control system for managing source code.                                                     | Stores and tracks code changes for project.                           |
| `Google Cloud Build`       | CI/CD automation service that executes build and deploy steps defined in YAML.                       | Builds Docker containers, deploys Cloud Run jobs, and updates Workflows with environment variables. |
| `Docker`                   | Containerization platform for packaging code and dependencies.                                       | Ensures consistent execution environments for all pipeline steps.                                   |
| `Artifact Registry`        | Secure container image repository.                                                                   | Stores built Docker images for use by Cloud Run Jobs.                                               |
| `Google Cloud Run Jobs`    | Serverless compute service for running containerized batch tasks.                                   | Executes ETL tasks such as data extraction, transformation, and loading.                            |
| `Google Workflows`         | Orchestration service that connects multiple Google Cloud services using YAML instructions.          | Chains Cloud Run Jobs sequentially, managing execution flow and dependencies.                       |
| `Cloud Scheduler`          | Cron-like scheduling service in Google Cloud.                                                        | Triggers the Workflow automatically on a defined schedule.                  |


<br>


# Analytics

coming soon ...