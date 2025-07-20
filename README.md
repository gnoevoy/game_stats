# Project Overview

**Objective:** Build a data pipeline that scrapes data from a website and delivers a clean, analytics-ready dataset. The pipeline is automated and scheduled to run in the cloud.

**Data Source:** Player statistics and rankings collected from a Counter-Strike: Source game server leaderboard.

**Used Tools:** `Python` `SQL` `dbt` `Git` `Docker` `Google Cloud Storage` `BigQuery` `Google Cloud`


<br>


## Pipeline Logic

![Pipeline Logic](images/pipeline.jpg)

### Python Features
- Applied `requests` + `BeautifulSoup4` for scraping player stats from the leaderboard  
- Used `pandas` to clean and transform raw data into structured format  
- Set up structured logging using Python's built-in `logging` module  
- Added `try-except` blocks for basic error handling  
- Designed a modular codebase with reusable functions across scripts  

### DBT Features
- Configured multiple `profiles` to support both local development and cloud execution  
- Built `incremental models` to efficiently update only new or changed records  
- Created reusable `macros` to simplify and modularize SQL logic  
- Loaded static reference data using `seeds`  
- Implemented `snapshots` to track historical changes in key tables  
- Added basic `tests` to ensure data quality and catch issues early  


<br>


## Cloud Automation & Orchestration

![Cloud](images/cloud.jpg)

| Service / Tool       | Use Case                                                                 |
|----------------------|----------------------------------------------------------------------------------------|
| **GitHub**           | Stores pipeline code and deployment configs (YAML files) |
| **Docker**           | Packages code into containers. Ensures consistency across environments. |
| **Cloud Build**      | CI/CD tool that builds and deploys containers automatically. Also updates workflow yaml file             |
| **Artifact Registry**| Secure repository where Docker images are stored before deployment.                   |
| **Cloud Run**        | Runs containers in a scalable serverless environment. No need to manage infrastructure.|
| **Workflows**        | Orchestrates the flow of containers to ensure tasks run in proper sequence.           |
| **Cloud Scheduler**  | Triggers workflows based on a schedule (like a CRON job). Enables automated runs.     |

This architecture automates a data pipeline using Google Cloud services. Code is versioned in GitHub, containerized with Docker, and deployed via Cloud Build into a serverless environment (Cloud Run). Workflow and Scheduler ensure tasks are orchestrated and executed on a schedule.


<br>


### Challenges During Development
- Understanding the data source and interpreting the meaning of various values  
- Handling timezone differences to ensure accurate data and output 
- Managing missing unique IDs for simultaneous events
- Orchestrating Python and DBT code to run sequentially in the cloud with scheduling, logging, and automated deployment on code changes


<br>


## Possible Improvements

1. Improve logging to capture more detailed information about scraped player data  
2. Add automated tests for Python code to ensure quality before deployment 
3. Use Apache Airflow on a cloud VM to orchestrate and schedule the pipeline instead of current setup  
