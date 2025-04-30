# Data Crawler Project

This repository contains the source code and documentation for the **Data Crawler** project. The aim is to develop a Python-based tool for extracting, transforming, and loading data from various sources, utilizing open-source libraries and PostgreSQL for storage and analysis.

## Table of Contents
1. [Project Objectives](#project-objectives)
2. [Features](#features)
3. [Data Sources](#data-sources)
4. [References](#references)
5. [Requirements](#requirements)
6. [Installation](#installation)
7. [Deliverables](#deliverables)
8. [Contribution](#contribution)

## Project Objectives
1. Develop a Python data crawler to source data from multiple platforms.
2. Utilize open-source libraries for data extraction, transformation, and loading (ETL).
3. Set up PostgreSQL (or another DBMS) with a well-designed schema and entity-relationship models.
4. Integrate SQL within Python for database operations (insert, update, view).
5. Provide tools and scripts for subsequent data analysis.

## Features
- **Data Crawling**: Extract data from various APIs or web sources (e.g., government datasets).
- **Data Transformation**: Clean, parse, and reshape data for effective storage.
- **Database Integration**: Use PostgreSQL for storing and managing sourced data.
- **Analytics Support**: Leverage Python scripts and Jupyter Notebooks for data analysis.

## Data Sources
The primary data sources used in this project:
- [Data.gov.sg](https://data.gov.sg/) – Provides datasets from various Singapore government agencies.
- [SingStat](https://www.singstat.gov.sg/) – Offers official Singapore statistical data and reports.

## References
The project draws inspiration and methods from these resources:
- [DataGovSG Exploration Notebook](https://github.com/datagovsg/dgs-exploration/blob/master/DataGovSG.ipynb) – Demonstrates approaches to sourcing and analyzing Singapore government datasets.
- [SingStat Table Builder API for Developers](https://tablebuilder.singstat.gov.sg/view-api/for-developers) – Provides access to detailed datasets via API.

## Requirements
- Python 3.7+
- PostgreSQL (or another preferred DBMS)
- Python libraries (install via `pip`):
  - `pandas`
  - `SQLAlchemy`
  - `psycopg2`
  - `requests`

## Installation
1. **Clone the repository:**
   ```bash
   git clone https://github.com/Derrickeee/Data-Jedi.git
   cd Data-Jedi

2. Install required Python libraries:
   ```bash
   pip install -r requirements.txt

3. Set up PostgreSQL:
   - Install PostgreSQL.
   - Create a database and configure connection credentials.

4. Run the data crawler:
   - Key in the Dataset ID in the executable.
   - Execute the script to fetch and store data.

## Deliverables
1. **Source Code:** Python scripts and Jupyter Notebook files.
2. **Documentation:** Detailed project guides and reports.
3. **Presentation Deck:** A slide deck summarizing the project objectives, processes, and outcomes.

## Contribution
Contributions are welcome! Please submit a pull request or open an issue for any enhancements, bugs, or ideas.

