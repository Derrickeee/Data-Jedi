# Data Crawler Project

This repository contains the source code and documentation for the **Data Crawler** project. The aim is to develop a Python-based tool for extracting, transforming, and loading data from various sources, utilizing open-source libraries and PostgreSQL for storage and analysis.

## Project Objectives
1. Use Python to develop a data crawler for sourcing data from unrestricted sources.
2. Utilize open-source libraries for data extraction, transformation, and loading (ETL).
3. Set up PostgreSQL (or another DBMS) with a well-designed schema and entity-relationship models.
4. Integrate SQL with Python for seamless database operations (insert, update, view).
5. Enable possible analyses of the sourced data.

## Features
- **Data Crawling**: Extract data from APIs or websites (e.g., Twitter API).
- **Data Transformation**: Clean, parse, and restructure data for efficient storage.
- **Database Integration**: Store and retrieve data using PostgreSQL.
- **Python-Powered Analytics**: Provide scripts for querying and analyzing the data.

## Data Sources
Data sources that we used:
- [Data.gov.sg](https://www.data.gov.sg/)
- [SingStat Table Builder](https://tablebuilder.singstat.gov.sg/)

## Requirements
- Python 3.7+ 
- PostgreSQL (or any other preferred DBMS)
- Dependencies (install using `pip`):
  - 'requests'
  - 'pandas'
  - 'SQLAlchemy'
  - 'psycopg2'
  - 'bs4'

## Installation
1. Clone this repository:
   ```bash
   git clone https://github.com/Derrickeee/Data-Jedi.git
   cd Data-Jedi

