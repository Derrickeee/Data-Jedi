#!/usr/bin/env python3
"""
CPI Data Crawler - Collects CPI data from multiple sources including:
1. Singapore Data.gov.sg API
2. Singapore SingStat Table Builder API
"""

import json
import logging
import requests
import pandas as pd
import time
import os
import urllib3  # Import urllib3 for HTTP requests
import certifi  # Import certifi for SSL certificate verification
from urllib.request import Request, urlopen
from datetime import datetime

# --- Configuration ---
OUTPUT_DIR = "cpi_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
HEADERS = {
    'User-Agent': USER_AGENT,
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
}

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("crawler.log"),
        logging.StreamHandler()
    ]
)


class CPIDataCrawler:
    def __init__(self):

        # Initialize a PoolManager for HTTP requests with SSL verification
        self.http = urllib3.PoolManager(
            cert_reqs='CERT_REQUIRED',
            ca_certs=certifi.where()
        )

        # Data source configurations
        self.sources = {
            'sg_gov': {
                'base_url': "https://api-production.data.gov.sg",
                'active': True
            },
            'sg_singstat': {
                'base_url': "https://tablebuilder.singstat.gov.sg",
                'active': True
            }
        }

    @staticmethod
    def fetch_data_gov(dataset_ids):
        """Fetch data from Singapore's Data.gov.sg API for multiple dataset IDs"""
        if not dataset_ids:
            logging.warning("No dataset IDs provided for Data.gov.sg")
            return None

        dfs = []

        for dataset_id in dataset_ids:
            logging.info(f"Fetching from Data.gov.sg dataset: {dataset_id}")
            session = requests.Session()
            try:
                initiate_resp = session.get(
                    f"https://api-open.data.gov.sg/v1/public/api/datasets/{dataset_id}/initiate-download"
                ).json()

                logging.info(initiate_resp['data']['message'])

                for _ in range(5):
                    poll_resp = session.get(
                        f"https://api-open.data.gov.sg/v1/public/api/datasets/{dataset_id}/poll-download"
                    ).json()
                    url = poll_resp['data'].get('url')
                    if url:
                        logging.info(f"Download URL: {url}")
                        df = pd.read_csv(url)
                        dfs.append(df)
                        break
                    time.sleep(2)
                else:
                    logging.error(f"Download failed after retries: {dataset_id}")

            except Exception as e:
                logging.exception(f"Error fetching dataset {dataset_id}: {e}")

        return pd.concat(dfs, ignore_index=True) if dfs else None

    def fetch_singstat_api(self, table_ids):
        """Fetch data from SingStat Table Builder API for multiple table IDs"""
        if not table_ids:
            logging.warning("No SingStat table IDs provided")
            return None

        dfs = []
        for table_id in table_ids:
            url = f"{self.sources['sg_singstat']['base_url']}/api/table/tabledata/{table_id}"

            try:
                request = Request(url, headers=HEADERS)
                with urlopen(request) as response:
                    data = json.loads(response.read().decode())

                    # Process the raw data
                    if 'Data' in data:
                        df = pd.DataFrame(data['Data'])
                        df.columns = df.columns.str.strip()
                        df['extraction_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        df['source'] = 'SingStat API'
                        dfs.append(df)
                    else:
                        logging.warning(f"No data found for table ID {table_id}")

            except Exception as e:
                logging.exception(f"Error fetching SingStat API data: {e}")
                continue

        if dfs:
            return pd.concat(dfs, ignore_index=True)
        return None

    @staticmethod
    def clean_and_transform(df, source, dataset_id):
        """Clean and transform the raw data based on source"""
        print(f"\nCleaning {source} data...")
        if df is None or df.empty:
            return None
        # Source-specific transformations
        if source == 'sg_gov':
            # Standardize Singapore data columns
            column_mapping = {
                'year': 'year',
                'value': 'cpi_value',
                'category': 'DataSeries'
            }
            for col in df.columns:
                lower_col = col.lower()
                if lower_col in column_mapping:
                    df = df.rename(columns={col: column_mapping[lower_col]})

            # Add processing for half-yearly, quarterly and monthly data
            years = [str(year) for year in range(1000, 2025)]
            half_years = ["1H", "2H", "3H", "4H"]
            quarter_years = ["1Q", "2Q", "3Q", "4Q"]
            months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
            half_year_cols_exist = all(
                any(f"{year} {half}" in df.columns for year in years) for half in half_years)
            quarter_year_cols_exist = all(
                any(f"{year} {quarter}" in df.columns for year in years) for quarter in quarter_years)
            month_cols_exist = all(any(f"{year} {month}" in df.columns for year in years) for month in months)
            # Add missing columns if needed
            if 'frequency' not in df.columns:
                if half_year_cols_exist:
                    df['frequency'] = 'Semiannual'
                elif quarter_year_cols_exist:
                    df['frequency'] = 'Quarterly'
                elif month_cols_exist:
                    df['frequency'] = 'Monthly'
                else:
                    df['frequency'] = 'Annual'
            if 'income_group' not in df.columns:
                # Singapore specific processing
                dataset_map = {
                    "d_c5bde9ed17cef8c365629311f8550ce2": "Highest 20%",
                    "d_8f3660871b62f38609915ee7ef45ee2c": "Middle 60%",
                    "d_36c4af91ffd0a75f6b557960efcb476e": "Lowest 60%"
                }

                # Iterate through the dataset_id list
                for dataset in dataset_id:
                    if dataset in dataset_map:
                        df['income_group'] = dataset_map[dataset]
                        break  # Exit the loop once a match is found
            # Add source identifier
            df['data_source'] = source

        elif source == 'sg_singstat':
            if 'row' in df.columns:
                # Normalize the nested JSON data in 'row' column
                normalized_data = pd.json_normalize(df['row'], 'columns', 'rowText')
                # Concatenate with the original DataFrame and drop the original 'row' column
                df = pd.concat([df.drop(columns=['row']), normalized_data], axis=1)

            # Standardize SingStat data columns
            column_mapping = {
                'year': 'year',
                'value': 'cpi_value',
                'rowtext': 'data_series'
            }
            for col in df.columns:
                lower_col = col.lower()
                if lower_col in column_mapping:
                    df = df.rename(columns={col: column_mapping[lower_col]})

            df = df.drop_duplicates(subset=["data_series", "key"])
            # Pivot the DataFrame to rearrange the data by 'data_series'
            df = df.pivot(index="data_series", columns="key", values="cpi_value")

            # Reset the index if you need a clean DataFrame
            df.reset_index(inplace=True)
            # Add processing for half-yearly data
            years = [str(year) for year in range(1000, 2025)]
            half_years = ["1H", "2H", "3H", "4H"]
            quarter_years = ["1Q", "2Q", "3Q", "4Q"]
            months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
            half_year_cols_exist = all(any(f"{year} {half}" in df.columns for year in years) for half in half_years)
            quarter_year_cols_exist = all(
                any(f"{year} {quarter}" in df.columns for year in years) for quarter in quarter_years)
            month_cols_exist = all(any(f"{year} {month}" in df.columns for year in years) for month in months)
            # Add missing columns if needed
            if 'frequency' not in df.columns:
                if half_year_cols_exist:
                    df['frequency'] = 'Semiannual'
                elif quarter_year_cols_exist:
                    df['frequency'] = 'Quarterly'
                elif month_cols_exist:
                    df['frequency'] = 'Monthly'
                else:
                    df['frequency'] = 'Annual'

            if 'income_group' not in df.columns:
                # Singapore specific processing
                dataset_map = {
                    "M213051": "Highest 20%",
                    "M213041": "Middle 60%",
                    "M213071": "Middle 60%",
                    "M213031": "Lowest 60%"
                }

                # Iterate through the dataset_id list
                for dataset in dataset_id:
                    if dataset in dataset_map:
                        df['income_group'] = dataset_map[dataset]
                        break  # Exit the loop once a match is found
            # Add source identifier

            df['data_source'] = source

        # Basic cleaning - replace "na" strings with pd.NA and drop any columns containing NA values
        df = df.dropna(how='all')
        df = df.replace("na", pd.NA).dropna(axis=1, how='any')
        df = df.rename(columns=lambda x: x.strip().replace(' ', '_'))

        # Common transformations
        if 'year' in df.columns:
            df['year'] = df['year'].astype(int)
        if 'cpi_value' in df.columns:
            df['cpi_value'] = pd.to_numeric(df['cpi_value'], errors='coerce')

        return df

    @staticmethod
    def save_data(df, source):
        """Save processed data to CSV"""
        if df is None or df.empty:
            logging.info(f"No data to save for {source}")
            return None

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"cpi_{source}_{timestamp}.csv"
        output_path = os.path.join(OUTPUT_DIR, filename)
        df.to_csv(output_path, index=False)
        logging.info(f"Data saved to {output_path}")
        return output_path

    def run(self, sg_dataset_id=None, singstat_table_id=None):
        """Main execution method"""
        print("Starting CPI Data Crawler...")

        processed_files = []

        # Fetch Singapore Data.gov.sg data if enabled and dataset IDs provided
        if self.sources['sg_gov']['active'] and sg_dataset_id:
            # Convert single ID to list if needed
            if isinstance(sg_dataset_id, str):
                sg_dataset_id = [sg_dataset_id]

            sg_df = self.fetch_data_gov(sg_dataset_id)
            if sg_df is not None:
                sg_df = self.clean_and_transform(sg_df, 'sg_gov', sg_dataset_id)
                sg_file = self.save_data(sg_df, 'sg_gov')
                if sg_file:
                    processed_files.append(sg_file)

        # Fetch Singapore SingStat API data if enabled
        if self.sources['sg_singstat']['active'] and singstat_table_id:
            # Convert single ID to list if needed
            if isinstance(singstat_table_id, str):
                singstat_table_id = [singstat_table_id]

            singstat_df = self.fetch_singstat_api(singstat_table_id)
            if singstat_df is not None:
                singstat_df = self.clean_and_transform(singstat_df, 'sg_singstat', singstat_table_id)
                singstat_file = self.save_data(singstat_df, 'sg_singstat')
                if singstat_file:
                    processed_files.append(singstat_file)

        print("\nCPI Data Crawler completed successfully.")


if __name__ == "__main__":
    crawler = CPIDataCrawler()

    # Singapore CPI dataset IDs
    SG_CPI_DATASET_ID = "d_c5bde9ed17cef8c365629311f8550ce2"

    # SingStat table IDs
    SINGSTAT_TABLE_ID = "M213051"

    crawler.run(
        sg_dataset_id=SG_CPI_DATASET_ID,
        singstat_table_id=SINGSTAT_TABLE_ID
    )
