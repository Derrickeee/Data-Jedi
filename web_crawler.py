#!/usr/bin/env python3
"""
CPI Data Crawler - Collects CPI data from multiple sources including:
1. Singapore Data.gov.sg API
2. Singapore SingStat Table Builder API
"""

import json
import requests
import pandas as pd
import time
import os
import urllib3  # Import urllib3 for HTTP requests
import certifi  # Import certifi for SSL certificate verification
from urllib.request import Request, urlopen
from datetime import datetime
from bs4 import BeautifulSoup

class CPIDataCrawler:
    def __init__(self):
        self.output_dir = "cpi_data"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        }
        os.makedirs(self.output_dir, exist_ok=True)

        # Initialize a PoolManager for HTTP requests with SSL verification
        self.http = urllib3.PoolManager(
            cert_reqs='CERT_REQUIRED',
            ca_certs=certifi.where()
        )

        # Data source configurations
        self.sources = {
            'sg_gov': {
                'base_url': "https://api-production.data.gov.sg",
                'active': True,
                'dataset_ids': []  # Can be populated with Singapore CPI dataset IDs
            },
            'sg_singstat': {
                'base_url': "https://tablebuilder.singstat.gov.sg",
                'active': True
            }
        }

    def fetch_sg_datasets(self, dataset_id=None):
        """Fetch data from Singapore's Data.gov.sg API"""
        print("\nFetching Singapore CPI data from Data.gov.sg...")

        if not dataset_id:
            print("No dataset ID provided for Singapore data")
            return None

        try:
            # Initialize session
            s = requests.Session()
            s.headers.update({
                'referer': 'https://colab.research.google.com',
                'User-Agent': self.headers['User-Agent']
            })

            # Get metadata first
            url = f"{self.sources['sg_gov']['base_url']}/v2/public/api/datasets/{dataset_id}/metadata"
            print(f"Fetching metadata from: {url}")
            response = s.get(url)
            data = response.json()['data']
            column_metadata = data.pop('columnMetadata', None)

            print("\nSingapore Dataset Metadata:")
            print(json.dumps(data, indent=2))

            if column_metadata:
                print("\nColumns:", list(column_metadata['map'].values()))

            # Download the actual data
            print("\nInitiating data download...")
            initiate_url = f"https://api-open.data.gov.sg/v1/public/api/datasets/{dataset_id}/initiate-download"
            initiate_response = s.get(
                initiate_url,
                headers={"Content-Type": "application/json"},
                json={}
            )
            print(initiate_response.json()['data']['message'])

            # Poll for download URL
            max_polls = 5
            download_url = None
            for i in range(max_polls):
                poll_url = f"https://api-open.data.gov.sg/v1/public/api/datasets/{dataset_id}/poll-download"
                poll_response = s.get(
                    poll_url,
                    headers={"Content-Type": "application/json"},
                    json={}
                )

                poll_data = poll_response.json()['data']
                if "url" in poll_data:
                    download_url = poll_data['url']
                    print(f"Download URL obtained: {download_url}")
                    break

                print(f"{i + 1}/{max_polls}: Polling...")
                time.sleep(3)

            if download_url:
                df = pd.read_csv(download_url)
                print("\nSample of Singapore CPI data:")
                print(df.head())
                return df
            else:
                print("Failed to obtain download URL after multiple attempts")
                return None

        except Exception as e:
            print(f"Error fetching Singapore data: {e}")
            return None

    def fetch_singstat_data(self, table_id="M213071", series_filter="1.1", time_filter="2023 1H"):
        """Fetch data from SingStat Table Builder API"""
        print("\nFetching Singapore CPI data from SingStat Table Builder...")
        api_url = f"{self.sources['sg_singstat']['base_url']}/api/table/tabledata/{table_id}"
        params = {
            "seriesNoORrowNo": series_filter,
            "offset": 0,
            "limit": 3000,
            "sortBy": "rowtext asc",
            "timeFilter": time_filter,
            "between": "0,9000",
            "search": "food"
        }

        try:
            request = Request(api_url, headers=self.headers)
            with urlopen(request) as response:
                data = json.loads(response.read().decode())

                # Process the raw data
                if 'Data' in data:
                    df = pd.DataFrame(data['Data'])
                    df.columns = df.columns.str.strip()
                    df['extraction_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    df['source'] = 'SingStat Table Builder API'

                    print("\nSample of SingStat CPI data:")
                    print(df.head())
                    return df
                return None

        except Exception as e:
            print(f"Error fetching SingStat API data: {e}")
            return None

    def scrape_singstat_table(self, table_url):
        """Scrape data from SingStat Table Builder interface"""
        print("\nScraping Singapore CPI data from SingStat Table Builder page...")
        try:
            response = requests.get(table_url, headers=self.headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Extract the main data table
            table = soup.find('table', {'class': 'data-table'})
            if not table:
                print("No data table found on page")
                return None

            # Extract headers
            headers = [th.text.strip() for th in table.find_all('th')]

            # Extract rows
            data = []
            for row in table.find_all('tr')[1:]:  # Skip header row
                cols = row.find_all('td')
                data.append([col.text.strip() for col in cols])

            # Create DataFrame
            df = pd.DataFrame(data, columns=headers)

            print("\nSample of scraped SingStat data:")
            print(df.head())
            return df

        except Exception as e:
            print(f"Error scraping SingStat table: {e}")
            return None


    @staticmethod
    def clean_and_transform(df, source):
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
                'category': 'data_series'
            }
            for col in df.columns:
                lower_col = col.lower()
                if lower_col in column_mapping:
                    df = df.rename(columns={col: column_mapping[lower_col]})

            # Singapore specific processing
            if 'year' not in df.columns:
                df['year'] = datetime.now().year
            df['period'] = df['year'].apply(lambda x: 'Post-COVID' if x >= 2020 else 'Pre-COVID')
            df['income_group'] = 'All'


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
                'rowText': 'data_series',
                'time': 'period'
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
            # Add missing columns if needed
            if 'period' not in df.columns:
                df['period'] = 'Annual'
            if 'income_group' not in df.columns:
                df['income_group'] = 'All'
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


    def save_data(self, df, source):
        """Save processed data to CSV"""
        if df is None or df.empty:
            print(f"No data to save for {source}")
            return None

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"cpi_{source}_{timestamp}.csv"
        output_path = os.path.join(self.output_dir, filename)
        df.to_csv(output_path, index=False)
        print(f"Data saved to {output_path}")
        return output_path

    def run(self, sg_dataset_id=None, singstat_table_id=None, singstat_url=None):
        """Main execution method"""
        print("Starting CPI Data Crawler...")

        processed_files = []

        # Fetch Singapore Data.gov.sg data if enabled and dataset ID provided
        if self.sources['sg_gov']['active'] and sg_dataset_id:
            sg_df = self.fetch_sg_datasets(sg_dataset_id)
            if sg_df is not None:
                sg_df = self.clean_and_transform(sg_df, 'sg_gov')
                sg_file = self.save_data(sg_df, 'sg_gov')
                if sg_file:
                    processed_files.append(sg_file)

        # Fetch Singapore SingStat API data if enabled
        if self.sources['sg_singstat']['active'] and singstat_table_id:
            singstat_df = self.fetch_singstat_data(table_id=singstat_table_id)
            if singstat_df is not None:
                singstat_df = self.clean_and_transform(singstat_df, 'sg_singstat')
                singstat_file = self.save_data(singstat_df, 'sg_singstat')
                if singstat_file:
                    processed_files.append(singstat_file)

        # Scrape Singapore SingStat table if URL provided
        if self.sources['sg_singstat']['active'] and singstat_url:
            scraped_df = self.scrape_singstat_table(singstat_url)
            if scraped_df is not None:
                scraped_df = self.clean_and_transform(scraped_df, 'sg_singstat')
                scraped_file = self.save_data(scraped_df, 'sg_singstat_scraped')
                if scraped_file:
                    processed_files.append(scraped_file)

        print("\nCPI Data Crawler completed successfully.")


if __name__ == "__main__":
    crawler = CPIDataCrawler()

    # Example Singapore CPI dataset ID (replace with actual ID)
    SG_CPI_DATASET_ID = "d_c5bde9ed17cef8c365629311f8550ce2"  # Should be a real CPI dataset ID

    # Example SingStat parameters (replace with actual values)
    SINGSTAT_TABLE_ID = "M213071"  # Example table ID for CPI data
    SINGSTAT_TABLE_URL = "https://tablebuilder.singstat.gov.sg/table/TS/M213071"  # Example table URL

    crawler.run(
        sg_dataset_id=SG_CPI_DATASET_ID,
        singstat_table_id=SINGSTAT_TABLE_ID,
        singstat_url=SINGSTAT_TABLE_URL
    )
