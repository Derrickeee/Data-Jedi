#!/usr/bin/env python3
"""
CPI Data Crawler - Collects CPI data from multiple sources including:
1. US Bureau of Labor Statistics (BLS)
2. Singapore Data.gov.sg API
"""

import requests
import pandas as pd
import time
import os
import json
from datetime import datetime


class CPIDataCrawler:
    def __init__(self):
        self.output_dir = "cpi_data"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        os.makedirs(self.output_dir, exist_ok=True)

        # Data source configurations
        self.sources = {
            'us_bls': {
                'base_url': "https://www.bls.gov/cpi/",
                'active': True
            },
            'sg_gov': {
                'base_url': "https://api-production.data.gov.sg",
                'active': True,
                'dataset_ids': []  # Can be populated with Singapore CPI dataset IDs
            }
        }

    def fetch_sg_datasets(self, dataset_id=None):
        """Fetch data from Singapore's Data.gov.sg API"""
        print("\nFetching Singapore CPI data...")

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

    @staticmethod
    def fetch_bls_data():
        """Fetch data from US Bureau of Labor Statistics"""
        print("\nFetching US BLS CPI data...")
        try:
            # This would be replaced with actual BLS API calls or web scraping
            # For prototype, we'll simulate with sample data
            data = {
                'DataSeries': ['All Items', 'Food', 'Housing', 'Transportation'],
                'Year': [2023, 2023, 2023, 2023],
                'CPI': [304.127, 320.354, 334.029, 295.491],
                'Period': ['Post-COVID'] * 4,
                'Income_Group': ['All'] * 4
            }
            df = pd.DataFrame(data)
            print("\nSample of US BLS CPI data:")
            print(df.head())
            return df

        except Exception as e:
            print(f"Error fetching BLS data: {e}")
            return None

    @staticmethod
    def clean_and_transform(df, source):
        """Clean and transform the raw data based on source"""
        print(f"\nCleaning {source} data...")

        # Basic cleaning
        df = df.dropna(how='all')
        df = df.rename(columns=lambda x: x.strip().replace(' ', '_'))

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
            df['income_group'] = 'All'  # Singapore data might not have income groups

        elif source == 'us_bls':
            # Standardize US data columns
            column_mapping = {
                'dataseries': 'data_series',
                'year': 'year',
                'cpi': 'cpi_value',
                'period': 'period',
                'income_group': 'income_group'
            }
            for col in df.columns:
                lower_col = col.lower()
                if lower_col in column_mapping:
                    df = df.rename(columns={col: column_mapping[lower_col]})

        # Common transformations
        if 'year' in df.columns:
            df['year'] = df['year'].astype(int)
        if 'cpi_value' in df.columns:
            df['cpi_value'] = pd.to_numeric(df['cpi_value'], errors='coerce')

        # Add source identifier
        df['data_source'] = source

        return df

    def save_data(self, df, source):
        """Save processed data to CSV"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"cpi_{source}_{timestamp}.csv"
        output_path = os.path.join(self.output_dir, filename)
        df.to_csv(output_path, index=False)
        print(f"Data saved to {output_path}")
        return output_path

    def combine_datasets(self, file_paths):
        """Combine multiple CPI datasets into one"""
        if not file_paths:
            return None

        dfs = []
        for path in file_paths:
            try:
                dfs.append(pd.read_csv(path))
            except Exception as e:
                print(f"Error reading {path}: {e}")

        if dfs:
            combined_df = pd.concat(dfs, ignore_index=True)
            combined_path = os.path.join(self.output_dir, "combined_cpi_analysis.csv")
            combined_df.to_csv(combined_path, index=False)
            print(f"\nCombined data saved to: {combined_path}")
            print("\nSample of combined data:")
            print(combined_df.head())
            return combined_path
        return None

    def run(self, sg_dataset_id=None):
        """Main execution method"""
        print("Starting CPI Data Crawler...")

        processed_files = []

        # Fetch Singapore data if enabled and dataset ID provided
        if self.sources['sg_gov']['active'] and sg_dataset_id:
            sg_df = self.fetch_sg_datasets(sg_dataset_id)
            if sg_df is not None:
                sg_df = self.clean_and_transform(sg_df, 'sg_gov')
                sg_file = self.save_data(sg_df, 'sg_gov')
                processed_files.append(sg_file)

        # Combine all datasets
        if processed_files:
            self.combine_datasets(processed_files)

        print("\nCPI Data Crawler completed successfully.")


if __name__ == "__main__":
    crawler = CPIDataCrawler()

    # Example Singapore CPI dataset ID (replace with actual ID)
    SG_CPI_DATASET_ID = "d_69b3380ad7e51aff3a7dcc84eba52b8a"  # This should be a real CPI dataset ID

    crawler.run(sg_dataset_id=SG_CPI_DATASET_ID)