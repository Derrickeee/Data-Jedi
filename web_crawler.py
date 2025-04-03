#!/usr/bin/env python3
"""
Enhanced CPI Data Crawler - Combines CPI data collection with general web scraping capabilities.
"""

import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
from datetime import datetime
from urllib.parse import urljoin


class EnhancedCPIDataCrawler:
    def __init__(self):
        self.output_dir = "cpi_data"
        self.cpi_base_url = "https://www.data.gov.sg/"
        self.news_url = "https://www.channelnewsasia.com/"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        os.makedirs(self.output_dir, exist_ok=True)

    def fetch_page(self, url):
        """Fetches a webpage and returns its HTML content."""
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            print(f"Request failed for {url}: {e}")
            return None

    @staticmethod
    def extract_news_links(soup, base_url):
        """Extracts and returns all links from a news page."""
        links = []
        for link in soup.find_all('a'):
            text = link.text.strip()
            href = link.get('href', '')
            full_url = urljoin(base_url, href) if href else None
            if full_url and text:  # Only include links with both text and URL
                links.append({'text': text, 'url': full_url})
        return links

    @staticmethod
    def extract_headline(soup):
        """Extracts the main headline if available."""
        title_tag = soup.find('h1')
        return title_tag.text.strip() if title_tag else "No headline found."

    @staticmethod
    def extract_elements_by_class(soup, class_name):
        """Extracts elements based on class name."""
        elements = soup.find_all(class_=class_name)
        return [element.text.strip() for element in elements]

    def scrape_news_site(self):
        """Scrapes the news website for headlines and links."""
        print("\nScraping news website for economic indicators...")
        html = self.fetch_page(self.news_url)
        if not html:
            return None

        soup = BeautifulSoup(html, 'html.parser')

        # Extract headline
        headline = self.extract_headline(soup)
        print(f"Main Headline: {headline}")

        # Extract links
        links = self.extract_news_links(soup, self.news_url)
        print(f"\nFound {len(links)} links on the news page")

        # Extract elements by specific class (economic news)
        economic_news = []
        class_name = "h6__link list-object__heading-link"
        class_elements = self.extract_elements_by_class(soup, class_name)

        for element in class_elements:
            if any(keyword.lower() in element.lower()
                   for keyword in ['inflation', 'CPI', 'price', 'economic']):
                economic_news.append(element)

        print("\nRelevant Economic News Headlines:")
        for news in economic_news:
            print(f"- {news}")

        return {
            'headline': headline,
            'links': links[:10],  # Return first 10 links for demo
            'economic_news': economic_news
        }

    def fetch_cpi_data_sources(self):
        """Identify available CPI data sources from the data.gov.sg website"""
        print("\nIdentifying CPI data sources...")
        html = self.fetch_page(self.cpi_base_url + "datasets")
        if not html:
            return []

        soup = BeautifulSoup(html, 'html.parser')
        tables = soup.find_all('table', {'class': 'regular-data'})

        data_sources = []
        for table in tables:
            for row in table.find_all('tr')[1:]:  # Skip header row
                cols = row.find_all('td')
                if len(cols) >= 2:
                    data_sources.append({
                        'title': cols[0].text.strip(),
                        'url': urljoin(self.cpi_base_url, cols[1].find('a')['href']) if cols[1].find('a') else None
                    })

        print(f"Found {len(data_sources)} CPI data sources")
        return data_sources

    def download_cpi_data(self, url, data_type):
        """Download and parse CPI data from a specific URL"""
        print(f"\nDownloading {data_type} data...")
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()

            # For prototype, we'll simulate parsing different data formats
            if url.endswith('.xlsx'):
                df = pd.read_excel(response.content)
            elif url.endswith('.csv'):
                df = pd.read_csv(url)
            else:
                # For HTML tables
                soup = BeautifulSoup(response.text, 'html.parser')
                table = soup.find('table')
                df = pd.read_html(str(table))[0]

            # Standardize column names
            df.columns = df.columns.str.strip()
            df['Data_Type'] = data_type
            df['Download_Date'] = datetime.now().strftime('%Y-%m-%d')

            return df

        except Exception as e:
            print(f"Error downloading {data_type} data: {e}")
            return None

    @staticmethod
    def clean_and_transform_cpi_data(df):
        """Clean and transform the raw CPI data"""
        print("Cleaning and transforming CPI data...")

        # Basic cleaning
        df = df.dropna(how='all')
        df = df.rename(columns=lambda x: x.strip().replace(' ', '_'))

        # Standardize column names
        column_mapping = {
            'Series_ID': 'data_series',
            'Year': 'year',
            'Period': 'period',
            'Value': 'cpi_value',
            'Income_Group': 'income_group'
        }

        for old, new in column_mapping.items():
            if old in df.columns:
                df = df.rename(columns={old: new})

        # Convert data types
        if 'year' in df.columns:
            df['year'] = df['year'].astype(int)
        if 'cpi_value' in df.columns:
            df['cpi_value'] = pd.to_numeric(df['cpi_value'], errors='coerce')

        # Add period classification
        if 'year' in df.columns:
            df['period'] = df['year'].apply(lambda x: 'Post-COVID' if x >= 2020 else 'Pre-COVID')

        return df

    def save_data(self, df, filename):
        """Save processed data to CSV"""
        output_path = os.path.join(self.output_dir, filename)
        df.to_csv(output_path, index=False)
        print(f"Data saved to {output_path}")
        return output_path

    def run(self):
        """Main execution method for the crawler"""
        print("Starting Enhanced CPI Data Crawler...")

        # Step 1: Scrape news site for economic context
        news_data = self.scrape_news_site()

        # Step 2: Identify CPI data sources
        cpi_data_sources = self.fetch_cpi_data_sources()
        if not cpi_data_sources:
            print("No CPI data sources found. Exiting.")
            return

        # Step 3: Process CPI data (first 2 sources for demo)
        processed_data = []
        for source in cpi_data_sources[:2]:
            if source['url']:
                df = self.download_cpi_data(source['url'], source['title'])
                if df is not None:
                    cleaned_df = self.clean_and_transform_cpi_data(df)
                    filename = f"cpi_{source['title'].lower().replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.csv"
                    output_path = self.save_data(cleaned_df, filename)
                    processed_data.append(output_path)

        # Combine all processed CPI data
        if processed_data:
            combined_df = pd.concat([pd.read_csv(f) for f in processed_data], ignore_index=True)
            combined_path = os.path.join(self.output_dir, "combined_cpi_analysis.csv")
            combined_df.to_csv(combined_path, index=False)
            print(f"\nCombined CPI data saved to: {combined_path}")

            # Display sample of the final data
            print("\nSample of processed CPI data:")
            print(combined_df.head())

        print("\nEnhanced CPI Data Crawler completed successfully.")


if __name__ == "__main__":
    crawler = EnhancedCPIDataCrawler()
    crawler.run()