#!/usr/bin/env python3
"""
CPI Data Crawler GUI - Provides a user interface for the CPI data crawler
"""
import tkinter as tk
from tkinter import ttk, messagebox
from web_crawler2 import CPIDataCrawler


class CPICrawlerGUI:
    def __init__(self, root):
        # Create notebook for tabs
        self.notebook = ttk.Notebook(root)
        self.notebook.pack(pady=10, padx=10, fill='both', expand=True)
        # Create frames for each tab
        self.main_frame = ttk.Frame(self.notebook)
        self.data_gov_frame = ttk.Frame(self.notebook)
        self.singstat_frame = ttk.Frame(self.notebook)
        self.scrape_frame = ttk.LabelFrame(self.singstat_frame, text="Web Scraping Configuration")
        self.api_frame = ttk.LabelFrame(self.singstat_frame, text="API Configuration")
        self.notebook.add(self.main_frame, text='Main')
        self.notebook.add(self.data_gov_frame, text='Data.gov.sg')
        self.notebook.add(self.singstat_frame, text='SingStat')


        self.table_url_entry = ttk.Entry(self.scrape_frame, width=50)
        self.time_filter_entry = ttk.Entry(self.api_frame, width=30)
        self.series_filter_entry = ttk.Entry(self.api_frame, width=30)
        self.table_id_entry = ttk.Entry(self.api_frame, width=30)

        self.singstat_mode = tk.StringVar(value="api")
        self.singstat_enabled = tk.BooleanVar(value=True)
        self.dataset_id_entry = ttk.Entry(self.data_gov_frame, width=50)
        self.data_gov_enabled = tk.BooleanVar(value=True)
        self.root = root
        self.root.title("Singapore CPI Data Crawler")
        self.root.geometry("600x500")

        self.crawler = CPIDataCrawler()



        # Initialize tabs
        self.create_main_tab()
        self.create_data_gov_tab()
        self.create_singstat_tab()

        # Status bar
        self.status_var = tk.StringVar()
        self.status_var.set("Ready")
        self.status_bar = ttk.Label(root, textvariable=self.status_var, relief='sunken')
        self.status_bar.pack(side='bottom', fill='x')

    def create_main_tab(self):
        """Create the main tab with general controls"""
        # Title
        ttk.Label(self.main_frame, text="Singapore CPI Data Crawler",
                  font=('Helvetica', 14, 'bold')).pack(pady=10)

        # Description
        desc = """Select a data source from the tabs above and configure the parameters.
          Then click 'Run Crawler' to collect the data."""
        ttk.Label(self.main_frame, text=desc, wraplength=500).pack(pady=10)

        # Run button
        ttk.Button(self.main_frame, text="Run Crawler", command=self.run_crawler).pack(pady=20)

        # Output directory
        ttk.Label(self.main_frame, text="Output Directory:").pack()
        ttk.Label(self.main_frame, text=self.crawler.output_dir).pack()

        # Open folder button
        ttk.Button(self.main_frame, text="Open Output Folder",
                   command=self.open_output_folder).pack(pady=10)

    def create_data_gov_tab(self):
        """Create the Data.gov.sg configuration tab"""
        # Title
        ttk.Label(self.data_gov_frame, text="Data.gov.sg Configuration",
                  font=('Helvetica', 12, 'bold')).pack(pady=10)

        # Enable checkbox
        ttk.Checkbutton(self.data_gov_frame, text="Enable Data.gov.sg",
                        variable=self.data_gov_enabled).pack(anchor='w', padx=10)

        # Dataset ID
        ttk.Label(self.data_gov_frame, text="Dataset ID:").pack(anchor='w', padx=10, pady=(10, 0))
        self.dataset_id_entry.pack(anchor='w', padx=10, pady=(0, 10))
        self.dataset_id_entry.insert(0, "d_c5bde9ed17cef8c365629311f8550ce2")

        # Example IDs
        example_frame = ttk.LabelFrame(self.data_gov_frame, text="Example Dataset IDs")
        example_frame.pack(padx=10, pady=10, fill='x')

        examples = [
            "Consumer Price Index (CPI) (Highest 20%) - d_c5bde9ed17cef8c365629311f8550ce2",
            "Household Expenditure - d_8b84c4ee58e3cfc0a5d363b7c8d7b12f"
        ]

        for example in examples:
            ttk.Label(example_frame, text=example).pack(anchor='w')

    def create_singstat_tab(self):
        """Create the SingStat configuration tab"""
        # Title
        ttk.Label(self.singstat_frame, text="SingStat Configuration",
                  font=('Helvetica', 12, 'bold')).pack(pady=10)

        # Enable checkbox
        ttk.Checkbutton(self.singstat_frame, text="Enable SingStat",
                        variable=self.singstat_enabled).pack(anchor='w', padx=10)

        # API vs Web Scraping selection
        ttk.Radiobutton(self.singstat_frame, text="API Mode",
                        variable=self.singstat_mode, value="api").pack(anchor='w', padx=10)
        ttk.Radiobutton(self.singstat_frame, text="Web Scraping Mode",
                        variable=self.singstat_mode, value="scrape").pack(anchor='w', padx=10)

        # API Configuration
        self.api_frame.pack(padx=10, pady=10, fill='x')

        ttk.Label(self.api_frame, text="Table ID:").pack(anchor='w', padx=10, pady=(10, 0))
        self.table_id_entry.pack(anchor='w', padx=10, pady=(0, 5))
        self.table_id_entry.insert(0, "M213071")

        # ttk.Label(self.api_frame, text="Series Filter:").pack(anchor='w', padx=10, pady=(5, 0))
        # self.series_filter_entry.pack(anchor='w', padx=10, pady=(0, 5))
        # self.series_filter_entry.insert(0, "1.1")
        #
        # ttk.Label(self.api_frame, text="Time Filter:").pack(anchor='w', padx=10, pady=(5, 0))
        # self.time_filter_entry.pack(anchor='w', padx=10, pady=(0, 10))
        # self.time_filter_entry.insert(0, "2023 1H")

        # Web Scraping Configuration
        self.scrape_frame.pack(padx=10, pady=10, fill='x')

        ttk.Label(self.scrape_frame, text="Table URL:").pack(anchor='w', padx=10, pady=(10, 0))
        self.table_url_entry.pack(anchor='w', padx=10, pady=(0, 10))
        self.table_url_entry.insert(0, "https://tablebuilder.singstat.gov.sg/table/TS/M213071")

    def run_crawler(self):
        """Execute the crawler based on user configuration"""
        self.status_var.set("Running crawler...")
        self.root.update()

        try:
            # Update crawler source activation based on GUI settings
            self.crawler.sources['sg_gov']['active'] = self.data_gov_enabled.get()
            self.crawler.sources['sg_singstat']['active'] = self.singstat_enabled.get()

            # Get parameters
            sg_dataset_id = self.dataset_id_entry.get() if self.data_gov_enabled.get() else None

            if self.singstat_enabled.get():
                if self.singstat_mode.get() == "api":
                    singstat_table_id = self.table_id_entry.get()
                    singstat_url = None
                else:
                    singstat_table_id = None
                    singstat_url = self.table_url_entry.get()

            else:
                singstat_table_id = None
                singstat_url = None

            # Run crawler
            self.crawler.run(
                sg_dataset_ids=sg_dataset_id,
                singstat_table_ids=singstat_table_id,
                singstat_urls=singstat_url
            )

            self.status_var.set("Crawler completed successfully")
            messagebox.showinfo("Success", "Data collection completed successfully!")

        except Exception as e:
            self.status_var.set(f"Error: {str(e)}")
            messagebox.showerror("Error", f"An error occurred: {str(e)}")

    def open_output_folder(self):
        """Open the output folder in the system file explorer"""
        import os
        import platform
        import subprocess

        path = os.path.abspath(self.crawler.output_dir)

        try:
            if platform.system() == "Windows":
                os.startfile(path)
            elif platform.system() == "Darwin":
                subprocess.Popen(["open", path])
            else:
                subprocess.Popen(["xdg-open", path])
        except Exception as e:
            messagebox.showerror("Error", f"Could not open folder: {str(e)}")


if __name__ == "__main__":
    root = tk.Tk()
    app = CPICrawlerGUI(root)
    root.mainloop()