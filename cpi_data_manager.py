#!/usr/bin/env python3
"""
Combined CPI Data Crawler and Database Manager GUI
"""
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from web_crawler import CPIDataCrawler
from database import DatabaseManager

class CPIApp:
    def __init__(self, master):
        self.dataset_id_entry = None
        self.table_id_entry = None
        self.table_url_entry = None
        self.db_name_entry = None
        self.connection_status = None
        self.csv_dir_entry = None
        self.status_text = None
        self.root = master
        self.root.title("CPI Data Management System")
        self.root.geometry("900x700")

        # Initialize components
        self.crawler = CPIDataCrawler()
        self.db_manager = DatabaseManager()

        # Create notebook for tabs
        self.notebook = ttk.Notebook(master)
        self.notebook.pack(pady=10, padx=10, fill='both', expand=True)

        # Create frames for each tab
        self.main_frame = ttk.Frame(self.notebook)
        self.crawler_frame = ttk.Frame(self.notebook)
        self.database_frame = ttk.Frame(self.notebook)

        self.notebook.add(self.main_frame, text='Main')
        self.notebook.add(self.crawler_frame, text='Data Crawler')
        self.notebook.add(self.database_frame, text='Database Manager')

        # Initialize variables
        self.singstat_mode = tk.StringVar(value="api")
        self.singstat_enabled = tk.BooleanVar(value=True)
        self.data_gov_enabled = tk.BooleanVar(value=True)

        # Status bar
        self.status_var = tk.StringVar()
        self.status_var.set("Ready")
        self.status_bar = ttk.Label(master, textvariable=self.status_var, relief='sunken')
        self.status_bar.pack(side='bottom', fill='x')

        # Initialize tabs
        self.create_main_tab()
        self.create_crawler_tab()
        self.create_database_tab()

    def create_main_tab(self):
        """Create the main tab with general information"""
        # Title
        ttk.Label(self.main_frame, text="CPI Data Management System",
                  font=('Helvetica', 16, 'bold')).pack(pady=20)

        # Description
        desc = """This application combines CPI data collection and database management.
        Use the Data Crawler tab to collect CPI data from various sources.
        Use the Database Manager tab to create and manage the database."""
        ttk.Label(self.main_frame, text=desc, wraplength=700).pack(pady=20)

        # Quick actions
        ttk.Label(self.main_frame, text="Quick Actions:", font=('Helvetica', 12)).pack(pady=10)

        action_frame = ttk.Frame(self.main_frame)
        action_frame.pack()

        ttk.Button(action_frame, text="Run Data Crawler", command=self.run_crawler).grid(row=0, column=0, padx=5)
        ttk.Button(action_frame, text="Create Database", command=self.create_database).grid(row=0, column=1, padx=5)
        ttk.Button(action_frame, text="Open Output Folder", command=self.open_output_folder).grid(row=0, column=2,
                                                                                                  padx=5)

    def create_crawler_tab(self):
        """Create the Data Crawler tab"""
        # Create sub-notebook for crawler sources
        crawler_notebook = ttk.Notebook(self.crawler_frame)
        crawler_notebook.pack(fill='both', expand=True)

        # Data.gov.sg frame
        data_gov_frame = ttk.Frame(crawler_notebook)
        crawler_notebook.add(data_gov_frame, text='Data.gov.sg')

        # SingStat frame
        singstat_frame = ttk.Frame(crawler_notebook)
        crawler_notebook.add(singstat_frame, text='SingStat')

        # Data.gov.sg configuration
        ttk.Label(data_gov_frame, text="Data.gov.sg Configuration",
                  font=('Helvetica', 12, 'bold')).pack(pady=10)

        ttk.Checkbutton(data_gov_frame, text="Enable Data.gov.sg",
                        variable=self.data_gov_enabled).pack(anchor='w', padx=10)

        ttk.Label(data_gov_frame, text="Dataset ID:").pack(anchor='w', padx=10, pady=(10, 0))
        self.dataset_id_entry = ttk.Entry(data_gov_frame, width=50)
        self.dataset_id_entry.pack(anchor='w', padx=10, pady=(0, 10))
        self.dataset_id_entry.insert(0, "d_c5bde9ed17cef8c365629311f8550ce2")

        # SingStat configuration
        ttk.Label(singstat_frame, text="SingStat Configuration",
                  font=('Helvetica', 12, 'bold')).pack(pady=10)

        ttk.Checkbutton(singstat_frame, text="Enable SingStat",
                        variable=self.singstat_enabled).pack(anchor='w', padx=10)

        # API vs Web Scraping selection
        ttk.Radiobutton(singstat_frame, text="API Mode",
                        variable=self.singstat_mode, value="api").pack(anchor='w', padx=10)
        ttk.Radiobutton(singstat_frame, text="Web Scraping Mode",
                        variable=self.singstat_mode, value="scrape").pack(anchor='w', padx=10)

        # API Configuration
        api_frame = ttk.LabelFrame(singstat_frame, text="API Configuration")
        api_frame.pack(padx=10, pady=10, fill='x')

        ttk.Label(api_frame, text="Table ID:").pack(anchor='w', padx=10, pady=(10, 0))
        self.table_id_entry = ttk.Entry(api_frame, width=30)
        self.table_id_entry.pack(anchor='w', padx=10, pady=(0, 10))
        self.table_id_entry.insert(0, "M213041")

        # Web Scraping Configuration
        scrape_frame = ttk.LabelFrame(singstat_frame, text="Web Scraping Configuration")
        scrape_frame.pack(padx=10, pady=10, fill='x')

        ttk.Label(scrape_frame, text="Table URL:").pack(anchor='w', padx=10, pady=(10, 0))
        self.table_url_entry = ttk.Entry(scrape_frame, width=50)
        self.table_url_entry.pack(anchor='w', padx=10, pady=(0, 10))
        self.table_url_entry.insert(0, "https://tablebuilder.singstat.gov.sg/table/TS/M213041")

        # Run button
        ttk.Button(self.crawler_frame, text="Run Crawler", command=self.run_crawler).pack(pady=20)

    def create_database_tab(self):
        """Create the Database Manager tab"""
        # Connection Frame
        conn_frame = ttk.LabelFrame(self.database_frame, text="Database Connection", padding=10)
        conn_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(conn_frame, text="Database Name:").grid(row=0, column=0, sticky=tk.W)
        self.db_name_entry = ttk.Entry(conn_frame, width=30)
        self.db_name_entry.grid(row=0, column=1, padx=5, pady=5)
        self.db_name_entry.insert(0, "inflation_analysis")

        ttk.Button(conn_frame, text="Test Connection", command=self.test_connection).grid(row=0, column=2, padx=5)

        self.connection_status = ttk.Label(conn_frame, text="Not connected", foreground="red")
        self.connection_status.grid(row=1, column=0, columnspan=3, sticky=tk.W)

        # Operations Frame
        ops_frame = ttk.LabelFrame(self.database_frame, text="Database Operations", padding=10)
        ops_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        ttk.Button(ops_frame, text="Create Database", command=self.create_database).pack(fill=tk.X, pady=5)
        ttk.Button(ops_frame, text="Create Tables", command=self.create_tables).pack(fill=tk.X, pady=5)

        # CSV import section
        csv_frame = ttk.Frame(ops_frame)
        csv_frame.pack(fill=tk.X, pady=10)

        ttk.Label(csv_frame, text="CSV Directory:").pack(side=tk.LEFT)
        self.csv_dir_entry = ttk.Entry(csv_frame, width=40)
        self.csv_dir_entry.pack(side=tk.LEFT, padx=5, expand=True, fill=tk.X)
        ttk.Button(csv_frame, text="Browse", command=self.browse_csv_dir).pack(side=tk.LEFT)

        ttk.Button(ops_frame, text="Import CSV Data", command=self.import_csv_data).pack(fill=tk.X, pady=5)

        # Status Frame
        status_frame = ttk.LabelFrame(self.database_frame, text="Operation Status", padding=10)
        status_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        self.status_text = tk.Text(status_frame, wrap=tk.WORD, height=10)
        self.status_text.pack(fill=tk.BOTH, expand=True)

        scrollbar = ttk.Scrollbar(status_frame, orient=tk.VERTICAL, command=self.status_text.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.status_text.config(yscrollcommand=scrollbar.set)
        self.status_text.config(state=tk.DISABLED)

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

    def test_connection(self):
        try:
            self.db_manager.test_connection()
            self.connection_status.config(text="Connection successful", foreground="green")
            self.log_status("Connection to PostgreSQL server successful.")
        except Exception as e:
            self.connection_status.config(text="Connection failed", foreground="red")
            self.log_status(f"Connection failed: {e}")
            messagebox.showerror("Connection Error", f"Failed to connect to PostgreSQL: {e}")

    def create_database(self):
        db_name = self.db_name_entry.get().strip()
        if not db_name:
            messagebox.showwarning("Input Error", "Please enter a database name")
            return

        try:
            self.db_manager.create_database(db_name, self.log_status)
            # Update connection params to use new database
            self.db_manager.conn_params['database'] = db_name
        except Exception as e:
            self.log_status(f"Error creating database: {e}")
            messagebox.showerror("Database Error", f"Error creating database: {e}")

    def create_tables(self):
        db_name = self.db_name_entry.get().strip()
        if not db_name:
            messagebox.showwarning("Input Error", "Please enter a database name")
            return

        try:
            self.db_manager.create_tables(db_name, self.log_status)
        except Exception as e:
            self.log_status(f"Error creating tables: {e}")
            messagebox.showerror("Table Error", f"Error creating tables: {e}")

    def browse_csv_dir(self):
        directory = filedialog.askdirectory(title="Select CSV Directory")
        if directory:
            self.csv_dir_entry.delete(0, tk.END)
            self.csv_dir_entry.insert(0, directory)

    def import_csv_data(self):
        db_name = self.db_name_entry.get().strip()
        if not db_name:
            messagebox.showwarning("Input Error", "Please enter a database name")
            return

        csv_dir = self.csv_dir_entry.get().strip()
        if not csv_dir:
            messagebox.showwarning("Input Error", "Please select a CSV directory")
            return

        try:
            self.db_manager.import_csv_data(db_name, csv_dir, self.log_status)
        except Exception as e:
            self.log_status(f"Error: {e}")
            messagebox.showerror("Error", f"Unexpected error: {e}")

    def log_status(self, message):
        self.status_text.config(state=tk.NORMAL)
        self.status_text.insert(tk.END, message + "\n")
        self.status_text.see(tk.END)
        self.status_text.config(state=tk.DISABLED)


if __name__ == "__main__":
    root = tk.Tk()
    app = CPIApp(root)
    root.mainloop()