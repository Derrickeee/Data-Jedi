#!/usr/bin/env python3
"""
CPI Data Crawler GUI
"""
import logging
import os
import platform
import subprocess
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import web_crawler
from database import DatabaseManager

# --- Configuration ---
OUTPUT_DIR = "cpi_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)


class CPIApp:
    def __init__(self, master):
        self.table_id_validation_label = None
        self.table_id_var = None
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
        self.crawler = web_crawler.CPIDataCrawler()
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

    @staticmethod
    def validate_table_id(table_id):
        """Validate the SingStat table ID format (M followed by 6 digits)"""
        if len(table_id) != 7:
            return False
        if table_id[0].upper() != 'M':
            return False
        if not table_id[1:].isdigit():
            return False
        return True

    def on_table_id_change(self):
        """Handle changes to the table ID entry"""
        table_id = self.table_id_var.get()
        if table_id:  # Only validate if there's input
            if not self.validate_table_id(table_id):
                self.table_id_entry.config(foreground='red')
            else:
                self.table_id_entry.config(foreground='black')

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
        # SingStat frame
        singstat_frame = ttk.Frame(crawler_notebook)
        crawler_notebook.add(singstat_frame, text='SingStat')
        # Data.gov.sg frame
        data_gov_frame = ttk.Frame(crawler_notebook)
        crawler_notebook.add(data_gov_frame, text='Data.gov.sg')
        # SingStat configuration
        ttk.Label(singstat_frame, text="SingStat Configuration",
                  font=('Helvetica', 12, 'bold')).pack(pady=10)
        ttk.Checkbutton(singstat_frame, text="Enable SingStat",
                        variable=self.singstat_enabled).pack(anchor='w', padx=10)
        # API Configuration
        api_frame = ttk.LabelFrame(singstat_frame, text="API Configuration")
        api_frame.pack(padx=10, pady=10, fill='x')
        ttk.Label(api_frame, text="Table ID:").pack(anchor='w', padx=10, pady=(10, 0))
        # Use a StringVar to track changes
        self.table_id_var = tk.StringVar()
        self.table_id_var.trace('w', self.on_table_id_change)
        self.table_id_entry = ttk.Entry(api_frame, width=30)
        self.table_id_entry.pack(anchor='w', padx=10, pady=(0, 10))
        # Add validation label
        self.table_id_validation_label = ttk.Label(api_frame, text="Format: M followed by 6 digits", foreground='gray')
        self.table_id_validation_label.pack(anchor='w', padx=10, pady=(0, 5))
        # Run button
        ttk.Button(self.crawler_frame, text="Run Crawler", command=self.run_crawler).pack(pady=20)
        # Data.gov.sg configuration
        ttk.Label(data_gov_frame, text="Data.gov.sg Configuration",
                  font=('Helvetica', 12, 'bold')).pack(pady=10)
        ttk.Checkbutton(data_gov_frame, text="Enable Data.gov.sg",
                        variable=self.data_gov_enabled).pack(anchor='w', padx=10)
        ttk.Label(data_gov_frame, text="Dataset ID:").pack(anchor='w', padx=10, pady=(10, 0))
        self.dataset_id_entry = ttk.Entry(data_gov_frame, width=50)
        self.dataset_id_entry.pack(anchor='w', padx=10, pady=(0, 10))
        self.dataset_id_entry.insert(0, "d_c5bde9ed17cef8c365629311f8550ce2")

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
        # Create a frame to hold both text widget and scrollbar
        text_frame = ttk.Frame(status_frame)
        text_frame.pack(fill=tk.BOTH, expand=True)
        self.status_text = tk.Text(text_frame, wrap=tk.WORD, height=10)
        self.status_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        # Add scrollbar - now properly placed next to the text widget
        scrollbar = ttk.Scrollbar(text_frame, orient=tk.VERTICAL, command=self.status_text.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.status_text.config(yscrollcommand=scrollbar.set)
        # Make text widget read-only
        self.status_text.config(state=tk.DISABLED)

    def run_crawler(self):
        """Execute the crawler based on user configuration"""
        # Validate at least one source is enabled
        if not self.data_gov_enabled.get() and not self.singstat_enabled.get():
            messagebox.showerror("Configuration Error",
                                 "Please enable at least one data source (Data.gov.sg or SingStat)")
            self.status_var.set("No data sources enabled")
            return
        # Validate SingStat API configuration if enabled
        if self.singstat_enabled.get() and self.singstat_mode.get() == "api":
            table_id = self.table_id_entry.get().strip()
            if not self.validate_table_id(table_id):
                messagebox.showerror("Invalid Table ID",
                                     "Table ID must be in format M###### (M followed by 6 digits)")
                self.status_var.set("Invalid Table ID format")
                return
        # Validate Data.gov.sg configuration if enabled
        if self.data_gov_enabled.get():
            dataset_id = self.dataset_id_entry.get().strip()
            if not dataset_id:
                messagebox.showerror("Configuration Error",
                                     "Please enter a Dataset ID for Data.gov.sg")
                self.status_var.set("Missing Dataset ID")
                return
        self.status_var.set("Running crawler...")
        self.root.update()
        try:
            # Update crawler source activation based on GUI settings
            self.crawler.sources['sg_gov']['active'] = self.data_gov_enabled.get()
            self.crawler.sources['sg_singstat']['active'] = self.singstat_enabled.get()
            # Get parameters
            sg_dataset_id = self.dataset_id_entry.get().strip() if self.data_gov_enabled.get() else None
            # Get the list of existing files BEFORE the operation
            existing_files = set()
            if os.path.exists(OUTPUT_DIR):
                existing_files = set(os.listdir(OUTPUT_DIR))
            if self.singstat_enabled.get():
                if self.singstat_mode.get() == "api":
                    singstat_table_id = self.table_id_entry.get().strip()
                else:
                    singstat_table_id = None
            else:
                singstat_table_id = None
            # Run crawler
            self.crawler.run(
                sg_dataset_id=sg_dataset_id,
                singstat_table_id=singstat_table_id
            )
            # After the operation, check for new files
            current_files = set()
            if os.path.exists(OUTPUT_DIR):
                current_files = set(os.listdir(OUTPUT_DIR))
            new_files = current_files - existing_files
            if new_files:
                # Filter out directories and only keep files
                new_files = [f for f in new_files if os.path.isfile(os.path.join(OUTPUT_DIR, f))]
                if new_files:
                    # Create a message showing all new files
                    if len(new_files) == 1:
                        file_message = f"New dataset added: {new_files[0]}"
                    else:
                        file_message = f"{len(new_files)} new datasets added:\n" + "\n".join(new_files)
                    self.status_var.set("Crawler completed successfully")
                    messagebox.showinfo("Success", file_message)
                else:
                    self.status_var.set("No new dataset was added")
                    messagebox.showwarning("No Update", "No new dataset was generated.")
            else:
                self.status_var.set("No new dataset was added")
                messagebox.showwarning("No Update", "No new dataset was generated.")
        except Exception as e:
            self.status_var.set(f"Error: {str(e)}")
            messagebox.showerror("Error", f"An error occurred: {str(e)}")

    @staticmethod
    def open_output_folder():
        """Open the output folder in the system file explorer"""
        path = os.path.abspath(web_crawler.OUTPUT_DIR)
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
            # First check if database already exists
            if self.db_manager.database_exists(db_name):
                response = messagebox.askyesno(
                    "Database Exists",
                    f"Database '{db_name}' already exists. Do you want to recreate it?\n"
                    "Warning: This will DROP the existing database and all its data!"
                )
                if not response:
                    self.log_status(f"Database creation cancelled - '{db_name}' already exists")
                    return
                # Drop existing database if user confirms
                self.db_manager.drop_database(db_name, self.log_status)
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
            # First check if database exists
            if not self.db_manager.database_exists(db_name):
                response = messagebox.askyesno(
                    "Database Missing",
                    f"Database '{db_name}' doesn't exist. Create it first?"
                )
                if response:
                    self.create_database()  # This will handle the creation
                else:
                    self.log_status("Table creation cancelled - database doesn't exist")
                    return
            # Check if tables already exist
            if self.db_manager.tables_exist(db_name):
                response = messagebox.askyesno(
                    "Tables Exist",
                    "Tables already exist in this database. Recreate them?\n"
                    "Warning: This will DROP existing tables and all their data!"
                )
                if not response:
                    self.log_status("Table creation cancelled - tables already exist")
                    return
                # Drop existing tables if user confirms
                self.db_manager.drop_tables(db_name, self.log_status)
            # Create the tables
            self.db_manager.create_tables(db_name, self.log_status)
            messagebox.showinfo("Success", "Tables created successfully")
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
