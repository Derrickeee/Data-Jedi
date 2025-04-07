import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import csv
import os
import psycopg2
from psycopg2 import sql

conn = None

class DatabaseManagerApp:
    def __init__(self, root):

        self.root = root
        self.root.title("Database Management Tool")
        self.root.geometry("800x600")

        # Connection parameters
        self.conn_params = {
            'host': 'localhost',
            'port': '5432',
            'user': 'postgres',
            'password': 'admin',
            'database': 'postgres'  # Default connection database
        }
        self.conn = None

        # Initialize instance attributes
        self.db_name_entry = None
        self.connection_status = None
        self.csv_dir_entry = None
        self.status_text = None

        # Create main frames
        self.create_connection_frame()
        self.create_operations_frame()
        self.create_status_frame()

    def create_connection_frame(self):
        frame = ttk.LabelFrame(self.root, text="Database Connection", padding=10)
        frame.pack(fill=tk.X, padx=10, pady=5)

        # Database name input
        ttk.Label(frame, text="Database Name:").grid(row=0, column=0, sticky=tk.W)
        self.db_name_entry = ttk.Entry(frame, width=30)
        self.db_name_entry.grid(row=0, column=1, padx=5, pady=5)
        self.db_name_entry.insert(0, "inflation_analysis")  # Default name

        # Connection test button
        ttk.Button(frame, text="Test Connection", command=self.test_connection).grid(row=0, column=2, padx=5)

        # Connection status
        self.connection_status = ttk.Label(frame, text="Not connected", foreground="red")
        self.connection_status.grid(row=1, column=0, columnspan=3, sticky=tk.W)

    def create_operations_frame(self):
        frame = ttk.LabelFrame(self.root, text="Database Operations", padding=10)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        # Operation buttons
        ttk.Button(frame, text="Create Database", command=self.create_database).pack(fill=tk.X, pady=5)
        ttk.Button(frame, text="Create Tables", command=self.create_tables).pack(fill=tk.X, pady=5)

        # CSV import section
        csv_frame = ttk.Frame(frame)
        csv_frame.pack(fill=tk.X, pady=10)

        ttk.Label(csv_frame, text="CSV Directory:").pack(side=tk.LEFT)
        self.csv_dir_entry = ttk.Entry(csv_frame, width=40)
        self.csv_dir_entry.pack(side=tk.LEFT, padx=5, expand=True, fill=tk.X)
        ttk.Button(csv_frame, text="Browse", command=self.browse_csv_dir).pack(side=tk.LEFT)

        ttk.Button(frame, text="Import CSV Data", command=self.import_csv_data).pack(fill=tk.X, pady=5)

    def create_status_frame(self):
        frame = ttk.LabelFrame(self.root, text="Operation Status", padding=10)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        self.status_text = tk.Text(frame, wrap=tk.WORD, height=10)
        self.status_text.pack(fill=tk.BOTH, expand=True)

        # Add scrollbar
        scrollbar = ttk.Scrollbar(frame, orient=tk.VERTICAL, command=self.status_text.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.status_text.config(yscrollcommand=scrollbar.set)

        # Make text widget read-only
        self.status_text.config(state=tk.DISABLED)

    def test_connection(self):
        global conn
        try:
            conn = psycopg2.connect(**self.conn_params)
            conn.close()
            self.connection_status.config(text="Connection successful", foreground="green")
            self.log_status("Connection to PostgreSQL server successful.")
        except psycopg2.Error as e:
            self.connection_status.config(text="Connection failed", foreground="red")
            self.log_status(f"Connection failed: {e}")
            messagebox.showerror("Connection Error", f"Failed to connect to PostgreSQL: {e}")

    def create_database(self):
        global conn
        db_name = self.db_name_entry.get().strip()
        if not db_name:
            messagebox.showwarning("Input Error", "Please enter a database name")
            return

        try:
            # Connect to default PostgreSQL database
            conn = psycopg2.connect(**self.conn_params)
            conn.autocommit = True
            cursor = conn.cursor()

            # Check if database exists
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
            exists = cursor.fetchone()

            if exists:
                if messagebox.askyesno("Database Exists",
                                       f"Database '{db_name}' already exists. Drop and recreate it?"):
                    cursor.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(db_name)))
                    self.log_status(f"Dropped existing database '{db_name}'")
                else:
                    self.log_status("Database creation cancelled")
                    return

            # Create new database
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
            self.log_status(f"Database '{db_name}' created successfully")

            # Update connection params to use new database
            self.conn_params['database'] = db_name

        except psycopg2.Error as e:
            self.log_status(f"Error creating database: {e}")
            messagebox.showerror("Database Error", f"Error creating database: {e}")
        finally:
            if 'conn' in locals() and conn:
                conn.close()

    def create_tables(self):
        global conn
        db_name = self.db_name_entry.get().strip()
        if not db_name:
            messagebox.showwarning("Input Error", "Please enter a database name")
            return

        try:
            # Connect to the specified database
            conn_params = self.conn_params.copy()
            conn_params['database'] = db_name
            conn = psycopg2.connect(**conn_params)
            conn.autocommit = True
            cursor = conn.cursor()

            # Table creation queries (same as in your original script)
            table_creation_queries = [
                """
                CREATE TABLE sector (
                    sector_id INT PRIMARY KEY,
                    sector_name VARCHAR(50) NOT NULL
                )
                """,
                """
                CREATE TABLE year (
                    year INT PRIMARY KEY
                )
                """,
                """
                CREATE TABLE transport (
                    transport_id INT PRIMARY KEY,
                    transport_cat VARCHAR(50) NOT NULL,
                    year INT NOT NULL REFERENCES year(year),
                    sector_id INT NOT NULL REFERENCES sector(sector_id)
                )
                """,
                """
                CREATE TABLE food (
                    food_id INT PRIMARY KEY,
                    food_item VARCHAR(50) NOT NULL,
                    year INT NOT NULL REFERENCES year(year),
                    sector_id INT NOT NULL REFERENCES sector(sector_id)
                )
                """,
                """
                CREATE TABLE utility (
                    utility_id INT PRIMARY KEY,
                    utility_cat VARCHAR(50) NOT NULL,
                    year INT NOT NULL REFERENCES year(year),
                    sector_id INT NOT NULL REFERENCES sector(sector_id)
                )
                """,
                """
                CREATE TABLE gdp (
                    gdp_id INT PRIMARY KEY,
                    year INT NOT NULL REFERENCES year(year),
                    gdp_value DECIMAL(10,2) NOT NULL
                )
                """,
                """
                CREATE TABLE cpi (
                    cpi_id INT PRIMARY KEY,
                    cpi_value DECIMAL(10,3),
                    sector_id INT NOT NULL REFERENCES sector(sector_id),
                    year INT NOT NULL REFERENCES year(year),
                    base_year INT,
                    period_category VARCHAR(20)
                )
                """,
                """
                CREATE TABLE inflation (
                    infla_id INT PRIMARY KEY,
                    inflation_value DECIMAL(10,4) NOT NULL,
                    year INT NOT NULL REFERENCES year(year)
                )
                """
            ]

            for query in table_creation_queries:
                try:
                    cursor.execute(query)
                    self.log_status(f"Executed: {query.split()[2]} table creation")
                except psycopg2.Error as e:
                    self.log_status(f"Error creating table: {e}")
                    continue

            self.log_status("All tables created successfully")

        except psycopg2.Error as e:
            self.log_status(f"Error creating tables: {e}")
            messagebox.showerror("Table Error", f"Error creating tables: {e}")
        finally:
            if 'conn' in locals() and conn:
                conn.close()

    def browse_csv_dir(self):
        directory = filedialog.askdirectory(title="Select CSV Directory")
        if directory:
            self.csv_dir_entry.delete(0, tk.END)
            self.csv_dir_entry.insert(0, directory)

    def import_csv_data(self):
        global conn
        db_name = self.db_name_entry.get().strip()
        if not db_name:
            messagebox.showwarning("Input Error", "Please enter a database name")
            return

        csv_dir = self.csv_dir_entry.get().strip()
        if not csv_dir:
            messagebox.showwarning("Input Error", "Please select a CSV directory")
            return

        # Mapping of CSV files to tables and their columns (same as in your original script)
        table_config = {
            'sector.csv': {
                'table': 'sector',
                'columns': ['sector_id', 'sector_name']
            },
            'year.csv': {
                'table': 'year',
                'columns': ['year']
            },
            'transport.csv': {
                'table': 'transport',
                'columns': ['transport_id', 'transport_cat', 'year', 'sector_id']
            },
            'food.csv': {
                'table': 'food',
                'columns': ['food_id', 'food_item', 'year', 'sector_id']
            },
            'utility.csv': {
                'table': 'utility',
                'columns': ['utility_id', 'utility_cat', 'year', 'sector_id']
            },
            'gdp.csv': {
                'table': 'gdp',
                'columns': ['gdp_id', 'year', 'gdp_value']
            },
            'cpi.csv': {
                'table': 'cpi',
                'columns': ['cpi_id', 'cpi_value', 'sector_id', 'year', 'base_year', 'period_category']
            },
            'inflation.csv': {
                'table': 'inflation',
                'columns': ['infla_id', 'inflation_value', 'year']
            }
        }

        try:
            # Connect to the specified database
            conn_params = self.conn_params.copy()
            conn_params['database'] = db_name
            conn = psycopg2.connect(**conn_params)
            cursor = conn.cursor()

            # Process each CSV file
            for csv_file, config in table_config.items():
                csv_path = os.path.join(csv_dir, csv_file)

                if not os.path.exists(csv_path):
                    self.log_status(f"Warning: CSV file not found - {csv_path}")
                    continue

                self.log_status(f"Importing data from {csv_file} to {config['table']} table...")

                with open(csv_path, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    next(reader)  # Skip header row

                    # Prepare the INSERT statement
                    insert_query = sql.SQL("""
                        INSERT INTO {} ({})
                        VALUES ({})
                    """).format(
                        sql.Identifier(config['table']),
                        sql.SQL(', ').join(map(sql.Identifier, config['columns'])),
                        sql.SQL(', ').join([sql.Placeholder()] * len(config['columns']))
                    )

                    # Insert each row
                    for row in reader:
                        try:
                            # Convert empty strings to None
                            row = [None if x == '' else x for x in row]
                            cursor.execute(insert_query, row)
                        except psycopg2.Error as e:
                            conn.rollback()
                            self.log_status(f"Error inserting row {row}: {e}")
                            continue

                    conn.commit()
                    self.log_status(f"Successfully imported data to {config['table']} table")

            self.log_status("\nAll data imported successfully!")

        except psycopg2.Error as e:
            self.log_status(f"Database error: {e}")
            messagebox.showerror("Database Error", f"Error importing data: {e}")
        except Exception as e:
            self.log_status(f"Error: {e}")
            messagebox.showerror("Error", f"Unexpected error: {e}")
        finally:
            if 'conn' in locals() and conn:
                conn.close()

    def log_status(self, message):
        self.status_text.config(state=tk.NORMAL)
        self.status_text.insert(tk.END, message + "\n")
        self.status_text.see(tk.END)
        self.status_text.config(state=tk.DISABLED)


if __name__ == "__main__":
    root = tk.Tk()
    app = DatabaseManagerApp(root)
    root.mainloop()