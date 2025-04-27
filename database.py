import psycopg2
from psycopg2 import sql
import csv
import os


class DatabaseManager:
    def __init__(self):
        # Connection parameters
        self.conn_params = {
            'host': 'localhost',
            'port': '5432',
            'user': 'postgres',
            'password': 'admin',
            'database': 'postgres'  # Default connection database
        }
        self.conn = None

    def test_connection(self):
        try:
            self.conn = psycopg2.connect(**self.conn_params)
            self.conn.close()
        except psycopg2.Error as e:
            raise Exception(f"Connection failed: {e}")

    def create_database(self, db_name, log_callback):
        try:
            # Connect to default PostgreSQL database
            self.conn = psycopg2.connect(**self.conn_params)
            self.conn.autocommit = True
            cursor = self.conn.cursor()

            # Check if database exists
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
            exists = cursor.fetchone()

            if exists:
                raise Exception(f"Database '{db_name}' already exists")

            # Create new database
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
            log_callback(f"Database '{db_name}' created successfully")

            # Update connection params to use new database
            self.conn_params['database'] = db_name

        except psycopg2.Error as e:
            raise Exception(f"Error creating database: {e}")
        finally:
            if self.conn:
                self.conn.close()

    def create_tables(self, db_name, log_callback):
        try:
            # Connect to the specified database
            conn_params = self.conn_params.copy()
            conn_params['database'] = db_name
            self.conn = psycopg2.connect(**conn_params)
            self.conn.autocommit = True
            cursor = self.conn.cursor()

            # Table creation queries
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
                    log_callback(f"Executed: {query.split()[2]} table creation")
                except psycopg2.Error as e:
                    log_callback(f"Error creating table: {e}")
                    continue

            log_callback("All tables created successfully")

        except psycopg2.Error as e:
            raise Exception(f"Error creating tables: {e}")
        finally:
            if self.conn:
                self.conn.close()

    def import_csv_data(self, db_name, csv_dir, log_callback):
        # Mapping of CSV files to tables and their columns
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
            self.conn = psycopg2.connect(**conn_params)
            cursor = self.conn.cursor()
            success_count = 0
            total_tables = len(table_config)
            # Process each CSV file
            for csv_file, config in table_config.items():
                csv_path = os.path.join(csv_dir, csv_file)
                if not os.path.exists(csv_path):
                    log_callback(f"Warning: CSV file not found - {csv_path}")
                    continue
                log_callback(f"Importing data from {csv_file} to {config['table']} table...")
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
                    rows_imported = 0
                    for row in reader:
                        try:
                            # Convert empty strings to None
                            row = [None if x == '' else x for x in row]
                            cursor.execute(insert_query, row)
                            rows_imported += 1
                        except psycopg2.Error as e:
                            self.conn.rollback()
                            log_callback(f"Error inserting row {row}: {e}")
                            continue
                    self.conn.commit()
                    if rows_imported > 0:
                        log_callback(f"Successfully imported data to {config['table']} table")
                        success_count += 1
                    else:
                        log_callback(
                            f"\nImport completed with {success_count} out of {total_tables} tables successfully imported")
        except psycopg2.Error as e:
            raise Exception(f"Database error: {e}")
        finally:
            if self.conn:
                self.conn.close()

    def database_exists(self, db_name):
        """Check if a database exists"""
        try:
            # Connect to postgres database to check
            conn = psycopg2.connect(
                host=self.conn_params['host'],
                user=self.conn_params['user'],
                password=self.conn_params['password'],
                dbname='postgres'  # Connect to default DB to check for others
            )
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
            exists = cur.fetchone() is not None
            cur.close()
            conn.close()
            return exists
        except Exception as e:
            raise Exception(f"Error checking database existence: {str(e)}")

    def drop_database(self, db_name, log_callback):
        """Drop an existing database"""
        try:
            # Connect to postgres database to perform drop
            conn = psycopg2.connect(
                host=self.conn_params['host'],
                user=self.conn_params['user'],
                password=self.conn_params['password'],
                dbname='postgres'  # Connect to default DB to drop others
            )
            conn.autocommit = True
            cur = conn.cursor()
            # Terminate all connections to the target database first
            cur.execute(f"""
                   SELECT pg_terminate_backend(pg_stat_activity.pid)
                   FROM pg_stat_activity
                   WHERE pg_stat_activity.datname = '{db_name}'
                   AND pid <> pg_backend_pid();
               """)
            # Now drop the database
            cur.execute(f"DROP DATABASE IF EXISTS {db_name}")
            if log_callback:
                log_callback(f"Dropped database '{db_name}'")
            cur.close()
            conn.close()
        except Exception as e:
            raise Exception(f"Error dropping database: {str(e)}")

    def tables_exist(self, db_name):
        """Check if tables already exist in the database"""
        try:
            # Connect to the specified database
            conn = psycopg2.connect(
                host=self.conn_params['host'],
                user=self.conn_params['user'],
                password=self.conn_params['password'],
                dbname=db_name
            )
            cur = conn.cursor()
            # Check for existence of key tables (adjust according to your schema)
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'cpi_data'
                )
            """)
            exists = cur.fetchone()[0]
            cur.close()
            conn.close()
            return exists
        except Exception as e:
            raise Exception(f"Error checking table existence: {str(e)}")

    def drop_tables(self, db_name, log_callback):
        """Drop all tables in the database"""
        try:
            # Connect to the specified database
            conn = psycopg2.connect(
                host=self.conn_params['host'],
                user=self.conn_params['user'],
                password=self.conn_params['password'],
                dbname=db_name
            )
            conn.autocommit = True
            cur = conn.cursor()
            # Get all tables in the database
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
            tables = [row[0] for row in cur.fetchall()]
            if tables:
                # Drop all tables with CASCADE to handle dependencies
                for table in tables:
                    cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
                    if log_callback:
                        log_callback(f"Dropped table: {table}")
            cur.close()
            conn.close()
        except Exception as e:
            raise Exception(f"Error dropping tables: {str(e)}")
