import csv
import os

# %%
import psycopg2
from psycopg2 import sql

conn = None

def create_database():
    # Connection parameters for the default 'postgres' database
    global conn
    conn_params = {
        'host': 'localhost',
        'port': '5432',
        'user': 'postgres',
        'password': 'admin',
        'database': 'postgres'  # Connect to default DB to create new DB
    }
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = True  # Required for database creation
        cursor = conn.cursor()
        
        # Create database if it doesn't exist
        cursor.execute(sql.SQL("CREATE DATABASE inflation_analysis"))
        print("Database 'inflation_analysis' created successfully")
        
    except psycopg2.Error as e:
        print(f"Error creating database: {e}")
    finally:
        if conn:
            conn.close()


def create_tables():
    global conn
    # Connection parameters for the new database
    conn_params = {
        'host': 'localhost',
        'port': '5432',
        'user': 'postgres',
        'password': 'admin',
        'database': 'inflation_analysis'
    }
    
    try:
        # Connect to the new database
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Create tables
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
            cursor.execute(query)
        
        print("All tables created successfully in 'inflation_analysis' database")
        
    except psycopg2.Error as e:
        print(f"Error creating tables: {e}")
    finally:
        if conn:
            conn.close()


def import_csv_data():
    global conn
    # Connection parameters
    conn_params = {
        'host': 'localhost',
        'port': '5432',
        'user': 'postgres',
        'password': 'admin',
        'database': 'inflation_analysis'
    }
    
    # Directory containing CSV files
    csv_dir = os.path.dirname(os.path.abspath(__file__))
    
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
        # Connect to the database
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        # Process each CSV file
        for csv_file, config in table_config.items():
            csv_path = os.path.join(csv_dir, csv_file)

            if not os.path.exists(csv_path):
                print(f"Warning: CSV file not found - {csv_path}")
                continue
            
            print(f"Importing data from {csv_file} to {config['table']} table...")
            
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
                        print(f"Error inserting row {row}: {e}")
                        continue
                
                conn.commit()
                print(f"Successfully imported data to {config['table']} table")
        
        print("\nAll data imported successfully!")
        
    except psycopg2.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    # First make sure you have:
    # 1. Created the database (using your create_database() function)
    # 2. Created the tables (using create_tables() function)
    # 3. Have CSV files in a 'data' directory
    create_database()
    create_tables()
    import_csv_data()