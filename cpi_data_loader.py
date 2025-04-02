#!/usr/bin/env python3
"""
CPI Data Loader - A script to load CPI data into a PostgreSQL database.
"""

import os
import pandas as pd
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Database configuration
DATABASE_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'user': 'postgres',
    'password': 'admin',
    'default_db': 'postgres',
    'target_db': 'cpi_analysis'
}

# File configuration
CPI_DATA_FILE = "combined_cpi_analysis1.csv"

# SQLAlchemy Base for ORM
Base = declarative_base()


class CPIData(Base):
    """CPI Data ORM model"""
    __tablename__ = 'cpi_data'

    id = Column(Integer, primary_key=True)
    data_series = Column(String(255), nullable=False)
    year = Column(Integer, nullable=False)
    cpi_value = Column(Float, nullable=False)
    period = Column(String(50), nullable=False)
    income_group = Column(String(50), nullable=False)
    created_at = Column(Date, default='now()')
    updated_at = Column(Date, onupdate='now()')


def find_data_file(file_name):
    """Search for the data file in all directories"""
    for root, dirs, files in os.walk('/'):
        if file_name in files:
            return os.path.join(root, file_name)
    return None


def create_database():
    """Create the target database if it doesn't exist"""
    try:
        conn = psycopg2.connect(
            host=DATABASE_CONFIG['host'],
            port=DATABASE_CONFIG['port'],
            user=DATABASE_CONFIG['user'],
            password=DATABASE_CONFIG['password'],
            database=DATABASE_CONFIG['default_db']
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Check if database exists
        cursor.execute(
            sql.SQL("SELECT 1 FROM pg_database WHERE datname = {}")
            .format(sql.Literal(DATABASE_CONFIG['target_db']))
        )

        if not cursor.fetchone():
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(
                sql.Identifier(DATABASE_CONFIG['target_db'])))
            print(f"Database '{DATABASE_CONFIG['target_db']}' created successfully")
        else:
            print(f"Database '{DATABASE_CONFIG['target_db']}' already exists")

    except psycopg2.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()


def create_tables():
    """Create database tables using SQLAlchemy"""
    try:
        engine = create_engine(
            f"postgresql://{DATABASE_CONFIG['user']}:{DATABASE_CONFIG['password']}@"
            f"{DATABASE_CONFIG['host']}:{DATABASE_CONFIG['port']}/{DATABASE_CONFIG['target_db']}"
        )
        Base.metadata.create_all(engine)
        print("Tables created successfully")
    except Exception as e:
        print(f"Error creating tables: {e}")


def load_and_clean_data(file_path):
    """Load and clean the CPI data from CSV"""
    try:
        df = pd.read_csv(file_path)

        # Clean and rename columns
        df = df.rename(columns={
            'DataSeries': 'data_series',
            'Year': 'year',
            'CPI': 'cpi_value',
            'Period': 'period',
            'Income_Group': 'income_group'
        })

        # Handle missing values and data types
        df = df.dropna(subset=['data_series', 'year', 'cpi_value'])
        df['year'] = df['year'].astype(int)
        df['cpi_value'] = df['cpi_value'].astype(float)

        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        return None


def insert_data():
    """Insert data into the database"""
    try:
        # Initialize database connection
        engine = create_engine(
            f"postgresql://{DATABASE_CONFIG['user']}:{DATABASE_CONFIG['password']}@"
            f"{DATABASE_CONFIG['host']}:{DATABASE_CONFIG['port']}/{DATABASE_CONFIG['target_db']}"
        )
        Session = sessionmaker(bind=engine)
        session = Session()

        # Find and load data file
        file_path = find_data_file(CPI_DATA_FILE)
        if not file_path:
            print(f"Error: Could not find data file '{CPI_DATA_FILE}'")
            return

        df = load_and_clean_data(file_path)
        if df is None:
            return

        # Check for existing data to avoid duplicates
        existing = session.query(CPIData.data_series, CPIData.year, CPIData.income_group).all()
        existing = set((d.data_series, d.year, d.income_group) for d in existing)

        # Prepare batch insert
        batch = []
        for _, row in df.iterrows():
            key = (row['data_series'], row['year'], row['income_group'])
            if key not in existing:
                batch.append(CPIData(
                    data_series=row['data_series'],
                    year=row['year'],
                    cpi_value=row['cpi_value'],
                    period=row['period'],
                    income_group=row['income_group']
                ))

        # Insert in batches
        if batch:
            session.bulk_save_objects(batch)
            session.commit()
            print(f"Successfully inserted {len(batch)} records")
        else:
            print("No new records to insert")

    except Exception as e:
        print(f"Error inserting data: {e}")
        session.rollback()
    finally:
        session.close()


def main():
    """Main execution function"""
    print("CPI Data Loader - Starting process")

    # Step 1: Create database if needed
    create_database()

    # Step 2: Create tables
    create_tables()

    # Step 3: Insert data
    insert_data()

    print("CPI Data Loader - Process completed")


if __name__ == "__main__":
    main()