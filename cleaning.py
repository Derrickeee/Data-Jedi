import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy import MetaData, Table, Column, Integer, String, Numeric

def process_cpi_file(filepath, income_group):
    """Process CPI CSV file and add income group identifier"""
    # Load and clean data
    df = pd.read_csv(filepath)
    df['DataSeries'] = df['DataSeries'].str.strip()
    
    # Convert all CPI values to numeric
    for col in df.columns[1:]:  # Skip 'DataSeries' column
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Melt to long format
    df_long = df.melt(id_vars='DataSeries', var_name='Year', value_name='CPI')
    df_long['Year'] = df_long['Year'].astype(int)
    
    # Add period classification
    conditions = [
        (df_long['Year'] <= 2019),
        (df_long['Year'].between(2020, 2022)),
        (df_long['Year'] >= 2023)
    ]
    choices = ['Pre-COVID', 'COVID', 'Post-COVID']
    df_long['Period'] = np.select(conditions, choices, default='Unknown')
    
    # Add income group identifier
    df_long['Income_Group'] = income_group
    
    # Calculate inflation rates
    #df_long = df_long.sort_values(['DataSeries', 'Year'])
    #df_long['Inflation_Rate'] = df_long.groupby('DataSeries')['CPI'].pct_change() * 100
    
    return df_long


# Process files for different income groups
files = [
    ('cpi_data/CPIByHouseholdIncomeGrouphighest20_2018_2024.csv', 'Highest 20%'),
    ('cpi_data/CPIByHouseholdIncomeGroupmiddle60_2018_2024.csv', 'Middle 60%'),
    ('cpi_data/CPIByHouseholdIncomeGrouplowest20_2018_2024.csv', 'Lowest 20%')
]

# Combine all data
all_data = pd.concat([process_cpi_file(f, group) for f, group in files])

# Save combined results
all_data.to_csv('combined_cpi_analysis1.csv', index=False)

print("Processing complete. Combined data saved to 'combined_cpi_analysis.csv'")
print(f"Total rows processed: {len(all_data)}")
print("Income groups included:", all_data['Income_Group'].unique())


engine = create_engine('postgresql://postgres:admin@localhost:5432/inflation_analysis')



metadata = MetaData()

employees = Table(
    "employees",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String(100)),
    Column("age", Integer),
    Column("department", String(50)),
    Column("salary", Numeric(10,2))
)

# Create table if it doesn't exist
metadata.create_all(engine)
