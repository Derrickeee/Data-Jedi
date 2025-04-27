import pandas as pd
import os

# File paths
INPUT_EXCEL = "data_to_input_sql.xlsx"
OUTPUT_DIR = "cpi_data"

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)
# Verify input file exists
if not os.path.isfile(INPUT_EXCEL):
    raise FileNotFoundError(f"Input file not found: {INPUT_EXCEL}")
try:
    # Load the Excel file
    xls = pd.ExcelFile(INPUT_EXCEL)
    # Process each sheet
    for sheet in xls.sheet_names:
        # Read and clean data
        df = pd.read_excel(xls, sheet_name=sheet)
        df = df.dropna(how='all')  # Remove blank rows
        if len(df) < 1:
            print(f"Skipping empty sheet: {sheet}")
            continue
            # Set first row as header and remove it from data
        df.columns = df.iloc[0]
        df = df[1:].reset_index(drop=True)
        # Create output path
        output_path = os.path.join(OUTPUT_DIR, f"{sheet}.csv")
        # Save to CSV
        df.to_csv(output_path, index=False)
        print(f"Created: {output_path}")
    print(f"\nSuccessfully created {len(xls.sheet_names)} CSV templates in '{OUTPUT_DIR}'")
except Exception as e:
    print(f"Error processing file: {e}")
