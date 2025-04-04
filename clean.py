import os
import pandas as pd
import ast


def find_data_file(file_name):
    """Search for the data file in all directories"""
    try:
        for root, dirs, files in os.walk('/'):
            if file_name in files:
                file_path = os.path.join(root, file_name)
                print(f"File found: {file_path}")
                return file_path
        raise FileNotFoundError(f"Error: Could not find data file '{file_name}'")
    except Exception as e:
        print(f"An error occurred while searching for the file: {e}")
        return None


def parse_row(row):
    """Parse JSON-like row data"""
    try:
        return ast.literal_eval(row)  # Safely evaluate the JSON-like string
    except (ValueError, SyntaxError) as e:
        print(f"Error parsing row: {e}")
        return None


def extract_columns(parsed_row):
    """Extract specific columns from parsed row"""
    try:
        if parsed_row and "columns" in parsed_row:
            return pd.DataFrame(parsed_row["columns"]).set_index("key")["value"]
    except KeyError as e:
        print(f"KeyError while extracting columns: {e}")
    except Exception as e:
        print(f"Unexpected error during column extraction: {e}")


def clean_csv(file_name):
    """Main function to clean the CSV file"""
    try:
        file_path = find_data_file(file_name)
        if file_path is None:
            return  # Exit if the file isn't found

        # Load the CSV file
        try:
            df = pd.read_csv(file_path)
        except FileNotFoundError:
            print(f"File not found: {file_path}")
            return
        except pd.errors.ParserError as e:
            print(f"Error parsing CSV file: {e}")
            return

        # Parse the 'row' column to extract the JSON-like data
        df["parsed_row"] = df["row"].apply(parse_row)

        # Extract specific data from the parsed rows
        extracted_data = df["parsed_row"].apply(
            extract_columns)

        # Concatenate the extracted data with the original dataframe
        try:
            cleaned_df = pd.concat([df.drop(columns=["row", "parsed_row"]), extracted_data], axis=1)
        except ValueError as e:
            print(f"Error concatenating dataframes: {e}")
            return

        # Remove columns containing "na" values
        try:
            cleaned_df = cleaned_df.loc[:, ~cleaned_df.isin(["na"]).any()]
        except Exception as e:
            print(f"Error during column cleaning: {e}")
            return

        # Save the cleaned dataframe to a new CSV file
        cleaned_df.to_csv("cleaned_data.csv", index=False)
        print("Data cleaned and saved as 'cleaned_data.csv'")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


# Example usage
clean_csv("cpi_sg_singstat_20250404_141959.csv")
