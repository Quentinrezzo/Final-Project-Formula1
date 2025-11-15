"""
File allowing you to filter the Formula 1 World Championship dataset 
to include only the most recent seasons (2020–2024), keeping 
the data consistent with the modern F1 regulations and structure. 
The 2025 season will be added later.

When a CSV file is filtered, it will go directly into a new folder called 'processed' 
located under the main 'data' directory. This allows the original 
files to remain untouched in 'raw', while the cleaned and filtered 
versions are stored in 'processed' for analysis and modeling purposes.
"""

from pathlib import Path
import pandas as pd

def create_processed_folder() -> Path:
    """
    Create the new folder called 'processed' in order to store my modified CSV files
    Returns the path to the folder.
    """
    
    # Create the 'processed' folder under 'data' if it doesn't exist yet.
    processed_dir = Path("Final-Project-Formula1/data/processed")
    processed_dir.mkdir(parents = True, exist_ok = True)

    # Check if 'processed' exists
    if processed_dir.exists():
        print(f"📁 Folder created (or already exists): {processed_dir}")
    else:
        print(f"❌ Folder not created: {processed_dir}")

    return processed_dir


def filter_races_by_year(start_year: int = 2020, end_year: int = 2024) -> Path:
    """
    Filter the 'races.csv' file to include only the races 
    between the specified start and end years (2020–2024).
    The filtered version is saved into the 'processed' folder.

    Args:
        start_year (int): Starting year for the filter.
        end_year (int): Ending year for the filter.

    Returns:
        Path: Path to the saved filtered CSV file.
    """

    # Define file paths
    raw_file = Path("Final-Project-Formula1/data/raw/races.csv")
    output_file = Path("Final-Project-Formula1/data/processed/races_cleaned.csv")

    # Load the races.csv file and filter for selected years
    df = pd.read_csv(raw_file)
    total_rows = len(df)
    df_cleaned = df[df["year"].between(start_year, end_year)]
    kept_rows = len(df_cleaned)
    ignored_rows = total_rows - kept_rows

    # Save cleaned data to 'processed' folder
    output_file.parent.mkdir(parents = True, exist_ok = True)
    df_cleaned.to_csv(output_file, index = False)

    # Check
    try:
        if not output_file.exists():
            print(f"❌ File not found in processed folder: {output_file}")
            return None

        check_df = pd.read_csv(output_file)
        unique_years = check_df["year"].unique()
        
        if any((unique_years < start_year) | (unique_years > end_year)):
            print("⚠️ Warning: Some years fall outside the selected range.")
        else:
            print("✅ Filtered file successfully verified!")
        
        print(f"📁 Saved to: {output_file}")
        print(f" Rows kept: {kept_rows} / {total_rows} total")
        print(f" Rows ignored (outside {start_year}-{end_year}): {ignored_rows}")

        return output_file

    except Exception as e:
        print(f"⚠️ Error verifying filtered file: {e}")
        return None

    return output_file


"""
From now on, each CSV file will be taken individually to be modified and cleaned up so that we can work properly afterwards.
"""


def filter_circuits_by_races() -> Path:
    """
    Filter the 'circuits.csv' file to include only the circuits that appear
    in the filtered 'races_cleaned.csv' file (2020–2024 seasons).
    The filtered version is saved into the 'processed' folder as: circuits_cleaned.csv

    Returns:
        Path: Path to the saved filtered CSV file.
    """
    
    # Define file paths
    races_file = Path("Final-Project-Formula1/data/processed/races_cleaned.csv")
    circuits_file = Path("Final-Project-Formula1/data/raw/circuits.csv")
    output_file = Path("Final-Project-Formula1/data/processed/circuits_cleaned.csv")

    # Load the CSV files needed and filter the circuitId used in years 2020-2024
    races_df = pd.read_csv(races_file)
    circuits_df = pd.read_csv(circuits_file)
    circuitId_recent = races_df["circuitId"].unique()
    circuits_cleaned = circuits_df[circuits_df["circuitId"].isin(circuitId_recent)]

    # Save cleaned data to 'processed' folder
    output_file.parent.mkdir(parents = True, exist_ok = True)
    circuits_cleaned.to_csv(output_file, index = False)

    # Check
    try:
        if not output_file.exists():
            print(f"❌ File not found after saving: {output_file}")
            return None

        check_df = pd.read_csv(output_file)
        kept_rows = len(check_df)
        ignored_rows = len(circuits_df) - kept_rows

        print(f"📁 Saved to: {output_file}")
        print("✅ Circuits_cleaned successfully filtered and verified!")
        print(f" Circuits kept: {kept_rows} / {len(circuits_df)} total")
        print(f" Circuits ignored (not used between 2020–2024): {ignored_rows}")

    except Exception as e:
        print(f"⚠️ Error verifying filtered file: {e}")
        return None

    return output_file


def filter_constructor_results_by_races() -> Path:
    """
    Filter the 'constructor_results.csv' file to include only the constructor_results
    that belong to races appearing in 'races_cleaned.csv' (2020–2024 seasons).
    The filtered version is saved into the 'processed' folder as: constructor_results_cleaned.csv

    Returns:
        Path: Path to the saved filtered CSV file.
    """

    # Define file paths
    races_file = Path("Final-Project-Formula1/data/processed/races_cleaned.csv")
    constructor_results_file = Path("Final-Project-Formula1/data/raw/constructor_results.csv")
    output_file = Path("Final-Project-Formula1/data/processed/constructor_results_cleaned.csv")

    # Load the CSV files needed and filter the raceId used in years 2020-2024 to get the constructor_results
    races_df = pd.read_csv(races_file)
    constructor_df = pd.read_csv(constructor_results_file)
    recent_races = races_df["raceId"].unique()
    constructor_results_cleaned = constructor_df[constructor_df["raceId"].isin(recent_races)]

    # Save cleaned data to 'processed' folder
    output_file.parent.mkdir(parents = True, exist_ok = True)
    constructor_results_cleaned.to_csv(output_file, index = False)
    
    # Check
    try:
        if not output_file.exists():
            print(f"❌ File not found after saving: {output_file}")
            return None

        check_df = pd.read_csv(output_file)
        kept_rows = len(check_df)
        ignored_rows = len(constructor_df) - kept_rows

        print(f"📁 Saved to: {output_file}")
        print("✅ Constructor_results successfully filtered and verified!")
        print(f" Constructor_results kept: {kept_rows} / {len(constructor_df)} total")
        print(f" Constructor_results ignored (not used between 2020–2024): {ignored_rows}")

    except Exception as e:
        print(f"⚠️ Error verifying filtered file: {e}")
        return None

    return output_file


def





"""
def filter


def fill_circuit_lengths() -> None:
    
    Read the circuits_cleaned.csv file and fill in the “length_km” column with the values corresponding to
    each circuit using a dictionary of circuitId:length_km pairs.
    
    
    # Define paths and load the right file
    file_path = Path("Final-Project-Formula1/data/processed/circuits_cleaned.csv")
    df = pd.read_csv(file_path)

    # Dictionary of circuitId:length_km pairs
    circuit_lengths = {
        "1":
"""







