"""
File allowing you to filter the 'Formula 1 Race Data' dataset
to include only the most recent seasons (2020–2025), keeping
the data consistent with the modern F1 regulations and structure.

When a CSV file is filtered, it will go directly into a new folder called 'processed'
located under the main 'data' directory. This allows the original
files to remain untouched in 'raw', while the cleaned and filtered
versions are stored in 'processed' for analysis and modeling purposes.
"""

from pathlib import Path
import pandas as pd

# Import the shared project root from downloading_dataset
from .downloading_dataset import project_root

raw_direction = project_root / "data" / "raw"
processed_direction = project_root / "data" / "processed"

def create_processed_folder() -> Path:
    """
    Create the processed/ folder inside the project's data/ directory
    
    Returns:
        Path: path to the processed/ directory.
    """
    
    # Create the processed/ folder
    processed_direction.mkdir(parents = True, exist_ok = True)

    # Check if processed/ exists
    if processed_direction.exists():
        print(f"📁 Folder created: {processed_direction}")
    else:
        print(f"❌ Folder not created: {processed_direction}")

    return processed_direction


def filter_races_by_year(start_year: int = 2020, end_year: int = 2025) -> Path:
    """
    Filter the 'races.csv' file to include only races whose 'year'
    is between start_year and end_year (inclusive).
    The filtered version is saved into the data/processed/ folder as: races_cleaned.csv.

    Args:
        start_year (int): Starting year for the filter.
        end_year (int): Ending year for the filter.

    Returns:
        Path: Path to the saved filtered CSV file.
    """

    # Define file paths
    raw_file = raw_direction / "races.csv"
    output_file = processed_direction / "races_cleaned.csv"

    # Load the races.csv file and filter for selected years
    raw_df = pd.read_csv(raw_file)
    total_rows = len(raw_df)

    mask = raw_df["year"].between(start_year, end_year)
    df_cleaned = raw_df[mask].copy()
    kept_rows = len(df_cleaned)
    ignored_rows = total_rows - kept_rows

    # Save cleaned data to processed/ folder
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
            print(f"⚠️ Warning: Some years fall outside {start_year}, {end_year}")
        else:
            print(f"✅ {output_file.name} successfully verified")
        
        print(f"📁 Saved to: {output_file}")
        print(f" Rows kept: {kept_rows} / {total_rows} total")
        print(f" Rows ignored (outside {start_year}-{end_year}): {ignored_rows}")
        
    except Exception as e:
        print(f"⚠️ Error verifying filtered file {output_file.name}: {e}")
        
    return output_file


def filter_table_by_race_ids(table_name: str, race_ids: set[int], raw_filename: str,) -> Path:
    """
    Filter a raw table that has a 'raceId' column to keep only rows
    whose raceId is in the given set. Save the filtered version into
    data/processed/ as: {table_name}_cleaned.csv

    Args:
        table_name (str): Logical name of the table (used for the output filename).
        race_ids (set[int]): Set of raceId values to keep.
        raw_filename (str): Name of the raw CSV file from data/raw.

    Returns:
        Path: Path to the saved filtered CSV file.
    """

    # Define file paths
    raw_file = raw_direction / raw_filename
    output_file = processed_direction / f"{table_name}_cleaned.csv"

    # Load data
    df = pd.read_csv(raw_file)

    if "raceId" not in df.columns:
        raise ValueError(f"Table {table_name} has no 'raceId' column")

    # Filter rows by raceId
    df_cleaned = df[df["raceId"].isin(race_ids)].copy()

    # Save cleaned data to processed/ folder
    df_cleaned.to_csv(output_file, index = False)

    # Check
    try:
        if not output_file.exists():
            print(f"❌ File not found after saving: {output_file}")
            return None

        check_df = pd.read_csv(output_file)
        kept_rows = len(df_cleaned)
        total_rows = len(df)

        if not set(check_df["raceId"]).issubset(race_ids):
            print(f"⚠️ Warning: some entries contain raceId outside the expected set")

        print(f"✅ {output_file.name} successfully verified")
        print(f"📁 Saved to: {output_file}")
        print(f" Rows kept: {kept_rows} / {total_rows} total")

    except Exception as e:
        print(f"⚠️ Error verifying filtered file {output_file.name}: {e}")
        return None

    return output_file


def










def filter_circuits_by_races() -> Path:
    """
    Filter the 'circuits.csv' file to include only the circuits that appear
    in the filtered 'races_cleaned.csv' file (2020–2025 seasons).
    The filtered version is saved into the 'processed' folder as: circuits_cleaned.csv

    Returns:
        Path: Path to the saved filtered CSV file.
    """
    
    # Define file paths
    races_file = Path("Final-Project-Formula1/data/processed/races_cleaned.csv")
    circuits_file = Path("Final-Project-Formula1/data/raw/circuits.csv")
    output_file = Path("Final-Project-Formula1/data/processed/circuits_cleaned.csv")

    # Load the CSV files needed and filter the circuitId used in years 2020-2025
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
        print(f" Circuits ignored (not used between 2020–2025): {ignored_rows}")

    except Exception as e:
        print(f"⚠️ Error verifying filtered file: {e}")
        return None

    return output_file


def filter_constructor_results_by_races() -> Path:
    """
    Filter the 'constructor_results.csv' file to include only the constructor_results
    that belong to races appearing in 'races_cleaned.csv' (2020–2025 seasons).
    The filtered version is saved into the 'processed' folder as: constructor_results_cleaned.csv

    Returns:
        Path: Path to the saved filtered CSV file.
    """

    # Define file paths
    races_file = Path("Final-Project-Formula1/data/processed/races_cleaned.csv")
    constructor_results_file = Path("Final-Project-Formula1/data/raw/constructor_results.csv")
    output_file = Path("Final-Project-Formula1/data/processed/constructor_results_cleaned.csv")

    # Load the CSV files needed and filter the raceId used in years 2020-2025 to get the constructor_results
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
        print("✅ Constructor_results_cleaned successfully filtered and verified!")
        print(f" Constructor_results kept: {kept_rows} / {len(constructor_df)} total")
        print(f" Constructor_results ignored (not used between 2020–2025): {ignored_rows}")

    except Exception as e:
        print(f"⚠️ Error verifying filtered file: {e}")
        return None

    return output_file





def filter_seasons_by_year() -> Path:
    """
    Filter the 'seasons.csv' file to include only the seasons
    that belong to years appearing in 'races_cleaned.csv' (2020–2025 seasons).
    (Instead of filtering by year again, I use races_cleaned.csv so that if the user changes the year range
    in 'filter_races_by_year', all other filters update automatically).
    The filtered version is saved into the 'processed' folder as: seasons_cleaned.csv

    Returns:
        Path: Path to the saved filtered CSV file.
    """

    # Define file paths
    races_file = Path("Final-Project-Formula1/data/processed/races_cleaned.csv")
    seasons_file = Path("Final-Project-Formula1/data/raw/seasons.csv")
    output_file = Path("Final-Project-Formula1/data/processed/seasons_cleaned.csv")

    # Load the CSV files needed and filter the selected years to get the seasons
    races_df = pd.read_csv(races_file)
    seasons_df = pd.read_csv(seasons_file)
    years_in_races = races_df["year"].unique()
    seasons_cleaned = seasons_df[seasons_df["year"].isin(years_in_races)]

    # Save cleaned data to 'processed' folder
    output_file.parent.mkdir(parents = True, exist_ok = True)
    seasons_cleaned.to_csv(output_file, index = False)

    # Check
    try:
        if not output_file.exists():
            print(f"❌ File not found after saving: {output_file}")
            return None

        check_df = pd.read_csv(output_file)
        kept_rows = len(check_df)
        ignored_rows = len(seasons_df) - kept_rows

        print(f"📁 Saved to: {output_file}")
        print("✅ seasons_cleaned successfully filtered and verified!")
        print(f" seasons kept: {kept_rows} / {len(seasons_df)} total")
        print(f" seasons ignored (not used between 2020–2025): {ignored_rows}")

    except Exception as e:
        print(f"⚠️ Error verifying filtered file: {e}")
        return None

    return output_file





def filter_status_by_results() -> Path:
    """
    Filter the 'status.csv' file to include only the status
    that belong to results appearing in 'results_cleaned.csv' (2020–2025 seasons).
    The filtered version is saved into the 'processed' folder as: status_cleaned.csv

    Returns:
        Path: Path to the saved filtered CSV file.
    """

    # Define file paths
    results_file = Path("Final-Project-Formula1/data/processed/results_cleaned.csv")
    status_file = Path("Final-Project-Formula1/data/raw/status.csv")
    output_file = Path("Final-Project-Formula1/data/processed/status_cleaned.csv")
    
    # Load the CSV files needed and filter the statusId used in years 2020-2025 to get the status
    results_df = pd.read_csv(results_file)
    status_df = pd.read_csv(status_file)
    recent_results = results_df["statusId"].unique()
    status_cleaned = status_df[status_df["statusId"].isin(recent_results)]

    # Save cleaned data to 'processed' folder
    output_file.parent.mkdir(parents = True, exist_ok = True)
    status_cleaned.to_csv(output_file, index = False)
    
    # Check
    try:
        if not output_file.exists():
            print(f"❌ File not found after saving: {output_file}")
            return None

        check_df = pd.read_csv(output_file)
        kept_rows = len(check_df)
        ignored_rows = len(status_df) - kept_rows

        print(f"📁 Saved to: {output_file}")
        print("✅ status_cleaned successfully filtered and verified!")
        print(f" status kept: {kept_rows} / {len(status_df)} total")
        print(f" status ignored (not used between 2020–2025): {ignored_rows}")

    except Exception as e:
        print(f"⚠️ Error verifying filtered file: {e}")
        return None

    return output_file


def load_cleaned_csv() -> pd.DataFrame:
    """
    Ask the user for a Cleaned CSV file name and load it from the processed folder. Then show the
    first 5 rows of this specific csv file.
    
    Arg:
        file_name (input): Ask the user which file their wants to load.
    Returns:
        pd.DataFrame: Loaded Cleaned CSV file as a pandas DataFrame.
    """

    # Folder where your data is stored
    processed_dir = "Final-Project-Formula1/data/processed"
    
    # Show all CSV files in the processed folder
    for file in Path(processed_dir).glob("*.csv"):
        print("•", file.name)

    # Ask the user which Cleaned CSV file to load
    file_name = str(input("type the name of the cleaned csv file you want to preview (e.g., circuits_cleaned.csv): ")).strip()

    file_path = Path(processed_dir) / file_name
    
    # Check if the Cleaned CSV file exists
    if not file_path.exists():
        print(f"❌ Cleaned CSV file not found in the list above (check if the spelling is right)")
        return None

    # Read the Cleaned CSV file using pandas and show the first 5 rows as a preview
    df = pd.read_csv(file_path)
    print(f"✅ {file_name} loaded")
    print(df.head())

    return df