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
    try:
        raw_df = pd.read_csv(raw_file)
    except Exception as e:
        print(f"⚠️ Error while reading {raw_file}: {e}")
        return None
        
    mask = raw_df["year"].between(start_year, end_year)
    df_cleaned = raw_df[mask].copy()
    kept_rows = len(df_cleaned)
    total_rows = len(raw_df)
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
    try:
        df = pd.read_csv(raw_file)
    except Exception as e:
        print(f"⚠️ Error while reading {raw_file}: {e}")
        return None

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
        kept_rows = len(check_df)
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


def filter_circuits_by_races() -> Path:
    """
    Filter the 'circuits.csv' file to include only the circuits used in the
    filtered 'races_cleaned.csv' (2020–2025).
    The filtered version is saved to data/processed/ as: circuits_cleaned.csv.

    Returns:
        Path: Path to the saved filtered CSV file.
    """

    # Define file paths
    races_file = processed_direction / "races_cleaned.csv"
    circuits_file = raw_direction / "circuits.csv"
    output_file = processed_direction / "circuits_cleaned.csv"

    # Load data
    try:
        races_df = pd.read_csv(races_file)
        circuits_df = pd.read_csv(circuits_file)
    except Exception as e:
        print(f"⚠️ Error while reading {races_file} or {circuits_file}: {e}")
        return None

    valid_circuits = set(races_df["circuitId"].unique())

    # Filter circuits.csv
    circuits_cleaned = circuits_df[circuits_df["circuitId"].isin(valid_circuits)].copy()

    # Save cleaned data to processed/ folder
    circuits_cleaned.to_csv(output_file, index = False)

    # Check
    try:
        if not output_file.exists():
            print(f"❌ File not found after saving: {output_file}")
            return None

        check_df = pd.read_csv(output_file)
        kept_rows = len(check_df)
        total_rows = len(circuits_df)

        if not set(check_df["circuitId"]).issubset(valid_circuits):
            print(f"⚠️ Warning: some entries contain circuitId outside the expected set")

        print(f"✅ {output_file.name} successfully verified")
        print(f"📁 Saved to: {output_file}")
        print(f" Rows kept: {kept_rows} / {total_rows} total")

    except Exception as e:
        print(f"⚠️ Error verifying filtered file {output_file.name}: {e}")
        return None

    return output_file


def filter_constructors_by_races() -> Path:
    """
    Filter the 'constructors.csv' file to include only the constructors that appear
    in 'constructor_results_cleaned.csv'.
    The filtered version is saved into data/processed/ as: 'constructors_cleaned.csv'.

    Returns:
        Path: Path to the saved filtered CSV file.
    """

    # Define file paths
    constructor_results_file = processed_direction / "constructor_results_cleaned.csv"
    constructors_file = raw_direction / "constructors.csv"
    output_file = processed_direction / "constructors_cleaned.csv"

    # Load data
    try:
        results_df = pd.read_csv(constructor_results_file)
        constructors_df = pd.read_csv(constructors_file)
    except Exception as e:
        print(f"⚠️ Error while reading {constructor_results_file} or {constructors_file}: {e}")
        return None

    valid_constructor_ids = set(results_df["constructorId"].unique())

    # Filter constructors.csv
    constructors_cleaned = constructors_df[constructors_df["constructorId"].isin(valid_constructor_ids)].copy()

    # Save cleaned data to processed/ folder
    constructors_cleaned.to_csv(output_file, index = False)

    # Check
    try:
        if not output_file.exists():
            print(f"❌ File not found after saving: {output_file}")
            return None

        check_df = pd.read_csv(output_file)
        kept_rows = len(check_df)
        total_rows = len(constructors_df)

        if not set(check_df["constructorId"]).issubset(valid_constructor_ids):
            print(f"⚠️ Warning: some entries contain constructorId outside the expected set")

        print(f"✅ {output_file.name} successfully verified")
        print(f"📁 Saved to: {output_file}")
        print(f" Rows kept: {kept_rows} / {total_rows} total")

    except Exception as e:
        print(f"⚠️ Error verifying filtered file {output_file.name}: {e}")
        return None

    return output_file


def filter_drivers_by_races() -> Path:
    """
    Filter the 'drivers.csv' file to include only the drivers that appear
    in 'results_cleaned.csv'.
    The filtered version is saved into data/processed/ as: 'drivers_cleaned.csv'.

    Returns:
        Path: Path to the saved filtered CSV file.
    """

    # Define file paths
    results_file = processed_direction / "results_cleaned.csv"
    drivers_file = raw_direction / "drivers.csv"
    output_file = processed_direction / "drivers_cleaned.csv"

    # Load data
    try:
        results_df = pd.read_csv(results_file)
        drivers_df = pd.read_csv(drivers_file)
    except Exception as e:
        print(f"⚠️ Error while reading {results_file} or {drivers_file}: {e}")
        return None
    
    valid_driver_ids = set(results_df["driverId"].unique())

    # Filter drivers.csv
    drivers_cleaned = drivers_df[drivers_df["driverId"].isin(valid_driver_ids)].copy()

    # Save cleaned data to processed/ folder
    drivers_cleaned.to_csv(output_file, index = False)

    # Check
    try:
        if not output_file.exists():
            print(f"❌ File not found after saving: {output_file}")
            return None

        check_df = pd.read_csv(output_file)
        kept_rows = len(check_df)
        total_rows = len(drivers_df)

        if not set(check_df["driverId"]).issubset(valid_driver_ids):
            print(f"⚠️ Warning: some entries contain driverId outside the expected set")

        print(f"✅ {output_file.name} successfully verified")
        print(f"📁 Saved to: {output_file}")
        print(f" Rows kept: {kept_rows} / {total_rows} total")

    except Exception as e:
        print(f"⚠️ Error verifying filtered file {output_file.name}: {e}")
        return None

    return output_file


def filter_seasons_by_year() -> Path:
    """
    Filter the 'seasons.csv' file to include the seasons that actually appear
    in 'races_cleaned.csv'.
    The filtered version is saved into data/processed/ as: 'seasons_cleaned.csv'.

    Returns:
        Path: Path to the saved filtered CSV file.
    """

    # Define file paths
    races_file = processed_direction / "races_cleaned.csv"
    seasons_file = raw_direction / "seasons.csv"
    output_file = processed_direction / "seasons_cleaned.csv"

    # Load data
    try:
        races_df = pd.read_csv(races_file)
        seasons_df = pd.read_csv(seasons_file)
    except Exception as e:
        print(f"⚠️ Error while reading {races_file} or {seasons_file}: {e}")
        return None

    valid_years = set(races_df["year"].unique())

    # Filter seasons.csv
    seasons_cleaned = seasons_df[seasons_df["year"].isin(valid_years)].copy()

    # Save cleaned data to processed/ folder
    seasons_cleaned.to_csv(output_file, index = False)

    # Check
    try:
        if not output_file.exists():
            print(f"❌ File not found after saving: {output_file}")
            return None

        check_df = pd.read_csv(output_file)
        kept_rows = len(check_df)
        total_rows = len(seasons_df)
        unique_years = set(check_df["year"].unique())

        if not unique_years.issubset(valid_years):
            print(f"⚠️ Warning: some seasons contain years outside the races_cleaned years")

        print(f"✅ {output_file.name} successfully verified")
        print(f"📁 Saved to: {output_file}")
        print(f" Rows kept: {kept_rows} / {total_rows} total")

    except Exception as e:
        print(f"⚠️ Error verifying filtered file {output_file.name}: {e}")
        return None

    return output_file


def filter_status_by_results() -> Path:
    """
    Filter the 'status.csv' file to include only the statusId values
    that appear in 'results_cleaned.csv'.  
    The filtered version is saved into data/processed/ as: 'status_cleaned.csv'.

    Returns:
        Path: Path to the saved filtered CSV file.
    """

    # Define file paths
    results_file = processed_direction / "results_cleaned.csv"
    status_file = raw_direction / "status.csv"
    output_file = processed_direction / "status_cleaned.csv"

    # Load data
    try:
        results_df = pd.read_csv(results_file)
        status_df = pd.read_csv(status_file)
    except Exception as e:
        print(f"⚠️ Error while reading {results_file} or {status_file}: {e}")
        return None

    valid_status_ids = set(results_df["statusId"].unique())

    # Filter status.csv
    status_cleaned = status_df[status_df["statusId"].isin(valid_status_ids)].copy()

    # Save cleaned data to processed/ folder
    status_cleaned.to_csv(output_file, index = False)

    # Check
    try:
        if not output_file.exists():
            print(f"❌ File not found after saving: {output_file}")
            return None

        check_df = pd.read_csv(output_file)
        kept_rows = len(check_df)
        total_rows = len(status_df)

        if not set(check_df["statusId"]).issubset(valid_status_ids):
            print(f"⚠️ Warning: some entries contain statusId outside the expected set")

        print(f"✅ {output_file.name} successfully verified")
        print(f"📁 Saved to: {output_file}")
        print(f" Rows kept: {kept_rows} / {total_rows} total")

    except Exception as e:
        print(f"⚠️ Error verifying filtered file {output_file.name}: {e}")
        return None

    return output_file


def load_cleaned_csv(destination: str = "data/processed") -> pd.DataFrame | None:
    """
    Ask the user for a CSV file name and load it from the processed folder. Then show the
    first 5 rows of this specific csv file.
    
    Arg:
        destination (str): path (relative to the project root) where the cleaned CSV files are stored.
        Default: "data/processed".
        file_name (input): Ask the user which file their wants to load.
    Returns:
        pd.DataFrame: Loaded CSV file as a pandas DataFrame, or None if the file does not exist.
    """

    # Define the path
    destination_path = project_root / destination

    # Show all CSV files in the processed folder
    for file in Path(destination_path).glob("*.csv"):
        print("•", file.name)

    # Ask the user which CSV file to load
    file_name = str(input("type the name of the csv file you want to preview (e.g., circuits_cleaned.csv): ")).strip()

    file_path = Path(destination_path) / file_name

    # Check if the CSV file exists
    if not file_path.exists():
        print(f"❌ Cleaned CSV file not found in the list above (check if the spelling is right)")
        return None

    # Read the CSV file using pandas and show the first 5 rows as a preview
    df = pd.read_csv(file_path)
    print(f"✅ {file_name} loaded")
    print(df.head())

    return df