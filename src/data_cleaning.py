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


def filter_races_by_year(start_year: int = 2020, end_year: int = 2025) -> Path:
    """
    Filter the 'races.csv' file to include only the races 
    between the specified start and end years (2020–2025).
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


def filter_constructor_standings_by_races() -> Path:
    """
    Filter the 'constructor_standings.csv' file to include only the constructor_standings
    that belong to races appearing in 'races_cleaned.csv' (2020–2025 seasons).
    The filtered version is saved into the 'processed' folder as: constructor_standings_cleaned.csv

    Returns:
        Path: Path to the saved filtered CSV file.
    """

    # Define file paths
    races_file = Path("Final-Project-Formula1/data/processed/races_cleaned.csv")
    constructor_standings_file = Path("Final-Project-Formula1/data/raw/constructor_standings.csv")
    output_file = Path("Final-Project-Formula1/data/processed/constructor_standings_cleaned.csv")

    # Load the CSV files needed and filter the raceId used in years 2020-2025 to get the
    # constructor_standings
    races_df = pd.read_csv(races_file)
    standings_df = pd.read_csv(constructor_standings_file)
    recent_races = races_df["raceId"].unique()
    constructor_standings_cleaned = standings_df[standings_df["raceId"].isin(recent_races)]

    # Save cleaned data to 'processed' folder
    output_file.parent.mkdir(parents = True, exist_ok = True)
    constructor_standings_cleaned.to_csv(output_file, index = False)

    # Check
    try:
        if not output_file.exists():
            print(f"❌ File not found after saving: {output_file}")
            return None

        check_df = pd.read_csv(output_file)
        kept_rows = len(check_df)
        ignored_rows = len(standings_df) - kept_rows

        print(f"📁 Saved to: {output_file}")
        print("✅ Constructor_standings_cleaned successfully filtered and verified!")
        print(f" Constructor_standings kept: {kept_rows} / {len(standings_df)} total")
        print(f" Constructor_standings ignored (not used between 2020–2025): {ignored_rows}")

    except Exception as e:
        print(f"⚠️ Error verifying filtered file: {e}")
        return None

    return output_file


def filter_constructors_by_results() -> Path:
    """
    Filter the 'constructors.csv' file to include only the constructors
    that belong to results appearing in 'constructor_results_cleaned.csv' (2020–2025 seasons).
    The filtered version is saved into the 'processed' folder as: constructors_cleaned.csv

    Returns:
        Path: Path to the saved filtered CSV file.
    """

    # Define file paths
    results_file = Path("Final-Project-Formula1/data/processed/constructor_results_cleaned.csv")
    constructors_file = Path("Final-Project-Formula1/data/raw/constructors.csv")
    output_file = Path("Final-Project-Formula1/data/processed/constructors_cleaned.csv")

    # Load the CSV files needed and filter the constructorId used in years 2020-2025 to get the constructors
    results_df = pd.read_csv(results_file)
    constructors_df = pd.read_csv(constructors_file)
    recent_constructors = results_df["constructorId"].unique()
    constructors_cleaned = constructors_df[constructors_df["constructorId"].isin(recent_constructors)]

    # Save cleaned data to 'processed' folder
    output_file.parent.mkdir(parents = True, exist_ok = True)
    constructors_cleaned.to_csv(output_file, index = False)

    # Check
    try:
        if not output_file.exists():
            print(f"❌ File not found after saving: {output_file}")
            return None

        check_df = pd.read_csv(output_file)
        kept_rows = len(check_df)
        ignored_rows = len(constructors_df) - kept_rows

        print(f"📁 Saved to: {output_file}")
        print("✅ Constructors_cleaned successfully filtered and verified!")
        print(f" Constructors kept: {kept_rows} / {len(constructors_df)} total")
        print(f" Constructors ignored (not used between 2020–2025): {ignored_rows}")

    except Exception as e:
        print(f"⚠️ Error verifying filtered file: {e}")
        return None

    return output_file


def filter_driver_standings_by_races() -> Path:
    """
    Filter the 'driver_standings.csv' file to include only the driver_standings
    that belong to races appearing in 'races_cleaned.csv' (2020–2025 seasons).
    The filtered version is saved into the 'processed' folder as: driver_standings_cleaned.csv

    Returns:
        Path: Path to the saved filtered CSV file.
    """

    # Define file paths
    races_file = Path("Final-Project-Formula1/data/processed/races_cleaned.csv")
    driver_standings_file = Path("Final-Project-Formula1/data/raw/driver_standings.csv")
    output_file = Path("Final-Project-Formula1/data/processed/driver_standings_cleaned.csv")

    # Load the CSV files needed and filter the raceId used in years 2020-2025 to get the
    # driver_standings
    races_df = pd.read_csv(races_file)
    standings_df = pd.read_csv(driver_standings_file)
    recent_races = races_df["raceId"].unique()
    driver_standings_cleaned = standings_df[standings_df["raceId"].isin(recent_races)]

    # Save cleaned data to 'processed' folder
    output_file.parent.mkdir(parents = True, exist_ok = True)
    driver_standings_cleaned.to_csv(output_file, index = False)

    # Check
    try:
        if not output_file.exists():
            print(f"❌ File not found after saving: {output_file}")
            return None

        check_df = pd.read_csv(output_file)
        kept_rows = len(check_df)
        ignored_rows = len(standings_df) - kept_rows

        print(f"📁 Saved to: {output_file}")
        print("✅ driver_standings_cleaned successfully filtered and verified!")
        print(f" driver_standings kept: {kept_rows} / {len(standings_df)} total")
        print(f" driver_standings ignored (not used between 2020–2025): {ignored_rows}")

    except Exception as e:
        print(f"⚠️ Error verifying filtered file: {e}")
        return None

    return output_file


def filter_drivers_by_standings() -> Path:
    """
    Filter the 'drivers.csv' file to include only the drivers
    that belong to standings appearing in 'driver_standings_cleaned.csv' (2020–2025 seasons).
    The filtered version is saved into the 'processed' folder as: drivers_cleaned.csv

    Returns:
        Path: Path to the saved filtered CSV file.
    """

    # Define file paths
    standings_file = Path("Final-Project-Formula1/data/processed/driver_standings_cleaned.csv")
    drivers_file = Path("Final-Project-Formula1/data/raw/drivers.csv")
    output_file = Path("Final-Project-Formula1/data/processed/drivers_cleaned.csv")

    # Load the CSV files needed and filter the driverId used in years 2020-2025 to get the drivers
    standings_df = pd.read_csv(standings_file)
    drivers_df = pd.read_csv(drivers_file)
    recent_driverId = standings_df["driverId"].unique()
    drivers_cleaned = drivers_df[drivers_df["driverId"].isin(recent_driverId)]

    # Save cleaned data to 'processed' folder
    output_file.parent.mkdir(parents = True, exist_ok = True)
    drivers_cleaned.to_csv(output_file, index = False)

    # Check
    try:
        if not output_file.exists():
            print(f"❌ File not found after saving: {output_file}")
            return None

        check_df = pd.read_csv(output_file)
        kept_rows = len(check_df)
        ignored_rows = len(drivers_df) - kept_rows

        print(f"📁 Saved to: {output_file}")
        print("✅ drivers_cleaned successfully filtered and verified!")
        print(f" drivers kept: {kept_rows} / {len(drivers_df)} total")
        print(f" drivers ignored (not used between 2020–2025): {ignored_rows}")

    except Exception as e:
        print(f"⚠️ Error verifying filtered file: {e}")
        return None

    return output_file


def filter_lap_times_by_races() -> Path:
    """
    Filter the 'lap_times.csv' file to include only the lap_times
    that belong to races appearing in 'races_cleaned.csv' (2020–2025 seasons).
    The filtered version is saved into the 'processed' folder as: lap_times_cleaned.csv

    Returns:
        Path: Path to the saved filtered CSV file.
    """

    # Define file paths
    races_file = Path("Final-Project-Formula1/data/processed/races_cleaned.csv")
    lap_times_file = Path("Final-Project-Formula1/data/raw/lap_times.csv")
    output_file = Path("Final-Project-Formula1/data/processed/lap_times_cleaned.csv")

    # Load the CSV files needed and filter the raceId used in years 2020-2025 to get the lap_times
    races_df = pd.read_csv(races_file)
    lap_times_df = pd.read_csv(lap_times_file)
    recent_races = races_df["raceId"].unique()
    lap_times_cleaned = lap_times_df[lap_times_df["raceId"].isin(recent_races)]

    # Save cleaned data to 'processed' folder
    output_file.parent.mkdir(parents = True, exist_ok = True)
    lap_times_cleaned.to_csv(output_file, index = False)

    # Check
    try:
        if not output_file.exists():
            print(f"❌ File not found after saving: {output_file}")
            return None

        check_df = pd.read_csv(output_file)
        kept_rows = len(check_df)
        ignored_rows = len(lap_times_df) - kept_rows

        print(f"📁 Saved to: {output_file}")
        print("✅ lap_times_cleaned successfully filtered and verified!")
        print(f" lap_times kept: {kept_rows} / {len(lap_times_df)} total")
        print(f" lap_times ignored (not used between 2020–2025): {ignored_rows}")

    except Exception as e:
        print(f"⚠️ Error verifying filtered file: {e}")
        return None

    return output_file


def filter_pit_stops_by_races() -> Path:
    """
    Filter the 'pit_stops.csv' file to include only the pit_stops
    that belong to races appearing in 'races_cleaned.csv' (2020–2025 seasons).
    The filtered version is saved into the 'processed' folder as: pit_stops_cleaned.csv

    Returns:
        Path: Path to the saved filtered CSV file.
    """

    # Define file paths
    races_file = Path("Final-Project-Formula1/data/processed/races_cleaned.csv")
    pit_stops_file = Path("Final-Project-Formula1/data/raw/pit_stops.csv")
    output_file = Path("Final-Project-Formula1/data/processed/pit_stops_cleaned.csv")

    # Load the CSV files needed and filter the raceId used in years 2020-2025 to get the pit_stops
    races_df = pd.read_csv(races_file)
    pit_stops_df = pd.read_csv(pit_stops_file)
    recent_races = races_df["raceId"].unique()
    pit_stops_cleaned = pit_stops_df[pit_stops_df["raceId"].isin(recent_races)]

    # Save cleaned data to 'processed' folder
    output_file.parent.mkdir(parents = True, exist_ok = True)
    pit_stops_cleaned.to_csv(output_file, index = False)

    # Check
    try:
        if not output_file.exists():
            print(f"❌ File not found after saving: {output_file}")
            return None

        check_df = pd.read_csv(output_file)
        kept_rows = len(check_df)
        ignored_rows = len(pit_stops_df) - kept_rows

        print(f"📁 Saved to: {output_file}")
        print("✅ pit_stops_cleaned successfully filtered and verified!")
        print(f" pit_stops kept: {kept_rows} / {len(pit_stops_df)} total")
        print(f" pit_stops ignored (not used between 2020–2025): {ignored_rows}")

    except Exception as e:
        print(f"⚠️ Error verifying filtered file: {e}")
        return None

    return output_file


def filter_qualifying_by_races() -> Path:
    """
    Filter the 'qualifying.csv' file to include only the qualifying
    that belong to races appearing in 'races_cleaned.csv' (2020–2025 seasons).
    The filtered version is saved into the 'processed' folder as: qualifying_cleaned.csv

    Returns:
        Path: Path to the saved filtered CSV file.
    """

    # Define file paths
    races_file = Path("Final-Project-Formula1/data/processed/races_cleaned.csv")
    qualifying_file = Path("Final-Project-Formula1/data/raw/qualifying.csv")
    output_file = Path("Final-Project-Formula1/data/processed/qualifying_cleaned.csv")

    # Load the CSV files needed and filter the raceId used in years 2020-2025 to get the pit_stops
    races_df = pd.read_csv(races_file)
    qualifying_df = pd.read_csv(qualifying_file)
    recent_races = races_df["raceId"].unique()
    qualifying_cleaned = qualifying_df[qualifying_df["raceId"].isin(recent_races)]

    # Save cleaned data to 'processed' folder
    output_file.parent.mkdir(parents = True, exist_ok = True)
    qualifying_cleaned.to_csv(output_file, index = False)
    
    # Check
    try:
        if not output_file.exists():
            print(f"❌ File not found after saving: {output_file}")
            return None

        check_df = pd.read_csv(output_file)
        kept_rows = len(check_df)
        ignored_rows = len(qualifying_df) - kept_rows

        print(f"📁 Saved to: {output_file}")
        print("✅ qualifying_cleaned successfully filtered and verified!")
        print(f" qualifying kept: {kept_rows} / {len(qualifying_df)} total")
        print(f" qualifying ignored (not used between 2020–2025): {ignored_rows}")

    except Exception as e:
        print(f"⚠️ Error verifying filtered file: {e}")
        return None

    return output_file


def filter_results_by_races() -> Path:
    """
    Filter the 'results.csv' file to include only the results
    that belong to races appearing in 'races_cleaned.csv' (2020–2025 seasons).
    The filtered version is saved into the 'processed' folder as: results_cleaned.csv

    Returns:
        Path: Path to the saved filtered CSV file.
    """

    # Define file paths
    races_file = Path("Final-Project-Formula1/data/processed/races_cleaned.csv")
    results_file = Path("Final-Project-Formula1/data/raw/results.csv")
    output_file = Path("Final-Project-Formula1/data/processed/results_cleaned.csv")
    
    # Load the CSV files needed and filter the raceId used in years 2020-2025 to get the results
    races_df = pd.read_csv(races_file)
    results_df = pd.read_csv(results_file)
    recent_races = races_df["raceId"].unique()
    results_cleaned = results_df[results_df["raceId"].isin(recent_races)]
    
    # Save cleaned data to 'processed' folder
    output_file.parent.mkdir(parents = True, exist_ok = True)
    results_cleaned.to_csv(output_file, index = False)
    
    # Check
    try:
        if not output_file.exists():
            print(f"❌ File not found after saving: {output_file}")
            return None

        check_df = pd.read_csv(output_file)
        kept_rows = len(check_df)
        ignored_rows = len(results_df) - kept_rows

        print(f"📁 Saved to: {output_file}")
        print("✅ results_cleaned successfully filtered and verified!")
        print(f" results kept: {kept_rows} / {len(results_df)} total")
        print(f" results ignored (not used between 2020–2025): {ignored_rows}")

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


def filter_sprint_results_by_races() -> Path:
    """
    Filter the 'sprint_results.csv' file to include only the sprint_results
    that belong to races appearing in 'races_cleaned.csv' (2020–2025 seasons).
    The filtered version is saved into the 'processed' folder as: sprint_results_cleaned.csv

    Returns:
        Path: Path to the saved filtered CSV file.
    """

    # Define file paths
    races_file = Path("Final-Project-Formula1/data/processed/races_cleaned.csv")
    sprint_results_file = Path("Final-Project-Formula1/data/raw/sprint_results.csv")
    output_file = Path("Final-Project-Formula1/data/processed/sprint_results_cleaned.csv")
    
    # Load the CSV files needed and filter the raceId used in years 2020-2025 to get the sprint_results
    races_df = pd.read_csv(races_file)
    sprint_results_df = pd.read_csv(sprint_results_file)
    recent_races = races_df["raceId"].unique()
    sprint_results_cleaned = sprint_results_df[sprint_results_df["raceId"].isin(recent_races)]

    # Save cleaned data to 'processed' folder
    output_file.parent.mkdir(parents = True, exist_ok = True)
    sprint_results_cleaned.to_csv(output_file, index = False)
    
    # Check
    try:
        if not output_file.exists():
            print(f"❌ File not found after saving: {output_file}")
            return None

        check_df = pd.read_csv(output_file)
        kept_rows = len(check_df)
        ignored_rows = len(sprint_results_df) - kept_rows

        print(f"📁 Saved to: {output_file}")
        print("✅ sprint_results_cleaned successfully filtered and verified!")
        print(f" sprint_results kept: {kept_rows} / {len(sprint_results_df)} total")
        print(f" sprint_results ignored (not used between 2020–2025): {ignored_rows}")

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