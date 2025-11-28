"""
This file contains functions that improve the cleaned dataset with additional information.
The goal is to keep 'data_loader.py' focused on filtering cleaning (selecting years 2020-2025), while this file is used to add useful extra information that can later be used for feature engineering and modeling.
"""

from pathlib import Path
import pandas as pd

# Import processed_direction from data_loader
from src.data_loader import processed_direction

def add_extra_info_on_circuits() -> Path:
    """
    Add three new columns to 'circuits_cleaned.csv':
    - length_km: length of the circuit
    - is_night_race: if the race is usually a night race (boolean)
    - track_type: simple label (technical, high_speed, balanced)

    The new columns are inserted between the 'alt' and 'url' columns.

    Returns:
        Path: Path to the updated 'circuits_cleaned.csv' file.
    """

    # Define the file path
    circuits_cleaned = processed_direction / "circuits_cleaned.csv"

    # Load the circuits_cleaned.csv file and prepare three new columns
    try:
        circuits_df = pd.read_csv(circuits_cleaned)
    except Exception as e:
        print(f"⚠️ Error loading {circuits_cleaned}: {e}")
        return None
        
    alt_index = circuits_df.columns.get_loc("alt")
    new_columns = ["length_km", "is_night_race", "track_type"]

    # Add three new columns
    for col in new_columns:
        circuits_df.insert(alt_index + 1, col, pd.NA)
        alt_index += 1

    # Save update file
    circuits_df.to_csv(circuits_cleaned, index = False)

    # Check
    try:
        check_df = pd.read_csv(circuits_cleaned)
        all_columns_present = all(col in check_df.columns for col in new_columns)
        
        if not all_columns_present:
            print(f"❌ Some columns missing in saved file at {circuits_cleaned}")
        else:
            print(f"✅ New columns successfully added to circuits_cleaned.csv")
            print("   - length_km")
            print("   - is_night_race")
            print("   - track_type")

    except Exception as e:
        print(f"⚠️ Error verifying updated file: {e}")
        return None

    return circuits_cleaned


def fill_circuit_extra_info():
    """
    Create a dictionnary to add three new pieces of information to each circuitId in circuits_cleaned.csv:
    - length_km: length of the circuit in kilometers
    - is_night_race: True if the Grand Prix is usually held at night
    - track_type: "technical", "high_speed", or "balanced"

    Returns:
        Path: Path to the updated 'circuits_cleaned.csv' file.
    """

    # Define the file path
    circuits_cleaned = processed_direction / "circuits_cleaned.csv"

    # Dictionary containing extra information mapped by CircuitId
    circuits_info = {
        1: {"length_km": 5.278, "is_night_race": False, "track_type": "balanced"},
        3: {"length_km": 5.412, "is_night_race": True, "track_type": "balanced"},
        4: {"length_km": 4.657, "is_night_race": False, "track_type": "balanced"},
        5: {"length_km": 5.338, "is_night_race": False, "track_type": "balanced"},
        6: {"length_km": 3.337, "is_night_race": False, "track_type": "technical"},
        7: {"length_km": 4.361, "is_night_race": False, "track_type": "balanced"},
        9: {"length_km": 5.891, "is_night_race": False, "track_type": "high_speed"},
        11: {"length_km": 4.381, "is_night_race": False, "track_type": "technical"},
        13: {"length_km": 7.004, "is_night_race": False, "track_type": "high_speed"},
        14: {"length_km": 5.793, "is_night_race": False, "track_type": "high_speed"},
        15: {"length_km": 4.927, "is_night_race": True, "track_type": "technical"},
        17: {"length_km": 5.451, "is_night_race": False, "track_type": "balanced"},
        18: {"length_km": 4.309, "is_night_race": False, "track_type": "technical"},
        20: {"length_km": 5.148, "is_night_race": False, "track_type": "balanced"},
        21: {"length_km": 4.909, "is_night_race": False, "track_type": "technical"},
        22: {"length_km": 5.807, "is_night_race": False, "track_type": "technical"},
        80: {"length_km": 6.201, "is_night_race": True, "track_type": "high_speed"},
        24: {"length_km": 5.281, "is_night_race": True, "track_type": "technical"},
        32: {"length_km": 4.304, "is_night_race": False, "track_type": "balanced"},
        34: {"length_km": 5.842, "is_night_race": False, "track_type": "balanced"},
        39: {"length_km": 4.259, "is_night_race": False, "track_type": "balanced"},
        69: {"length_km": 5.513, "is_night_race": False, "track_type": "balanced"},
        70: {"length_km": 4.326, "is_night_race": False, "track_type": "high_speed"},
        71: {"length_km": 5.848, "is_night_race": False, "track_type": "balanced"},
        73: {"length_km": 6.003, "is_night_race": False, "track_type": "high_speed"},
        75: {"length_km": 4.653, "is_night_race": False, "track_type": "balanced"},
        76: {"length_km": 5.245, "is_night_race": False, "track_type": "high_speed"},
        77: {"length_km": 6.174, "is_night_race": True, "track_type": "high_speed"},
        78: {"length_km": 5.419, "is_night_race": True, "track_type": "balanced"},
        79: {"length_km": 5.412, "is_night_race": False, "track_type": "technical"},
    }

    # Load the circuits_cleaned.csv file and add each row with dictionary values
    try:
        df = pd.read_csv(circuits_cleaned)
    except Exception as e:
        print(f"⚠️ Error loading {circuits_cleaned}: {e}")
        return None

    # Create new columns with correct dtypes to avoid pandas warnings
    df["length_km"] = pd.Series(dtype = "float64")
    df["is_night_race"] = pd.Series(dtype = "boolean")
    df["track_type"] = pd.Series(dtype = "string")
    
    # Fill values from dictionary
    for index, row in df.iterrows():
        circuitId = row["circuitId"]
        
        if circuitId in circuits_info:
            df.at[index, "length_km"] = circuits_info[circuitId]["length_km"]
            df.at[index, "is_night_race"] = circuits_info[circuitId]["is_night_race"]
            df.at[index, "track_type"] = circuits_info[circuitId]["track_type"]
        else:
            print(f"⚠️ circuitId {circuitId} not found in dictionary, values left as NA")
        
    # Save update file
    df.to_csv(circuits_cleaned, index = False)

    # Check
    try:
        check_df = pd.read_csv(circuits_cleaned)
        all_info_in_columns = all(col in check_df.columns for col in ["length_km", "is_night_race", "track_type"])
        
        if not all_info_in_columns:
            print(f"❌ Extra info in new columns not found in {circuits_cleaned}")
        else:
            print("✅ circuits_cleaned.csv successfully updated with new circuit extra information")

    except Exception as e:
        print(f"⚠️ Error verifying updated file: {e}")
        return None
    
    return circuits_cleaned


def add_extra_info_on_races() -> Path:
    """
    Add a new column to 'races_cleaned.csv':
    - races_distance_km: total length of the Grand Prix
    
    Returns:
        Path: Path to the updated 'races_cleaned.csv' file.
    """

    # Define the file path
    races_cleaned = processed_direction / "races_cleaned.csv"
    
    # Load the races_cleaned.csv file and prepare the new column
    try:
        races_df = pd.read_csv(races_cleaned)
    except Exception as e:
        print(f"⚠️ Error loading {races_cleaned}: {e}")
        return None
        
    name_index = races_df.columns.get_loc("name")
    new_column = "race_distance_km"
    
    # Add the new column
    races_df.insert(name_index + 1, new_column, pd.NA)

    # Save update file
    races_df.to_csv(races_cleaned, index = False)

    # Check
    try:
        check_df = pd.read_csv(races_cleaned)
        
        if new_column not in check_df.columns:
            print(f"❌ Column '{new_column}' not found in {races_cleaned}")
        else:
            print("✅ Column successfully added to races_cleaned.csv")
            print(f"   - {new_column}")

    except Exception as e:
        print(f"⚠️ Error verifying updated file: {e}")
        return None

    return races_cleaned


def fill_races_distance_km():
    """
    Compute the total race distance (in km) for each Grand Prix between 2020–2025
    and store it in the 'race_distance_km' column of races_cleaned.csv.
    
    Arg:
        the formula is: race_distance_km = length_km * laps

    Results:
        Path: Path to the updated 'circuits_cleaned.csv' file.
    """
    # Define file paths
    races_cleaned = processed_direction / "races_cleaned.csv"
    circuits_cleaned = processed_direction / "circuits_cleaned.csv"
    results_cleaned = processed_direction / "results_cleaned.csv"
    
    # Load the CSV files needed
    try:
        races_df = pd.read_csv(races_cleaned)
        circuits_df = pd.read_csv(circuits_cleaned)
        results_df = pd.read_csv(results_cleaned)
    except Exception as e:
        print(f"⚠️ Error while reading one of the cleaned files: {e}")
        return None
        
    # Get the real number of laps per race (only finished drivers)
    finished = results_df[results_df["statusId"] == 1].copy()
    laps_by_race = (finished.groupby("raceId")["laps"].max().rename("laps_completed"))

    # Get the length of each circuit and merge
    merged = races_df.merge(circuits_df[["circuitId", "length_km"]], on = "circuitId", how = "left")
    merged = merged.merge(laps_by_race, on = "raceId", how = "left")

    # Compute race distance in kilometers
    races_df["race_distance_km"] = merged["length_km"] * merged["laps_completed"]
    races_df["race_distance_km"] = races_df["race_distance_km"].round(3)

    # Save update file
    races_df.to_csv(races_cleaned, index = False)

    # Check
    try:
        check_df = pd.read_csv(races_cleaned)
        
        if "race_distance_km" not in check_df.columns:
            print(f"❌ Column 'race_distance_km' not found in file: {races_cleaned}")
            return None

        if check_df["race_distance_km"].isna().all():
            print(f"❌ Column 'race_distance_km' is empty in file: {races_cleaned}")
            return None

        print("✅ Column 'race_distance_km' successfully filled in races_cleaned.csv")

    except Exception as e:
        print(f"⚠️ Error verifying updated file: {e}")
        return None

    return races_cleaned


def add_status_dnf_categories() -> Path:
    """
    Add new columns to 'status_cleaned.csv' with simple DNF categories:
    - is_mechanical: True if status is a mechanical/technical issue
    - is_crash: True if status is a crash/accident/contact
    - is_other_dnf:
    
    Returns:
        Path: Path to the updated 'status_cleaned.csv' file.
    """

    # Define the file path
    status_file = processed_direction / "status_cleaned.csv"

    # Load data
    try:
        status_df = pd.read_csv(status_file)
    except Exception as e:
        print(f"⚠️ Error while reading {status_file}: {e}")
        return None

    # Classification
    def classify_mech_crash_other(text: str) -> str:
        
        t = str(text).lower()

        # Crash/accident/contact DNF
        if any(word in t for word in ["accident", "collision", "crash", "contact", "spun off", "damage",]):
            return "crash"

        # Mechanical/technical DNF
        if any(word in t for word in [
            "engine", "gearbox", "hydraulics", "brakes", "suspension",
            "exhaust", "clutch", "power", "fuel", "overheating",
            "oil", "radiator", "turbo", "driveshaft", "mechanical",
            "transmission", "electrical", "differential", "puncture",
            "front wing", "water", "wheel", "steering", "electronics",
            "Handling", "rear wing", "vibrations", "undertray", "cooling system",
            "throttle", "technical", "handling",]):
            return "mechanical"

        # Other DNFs
        if any(word in t for word in [
            "retired", "withdrew", "disqualified", "illness", "debris", "underweight",]):
            return "other_dnf"

        return "no_dnf"

    status_df["dnf_category"] = status_df["status"].apply(classify_mech_crash_other)
    status_df["is_mechanical"] = status_df["dnf_category"] == "mechanical"
    status_df["is_crash"] = status_df["dnf_category"] == "crash"
    status_df["is_other_dnf"] = status_df["dnf_category"] == "other_dnf"
    status_df["is_no_dnf"] = status_df["dnf_category"] == "no_dnf"

    # Save updated file
    status_df.to_csv(status_file, index = False)

    # Check
    try:
        check_df = pd.read_csv(status_file)
        expected_colums = ["statusId", "status", "dnf_category", "is_mechanical", "is_crash", "is_other_dnf", "is_no_dnf",]
        
        if not all(col in check_df.columns for col in expected_colums):
            print(f"❌ Columns missing after enrichment in {status_file}")
            return None
        else:
            print("✅ Column successfully added to status_cleaned.csv")
            
    except Exception as e:
        print(f"⚠️ Error verifying updated file: {e}")
        return None

    return status_file