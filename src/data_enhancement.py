"""
This file contains functions that improve the cleaned dataset with additional information.
The goal is to keep 'data_cleaning.py' focused on filtering cleaning (selecting years 2020-2025), while this file is used
to add useful extra information that can later be used for feature engineering and modeling.
"""

from pathlib import Path
import pandas as pd

def add_extra_info_on_circuits() -> Path:
    """
    Add three new columns to 'circuits_cleaned.csv':
    - length_km: length of the circuit
    - is_night_race: if the race is usually a night race (boolean)
    - track_type: simple label (technical, high_speed, balanced)

    The columns are inserted between the 'alt' and 'url' columns.

    Returns:
        Path: Path to the updated 'circuits_cleaned.csv' file.
    """

    # Define the file path
    circuits_cleaned = Path("Final-Project-Formula1/data/processed/circuits_cleaned.csv")

    # Load the circuits_cleaned.csv file and prepare three new columns
    circuits_df = pd.read_csv(circuits_cleaned)
    alt_index = circuits_df.columns.get_loc("alt")
    new_columns = ["length_km", "is_night_race", "track_type"]

    # Add three new columns
    for col in (new_columns):
        circuits_df.insert(alt_index + 1, col, pd.NA)
        alt_index += 1

    # Save update file
    circuits_df.to_csv(circuits_cleaned, index = False)

    # Check
    try:
        check_df = pd.read_csv(circuits_cleaned)
        all_columns_present = all(col in check_df.columns for col in new_columns)
        
        if not all_columns_present:
            print(f"❌ columns not found in circuits_cleaned file saved to: {circuits_cleaned}")
        else:
            print("✅ Columns successfully added:")
            print("   - length_km")
            print("   - is_night_race")
            print("   - track_type")

    except Exception as e:
        print(f"⚠️ Error verifying updated file: {e}")
        return None

    return circuits_cleaned


def fill_circuit_length():
    """
    Create a dictionnary to add each length of a circuit and then match them with the right CircuitId.

    Returns:
        Path: Path to the updated length_km to 'circuits_cleaned.csv' file.
    """

    # Define the file path
    circuits_cleaned = Path("Final-Project-Formula1/data/processed/circuits_cleaned.csv")

    # Create the dictionnary for extra information values corresponding to CircuitId
    circuits_length = {
        1: {"length_km": 5.278, "is_night_race": False, "track_type": "balanced"},
        3: {"length_km": 5.412, "is_night_race": "transition", "track_type": "balanced"},
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
        24: {"length_km": 5.281, "is_night_race": "transition", "track_type": "technical"},
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
        











    
        
        