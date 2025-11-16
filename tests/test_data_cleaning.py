"""
Pytest file for data_cleaning.py
"""

from pathlib import Path
import pandas as pd

# step 1: Allow pytest to see the src folder
import sys
sys.path.append("/files/Final-Project-Formula1") #project root

# Import the functions stored in the data_cleaning.py file under the src folder
from src.data_cleaning import create_processed_folder, filter_races_by_year, filter_circuits_by_races

def test_create_processed_folder(monkeypatch, tmp_path):
    """
    Simulate a creating 'processed' folder. Check the folder is copied into data/processed.
    """

    # 1) Create a fake 'processed' folder
    monkeypatch.chdir(tmp_path)
    out_path = create_processed_folder()
    expected_folder = Path("Final-Project-Formula1/data/processed")

    # 2) Check that the returned path matches the expected one and the folder really exists
    assert out_path == expected_folder
    assert expected_folder.exists()
    assert expected_folder.is_dir()


def test_filter_races_by_year(monkeypatch, tmp_path):
    """
    This test checks that year filter for 2020-2025 works correctly into the races.csv file. In addition,
    it checks if there is no other years outside the range into the new 'races_cleaned.csv' file.
    """
    
    # 1) Create fake directory structure and races.csv
    monkeypatch.chdir(tmp_path)
    raw_dir = tmp_path / "Final-Project-Formula1"/"data"/"raw"
    raw_dir.mkdir(parents = True, exist_ok = True)
    races_path = raw_dir / "races.csv"

    # 2) Create a small dictionary, include years in and out of range
    df_src = pd.DataFrame(
        {"raceId":   [1, 2, 3, 4, 5, 6],
         "year":     [2019, 2020, 2021, 2022, 2024, 2025],
         "circuitId":[10,   11,   12,   13,   14,   15],
         "name":     ["A",  "B",  "C",  "D",  "E",  "F"],})
    df_src.to_csv(races_path, index = False)

    # 3) Run the function with the right range
    out_path = filter_races_by_year()

    # 4) Expected output path and check if the file exists
    expected = Path("Final-Project-Formula1/data/processed/races_cleaned.csv")
    assert out_path == expected
    assert expected.exists()
    assert expected.is_file()

    # 5) Load the saved file and verify filtering
    df_out = pd.read_csv(expected)
    kept_years = sorted(df_out["year"].unique().tolist())
    assert all(2020 <= y <= 2025 for y in kept_years)
    assert kept_years == [2020, 2021, 2022, 2024, 2025]
    assert len(df_out) == 5


def test_filter_circuits_by_races(monkeypatch, tmp_path):
    """
    This test checks that filter_circuits_by_races() keeps only the circuits
    that appear in 'races_cleaned.csv' (2020–2025 races) and saves the
    filtered result into processed/circuits_cleaned.csv.
    """

    # 1) Create fake directory structure
    monkeypatch.chdir(tmp_path)
    processed_dir = tmp_path / "Final-Project-Formula1"/"data"/"processed"
    raw_dir = tmp_path / "Final-Project-Formula1"/"data"/"raw"
    processed_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)

    # 2) Create a races_cleaned file in processed
    races_path = processed_dir / "races_cleaned.csv"
    races_df = pd.DataFrame(
        {"raceId":    [1, 2, 3, 4, 5],
         "year":      [2020, 2021, 2022, 2024, 2025],
         "circuitId": [10, 11, 10, 12, 13],
         "name":      ["GP A", "GP B", "GP C", "GP D", "GP E"],})
    races_df.to_csv(races_path, index = False)

    # 3) Create a circuits.csv in raw with extra unused circuits
    circuits_path = raw_dir / "circuits.csv"
    circuits_df = pd.DataFrame(
        {"circuitId": [10, 11, 12, 13, 99],
         "name":      ["Circuit A", "Circuit B", "Circuit C", "Circuit D", "Old Circuit"],
         "location":  ["W", "X", "Y", "Z", "Anywhere"],})
    circuits_df.to_csv(circuits_path, index = False)

    # 4) Run the function to keep only the relevant data
    out_path = filter_circuits_by_races()

    # 5) Expected output path and check if the file exists
    expected = Path("Final-Project-Formula1/data/processed/circuits_cleaned.csv")
    assert out_path == expected
    assert expected.exists()
    assert expected.is_file()

    # 6) Load the saved file and verify filtering
    df_out = pd.read_csv(expected)
    kept_Id = sorted(df_out["circuitId"].unique().tolist())
    assert kept_Id == [10, 11, 12, 13]
    assert 99 not in df_out["circuitId"].values
    assert len(df_out) == 4




    