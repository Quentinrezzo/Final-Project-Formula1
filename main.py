"""
Main entry point for the Formula 1 race outcome prediction project.

Pipeline:
1. Downloads the Kaggle dataset into data/raw/.
2. Lists the available CSV files.
3. Filter races to recent seasons (2020–2025).
4. Filter all race-based tables using raceId.
5. Filter dimension tables (circuits, constructors, drivers, seasons, status).
"""

from pathlib import Path
import pandas as pd

# 1,2) Download raw data
from src.downloading_dataset import download_dataset

# 3,4,5) Filter csv files
from src.data_loader import (
    create_processed_folder,
    filter_races_by_year,
    filter_table_by_race_ids,
    filter_circuits_by_races,
    filter_constructors_by_races,
    filter_drivers_by_results,
    filter_seasons_by_year,
    filter_status_by_results,)


def main() -> None:
    print("=== F1 Project: setup and data download ===")

    # 1. Download dataset into data/raw/
    data_raw_path: Path = download_dataset()

    # 2. List CSV files in data/raw/
    csv_files = sorted(data_raw_path.glob("*.csv"))
    if not csv_files:
        print("❌ No CSV files found in data/raw after download.")
    else:
        print("✅ CSV files available in data/raw/:")
        for f in csv_files:
            print("  -", f.name)

    print("=== End of main.py (pipeline to be extended later) ===")


if __name__ == "__main__":
    main()


def main() -> None:
    print("=== F1 Project: full data pipeline ===")

    # 1. Ensure data/processed/ exists
    processed_dir: Path = create_processed_folder()
    print(f"\n📂 Processed data directory: {processed_dir}")

    # 2. Filter races.csv by year (2020-2025)
    races_cleaned_path: Path = filter_races_by_year(start_year = 2020, end_year = 2025)

    # 3. Load filtered races to get recent raceIds
    races_df = pd.read_csv(races_cleaned_path)
    recent_race_ids = set(races_df["raceId"].unique())
    print(f"\n✅ Number of recent races: {len(recent_race_ids)}")

    # 4. Filter all tables that have a raceId column using filter_table_by_race_ids
    race_tables = [
        ("constructor_results", "constructor_results.csv"),
        ("constructor_standings", "constructor_standings.csv"),
        ("driver_standings", "driver_standings.csv"),
        ("lap_times", "lap_times.csv"),
        ("pit_stops", "pit_stops.csv"),
        ("qualifying", "qualifying.csv"),
        ("results", "results.csv"),
        ("sprint_results", "sprint_results.csv"),]

    for table_name, raw_filename in race_tables:
        print(f"\n--- Filtering {raw_filename} ---")
        filter_table_by_race_ids(
            table_name=table_name,
            race_ids=recent_race_ids,
            raw_filename=raw_filename,)

    # 5. Filter dimension tables (no raceId column)
    print("\n--- Filtering circuits.csv ---")
    filter_circuits_by_races()

    print("\n--- Filtering constructors.csv ---")
    filter_constructors_by_results()

    print("\n--- Filtering drivers.csv ---")
    filter_drivers_by_results()

    print("\n--- Filtering seasons.csv ---")
    filter_seasons_by_year()

    print("\n--- Filtering status.csv ---")
    filter_status_by_results()

    print("\n✅ Data pipeline finished.")
    print("   Cleaned files are available in data/processed/.\n")


if __name__ == "__main__":
    main()
    









    