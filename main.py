"""
Main entry point for the Formula 1 race outcome prediction project.

Pipeline:
1. Downloads the Kaggle dataset into data/raw/
2. Lists the available CSV files in data/raw/
3. Ensure data/processed/ exists
4. Filter races to recent seasons (2020–2025) and all race-based tables
5. Filter dimension tables (circuits, constructors, drivers, seasons, status)
6. Enriched processed tables (circuits, races, status) with extra information
7. Create progressive feature performance tables (drivers/constructors/sprint/qualifying/driver-circuit)
8. Create the final modelling dataset (model_dataset.csv) from the progressive performance tables
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

# 6) Enriched tables
from src.data_enrichment import (
    add_extra_info_on_circuits,
    fill_circuit_extra_info,
    add_extra_info_on_races,
    fill_races_distance_km,
    add_status_dnf_categories,)

# 7,8) Progressive feature performance tables and final modelling dataset
from src.features import (
    build_driver_race_base,
    build_driver_progressive_performance,
    build_constructor_progressive_performance,
    build_sprint_progressive_performance,
    build_qualifying_progressive_performance,
    build_driver_circuits_progressive_performance,
    build_model_dataset,)


def main() -> None:
    print("=== F1 Project: full data pipeline ===")

    # 1. Download dataset into data/raw/
    print("\n🟦 STEP 1 – Download raw dataset")
    data_raw_path: Path = download_dataset()

    # 2. List CSV files in data/raw/
    print("\n🟦 STEP 2 – List CSV files in data/raw/")
    csv_files = sorted(data_raw_path.glob("*.csv"))

    if not csv_files:
        print("❌ No CSV files found in data/raw/ after download.")
        print("    -> Check that the Kaggle download worked correctly.")
        return
    else:
        print("✅ CSV files available in data/raw/:")
        for f in csv_files:
            print("   -", f.name)
            
    # 3. Ensure data/processed/ exists
    print("\n🟦 STEP 3 – Ensure processed directory exists")
    processed_dir: Path = create_processed_folder()
    print(f"📁 Processed data directory: {processed_dir}")

    # 4. Filter races (2020–2025) and all race-based tables
    print("\n🟦 STEP 4 – Filter races and race-based tables")

    # 4.1 Filter races by year
    races_cleaned_path: Path = filter_races_by_year(start_year = 2020, end_year = 2025)

    # 4.2 Load filtered races to get raceIds
    races_df = pd.read_csv(races_cleaned_path)
    recent_race_ids = set(races_df["raceId"].unique())
    print(f"\n✅ Number of recent races: {len(recent_race_ids)}")

    # 4.3 Filter all tables that have a raceId column
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
            table_name = table_name,
            race_ids = recent_race_ids,
            raw_filename = raw_filename,)

    # 5. Filter dimension tables (no raceId column)
    print("\n🟦 STEP 5 – Filter dimension tables (circuits, constructors, drivers, seasons, status)")

    print("\n─── Filtering circuits.csv ───")
    filter_circuits_by_races()

    print("\n─── Filtering constructors.csv ───")
    filter_constructors_by_results()

    print("\n─── Filtering drivers.csv ───")
    filter_drivers_by_results()

    print("\n─── Filtering seasons.csv ───")
    filter_seasons_by_year()

    print("\n─── Filtering status.csv ───")
    filter_status_by_results()

    print("\n✅ Cleaning step finished. Cleaned files are available in data/processed/.")

    # 6. Data enrichment (circuits, races, status)
    print("\n🟦 STEP 6 – Data Enrichment")

    # 6.1 Circuits enrichment
    print("\n Enriching circuits_cleaned.csv with extra info...")
    circuits_file = add_extra_info_on_circuits()
    if circuits_file is None:
        print("❌ Error in add_extra_info_on_circuits()")
        return
    print("✅ circuits_cleaned enriched")

    print("\n Filling missing circuit fields...")
    circuits_file = fill_circuit_extra_info()
    if circuits_file is None:
        print("❌ Error in fill_circuit_extra_info()")
        return
    print("✅ circuits_cleaned fully filled")

    # 6.2 Races enrichment
    print("\n Enriching races_cleaned.csv with extra metadata...")
    races_file = add_extra_info_on_races()
    if races_file is None:
        print("❌ Error in add_extra_info_on_races()")
        return
    print("✅ races_cleaned enriched")

    print("\n Filling missing race distances (km)...")
    races_file = fill_races_distance_km()
    if races_file is None:
        print("❌ Error in fill_races_distance_km()")
        return
    print("✅ races distance info filled")

    # 6.3 Status enrichment
    print("\n Adding mechanical/crash/other categories to status_cleaned.csv...")
    status_file = add_status_dnf_categories()
    if status_file is None:
        print("❌ Error in add_status_dnf_categories()")
        return
    print("✅ Status categories enriched (mech/crash/other/no_dnf)")

    # 7. Create progressive feature performance tables
    print("\n🟦 STEP 7 – Build progressive feature performance tables")

    # 7.1 Base driver-race table
    print("\n Building driver_race_base.csv ...")
    driver_race_base_file = build_driver_race_base()
    if driver_race_base_file is None:
        print("❌ Error in build_driver_race_base()")
        return
    print(f"✅ driver_race_base created: {driver_race_base_file}")

    # 7.2 Drivers performance
    print("\n Building driver_progressive_performance.csv ...")
    drivers_perf_file = build_driver_progressive_performance()
    if drivers_perf_file is None:
        print("❌ Error in build_driver_progressive_performance()")
        return
    print(f"✅ driver_progressive_performance created: {drivers_perf_file}")

    # 7.3 Constructors performance
    print("\n Building constructor_progressive_performance.csv ...")
    constructors_perf_file = build_constructor_progressive_performance()
    if constructors_perf_file is None:
        print("❌ Error in build_constructor_progressive_performance()")
        return
    print(f"✅ constructor_progressive_performance created: {constructors_perf_file}")

    # 7.4 Sprint performance per driver
    print("\n Building sprint_progressive_performance.csv ...")
    sprint_perf_file = build_sprint_progressive_performance()
    if sprint_perf_file is None:
        print("❌ Error in build_sprint_progressive_performance()")
        return
    print(f"✅ sprint_progressive_performance created: {sprint_perf_file}")

    # 7.5 Qualifying performance per driver
    print("\n Building qualifying_progressive_performance.csv ...")
    quali_perf_file = build_qualifying_progressive_performance()
    if quali_perf_file is None:
        print("❌ Error in build_qualifying_progressive_performance()")
        return
    print(f"✅ qualifying_progressive_performance created: {quali_perf_file}")

    # 7.6 Driver x circuit performance
    print("\n Building driver_circuits_progressive_performance.csv ...")
    circuits_perf_file = build_driver_circuits_progressive_performance()
    if circuits_perf_file is None:
        print("❌ Error in build_driver_circuits_progressive_performance()")
        return
    print(f"✅ driver_circuits_progressive_performance created: {circuits_perf_file}")
    
    # 8. Create final modelling dataset
    print("\n🟦 STEP 8 – Build final modelling dataset")
    model_file = build_model_dataset()
    if model_file is None:
        print("❌ Error while building model_dataset.csv")
        return
    else:
        print(f"✅ model_dataset.csv successfully created")
        print(f"📂 Saved to: {model_file}")










if __name__ == "__main__":
    main()

    








    