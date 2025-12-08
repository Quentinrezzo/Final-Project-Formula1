"""
This module builds all time-aware (progressive) feature tables used in the
Formula 1 project.

Starting from the cleaned and enriched CSV files (2015–2025 seasons), it
computes progressive performance metrics for:

- drivers
- constructors (teams)
- sprints
- qualifying sessions
- driver–circuit combinations

For each entity, the progressive features at a given season or race use
only historical data available up to that point (N-1 logic). This ensures 
no data leakage when training machine-learning models to predict an
entire future season (e.g., forecasting 2026 using results up to 2025).

For exploratory data analysis (EDA) and visualisation, the driver_race_base.csv
table can provide a snapshot of drivers' performance. For example: filtering 
rows where year == 2025 gives the current performance. Note that in this table, 
columns prefixed with 'sprint_' correspond to sprint race outcomes, while the other 
columns describe the results of the main Grand Prix races.
"""

from pathlib import Path
import pandas as pd
import numpy as np

# Import processed_direction
from .data_loader import processed_direction

def build_driver_race_base() -> Path:
    """
    Create the base modelling table with one row per driver and race.

    It joins several cleaned tables from data/processed/:
    - results_cleaned.csv
    - races_cleaned.csv
    - drivers_cleaned.csv
    - constructors_cleaned.csv
    - circuits_cleaned.csv
    - sprint_results_cleaned.csv

    The merge table is saved as: data/processed/driver_race_base.csv

    Returns:
        Path: Path to the saved driver_race_base.csv file.
    """

    # Define file paths 
    results_file = processed_direction / "results_cleaned.csv"
    races_file = processed_direction / "races_cleaned.csv"
    drivers_file = processed_direction / "drivers_cleaned.csv"
    constructors_file = processed_direction / "constructors_cleaned.csv"
    circuits_file = processed_direction / "circuits_cleaned.csv"
    sprint_file = processed_direction / "sprint_results_cleaned.csv"
    output_file = processed_direction / "driver_race_base.csv"

    # Load data
    try:
        results_df = pd.read_csv(results_file)
        races_df = pd.read_csv(races_file)
        drivers_df = pd.read_csv(drivers_file)
        constructors_df = pd.read_csv(constructors_file)
        circuits_df = pd.read_csv(circuits_file)
        sprint_df = pd.read_csv(sprint_file) 
    except Exception as e:
        print(f"⚠️ Error while reading one of the cleaned files: {e}")
        return None

    # Prepare subsets of columns

    # Results: only useful columns
    results_columns = [
        "raceId",
        "driverId",
        "constructorId",
        "position",
        "points",
        "laps",
        "milliseconds",
        "statusId",]

    results_small = results_df[results_columns].copy()
    
    # Races: only useful columns
    races_columns = [
        "raceId",
        "year",
        "round",
        "circuitId",
        "name",
        "date",
        "race_distance_km",]

    races_small = races_df[races_columns].copy()
    races_small = races_small.rename(columns = {"name": "race_name"})

    # Drivers: identity information
    drivers_columns = [
        "driverId",
        "driverRef",
        "code",
        "forename",
        "surname",
        "nationality",]

    drivers_small = drivers_df[drivers_columns].copy()
    drivers_small = drivers_small.rename(columns = {"nationality": "driver_nationality"})

    # Constructors: identity information
    constructors_columns = [
        "constructorId",
        "constructorRef",
        "name",
        "nationality",]

    constructors_small = constructors_df[constructors_columns].copy()
    constructors_small = constructors_small.rename(columns = {"name": "constructor_name", "nationality": "constructor_nationality",})

    # Circuits: track information
    circuits_columns = [
        "circuitId",
        "name",
        "location",
        "country",
        "alt",
        "length_km",
        "is_night_race",
        "track_type",]

    circuits_small = circuits_df[circuits_columns].copy()
    circuits_small = circuits_small.rename(columns = {"name": "circuit_name"})

    # Sprint results: only useful columns
    sprint_columns = ["raceId", "driverId", "position", "points"]
    sprint_small = sprint_df[sprint_columns].copy()
    sprint_small = sprint_small.rename(columns = {"position": "sprint_position", "points": "sprint_points",})

    # Merge
    base_df = results_small.merge(races_small, on = "raceId", how = "left", validate = "many_to_one",)
    base_df = base_df.merge(drivers_small, on = "driverId", how = "left", validate = "many_to_one",)
    base_df = base_df.merge(constructors_small, on = "constructorId", how = "left", validate = "many_to_one",)
    base_df = base_df.merge(circuits_small, on = "circuitId", how = "left", validate = "many_to_one",)
    base_df = base_df.merge(sprint_small, on = ["raceId", "driverId"], how = "left", validate = "one_to_one")

    # Sort
    ordered_columns = [
        "raceId",
        "driverId",
        "constructorId",
        "circuitId",
        "statusId",
        "year",
        "round",
        "date",
        "driverRef",
        "code",
        "forename",
        "surname",
        "driver_nationality",
        "constructorRef",
        "constructor_name",
        "constructor_nationality",
        "circuit_name",
        "race_name",
        "location",
        "country",
        "alt",
        "length_km",
        "race_distance_km",
        "is_night_race",
        "track_type",
        "laps",
        "milliseconds",
        "position",
        "points",
        "sprint_position",
        "sprint_points",]
    
    base_df = base_df[ordered_columns]

    # Save new table to 'processed' folder
    base_df.to_csv(output_file, index = False)

    # Check
    try:
        check_df = pd.read_csv(output_file)
        expected_columns = ordered_columns

        all_columns_present = all(col in check_df.columns for col in expected_columns)
        
        if not all_columns_present:
            print(f"❌ Columns missing in driver_race_base file saved to: {output_file}")
            return None
        else:
            print("✅ driver_race_base successfully created and filled")
            print(f"📁 Saved to: {output_file}")
            print(f" Rows: {len(check_df)}")
            
    except Exception as e:
        print(f"⚠️ Error while checking driver_race_base file: {e}")
        return None

    return output_file


def build_driver_progressive_performance() -> Path:
    """
    Create a progressive driver performance table by season.
    
    For each (driverId, year), the function:
      - starts from race-level data in driver_race_base.csv
      - counts races, finishes, DNFs, wins, podiums, top-10, points
      - builds a cumulative history over seasons for each driver
        (e.g. 2022 = all races from 2020 + 2021)
      - recomputes the main rate features from these cumulative counts:
        finish_rate, dnf_rate, mech/crash/other_dnf_rate, reliability_rate,
        points_per_race.

    The progressive performance table is saved
    as:data/processed/drivers_progressive_performance.csv

    Returns:
        Path: Path to the saved drivers_progressive_performance.csv file.
    """

    # Define file paths
    driver_race_file = processed_direction / "driver_race_base.csv"
    status_file = processed_direction / "status_cleaned.csv"
    output_file = processed_direction / "drivers_progressive_performance.csv"

    # Load data
    try:
        base_df = pd.read_csv(driver_race_file)
        status_df = pd.read_csv(status_file)
    except Exception as e:
        print(f"⚠️ Error while reading {driver_race_file} or {status_file}: {e}")
        return None

    # Create helper for aggregation
    df = base_df.copy()
    df["position"] = pd.to_numeric(df["position"], errors = "coerce").astype("Int64")
    df["points"] = pd.to_numeric(df["points"], errors = "coerce").fillna(0)
    df["win"] = df["position"] == 1
    df["podium"] = df["position"].between(1, 3, inclusive = "both")
    df["top10"] = df["position"].between(1, 10, inclusive = "both")
    df["position_num"] = pd.to_numeric(df["position"], errors = "coerce")
    df["position_sq"]  = df["position_num"] ** 2

    # Merge DNF categories from status_cleaned.csv
    status_small = status_df[["statusId", "is_mechanical", "is_crash", "is_other_dnf"]].copy()
    df = df.merge(status_small, on = "statusId", how = "left")

    # Determine finished using FIA rule: completed >= 90% of winner laps
    laps_by_race = df.groupby("raceId")["laps"].transform("max")
    df["finished"] = ((df["statusId"] == 1) | (df["laps"] >= 0.9 * laps_by_race))

    # Mechanical / crash / other DNFs
    df["mech_dnf"] = (~df["finished"]) & (df["is_mechanical"] == True)
    df["crash_dnf"] = (~df["finished"]) & (df["is_crash"] == True)
    df["other_dnf"] = (~df["finished"]) & (df["is_other_dnf"] == True)

    # Aggregate per driverId and year
    grouped_all = df.groupby(["driverId", "year"], as_index = True)

    perf_all = grouped_all.agg(
        driverRef = ("driverRef", "first"),
        forename = ("forename", "first"),
        surname = ("surname", "first"),
        driver_nationality = ("driver_nationality", "first"),
        races_count = ("raceId", "count"),
        finished_races = ("finished", "sum"),
        win_count = ("win", "sum"),
        podiums = ("podium", "sum"),
        top10_finishes = ("top10", "sum"),
        total_points = ("points", "sum"),
        mech_dnf_count = ("mech_dnf", "sum"),
        crash_dnf_count = ("crash_dnf", "sum"),
        other_dnf_count = ("other_dnf", "sum"),
        pos_sum = ("position_num", "sum"),
        pos_sq_sum = ("position_sq", "sum"),)
    
    # Did not finish (DNF) information
    perf_all["dnf_count"] = perf_all["races_count"] - perf_all["finished_races"]
    
    # Make stats progressive over seasons
    grouped_all = perf_all.sort_values(["driverId", "year"])

    cum_columns = [
        "races_count",
        "finished_races",
        "dnf_count",
        "mech_dnf_count",
        "crash_dnf_count",
        "other_dnf_count",
        "win_count",
        "podiums",
        "top10_finishes",
        "total_points",
        "pos_sum",
        "pos_sq_sum",]

    # cumulative sum by driver over years
    for col in cum_columns:
        history = grouped_all.groupby("driverId")[col].cumsum().shift(1)
        grouped_all[col] = history.fillna(0)

    # Recompute Derived rates and scores from cumulative counts
    races_nonzero = grouped_all["races_count"].replace(0, np.nan)

    grouped_all["finish_rate"] = grouped_all["finished_races"] / races_nonzero
    grouped_all["dnf_rate"] = grouped_all["dnf_count"] / races_nonzero
    grouped_all["mech_dnf_rate"] = grouped_all["mech_dnf_count"] / races_nonzero
    grouped_all["crash_dnf_rate"] = grouped_all["crash_dnf_count"] / races_nonzero
    grouped_all["other_dnf_rate"] = grouped_all["other_dnf_count"] / races_nonzero
    grouped_all["reliability_rate"] = 1.0 - grouped_all["mech_dnf_rate"]
    grouped_all["points_per_race"] = grouped_all["total_points"] / races_nonzero
    grouped_all["avg_finish_position"] = grouped_all["pos_sum"] / races_nonzero
    grouped_all["std_finish_position"] = np.sqrt(grouped_all["pos_sq_sum"] / races_nonzero - grouped_all["avg_finish_position"]**2)

    # Consistency index: higher = more consistent (lower std of position)
    grouped_all["consistency_index"] = 1.0 / (grouped_all["std_finish_position"].fillna(0) + 1.0)

    # Simple overall performance score
    grouped_all["performance_score"] = (grouped_all["points_per_race"].fillna(0) * grouped_all["finish_rate"].fillna(0))

    # Replace any remaining NaNs in rates by 0 (for drivers with very few data)
    rate_columns = [
        "finish_rate",
        "dnf_rate",
        "mech_dnf_rate",
        "crash_dnf_rate",
        "other_dnf_rate",
        "reliability_rate",
        "points_per_race",]
    
    grouped_all[rate_columns] = grouped_all[rate_columns].fillna(0)

    # Reset the index to have driverId as a column
    grouped_all = grouped_all.reset_index()

    # Sort
    ordered_columns = [
        "driverId",
        "year",
        "driverRef",
        "forename",
        "surname",
        "driver_nationality",
        "races_count",
        "finished_races",
        "finish_rate",
        "dnf_count",
        "dnf_rate",
        "mech_dnf_count",
        "mech_dnf_rate",
        "crash_dnf_count",
        "crash_dnf_rate",
        "other_dnf_count",
        "other_dnf_rate",
        "reliability_rate",
        "win_count",
        "podiums",
        "top10_finishes",
        "total_points",
        "points_per_race",
        "avg_finish_position",
        "std_finish_position",
        "consistency_index",
        "performance_score",]

    perf_df = grouped_all[ordered_columns]

    # Save new table to 'processed' folder
    perf_df.to_csv(output_file, index = False)

    # Check
    try:
        check_df = pd.read_csv(output_file)
        expected_columns = ordered_columns

        all_columns_present = all(col in check_df.columns for col in expected_columns)
        
        if not all_columns_present:
            print(f"❌ Columns missing in drivers_progressive_performance file saved to: {output_file}")
            return None
        else:
            print("✅ drivers_progressive_performance successfully created and filled")
            print(f"📁 Saved to: {output_file}")
            print(f" Rows: {len(check_df)}")
            
    except Exception as e:
        print(f"⚠️ Error while checking drivers_progressive_performance file: {e}")
        return None

    return output_file


def build_constructor_progressive_performance() -> Path:
    """
    Create a progressive constructor performance table by season.
    
    For each (constructorId, year), the function:
      - starts from race-level data in driver_race_base.csv
      - counts races, finishes, DNFs, wins, podiums, top-10, points
      - builds a cumulative history over seasons for each constructor
        (e.g. 2022 = all races from 2020 + 2021)
      - recomputes the main rate features from these cumulative counts:
        finish_rate, dnf_rate, mech/crash/other_dnf_rate, reliability_rate,
        points_per_race.
        
    The progressive performance table is saved 
    as: data/processed/constructors_progressive_performance.csv

    Returns:
        Path: Path to the saved constructors_progressive_performance.csv file.
    """

    # Define file paths
    driver_race_file = processed_direction / "driver_race_base.csv"
    status_file = processed_direction / "status_cleaned.csv"
    output_file = processed_direction / "constructors_progressive_performance.csv"

    # Load data
    try:
        base_df = pd.read_csv(driver_race_file)
        status_df = pd.read_csv(status_file)
    except Exception as e:
        print(f"⚠️ Error while reading {driver_race_file} or {status_file}: {e}")
        return None
        
    # Create helper for aggregation
    df = base_df.copy()
    df["position"] = pd.to_numeric(df["position"], errors = "coerce").astype("Int64")
    df["points"] = pd.to_numeric(df["points"], errors = "coerce").fillna(0)
    df["win"] = df["position"] == 1
    df["podium"] = df["position"].between(1, 3, inclusive = "both")
    df["top10"] = df["position"].between(1, 10, inclusive = "both")
    df["position_num"] = pd.to_numeric(df["position"], errors="coerce")
    df["position_sq"] = df["position_num"] ** 2
    
    # Merge DNF categories from status_cleaned.csv
    status_small = status_df[["statusId", "is_mechanical", "is_crash", "is_other_dnf"]].copy()
    df = df.merge(status_small, on = "statusId", how = "left")

    # Determine finished using FIA rule: completed >= 90% of winner laps
    laps_by_race = df.groupby("raceId")["laps"].transform("max")
    df["finished"] = ((df["statusId"] == 1) | (df["laps"] >= 0.9 * laps_by_race))

    # Mechanical / crash / other DNFs
    df["mech_dnf"] = (~df["finished"]) & (df["is_mechanical"] == True)
    df["crash_dnf"] = (~df["finished"]) & (df["is_crash"] == True)
    df["other_dnf"] = (~df["finished"]) & (df["is_other_dnf"] == True)

    # Aggregate per constructorId and year
    grouped_all = df.groupby(["constructorId", "year"], as_index = True)

    perf_all = grouped_all.agg(
        constructorRef = ("constructorRef", "first"),
        constructor_name = ("constructor_name", "first"),
        constructor_nationality = ("constructor_nationality", "first"),
        races_count = ("raceId", "count"),
        finished_races = ("finished", "sum"),
        win_count = ("win", "sum"),
        podiums = ("podium", "sum"),
        top10_finishes = ("top10", "sum"),
        total_points = ("points", "sum"),
        mech_dnf_count = ("mech_dnf", "sum"),
        crash_dnf_count = ("crash_dnf", "sum"),
        other_dnf_count = ("other_dnf", "sum"),
        pos_sum = ("position_num", "sum"),
        pos_sq_sum = ("position_sq", "sum"),)

    # Did not finish (DNF) information
    perf_all["dnf_count"] = perf_all["races_count"] - perf_all["finished_races"]

    # Make stats progressive over seasons
    grouped_all = perf_all.sort_values(["constructorId", "year"])

    cum_columns = [
        "races_count",
        "finished_races",
        "dnf_count",
        "mech_dnf_count",
        "crash_dnf_count",
        "other_dnf_count",
        "win_count",
        "podiums",
        "top10_finishes",
        "total_points",
        "pos_sum",
        "pos_sq_sum",]

    # cumulative sum by constructor over years
    for col in cum_columns:
        history = grouped_all.groupby("constructorId")[col].cumsum().shift(1)
        grouped_all[col] = history.fillna(0)

    # Recompute derived rates and scores from cumulative counts
    races_nonzero = grouped_all["races_count"].replace(0, np.nan)

    grouped_all["finish_rate"] = grouped_all["finished_races"] / races_nonzero
    grouped_all["dnf_rate"] = grouped_all["dnf_count"] / races_nonzero
    grouped_all["mech_dnf_rate"] = grouped_all["mech_dnf_count"] / races_nonzero
    grouped_all["crash_dnf_rate"] = grouped_all["crash_dnf_count"] / races_nonzero
    grouped_all["other_dnf_rate"] = grouped_all["other_dnf_count"] / races_nonzero
    grouped_all["reliability_rate"] = 1.0 - grouped_all["mech_dnf_rate"]
    grouped_all["points_per_race"] = grouped_all["total_points"] / races_nonzero
    grouped_all["avg_finish_position"] = grouped_all["pos_sum"] / races_nonzero
    grouped_all["std_finish_position"] = np.sqrt(grouped_all["pos_sq_sum"] / races_nonzero - grouped_all["avg_finish_position"] ** 2)

    # Consistency index: higher = more consistent (lower std of position)
    grouped_all["consistency_index"] = 1.0 / (grouped_all["std_finish_position"].fillna(0) + 1.0)

    # Simple overall performance score
    grouped_all["performance_score"] = (grouped_all["points_per_race"].fillna(0) * grouped_all["finish_rate"].fillna(0))
    
    # Replace any remaining NaNs in rates by 0 (for constructors with very few data)
    rate_columns = [
        "finish_rate",
        "dnf_rate",
        "mech_dnf_rate",
        "crash_dnf_rate",
        "other_dnf_rate",
        "reliability_rate",
        "points_per_race",]

    grouped_all[rate_columns] = grouped_all[rate_columns].fillna(0)

    # Reset index to have constructorId and year as columns
    grouped_all = grouped_all.reset_index()
    
    # Sort
    ordered_columns = [
        "constructorId",
        "year",
        "constructorRef",
        "constructor_name",
        "constructor_nationality",
        "races_count",
        "finished_races",
        "finish_rate",
        "dnf_count",
        "dnf_rate",
        "mech_dnf_count",
        "mech_dnf_rate",
        "crash_dnf_count",
        "crash_dnf_rate",
        "other_dnf_count",
        "other_dnf_rate",
        "reliability_rate",
        "win_count",
        "podiums",
        "top10_finishes",
        "total_points",
        "points_per_race",
        "avg_finish_position",
        "std_finish_position",
        "consistency_index",
        "performance_score",]

    perf_df = grouped_all[ordered_columns]
    
    # Save new table to 'processed' folder
    perf_df.to_csv(output_file, index = False)

    # Check
    try:
        check_df = pd.read_csv(output_file)
        expected_columns = ordered_columns

        all_columns_present = all(col in check_df.columns for col in expected_columns)
        
        if not all_columns_present:
            print(f"❌ Columns missing in constructors_progressive_performance file saved to: {output_file}")
            return None
        else:
            print("✅ constructors_progressive_performance successfully created and filled")
            print(f"📁 Saved to: {output_file}")
            print(f" Rows: {len(check_df)}")
            
    except Exception as e:
        print(f"⚠️ Error while checking constructors_progressive_performance file: {e}")
        return None

    return output_file


def build_sprint_progressive_performance() -> Path:
    """
    Create a progressive sprint performance table by season.
    
    For each (driverId, year), the function:
      - starts from sprint-level data in sprint_results_cleaned.csv
      - counts sprints, finishes, DNFs, wins, podiums, top-8, points
      - builds a cumulative history over seasons for each driver
        (e.g. 2022 = all races from 2020 + 2021)
      - recomputes the main rate features from these cumulative counts:
        finish_rate, dnf_rate, mech/crash/other_dnf_rate, reliability_rate,
        points_per_race.
        
    The progressive performance table is saved 
    as: data/processed/drivers_sprint_progressive_performance.csv

    Returns:
        Path: Path to the saved drivers_sprint_progressive_performance.csv file.
    """

    # Define file paths
    sprint_results_file = processed_direction / "sprint_results_cleaned.csv"
    races_file = processed_direction / "races_cleaned.csv"
    status_file = processed_direction / "status_cleaned.csv"
    drivers_file = processed_direction / "drivers_cleaned.csv"
    output_file = processed_direction / "drivers_sprint_progressive_performance.csv"

    # Load data
    try:
        sprint_df = pd.read_csv(sprint_results_file)
        races_df = pd.read_csv(races_file)
        status_df = pd.read_csv(status_file)
        drivers_df = pd.read_csv(drivers_file)
    except Exception as e:
        print(f"⚠️ Error while reading {sprint_results_file} or {races_file} "
            f"or {status_file} or {drivers_file}: {e}")
        return None

    # Add year from races table
    sprint_df = sprint_df.merge(races_df[["raceId", "year"]], on = "raceId", how = "left")

    # Add driver information
    driver_small = drivers_df[["driverId", "driverRef", "forename", "surname", "nationality"]].rename(columns = {"nationality": "driver_nationality"})
    sprint_df = sprint_df.merge(driver_small, on = "driverId", how = "left")
    
    # Create helper for aggregation
    df = sprint_df.copy()
    df["position"] = pd.to_numeric(df["position"], errors = "coerce").astype("Int64")
    df["points"] = pd.to_numeric(df["points"], errors = "coerce").fillna(0)
    df["win"] = df["position"] == 1
    df["podium"] = df["position"].between(1, 3, inclusive = "both")
    df["top8"] = df["position"].between(1, 8, inclusive = "both")
    df["position_num"] = pd.to_numeric(df["position"], errors = "coerce")
    df["position_sq"] = df["position_num"] ** 2

    # Merge DNF categories from status_cleaned.csv
    status_small = status_df[["statusId", "is_mechanical", "is_crash", "is_other_dnf"]].copy()
    df = df.merge(status_small, on = "statusId", how = "left")

    # Determine finished using FIA rule: completed >= 90% of winner laps
    laps_by_race = df.groupby("raceId")["laps"].transform("max")
    df["finished"] = ((df["statusId"] == 1) | (df["laps"] >= 0.9 * laps_by_race))

    # Mechanical / crash / other DNFs
    df["mech_dnf"] = (~df["finished"]) & (df["is_mechanical"] == True)
    df["crash_dnf"] = (~df["finished"]) & (df["is_crash"] == True)
    df["other_dnf"] = (~df["finished"]) & (df["is_other_dnf"] == True)

    # Aggregate per driverId and year
    grouped_all = df.groupby(["driverId", "year"], as_index = True)

    perf_all = grouped_all.agg(
        driverRef = ("driverRef", "first"),
        forename = ("forename", "first"),
        surname = ("surname", "first"),
        driver_nationality = ("driver_nationality", "first"),
        races_count = ("raceId", "count"),
        finished_races = ("finished", "sum"),
        win_count = ("win", "sum"),
        podiums = ("podium", "sum"),
        top8_finishes = ("top8", "sum"),
        total_points = ("points", "sum"),
        mech_dnf_count = ("mech_dnf", "sum"),
        crash_dnf_count = ("crash_dnf", "sum"),
        other_dnf_count = ("other_dnf", "sum"),
        pos_sum = ("position_num", "sum"),
        pos_sq_sum = ("position_sq", "sum"),)

    # Did not finish (DNF) information
    perf_all["dnf_count"] = perf_all["races_count"] - perf_all["finished_races"]

    # Make stats progressive over seasons
    grouped_all = perf_all.sort_values(["driverId", "year"])

    cum_columns = [
        "races_count",
        "finished_races",
        "dnf_count",
        "mech_dnf_count",
        "crash_dnf_count",
        "other_dnf_count",
        "win_count",
        "podiums",
        "top8_finishes",
        "total_points",
        "pos_sum",
        "pos_sq_sum",]

    # cumulative sum by driver over years
    for col in cum_columns:
        history = grouped_all.groupby("driverId")[col].cumsum().shift(1)
        grouped_all[col] = history.fillna(0)
        
    # Recompute derived rates and scores from cumulative counts
    races_nonzero = grouped_all["races_count"].replace(0, np.nan)

    grouped_all["finish_rate"] = grouped_all["finished_races"] / races_nonzero
    grouped_all["dnf_rate"] = grouped_all["dnf_count"] / races_nonzero
    grouped_all["mech_dnf_rate"] = grouped_all["mech_dnf_count"] / races_nonzero
    grouped_all["crash_dnf_rate"] = grouped_all["crash_dnf_count"] / races_nonzero
    grouped_all["other_dnf_rate"] = grouped_all["other_dnf_count"] / races_nonzero
    grouped_all["reliability_rate"] = 1.0 - grouped_all["dnf_rate"]
    grouped_all["points_per_race"] = grouped_all["total_points"] / races_nonzero
    grouped_all["avg_finish_position"] = grouped_all["pos_sum"] / races_nonzero
    grouped_all["std_finish_position"] = np.sqrt(grouped_all["pos_sq_sum"] / races_nonzero - grouped_all["avg_finish_position"] ** 2)

    # Consistency index: higher = more consistent (lower std of position)
    grouped_all["consistency_index"] = 1.0 / (grouped_all["std_finish_position"].fillna(0) + 1.0)
    
    # Simple overall performance score
    grouped_all["performance_score"] = (grouped_all["points_per_race"].fillna(0) * grouped_all["finish_rate"].fillna(0))
    
    # Replace any remaining NaNs in rates by 0 (for drivers with very few data)
    rate_columns = [
        "finish_rate",
        "dnf_rate",
        "mech_dnf_rate",
        "crash_dnf_rate",
        "other_dnf_rate",
        "reliability_rate",
        "points_per_race",]

    grouped_all[rate_columns] = grouped_all[rate_columns].fillna(0)

    # Reset index to have driverId and year as columns
    grouped_all = grouped_all.reset_index()

    # Sort
    ordered_columns = [
        "driverId",
        "year",
        "driverRef",
        "forename",
        "surname",
        "driver_nationality",
        "races_count",
        "finished_races",
        "finish_rate",
        "dnf_count",
        "dnf_rate",
        "mech_dnf_count",
        "mech_dnf_rate",
        "crash_dnf_count",
        "crash_dnf_rate",
        "other_dnf_count",
        "other_dnf_rate",
        "reliability_rate",
        "win_count",
        "podiums",
        "top8_finishes",
        "total_points",
        "points_per_race",
        "avg_finish_position",
        "std_finish_position",
        "consistency_index",
        "performance_score",]

    perf_df = grouped_all[ordered_columns]

    # Save new table to 'processed' folder
    perf_df.to_csv(output_file, index = False)

    # Check
    try:
        check_df = pd.read_csv(output_file)
        expected_columns = ordered_columns

        all_columns_present = all(col in check_df.columns for col in expected_columns)
        
        if not all_columns_present:
            print(f"❌ Columns missing in drivers_sprint_progressive_performance file saved to: {output_file}")
            return None
        else:
            print("✅ drivers_sprint_progressive_performance successfully created and filled")
            print(f"📁 Saved to: {output_file}")
            print(f" Rows: {len(check_df)}")
            
    except Exception as e:
        print(f"⚠️ Error while checking drivers_sprint_progressive_performance file: {e}")
        return None

    return output_file


def build_qualifying_progressive_performance() -> Path:
    """
    Create a progressive qualifying performance table by season.
    
    For each (driverId, year), the function:
      - starts from qualifying-level data in qualifying_cleaned.csv
      - counts qualifying sessions, valid sessions, poles, front-row starts,
        top-5 / top-10 starts, Q2 / Q3 appearances
      - builds a cumulative history over seasons for each driver
        (e.g. 2022 = all races from 2020 + 2021)
      - recomputes the main rate features from these cumulative counts:
        sessions_rate, pole_rate, q3_rate, consistency_index,
        qualifying_performance_score.
        
    The progressive performance table is saved 
    as: data/processed/drivers_qualifying_progressive_performance.csv

    Returns:
        Path: Path to the saved drivers_qualifying_progressive_performance.csv file.
    """
    
    # Define file paths
    qualifying_file = processed_direction / "qualifying_cleaned.csv"
    races_file = processed_direction / "races_cleaned.csv"
    drivers_file = processed_direction / "drivers_cleaned.csv"
    output_file = processed_direction / "drivers_qualifying_progressive_performance.csv"

    # Load data
    try:
        quali_df = pd.read_csv(qualifying_file)
        races_df = pd.read_csv(races_file)
        drivers_df = pd.read_csv(drivers_file)
    except Exception as e:
        print(f"⚠️ Error while reading {qualifying_file}, {races_file} or {drivers_file}: {e}")
        return None

    # Add year from races table
    quali_df = quali_df.merge(races_df[["raceId", "year"]], on = "raceId", how = "left")

    # Add driver information
    driver_small = drivers_df[["driverId", "driverRef", "forename", "surname", "nationality"]].rename(columns = {"nationality": "driver_nationality"})
    quali_df = quali_df.merge(driver_small, on = "driverId", how = "left")

    # Create helper for aggregation
    df = quali_df.copy()
    df["position"] = pd.to_numeric(df["position"], errors = "coerce").astype("Int64")
    df["session"] = 1
    df["valid_session"] = df["position"].notna()
    df["pole"] = df["position"] == 1
    df["front_row"] = df["position"].between(1, 2, inclusive = "both")
    df["top5"] = df["position"].between(1, 5, inclusive = "both")
    df["top10"] = df["position"].between(1, 10, inclusive = "both")
    df["in_q3"] = df.get("q3").notna() if "q3" in df.columns else False
    df["in_q2"] = df.get("q2").notna() if "q2" in df.columns else False
    df["position_num"] = df["position"]
    df["position_sq"] = df["position_num"] ** 2

    # Aggregate per driverId (all qualifying sessions)
    grouped_all = df.groupby(["driverId", "year"], as_index = True)

    perf_all = grouped_all.agg(
        driverRef = ("driverRef", "first"),
        forename = ("forename", "first"),
        surname = ("surname", "first"),
        driver_nationality = ("driver_nationality", "first"),
        sessions_count = ("session", "sum"),
        sessions_quali = ("valid_session", "sum"),
        top5_finishes = ("top5", "sum"),
        top10_finishes = ("top10", "sum"),
        pole_count = ("pole", "sum"),
        front_row_count = ("front_row", "sum"),
        q2_appearances = ("in_q2", "sum"),
        q3_appearances = ("in_q3", "sum"),
        pos_sum = ("position_num", "sum"),
        pos_sq_sum = ("position_sq", "sum"),)

    # Make stats progressive over seasons
    grouped_all = perf_all.sort_values(["driverId", "year"])

    cum_columns = [
        "sessions_count",
        "sessions_quali",
        "top5_finishes",
        "top10_finishes",
        "pole_count",
        "front_row_count",
        "q2_appearances",
        "q3_appearances",
        "pos_sum",
        "pos_sq_sum",]

    # cumulative sum by driver over years
    for col in cum_columns:
        history = grouped_all.groupby("driverId")[col].cumsum().shift(1)
        grouped_all[col] = history.fillna(0)
    
    # Recompute derived rates and scores from cumulative counts
    sessions_nonzero = grouped_all["sessions_count"].replace(0, np.nan)
    valid_nonzero = grouped_all["sessions_quali"].replace(0, np.nan)

    grouped_all["sessions_rate"] = grouped_all["sessions_quali"] / sessions_nonzero
    grouped_all["pole_rate"] = grouped_all["pole_count"] / valid_nonzero
    grouped_all["q3_rate"] = grouped_all["q3_appearances"] / valid_nonzero
    grouped_all["top10_rate"] = grouped_all["top10_finishes"] / valid_nonzero
    grouped_all["front_row_rate"] = grouped_all["front_row_count"] / valid_nonzero
    grouped_all["avg_quali_position"] = grouped_all["pos_sum"] / sessions_nonzero
    grouped_all["std_quali_position"] = np.sqrt(grouped_all["pos_sq_sum"] / sessions_nonzero - grouped_all["avg_quali_position"] ** 2)

    # Consistency index: higher = more consistent (lower std of position)
    grouped_all["consistency_index"] = 1.0 / (grouped_all["std_quali_position"].fillna(0) + 1.0)
    
    # Simple overall performance score
    # 1.0 ~ always P1, 0.5 ~ around P11, ~0.0 ~ very far back
    grouped_all["performance_score"] = ((21.0 - grouped_all["avg_quali_position"].fillna(20.0)) / 20.0)
    
    # Replace any remaining NaNs in rates by 0 (for drivers with very few data)
    rate_columns = [
        "sessions_rate",
        "pole_rate",
        "q3_rate",
        "top10_rate",
        "front_row_rate",
        "consistency_index",
        "performance_score",]

    grouped_all[rate_columns] = grouped_all[rate_columns].fillna(0)
    
    # Reset index to have driverId and year as columns
    grouped_all = grouped_all.reset_index()
    
    # Sort
    ordered_columns = [
        "driverId",
        "year",
        "driverRef",
        "forename",
        "surname",
        "driver_nationality",
        "sessions_count",
        "sessions_quali",
        "sessions_rate",
        "top5_finishes",
        "top10_finishes",
        "top10_rate",
        "pole_count",
        "pole_rate",
        "front_row_count",
        "front_row_rate",
        "q2_appearances",
        "q3_appearances",
        "q3_rate",
        "avg_quali_position",
        "std_quali_position",
        "consistency_index",
        "performance_score",]

    perf_df = grouped_all[ordered_columns]
    
    # Save new table to 'processed' folder
    perf_df.to_csv(output_file, index = False)

    # Check
    try:
        check_df = pd.read_csv(output_file)
        expected_columns = ordered_columns

        all_columns_present = all(col in check_df.columns for col in expected_columns)
        
        if not all_columns_present:
            print(f"❌ Columns missing in drivers_qualifying_progressive_performance file saved to: {output_file}")
            return None
        else:
            print("✅ drivers_qualifying_progressive_performance successfully created and filled")
            print(f"📁 Saved to: {output_file}")
            print(f" Rows: {len(check_df)}")
            
    except Exception as e:
        print(f"⚠️ Error while checking drivers_qualifying_progressive_performance file: {e}")
        return None

    return output_file


def build_driver_circuits_progressive_performance() -> Path:
    """
    Create a progressive driver-circuit performance table by season.
    
    For each (driverId, circuitId, year), the function:
      - starts from race-level data in driver_race_base.csv
      - counts races, finishes, DNFs, wins, podiums, top-5, top-10, points
      - builds a cumulative history over seasons for each driver-circuit
        (e.g. 2022 = all races from 2020 + 2021)
      - recomputes the main rate features from these cumulative counts:
        finish_rate, dnf_rate, mech/crash/other_dnf_rate, reliability_rate,
        points_per_race.
        
    The progressive performance table is saved 
    as: data/processed/drivers_circuit_progressive_performance.csv

    Returns:
        Path: Path to the saved drivers_circuit_progressive_performance.csv file.
    """
    
    # Define file paths
    driver_race_file = processed_direction / "driver_race_base.csv"
    status_file = processed_direction / "status_cleaned.csv"
    output_file = processed_direction / "drivers_circuit_progressive_performance.csv"

    # Load data
    try:
        base_df = pd.read_csv(driver_race_file)
        status_df = pd.read_csv(status_file)
    except Exception as e:
        print(f"⚠️ Error while reading {driver_race_file} or {status_file}: {e}")
        return None
    
    # Create helper for aggregation
    df = base_df.copy()
    df["position"] = pd.to_numeric(df["position"], errors = "coerce").astype("Int64")
    df["win"] = df["position"] == 1
    df["podium"] = df["position"].between(1, 3, inclusive = "both")
    df["top5"] = df["position"].between(1, 5, inclusive = "both")
    df["top10"] = df["position"].between(1, 10, inclusive = "both")
    df["position_num"] = pd.to_numeric(df["position"], errors = "coerce")
    df["position_sq"] = df["position_num"] ** 2

    # Merge DNF categories from status_cleaned.csv
    status_small = status_df[["statusId", "is_mechanical", "is_crash", "is_other_dnf"]].copy()
    df = df.merge(status_small, on = "statusId", how = "left")

    # Determine finished using FIA rule: completed >= 90% of winner laps
    laps_by_race = df.groupby("raceId")["laps"].transform("max")
    df["finished"] = ((df["statusId"] == 1) | (df["laps"] >= 0.9 * laps_by_race))

    # Mechanical / crash / other DNFs
    df["mech_dnf"] = (~df["finished"]) & (df["is_mechanical"] == True)
    df["crash_dnf"] = (~df["finished"]) & (df["is_crash"] == True)
    df["other_dnf"] = (~df["finished"]) & (df["is_other_dnf"] == True)

    # Aggregate per driverId, circuitId and year
    grouped_all = df.groupby(["driverId", "circuitId", "year"], as_index = True)

    perf_all = grouped_all.agg(
        driverRef = ("driverRef", "first"),
        forename = ("forename", "first"),
        surname = ("surname", "first"),
        driver_nationality = ("driver_nationality", "first"),
        races_count = ("raceId", "count"),
        finished_races = ("finished", "sum"),
        win_count = ("win", "sum"),
        podiums = ("podium", "sum"),
        top5_finishes = ("top5", "sum"),
        top10_finishes = ("top10", "sum"),
        points_scored = ("points", "sum"),
        mech_dnf_count = ("mech_dnf", "sum"),
        crash_dnf_count = ("crash_dnf", "sum"),
        other_dnf_count = ("other_dnf", "sum"),
        pos_sum = ("position_num", "sum"),
        pos_sq_sum = ("position_sq", "sum"),)

    # Did not finish (DNF) information
    perf_all["dnf_count"] = perf_all["races_count"] - perf_all["finished_races"]

    # Make stats progressive over seasons
    grouped_all = perf_all.sort_values(["driverId", "circuitId", "year"])

    cum_cols = [
        "races_count",
        "finished_races",
        "dnf_count",
        "mech_dnf_count",
        "crash_dnf_count",
        "other_dnf_count",
        "win_count",
        "podiums",
        "top5_finishes",
        "top10_finishes",
        "points_scored",
        "pos_sum",
        "pos_sq_sum",]
    
    # cumulative sum by driver and circuit over years
    for col in cum_cols:
        history = grouped_all.groupby(["driverId", "circuitId"])[col].cumsum().shift(1)
        grouped_all[col] = history.fillna(0)

    # Recompute derived rates and scores from cumulative counts
    races_nonzero = grouped_all["races_count"].replace(0, np.nan)

    grouped_all["finish_rate"] = grouped_all["finished_races"] / races_nonzero
    grouped_all["dnf_rate"] = grouped_all["dnf_count"] / races_nonzero
    grouped_all["mech_dnf_rate"] = grouped_all["mech_dnf_count"] / races_nonzero
    grouped_all["crash_dnf_rate"] = grouped_all["crash_dnf_count"] / races_nonzero
    grouped_all["other_dnf_rate"] = grouped_all["other_dnf_count"] / races_nonzero
    grouped_all["points_per_race"] = grouped_all["points_scored"] / races_nonzero
    grouped_all["avg_finish_position"] = grouped_all["pos_sum"] / races_nonzero
    grouped_all["std_finish_position"] = np.sqrt(grouped_all["pos_sq_sum"] / races_nonzero - grouped_all["avg_finish_position"] ** 2)
    
    # Consistency index: higher = more consistent (lower std of position)
    grouped_all["consistency_index"] = 1.0 / (grouped_all["std_finish_position"].fillna(0) + 1.0)
    
    # Replace any remaining NaNs in rates by 0 (for drivers with very few data)
    rate_columns = [
        "finish_rate",
        "dnf_rate",
        "mech_dnf_rate",
        "crash_dnf_rate",
        "other_dnf_rate",
        "points_per_race",]

    grouped_all[rate_columns] = grouped_all[rate_columns].fillna(0)

    # Reset index to have driverId, circuitId and year as columns
    grouped_all = grouped_all.reset_index()

    # Sort
    ordered_columns = [
        "driverId",
        "circuitId",
        "year",
        "races_count",
        "finished_races",
        "finish_rate",
        "dnf_count",
        "dnf_rate",
        "mech_dnf_count",
        "mech_dnf_rate",
        "crash_dnf_count",
        "crash_dnf_rate",
        "other_dnf_count",
        "other_dnf_rate",
        "win_count",
        "podiums",
        "top5_finishes",
        "top10_finishes",
        "points_scored",
        "points_per_race",
        "avg_finish_position",
        "std_finish_position",
        "consistency_index",]

    perf_df = grouped_all[ordered_columns]

    # Save new table to 'processed' folder
    perf_df.to_csv(output_file, index = False)

    # Check
    try:
        check_df = pd.read_csv(output_file)
        expected_columns = ordered_columns

        all_columns_present = all(col in check_df.columns for col in expected_columns)
        
        if not all_columns_present:
            print(f"❌ Columns missing in drivers_circuit_progressive_performance file saved to: {output_file}")
            return None
        else:
            print("✅ drivers_circuit_progressive_performance successfully created and filled")
            print(f"📁 Saved to: {output_file}")
            print(f" Rows: {len(check_df)}")
            
    except Exception as e:
        print(f"⚠️ Error while checking drivers_circuit_progressive_performance file: {e}")
        return None

    return output_file


def build_model_dataset() -> Path:
    """
    Create the final modelling dataset at race-entry level. It keeps only the
    last 5 seasons from driver_race_base.csv. So the modelling dataset always focuses 
    on the most recent performance.

    It creates one row per driver/constructor in each race, with:
    - base race information
    - enriched status category (mechanical / crash / other / no_dnf),
    - race and circuit data (year, round, distance, etc.),
    - driver & constructor identity columns,
    - global progressive driver performance features,
    - global progressive constructor performance features,
    - driver progressive sprint performance features,
    - driver progressive qualifying performance features,
    - driver-circuit progressive performance features,
    - target columns for the prediction tasks (win, top3, top10, points).

    The final table is saved as: data/processed/model_dataset.csv

    Returns:
        Path: Path to the saved model_dataset.csv file.
    """

    # Define file paths
    driver_race_file = processed_direction / "driver_race_base.csv"
    drivers_perf_file = processed_direction / "drivers_progressive_performance.csv"
    constructors_perf_file = processed_direction / "constructors_progressive_performance.csv"
    sprint_perf_file = processed_direction / "drivers_sprint_progressive_performance.csv"
    quali_perf_file = processed_direction / "drivers_qualifying_progressive_performance.csv"
    circuit_perf_file = processed_direction / "drivers_circuit_progressive_performance.csv"
    races_file = processed_direction / "races_cleaned.csv"
    circuits_file = processed_direction / "circuits_cleaned.csv"
    status_file = processed_direction / "status_cleaned.csv"
    output_file = processed_direction / "model_dataset.csv"

    # Load data
    try:
        base_df = pd.read_csv(driver_race_file)
        drivers_perf_df = pd.read_csv(drivers_perf_file)
        constructors_perf_df = pd.read_csv(constructors_perf_file)
        sprint_perf_df = pd.read_csv(sprint_perf_file)
        quali_perf_df = pd.read_csv(quali_perf_file)
        circuit_perf_df = pd.read_csv(circuit_perf_file)
        races_df = pd.read_csv(races_file)
        circuits_df = pd.read_csv(circuits_file)
        status_df = pd.read_csv(status_file)
    except Exception as e:
        print(f"⚠️ Error while reading input files for model dataset: {e}")
        return None
        
    df = base_df.copy()
    
    # We remove existing race/circuit columns to avoid duplicates later on
    drop_base_cols = [
        "grid",
        "year",
        "round",
        "circuitId",
        "race_name",
        "date",
        "race_distance_km",
        "name",
        "location",
        "country",
        "alt",
        "length_km",]

    df = df.drop(columns = [c for c in drop_base_cols if c in df.columns]).copy()

    # Merge status (mechanical / crash / other / no_dnf)
    status_colums = ["statusId"]
    status_colums += [c for c in ["is_mechanical", "is_crash", "is_other_dnf", "is_no_dnf"]
                      if c in status_df.columns]

    status_small = status_df[status_colums].copy()
    df = df.merge(status_small, on = "statusId", how = "left")
    df["dnf_category"] = (df["is_mechanical"] | df["is_crash"] | df["is_other_dnf"]).astype(int)

    # Add race-level data (year, round, distance, etc.)
    race_columns = [
        "raceId",
        "year",
        "round",
        "circuitId",
        "race_name",
        "date",
        "race_distance_km",]

    race_cols = [c for c in race_columns if c in races_df.columns]
    races_small = races_df[race_cols].copy()
    df = df.merge(races_small, on = "raceId", how = "left")
    
    # Add circuit-level data
    circuit_columns = [
        "circuitId",
        "name",
        "location",
        "country",
        "alt",
        "length_km",
        "is_night_race",
        "track_type",]

    circuit_cols = [c for c in circuit_columns if c in circuits_df.columns]
    circuits_small = circuits_df[circuit_cols].copy()
    
    # Rename to avoid name conflicts
    rename_map = {
        "name": "circuit_name",
        "location": "circuit_location",
        "country": "circuit_country",}
    
    circuits_small = circuits_small.rename(columns = rename_map)
    df = df.merge(circuits_small, on = "circuitId", how = "left")
    
    # Add global progressive driver performance features
    drv_drop = [c for c in ["driverRef", "forename", "surname", "driver_nationality"]
                if c in drivers_perf_df.columns]

    drivers_features = drivers_perf_df.drop(columns = drv_drop, errors = "ignore").copy()
    drivers_features = drivers_features.add_prefix("drv_")
    drivers_features = drivers_features.rename(columns = {"drv_driverId": "driverId", "drv_year": "year"})
    df = df.merge(drivers_features, on = ["driverId", "year"], how = "left")

    # Add global progressive constructor performance features
    const_drop = [c for c in ["constructor_name", "constructor_nationality"]
                  if c in constructors_perf_df.columns]

    constructors_features = constructors_perf_df.drop(columns = const_drop, errors = "ignore").copy()
    constructors_features = constructors_features.add_prefix("team_")
    constructors_features = constructors_features.rename(columns = {"team_constructorId": "constructorId", "team_year": "year"})
    df = df.merge(constructors_features, on = ["constructorId", "year"], how = "left")

    # Add driver progressive sprint performance features
    sprint_drop = [c for c in ["driverRef", "forename", "surname", "driver_nationality"]
                   if c in sprint_perf_df.columns]

    sprint_features = sprint_perf_df.drop(columns = sprint_drop, errors = "ignore").copy()
    sprint_features = sprint_features.add_prefix("sprint_")
    sprint_features = sprint_features.rename(columns = {"sprint_driverId": "driverId", "sprint_year": "year"})
    df = df.merge(sprint_features, on = ["driverId", "year"], how = "left")

    # Add driver progressive qualifying performance features
    quali_drop = [c for c in ["driverRef", "forename", "surname", "driver_nationality"]
                  if c in quali_perf_df.columns]

    quali_features = quali_perf_df.drop(columns = quali_drop, errors = "ignore").copy()
    quali_features = quali_features.add_prefix("quali_")
    quali_features = quali_features.rename(columns = {"quali_driverId": "driverId", "quali_year": "year"})
    df = df.merge(quali_features, on = ["driverId", "year"], how = "left")

    # Add driver-circuit progressive performance features
    dc_drop = [c for c in ["driverRef", "forename", "surname", "driver_nationality"]
               if c in circuit_perf_df.columns]

    driver_circuit_features = circuit_perf_df.drop(columns = dc_drop, errors = "ignore").copy()
    driver_circuit_features = driver_circuit_features.add_prefix("circ_")
    driver_circuit_features = driver_circuit_features.rename(columns = {"circ_driverId": "driverId", "circ_circuitId": "circuitId", "circ_year": "year",})
    df = df.merge(driver_circuit_features, on = ["driverId", "circuitId", "year"], how = "left")

    # Numerical columns
    num_cols = [c for c in ["position", "points", "sprint_position", "sprint_points"] if c in df.columns]
    if num_cols:
        df[num_cols] = df[num_cols].apply(pd.to_numeric, errors = "coerce")

    # Create sprint flag before dropping sprint results
    df["has_sprint"] = df["sprint_position"].notna().astype(int)
    
    # Create target columns
    df["target_top10"] = df["position"].between(1, 10, inclusive = "both").astype(int)
    df["target_top3"] = df["position"].between(1, 3, inclusive = "both").astype(int)
    df["target_win"] = (df["position"] == 1).astype(int)
    df["target_top8_sprint"] = df["sprint_position"].between(1, 8, inclusive = "both").fillna(False).astype(int)
    df["target_top3_sprint"] = df["sprint_position"].between(1, 3, inclusive = "both").fillna(False).astype(int)
    df["target_win_sprint"] = (df["sprint_position"] == 1).fillna(False).astype(int)
    
    # Resolve duplicate columns with _x / _y siffixes
    duplicate_sets = [
        ("circuit_name_x", "circuit_name_y", "circuit_name"),
        ("is_night_race_x", "is_night_race_y", "is_night_race"),
        ("track_type_x", "track_type_y", "track_type"),]
    
    for col_x, col_y, final in duplicate_sets:
        if col_x in df.columns and col_y in df.columns:
            df[final] = df[col_x].combine_first(df[col_y])
            df = df.drop(columns=[col_x, col_y])
        
        elif col_x in df.columns:
            df = df.rename(columns={col_x: final})
        
        elif col_y in df.columns:
            df = df.rename(columns={col_y: final})

    # Encode track_type (string -> category codes)
    if "track_type" in df.columns:
        df["track_type"] = df["track_type"].astype("category").cat.codes
        
    # Basic missing-value handling

    # Missing values
    num_columns = df.select_dtypes(include = ["number", "float64", "int64", "Int64"]).columns
    df[num_columns] = df[num_columns].fillna(0)
    
    category_columns = df.select_dtypes(include = ["object"]).columns
    df[category_columns] = df[category_columns].fillna("Unknown")

    # Sort
    key_columns = [
        "raceId",
        "driverId",
        "constructorId",
        "circuitId",
        "year",
        "round",
        "race_distance_km",
        "is_night_race",
        "track_type",
        "alt",
        "length_km",]
    
    metric_order = [
        "races_count",
        "finished_races",
        "finish_rate",
        "dnf_count",
        "dnf_rate",
        "mech_dnf_count",
        "mech_dnf_rate",
        "crash_dnf_count",
        "crash_dnf_rate",
        "other_dnf_count",
        "other_dnf_rate",
        "reliability_rate",
        "win_count",
        "podiums",
        "sessions_count",
        "sessions_quali",
        "sessions_rate",
        "top5_finishes",
        "top8_finishes",
        "top10_finishes",
        "top10_rate",
        "pole_count",
        "pole_rate",
        "front_row_count",
        "front_row_rate",
        "q1_appearances",
        "q2_appearances",
        "q3_appearances",
        "q3_rate",
        "avg_finish_position",
        "std_finish_position",
        "avg_quali_position",
        "std_quali_position",
        "total_points",
        "points_per_race",
        "points_scored",
        "consistency_index",
        "performance_score",]
    
    perf_prefixes = ["drv_", "team_", "sprint_", "quali_", "circ_"]
    
    ordered_first = [c for c in key_columns if c in df.columns]
    perf_cols: list[str] = []
    for prefix in perf_prefixes:
        for metric in metric_order:
            col = prefix + metric
            if col in df.columns:
                perf_cols.append(col)
        
        remaining_prefixed = [c for c in df.columns 
                              if c.startswith(prefix) and c not in perf_cols]
        perf_cols.extend(remaining_prefixed)
    
    already_used = set(ordered_first) | set(perf_cols)
    other_cols = [c for c in df.columns if c not in already_used]
        
    df = df[ordered_first + perf_cols + other_cols]
    
    # Remove leakage and low value columns
    leakage_cols = [
        "position",
        "points",
        "sprint_position",
        "sprint_points",
        "laps",
        "milliseconds",
        "statusId",
        "is_mechanical",
        "is_crash",
        "is_other_dnf",
        "is_no_dnf",
        "dnf_category",
        "drv_year",
        "team_year",
        "sprint_year",
        "quali_year",
        "circ_year",
        "team_constructorRef",]

    low_value_cols = [
        "race_name",
        "date",
        "driverRef",
        "code",
        "forename",
        "surname",
        "driver_nationality",
        "constructorRef",
        "constructor_name",
        "constructor_nationality",
        "circuit_name",
        "circuit_location",
        "circuit_country",
        "quali_sessions_rate",
        "quali_q3_rate",
        "circ_other_dnf_count",
        "circ_other_dnf_rate",
        "length_km",]
    
    df = df.drop(columns = [c for c in (leakage_cols + low_value_cols) if c in df.columns])

    # Keep only the last 5 seasons for the model
    n_years = 5
    if "year" in df.columns:
        max_year = int(df["year"].max())
        min_year = max_year - n_years + 1
        df = df[(df["year"] >= min_year) & (df["year"] <= max_year)].copy()
        print(f"Keeping seasons from {min_year} to {max_year}")
    else:
        raise KeyError("Column 'year' missing before model dataset filtering.")

    
    # Save new table to 'processed' folder
    df.to_csv(output_file, index = False)
    
    # Check
    try:
        check_df = pd.read_csv(output_file)
        print("✅ model_dataset successfully created and filled")
        print(f"📁 Saved to: {output_file}")
        print(f" Rows: {len(check_df)} | Columns: {len(check_df.columns)}")
        
    except Exception as e:
        print(f"⚠️ Error while checking model_dataset file: {e}")
        return None

    return output_file


def build_future_model_dataset(season_year: int = 2025) -> Path:
    """
    Build a feature modelling dataset for a given season.

    This function reuses model_dataset.csv and:
    - filters the rows for the requested season
    - drops all target_* columns

    The table is saved as: data/processed/model_dataset_predictions_{season_year}.csv.

    Args:
        season_year (int): season we want to predict.

    Returns:
        Path: Path to the saved model_dataset_predictions_{season_year}.csv.
    """

    # Define file paths
    model_file = processed_direction / "model_dataset.csv"
    output_file = processed_direction / f"model_dataset_predictions_{season_year}.csv"

    # Load the dataset
    try:
        df = pd.read_csv(model_file)
    except Exception as e:
        print(f"⚠️ Error while reading {model_file}: {e}")
        return None

    # Keep only the requested season
    season_df = df[df["year"] == season_year].copy()

    # Drop all target columns
    target_columns = [c for c in season_df.columns if c.startswith("target_")]
    season_df = season_df.drop(columns = target_columns)

    # Save new table to 'processed' folder
    season_df.to_csv(output_file, index = False)

    print(f"✅ model_dataset_predictions_{season_year}.csv successfully created")
    print(f"📂 Saved to: {output_file.name}")

    return output_file