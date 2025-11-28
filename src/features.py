"""
This file builds the feature tables used for modelling Formula 1 race outcomes.

It starts from the cleaned and enriched CSV files (2020–2025 seasons) and
creates higher-level features that will later be used by the modelling step,
such as driver and team performance metrics, together with race and circuit
characteristics.
"""

from pathlib import Path
import pandas as pd
import numpy as np

# Import processed_direction from data_loader
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
    output_file = processed_direction / "driver_race_base.csv"

    # Load data
    try:
        results_df = pd.read_csv(results_file)
        races_df = pd.read_csv(races_file)
        drivers_df = pd.read_csv(drivers_file)
        constructors_df = pd.read_csv(constructors_file)
        circuits_df = pd.read_csv(circuits_file)
    except Exception as e:
        print(f"⚠️ Error while reading one of the cleaned files: {e}")
        return None

    # Prepare subsets of columns

    # Results: only useful columns
    results_columns = [
        "raceId",
        "driverId",
        "constructorId",
        "grid",
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

    # Merge
    base_df = results_small.merge(races_small, on = "raceId", how = "left", validate = "many_to_one",)
    base_df = base_df.merge(drivers_small, on = "driverId", how = "left", validate = "many_to_one",)
    base_df = base_df.merge(constructors_small, on = "constructorId", how = "left", validate = "many_to_one",)
    base_df = base_df.merge(circuits_small, on = "circuitId", how = "left", validate = "many_to_one",)

    # Sort
    sort_columns = [col for col in ["year", "round", "raceId", "driverId"] if col in base_df.columns]
    if sort_columns:
        base_df = base_df.sort_values(sort_columns).reset_index(drop = True)

    # Save new table to 'processed' folder
    base_df.to_csv(output_file, index = False)

    # Check
    try:
        check_df = pd.read_csv(output_file)
        expected_columns = [
            "raceId",
            "driverId",
            "constructorId",
            "grid",
            "position",
            "points",
            "laps",
            "milliseconds",
            "statusId",
            "year",
            "round",
            "race_name",
            "race_distance_km",
            "date",
            "circuitId",
            "driverRef",
            "code",
            "forename",
            "surname",
            "driver_nationality",
            "constructorRef",
            "constructor_name",
            "constructor_nationality",
            "circuit_name",
            "location",
            "country",
            "alt",
            "length_km",
            "is_night_race",
            "track_type",]

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


def build_drivers_performance() -> Path:
    """
    Create an aggregated performance table per driver from driver_race_base.csv.

    It creates one row per driver with summary statistics over the 2020–2025 seasons,
    such as number of races, finish rate, podiums and average finishing position.

    The aggregated performance table is saved as: data/processed/drivers_performance.csv

    Returns:
        Path: Path to the saved drivers_performance.csv file.
    """

    # Define file paths
    driver_race_file = processed_direction / "driver_race_base.csv"
    status_file = processed_direction / "status_cleaned.csv"
    output_file = processed_direction / "drivers_performance.csv"

    # Load data
    try:
        base_df = pd.read_csv(driver_race_file)
        status_df = pd.read_csv(status_file)
    except Exception as e:
        print(f"⚠️ Error while reading {driver_race_file} or {status_file} or : {e}")
        return None

    # Create helper for aggregation
    df = base_df.copy()
    df["position"] = df["position"] = pd.to_numeric(df["position"], errors = "coerce").astype("Int64")
    df["win"] = df["position"] == 1
    df["podium"] = df["position"].between(1, 3, inclusive = "both")
    df["top10"] = df["position"].between(1, 10, inclusive = "both")

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

    # Aggregate per driverId (all races)
    grouped_all = df.groupby("driverId", as_index = True)

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
        other_dnf_count = ("other_dnf", "sum"),)

    # Did not finish (DNF) information
    perf_all["dnf_count"] = perf_all["races_count"] - perf_all["finished_races"]

    # Position statistics (only for finished races)
    finished_df = df[df["finished"]].copy()

    if not finished_df.empty:
        grouped_finished = finished_df.groupby("driverId")["position"]
        
        pos_stats = grouped_finished.agg(
            avg_finish_position = "mean",
            med_finish_position = "median",
            std_finish_position = "std",)
    else:
        pos_stats = pd.DataFrame(columns = ["avg_finish_position", "med_finish_position", "std_finish_position"])

    perf_df = perf_all.join(pos_stats, how = "left")

    # Derived rates and scores
    races_nonzero = perf_df["races_count"].replace(0, np.nan)

    perf_df["finish_rate"] = perf_df["finished_races"] / races_nonzero
    perf_df["dnf_rate"] = perf_df["dnf_count"] / races_nonzero
    perf_df["mech_dnf_rate"] = perf_df["mech_dnf_count"] / races_nonzero
    perf_df["crash_dnf_rate"] = perf_df["crash_dnf_count"] / races_nonzero
    perf_df["reliability_rate"] = 1.0 - (perf_df["mech_dnf_rate"].fillna(0))
    perf_df["points_per_race"] = perf_df["total_points"] / races_nonzero

    # Consistency index: higher = more consistent (lower std of position)
    perf_df["consistency_index"] = 1.0 / (perf_df["std_finish_position"].fillna(0) + 1.0)

    # Simple overall performance score
    perf_df["performance_score"] = (perf_df["points_per_race"].fillna(0) * perf_df["finish_rate"].fillna(0))

    # Replace any remaining NaNs in rates by 0 (for drivers with very few data)
    rate_columns = [
        "finish_rate",
        "dnf_rate",
        "mech_dnf_rate",
        "crash_dnf_rate",
        "reliability_rate",
        "points_per_race",
        "consistency_index",
        "performance_score",]

    perf_df[rate_columns] = perf_df[rate_columns].fillna(0)

    # Reset the index to have driverId as a column
    perf_df = perf_df.reset_index()
    
    # Sort
    ordered_columns = [
        "driverId",
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
        "reliability_rate",
        "win_count",
        "podiums",
        "top10_finishes",
        "avg_finish_position",
        "med_finish_position",
        "std_finish_position",
        "total_points",
        "points_per_race",
        "consistency_index",
        "performance_score",]

    perf_df = perf_df[ordered_columns]

    # Save new table to 'processed' folder
    perf_df.to_csv(output_file, index = False)

    # Check
    try:
        check_df = pd.read_csv(output_file)
        expected_columns = ordered_columns
        
        all_columns_present = all(col in check_df.columns for col in expected_columns)
        
        if not all_columns_present:
            print(f"❌ Columns missing in drivers_performance file saved to: {output_file}")
            return None
        else:
            print("✅ drivers_performance successfully created and filled")
            print(f"📁 Saved to: {output_file}")
            print(f" Rows: {len(check_df)}")
            
    except Exception as e:
        print(f"⚠️ Error while checking drivers_performance file: {e}")
        return None

    return output_file


def build_constructors_performance() -> Path:
    """
    Create an aggregated performance table per constructor from driver_race_base.csv.

    It creates one row per constructor with summary statistics over the 2020–2025 seasons,
    such as number of races, finish rate, podiums and average finishing position.

    The aggregated performance table is saved as: data/processed/constructors_performance.csv

    Returns:
        Path: Path to the saved constructors_performance.csv file.
    """

    # Define file paths
    driver_race_file = processed_direction / "driver_race_base.csv"
    status_file = processed_direction / "status_cleaned.csv"
    output_file = processed_direction / "constructors_performance.csv"

    # Load data
    try:
        base_df = pd.read_csv(driver_race_file)
        status_df = pd.read_csv(status_file)
    except Exception as e:
        print(f"⚠️ Error while reading {driver_race_file} or {status_file}: {e}")
        return None

    # Create helper for aggregation
    df = base_df.copy()
    df["position"] = df["position"] = pd.to_numeric(df["position"], errors = "coerce").astype("Int64")
    df["win"] = df["position"] == 1
    df["podium"] = df["position"].between(1, 3, inclusive = "both")
    df["top10"] = df["position"].between(1, 10, inclusive = "both")

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

    # Aggregate per constructorId
    grouped_all = df.groupby("constructorId", as_index = True)
    
    perf_all = grouped_all.agg(
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
        other_dnf_count = ("other_dnf", "sum"),)
    
    # Did not finish (DNF) information
    perf_all["dnf_count"] = perf_all["races_count"] - perf_all["finished_races"]

    # Position statistics (only for finished races)
    finished_df = df[df["finished"]].copy()

    if not finished_df.empty:
        grouped_finished = finished_df.groupby("constructorId")["position"]

        pos_stats = grouped_finished.agg(
            avg_finish_position = "mean",
            med_finish_position = "median",
            std_finish_position = "std",)
    else:
        pos_stats = pd.DataFrame(columns = ["avg_finish_position", "med_finish_position", "std_finish_position"])

    perf_df = perf_all.join(pos_stats, how = "left")

    # Derived rates and scores
    races_nonzero = perf_df["races_count"].replace(0, np.nan)

    perf_df["finish_rate"] = perf_df["finished_races"] / races_nonzero
    perf_df["dnf_rate"] = perf_df["dnf_count"] / races_nonzero
    perf_df["mech_dnf_rate"] = perf_df["mech_dnf_count"] / races_nonzero
    perf_df["crash_dnf_rate"] = perf_df["crash_dnf_count"] / races_nonzero
    perf_df["reliability_rate"] = 1.0 - (perf_df["mech_dnf_rate"].fillna(0))
    perf_df["points_per_race"] = perf_df["total_points"] / races_nonzero

    # Consistency index: higher = more consistent (lower std of position)
    perf_df["consistency_index"] = 1.0 / (perf_df["std_finish_position"].fillna(0) + 1.0)

    # Simple overall performance score
    perf_df["performance_score"] = (perf_df["points_per_race"].fillna(0) * perf_df["finish_rate"].fillna(0))

    # Replace any remaining NaNs in rates by 0 (for drivers with very few data)
    rate_columns = [
        "finish_rate",
        "dnf_rate",
        "mech_dnf_rate",
        "crash_dnf_rate",
        "reliability_rate",
        "points_per_race",
        "consistency_index",
        "performance_score",]

    perf_df[rate_columns] = perf_df[rate_columns].fillna(0)

    # Reset the index to have constructorId as a column
    perf_df = perf_df.reset_index()
    
    # Sort
    ordered_columns = [
        "constructorId",
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
        "reliability_rate",
        "win_count",
        "podiums",
        "top10_finishes",
        "avg_finish_position",
        "med_finish_position",
        "std_finish_position",
        "total_points",
        "points_per_race",
        "consistency_index",
        "performance_score",]

    perf_df = perf_df[ordered_columns]

    # Save new table to 'processed' folder
    perf_df.to_csv(output_file, index = False)

    # Check
    try:
        check_df = pd.read_csv(output_file)
        expected_columns = ordered_columns
        
        all_columns_present = all(col in check_df.columns for col in expected_columns)
        
        if not all_columns_present:
            print(f"❌ Columns missing in constructors_performance file saved to: {output_file}")
            return None
        else:
            print("✅ constructors_performance successfully created and filled")
            print(f"📁 Saved to: {output_file}")
            print(f" Rows: {len(check_df)}")
            
    except Exception as e:
        print(f"⚠️ Error while checking constructors_performance file: {e}")
        return None

    return output_file


def build_sprint_performance() -> Path:
    """
    Create an aggregated performance table per driver from sprint_results_cleaned.csv
    and drivers_cleaned.csv.

    It creates one row per driver with summary statistics over the 2020–2025 seasons,
    such as number of sprints, finish rate, podiums and average finishing position.

    The aggregated performance table is saved as: data/processed/drivers_sprint_performance.csv

    Returns:
        Path: Path to the saved drivers_sprint_performance.csv file.
    """

    # Define file paths
    sprint_file = processed_direction / "sprint_results_cleaned.csv"
    drivers_file = processed_direction / "drivers_cleaned.csv"
    status_file = processed_direction / "status_cleaned.csv"
    output_file = processed_direction / "drivers_sprint_performance.csv"

    # Load data
    try:
        base_df = pd.read_csv(sprint_file)
        drivers_df = pd.read_csv(drivers_file)
        status_df = pd.read_csv(status_file)
    except Exception as e:
        print(f"⚠️ Error while reading {sprint_file} or {drivers_file} or {status_file}: {e}")
        return None
    
    # Create helper for aggregation
    df = base_df.copy()
    df["position"] = pd.to_numeric(df["position"], errors = "coerce").astype("Int64")
    df["win"] = df["position"] == 1
    df["podium"] = df["position"].between(1, 3, inclusive = "both")
    df["top8"] = df["position"].between(1, 8, inclusive = "both")
    
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

    # Aggregate per driverId (all races)
    grouped_all = df.groupby("driverId", as_index = True)

    perf_all = grouped_all.agg(
        sprints_count = ("raceId", "count"),
        finished_sprints = ("finished", "sum"),
        win_count = ("win", "sum"),
        podiums = ("podium", "sum"),
        top8_finishes = ("top8", "sum"),
        total_points = ("points", "sum"),
        mech_dnf_count = ("mech_dnf", "sum"),
        crash_dnf_count = ("crash_dnf", "sum"),
        other_dnf_count = ("other_dnf", "sum"),)

    # Did not finish (DNF) information
    perf_all["dnf_count"] = perf_all["sprints_count"] - perf_all["finished_sprints"]

    # Position statistics (only for finished races)
    finished_df = df[df["finished"]].copy()

    if not finished_df.empty:
        grouped_finished = finished_df.groupby("driverId")["position"]

        pos_stats = grouped_finished.agg(
            avg_finish_position = "mean",
            med_finish_position = "median",
            std_finish_position = "std",)
    else:
        pos_stats = pd.DataFrame(columns = ["avg_finish_position", "med_finish_position", "std_finish_position"])

    perf_df = perf_all.join(pos_stats, how = "left")
    
    # Derived rates and scores
    sprints_nonzero = perf_df["sprints_count"].replace(0, np.nan)

    perf_df["finish_rate"] = perf_df["finished_sprints"] / sprints_nonzero
    perf_df["dnf_rate"] = perf_df["dnf_count"] / sprints_nonzero
    perf_df["mech_dnf_rate"] = perf_df["mech_dnf_count"] / sprints_nonzero
    perf_df["crash_dnf_rate"] = perf_df["crash_dnf_count"] / sprints_nonzero
    perf_df["reliability_rate"] = 1.0 - (perf_df["mech_dnf_rate"].fillna(0))
    perf_df["points_per_sprint"] = perf_df["total_points"] / sprints_nonzero

    # Consistency index: higher = more consistent (lower std of position)
    perf_df["consistency_index"] = 1.0 / (perf_df["std_finish_position"].fillna(0) + 1.0)

    # Simple overall performance score
    perf_df["performance_score"] = (perf_df["points_per_sprint"].fillna(0) * perf_df["finish_rate"].fillna(0))

    # Replace any remaining NaNs in rates by 0 (for drivers with very few data)
    rate_columns = [
        "finish_rate",
        "dnf_rate",
        "mech_dnf_rate",
        "crash_dnf_rate",
        "reliability_rate",
        "points_per_sprint",
        "consistency_index",
        "performance_score",]

    perf_df[rate_columns] = perf_df[rate_columns].fillna(0)

    # Reset the index to have driverId as a column
    perf_df = perf_df.reset_index()

    # Add driver information
    drivers_small = drivers_df[["driverId", "driverRef", "forename", "surname", "nationality"]].copy()
    drivers_small = drivers_small.rename(columns = {"nationality": "driver_nationality"})
    
    perf_df = perf_df.merge(drivers_small, on = "driverId", how = "left")
    
    # Sort
    ordered_columns = [
        "driverId",
        "driverRef",
        "forename",
        "surname",
        "driver_nationality",
        "sprints_count",
        "finished_sprints",
        "finish_rate",
        "dnf_count",
        "dnf_rate",
        "mech_dnf_count",
        "mech_dnf_rate",
        "crash_dnf_count",
        "crash_dnf_rate",
        "other_dnf_count",
        "reliability_rate",
        "win_count",
        "podiums",
        "top8_finishes",
        "avg_finish_position",
        "med_finish_position",
        "std_finish_position",
        "total_points",
        "points_per_sprint",
        "consistency_index",
        "performance_score",]

    perf_df = perf_df[ordered_columns]

    # Save new table to 'processed' folder
    perf_df.to_csv(output_file, index = False)

    # Check
    try:
        check_df = pd.read_csv(output_file)
        expected_columns = ordered_columns

        all_columns_present = all(col in check_df.columns for col in expected_columns)
        
        if not all_columns_present:
            print(f"❌ Columns missing in drivers_sprint_performance file saved to: {output_file}")
            return None
        else:
            print("✅ drivers_sprint_performance successfully created and filled")
            print(f"📁 Saved to: {output_file}")
            print(f" Rows: {len(check_df)}")
            
    except Exception as e:
        print(f"⚠️ Error while checking drivers_sprint_performance file: {e}")
        return None

    return output_file


def build_qualifying_performance() -> Path:
    """
    Create an aggregated performance table per driver from qualifying_cleaned.csv
    and drivers_cleaned.csv.

    It creates one row per driver with summary statistics over the 2020–2025 seasons,
    such as number of qualifying sessions, pole positions, Q3 appearances,
    and average qualifying position.

    The aggregated performance table is saved as: data/processed/drivers_qualifying_performance.csv

    Returns:
        Path: Path to the saved drivers_qualifying_performance.csv file.
    """

    # Define file paths
    quali_file = processed_direction / "qualifying_cleaned.csv"
    drivers_file = processed_direction / "drivers_cleaned.csv"
    output_file = processed_direction / "drivers_qualifying_performance.csv"
    
    # Load data
    try:
        quali_df = pd.read_csv(quali_file)
        drivers_df = pd.read_csv(drivers_file)
    except Exception as e:
        print(f"⚠️ Error while reading {quali_file} or {drivers_file}: {e}")
        return None

    # Create helper for aggregation
    df = quali_df.copy()
    df["position"] = pd.to_numeric(df["position"], errors = "coerce").astype("Int64")
    df["valid_quali"] = df["position"].notna()
    df["pole"] = df["position"] == 1
    df["front_row"] = df["position"].between(1, 2, inclusive = "both")
    df["top5"] = df["position"].between(1, 5, inclusive = "both")
    df["top10"] = df["position"].between(1, 10, inclusive = "both")
    df["in_q3"] = df.get("q3").notna() if "q3" in df.columns else False
    df["in_q2"] = df.get("q2").notna() if "q2" in df.columns else False
    
    # Aggregate per driverId (all qualifying sessions)
    grouped_all = df.groupby("driverId", as_index = True)

    perf_all = grouped_all.agg(
        quali_count = ("raceId", "count"),
        valid_quali = ("valid_quali", "sum"),
        pole_count = ("pole", "sum"),
        front_row_count = ("front_row", "sum"),
        top5_count = ("top5", "sum"),
        top10_count = ("top10", "sum"),
        q3_appearances = ("in_q3", "sum"),
        q2_appearances = ("in_q2", "sum"),)
    
    # Position statistics (only valid sessions)
    valid_df = df[df["valid_quali"]].copy()

    if not valid_df.empty:
        grouped_valid = valid_df.groupby("driverId")["position"]
        pos_stats = grouped_valid.agg(
            avg_quali_position = "mean",
            med_quali_position = "median",
            std_quali_position = "std",)
    else:
        pos_stats = pd.DataFrame(columns = ["avg_quali_position", "med_quali_position", "std_quali_position"])
    
    perf_df = perf_all.join(pos_stats, how = "left")

    # Derived rates and scores
    quali_nonzero = perf_df["quali_count"].replace(0, np.nan)

    perf_df["valid_rate"] = perf_df["valid_quali"] / quali_nonzero
    perf_df["pole_rate"] = perf_df["pole_count"] / quali_nonzero
    perf_df["q3_rate"] = perf_df["q3_appearances"] / quali_nonzero
    perf_df["top10_rate"] = perf_df["top10_count"] / quali_nonzero

    # Consistency index: higher = more consistent (lower std of position)
    perf_df["consistency_index"] = 1.0 / (perf_df["std_quali_position"].fillna(0) + 1.0)

    # Simple overall qualifying performance score
    # 1.0 ~ always P1, 0.5 ~ around P11, ~0.0 ~ very far back
    perf_df["performance_score"] = ((21.0 - perf_df["avg_quali_position"].fillna(20.0)) / 20.0)

    # Replace any remaining NaNs in rates by 0 (for drivers with very few data)
    rate_columns = [
        "valid_rate",
        "pole_rate",
        "q3_rate",
        "top10_rate",
        "consistency_index",
        "performance_score",]

    perf_df[rate_columns] = perf_df[rate_columns].fillna(0)

    # Reset the index to have driverId as a column
    perf_df = perf_df.reset_index()

    # Add driver information
    drivers_small = drivers_df[["driverId", "driverRef", "forename", "surname", "nationality"]].copy()
    drivers_small = drivers_small.rename(columns = {"nationality": "driver_nationality"})
    
    perf_df = perf_df.merge(drivers_small, on = "driverId", how = "left")

    # Sort
    ordered_columns = [
        "driverId",
        "driverRef",
        "forename",
        "surname",
        "driver_nationality",
        "quali_count",
        "valid_quali",
        "valid_rate",
        "pole_count",
        "pole_rate",
        "front_row_count",
        "top5_count",
        "top10_count",
        "top10_rate",
        "q3_appearances",
        "q2_appearances",
        "q3_rate",
        "avg_quali_position",
        "med_quali_position",
        "std_quali_position",
        "consistency_index",
        "performance_score",]

    perf_df = perf_df[ordered_columns]

    # Save new table to 'processed' folder
    perf_df.to_csv(output_file, index = False)

    # Check
    try:
        check_df = pd.read_csv(output_file)
        expected_columns = ordered_columns

        all_columns_present = all(col in check_df.columns for col in expected_columns)
        
        if not all_columns_present:
            print(f"❌ Columns missing in drivers_qualifying_performance file saved to: {output_file}")
            return None
        else:
            print("✅ drivers_qualifying_performance successfully created and filled")
            print(f"📁 Saved to: {output_file}")
            print(f" Rows: {len(check_df)}")
            
    except Exception as e:
        print(f"⚠️ Error while checking drivers_qualifying_performance file: {e}")
        return None

    return output_file


def build_driver_circuits_performance() -> Path:
    """
    Create an aggregated performance table per driver and circuit from driver_race_base.csv
    and status_cleaned.csv.

    It creates one row per circuit per driver with summary statistics over the 2020–2025 seasons,
    such as number of races, finish rate, podiums and average finishing position.

    The aggregated performance table is saved as: data/processed/drivers_circuit_performance.csv

    Returns:
        Path: Path to the saved drivers_circuit_performance.csv file.
    """

    # Define file paths
    driver_race_file = processed_direction / "driver_race_base.csv"
    status_file = processed_direction / "status_cleaned.csv"
    output_file = processed_direction / "drivers_circuit_performance.csv"

    # Load data
    try:
        race_df = pd.read_csv(driver_race_file)
        status_df = pd.read_csv(status_file)
    except Exception as e:
        print(f"⚠️ Error while reading {driver_race_file} or {status_file}: {e}")
        return None
        
    # Create helper for aggregation
    df = race_df.copy()
    df["position"] = pd.to_numeric(df["position"], errors = "coerce").astype("Int64")
    df["win"] = df["position"] == 1
    df["podium"] = df["position"].between(1, 3, inclusive = "both")
    df["top5"] = df["position"].between(1, 5, inclusive = "both")
    df["top10"] = df["position"].between(1, 10, inclusive = "both")

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

    # Aggregate per driverId and circuitId
    grouped_all = df.groupby(["driverId", "circuitId"], as_index=True)

    perf_all = grouped_all.agg(
        driverRef = ("driverRef", "first"),
        forename = ("forename", "first"),
        surname = ("surname", "first"),
        driver_nationality = ("driver_nationality", "first"),
        races_count = ("raceId", "count"),
        finished_races = ("finished", "sum"),
        win_count = ("win", "sum"),
        podium_count = ("podium", "sum"),
        top5_count = ("top5", "sum"),
        top10_count = ("top10", "sum"),
        total_points = ("points", "sum"),
        mech_dnf_count = ("mech_dnf", "sum"),
        crash_dnf_count = ("crash_dnf", "sum"),
        other_dnf_count = ("other_dnf", "sum"),)

    # Did not finish (DNF) information
    perf_all["dnf_count"] = perf_all["races_count"] - perf_all["finished_races"]

    # Position statistics (only finished races)
    finished_df = df[df["finished"]].copy()

    if not finished_df.empty:
        grouped_finished = finished_df.groupby(["driverId", "circuitId"])["position"]
        pos_stats = grouped_finished.agg(
            avg_finish_position = "mean",
            best_finish_position = "min",
            std_finish_position = "std",)
    else:
        pos_stats = pd.DataFrame(columns = ["avg_finish_position", "best_finish_position", "std_finish_position"])

    perf_df = perf_all.join(pos_stats, how = "left")

    # Derived rates and scores
    races_nonzero = perf_df["races_count"].replace(0, np.nan)

    perf_df["finish_rate"] = perf_df["finished_races"] / races_nonzero
    perf_df["mech_dnf_rate"] = perf_df["mech_dnf_count"] / races_nonzero
    perf_df["crash_dnf_rate"] = perf_df["crash_dnf_count"] / races_nonzero
    perf_df["points_scored"] = perf_df["total_points"]
    perf_df["points_per_race"] = perf_df["total_points"] / races_nonzero

    # Consistency index: higher = more consistent (lower std of position)
    perf_df["consistency_index"] = 1.0 / (perf_df["std_finish_position"].fillna(0) + 1.0)
    
    # Replace any remaining NaNs in rates by 0 (for drivers with very few data)
    rate_columns = [
        "finish_rate",
        "mech_dnf_rate",
        "crash_dnf_rate",
        "points_per_race",
        "consistency_index",]

    perf_df[rate_columns] = perf_df[rate_columns].fillna(0)

    # Reset the index to have driverId and circuitId as columns
    perf_df = perf_df.reset_index()

    # Sort
    ordered_columns = [
        "driverId",
        "circuitId",
        "driverRef",
        "forename",
        "surname",
        "driver_nationality",
        "races_count",
        "finished_races",
        "finish_rate",
        "dnf_count",
        "win_count",
        "podium_count",
        "top5_count",
        "top10_count",
        "mech_dnf_count",
        "crash_dnf_count",
        "other_dnf_count",
        "mech_dnf_rate",
        "crash_dnf_rate",
        "points_scored",
        "points_per_race",
        "avg_finish_position",
        "best_finish_position",
        "std_finish_position",
        "consistency_index",]

    perf_df = perf_df[ordered_columns]

    # Save new table to 'processed' folder
    perf_df.to_csv(output_file, index = False)

    # Check
    try:
        check_df = pd.read_csv(output_file)
        expected_columns = ordered_columns

        all_columns_present = all(col in check_df.columns for col in expected_columns)
        
        if not all_columns_present:
            print(f"❌ Columns missing in drivers_circuit_performance file saved to: {output_file}")
            return None
        else:
            print("✅ drivers_circuit_performance successfully created and filled")
            print(f"📁 Saved to: {output_file}")
            print(f" Rows: {len(check_df)}")
            
    except Exception as e:
        print(f"⚠️ Error while checking drivers_circuit_performance file: {e}")
        return None

    return output_file


def build_model_dataset() -> Path:
    """
    Create the final modelling dataset at race-entry level.

    It creates one row per driver/constructor in each race, with:
    - base race information (grid, position, points, status, etc.),
    - enriched status category (mechanical / crash / other / no_dnf),
    - race and circuit data (year, round, distance, country, etc.),
    - driver & constructor identity columns,
    - global driver performance features,
    - global constructor performance features,
    - driver sprint performance features,
    - driver qualifying performance features,
    - driver-circuit performance features,
    - target columns for the prediction tasks (win, top3, top10, points).

    The final table is saved as: data/processed/model_dataset.csv

    Returns:
        Path: Path to the saved model_dataset.csv file.
    """

    # Define file paths
    driver_race_file = processed_direction / "driver_race_base.csv"
    drivers_perf_file = processed_direction / "drivers_performance.csv"
    constructors_perf_file = processed_direction / "constructors_performance.csv"
    sprint_perf_file = processed_direction / "drivers_sprint_performance.csv"
    quali_perf_file = processed_direction / "drivers_qualifying_performance.csv"
    circuit_perf_file = processed_direction / "drivers_circuit_performance.csv"
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

    # Start from base race-entry table
    df = base_df.copy()

    # We remove existing race/circuit columns to avoid duplicates later on
    drop_base_cols = [
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
    
    # Add global driver performance features
    drv_drop = [c for c in ["driverRef", "forename", "surname", "driver_nationality"]
                if c in drivers_perf_df.columns]

    drivers_features = drivers_perf_df.drop(columns = drv_drop, errors = "ignore").copy()
    drivers_features = drivers_features.add_prefix("drv_")

    # bring driverId back to its original name
    drivers_features = drivers_features.rename(columns = {"drv_driverId": "driverId"})
    df = df.merge(drivers_features, on = "driverId", how = "left")

    # Add global constructor performance features
    const_drop = [c for c in ["constructor_name", "constructor_nationality"]
                  if c in constructors_perf_df.columns]

    constructors_features = constructors_perf_df.drop(columns = const_drop, errors = "ignore").copy()
    constructors_features = constructors_features.add_prefix("team_")
    constructors_features = constructors_features.rename(columns = {"team_constructorId": "constructorId"})
    df = df.merge(constructors_features, on = "constructorId", how = "left")
    df["team_consistency"] = df["team_consistency_index"]
    df["constructorId"] = df["constructorId"].astype(int)

    # Add driver sprint performance features
    sprint_drop = [c for c in ["driverRef", "forename", "surname", "driver_nationality"]
                   if c in sprint_perf_df.columns]

    sprint_features = sprint_perf_df.drop(columns = sprint_drop, errors = "ignore").copy()
    sprint_features = sprint_features.add_prefix("sprint_")
    sprint_features = sprint_features.rename(columns = {"sprint_driverId": "driverId"})
    df = df.merge(sprint_features, on = "driverId", how = "left")

    # Add driver qualifying performance features
    quali_drop = [c for c in ["driverRef", "forename", "surname", "driver_nationality"]
                  if c in quali_perf_df.columns]

    quali_features = quali_perf_df.drop(columns = quali_drop, errors = "ignore").copy()
    quali_features = quali_features.add_prefix("quali_")
    quali_features = quali_features.rename(columns = {"quali_driverId": "driverId"})
    df = df.merge(quali_features, on = "driverId", how = "left")

    # Add driver-circuit performance features
    dc_drop = [c for c in ["driverRef", "forename", "surname", "driver_nationality"]
               if c in circuit_perf_df.columns]

    driver_circuit_features = circuit_perf_df.drop(columns = dc_drop, errors = "ignore").copy()
    driver_circuit_features = driver_circuit_features.add_prefix("circ_")
    driver_circuit_features = driver_circuit_features.rename(columns = {"circ_driverId": "driverId", "circ_circuitId": "circuitId",})
    df = df.merge(driver_circuit_features, on = ["driverId", "circuitId"], how = "left")

    # Numerical columns
    df[["position", "grid", "points"]] = df[["position", "grid", "points"]].apply(pd.to_numeric, errors = "coerce")

    # Resolve duplicate columns with _x / _y suffixes
    duplicate_sets = [
        ("circuit_name_x", "circuit_name_y", "circuit_name"),
        ("is_night_race_x", "is_night_race_y", "is_night_race"),
        ("track_type_x", "track_type_y", "track_type"),]

    for col_x, col_y, final in duplicate_sets:
        if col_x in df.columns and col_y in df.columns:
            df[final] = df[col_x].combine_first(df[col_y])
            df = df.drop(columns = [col_x, col_y])
        elif col_x in df.columns:
            df = df.rename(columns = {col_x: final})
        elif col_y in df.columns:
            df = df.rename(columns = {col_y: final})

    # Create target columns
    df["target_win"] = (df["position"] == 1).astype(int)
    df["target_top3"] = df["position"].between(1, 3, inclusive = "both").astype(int)
    df["target_top10"] = df["position"].between(1, 10, inclusive = "both").astype(int)
    df["target_points"] = (df["points"] > 0).astype(int)

    # Basic missing-value handling

    # Numeric features -> 0
    num_columns = df.select_dtypes(include = ["number", "float64", "int64", "Int64"]).columns
    df[num_columns] = df[num_columns].fillna(0)

    # Text / categorical features -> Unknown
    category_columns = df.select_dtypes(include = ["object"]).columns
    df[category_columns] = df[category_columns].fillna("Unknown")

    # Sort
    key_columns = [
        "raceId",
        "driverId",
        "constructorId",
        "circuitId",
        "grid",
        "position",
        "points",
        "laps",
        "milliseconds",
        "statusId",
        "year",
        "round",
        "date",
        "race_distance_km",
        "is_night_race",
        "track_type",
        "circuit_name",
        "circuit_location",
        "circuit_country",
        "alt",
        "length_km",
        "driverRef",
        "code",
        "forename",
        "surname",
        "driver_nationality",
        "constructorRef",
        "constructor_name",
        "constructor_nationality",
        "is_mechanical",
        "is_crash",
        "is_other_dnf",
        "is_no_dnf",
        "dnf_category",]
    
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
        "reliability_rate",
        "win_count",
        "podiums",
        "top10_finishes",
        "top8_finishes",
        "avg_finish_position",
        "med_finish_position",
        "std_finish_position",
        "total_points",
        "points_per_race",
        "points_per_sprint",
        "points_scored",
        "consistency_index",
        "performance_score",]
    
    perf_prefixes = ["drv_", "team_", "sprint_", "quali_", "circ_"]
    
    ordered_first = [c for c in key_columns if c in df.columns]
    perf_cols = []
    for prefix in perf_prefixes:
        for metric in metric_order:
            col = prefix + metric
            if col in df.columns:
                perf_cols.append(col)
        
        remaining_prefixed = [c for c in df.columns if c.startswith(prefix) and c not in perf_cols]
        perf_cols.extend(remaining_prefixed)
    
    already_used = set(ordered_first) | set(perf_cols)
    other_cols = [c for c in df.columns if c not in already_used]
        
    df = df[ordered_first + perf_cols + other_cols]
    
    # Save new table to 'processed' folder
    df.to_csv(output_file, index = False)
    
    # Check
    try:
        check_df = pd.read_csv(output_file)
        expected_columns = ordered_first + perf_cols + other_cols
        
        all_columns_present = all(col in check_df.columns for col in expected_columns)
        
        if not all_columns_present:
            print(f"❌ Columns missing in model_dataset file saved to: {output_file}")
            return None
        else:
            print("✅ model_dataset successfully created and filled")
            print(f"📁 Saved to: {output_file}")
            print(f" Rows: {len(check_df)} | Columns: {len(check_df.columns)}")
            
    except Exception as e:
        print(f"⚠️ Error while checking model_dataset file: {e}")
        return None

    return output_file