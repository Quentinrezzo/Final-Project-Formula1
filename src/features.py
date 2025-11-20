"""
This file builds the feature tables used for modelling Formula 1 race outcomes.

It starts from the cleaned and enhanced CSV files (2020–2025 seasons) and
creates higher-level features that will later be used by the modelling step,
such as driver and team performance metrics, together with race and circuit
characteristics.
"""

from pathlib import Path
import pandas as pd

def build_drivers_performance() -> Path:
    """
    Create an aggregated performance table for each driver between 2020–2025 using 'results_cleaned.csv'.
    
    For each driverId, compute:
    - races_count: number of races started
    - finished_races: number of races finished (statusId == 1)
    - finish_rate: finished_races / races_count
    - win_count: number of wins (positionOrder == 1)
    - podiums: number of podiums (positionOrder <= 3)
    - top10_finishes: number of Top 10 finishes (positionOrder <=10)
    - avg_finish_position: average finish positionOrder on finished races only
    - total_points: sum of points scored
    
    The result is saved to 'data/processed/drivers_performance.csv'.

    Returns:
        Path: Path to the saved 'drivers_performance.csv' file.
    """

    # Define file paths
    results_cleaned = Path("Final-Project-Formula1/data/processed/results_cleaned.csv")
    output_file = Path("Final-Project-Formula1/data/processed/drivers_performance.csv")
    
    # Load data
    try:
        results_df = pd.read_csv(results_cleaned)
    except Exception as e:
        print(f"⚠️ Error while reading {results_cleaned}: {e}")
        return None
    
    # Group by driverId
    grouped = results_df.groupby("driverId")

    # Basic parameters
    races_count = grouped.size().rename("races_count")
    finished_races = grouped.apply(lambda g: (g["statusId"] == 1).sum()).rename("finished_races")

    # Rates
    finish_rate = (finished_races / races_count).rename("finish_rate")

    # Additional parameters
    win_count = grouped.apply(lambda g: (g["positionOrder"] == 1).sum()).rename("win_count")
    podiums = grouped.apply(lambda g: (g["positionOrder"] <= 3).sum()).rename("podiums")
    top10_finishes = grouped.apply(lambda g: (g["positionOrder"] <= 10).sum()).rename("top10_finishes")

    # Average finish position on finished races only
    def _avg_finish_on_finished(g: pd.DataFrame) -> float:
        
        finished = g[g["statusId"] == 1]
        
        if finished.empty:
            return pd.NA
        return finished["positionOrder"].mean()

    avg_finish_position = grouped.apply(_avg_finish_on_finished).rename("avg_finish_position")

    # Total points scored
    total_points = grouped["points"].sum().rename("total_points")

    # Gather everything into a DataFrame
    drivers_performance = pd.concat(
        [
            races_count,
            finished_races,
            finish_rate,
            win_count,
            podiums,
            top10_finishes,
            avg_finish_position,
            total_points,], axis = 1,).reset_index()

    # Save new table to 'processed' folder
    drivers_performance.to_csv(output_file, index = False)
    
    # Check
    try:
        check_df = pd.read_csv(output_file)
        expected_columns = [
            "driverId",
            "races_count",
            "finished_races",
            "finish_rate",
            "win_count",
            "podiums",
            "top10_finishes",
            "avg_finish_position",
            "total_points"]

        all_columns_present = all(col in check_df.columns for col in expected_columns)
        
        if not all_columns_present:
            print(f"❌ Columns not found in drivers_performance file saved to: {output_file}")
        else:
            print("✅ drivers_performance successfully created and filled")
            print(f"📁 Saved to: {output_file}")
            
    except Exception as e:
        print(f"⚠️ Error while checking drivers_performance file: {e}")

    return output_file


def add_driver_surname() -> Path:
    """
    Add the 'surname' column to the 'drivers_performance.csv' file
    by merging it with 'drivers_cleaned.csv' using driverId.

    The columns is inserted between the 'driverId' and 'races_count' columns.

    Returns:
        Path: Path to the updated 'drivers_performance.csv' file.
    """

    # Define file paths
    performance_file = Path("Final-Project-Formula1/data/processed/drivers_performance.csv")
    drivers_cleaned = Path("Final-Project-Formula1/data/processed/drivers_cleaned.csv")

    # Load data
    try:
        performance_df = pd.read_csv(performance_file)
        drivers_cleaned_df = pd.read_csv(drivers_cleaned)
    except Exception as e:
        print(f"⚠️ Error while loading files: {e}")
        return None

    # Merge to bring in driverRef and add the new column
    merged = performance_df.merge(drivers_cleaned_df[["driverId", "surname"]], on = "driverId", how = "left")
    columns = ["driverId", "surname"] + [c for c in merged.columns if c not in ["driverId", "surname"]]
    merged = merged[columns]

    # Save updated file
    merged.to_csv(performance_file, index = False)

    # Check
    try:
        check_df = pd.read_csv(performance_file)
        if "surname" not in check_df.columns:
            print(f"❌ Column 'surname' not added to file: {performance_file}")
        else:
            print("✅ 'surname' successfully added to drivers_performance.csv")
            print(f"📄 Saved to: {performance_file}")
            
    except Exception as e:
        print(f"⚠️ Error verifying updated file: {e}")
        return None

    return performance_file


def build_constructors_performance() -> Path:
    """
    Create an aggregated performance table for each constructor between 2020–2025 using 'results_cleaned.csv'.
    
    For each constructorId, compute:
    - races_count: number of different race started
    - finished_races: number of finished results (statusId == 1)
    - finish_rate: finished_races / races_count
    - win_count: number of wins (positionOrder == 1)
    - podiums: number of podiums (positionOrder <= 3)
    - top10_finishes: number of Top 10 finishes (positionOrder <= 10)
    - avg_finish_position: average finish positionOrder on finished races only
    - total_points: sum of points scored
    
    The result is saved to 'data/processed/constructors_performance.csv'.

    Returns:
        Path: Path to the saved 'constructors_performance.csv' file.
    """

    # Define file paths
    results_cleaned = Path("Final-Project-Formula1/data/processed/results_cleaned.csv")
    output_file = Path("Final-Project-Formula1/data/processed/constructors_performance.csv")

    # Load data
    try:
        results_df = pd.read_csv(results_cleaned)
    except Exception as e:
        print(f"⚠️ Error while reading {results_cleaned}: {e}")
        return None

    # Group by constructorId
    grouped = results_df.groupby("constructorId")

    # Basic parameters
    races_count = grouped["raceId"].nunique().rename("races_count")
    finished_races = grouped.apply(lambda g: (g["statusId"] == 1).sum()).rename("finished_races")

    # Rates
    finish_rate = (finished_races / races_count).rename("finish_rate")

    # Additional parameters
    win_count = grouped.apply(lambda g: (g["positionOrder"] == 1).sum()).rename("win_count")
    podiums = grouped.apply(lambda g: (g["positionOrder"] <= 3).sum()).rename("podiums")
    top10_finishes = grouped.apply(lambda g: (g["positionOrder"] <= 10).sum()).rename("top10_finishes")

    # Average finish position on finished races only
    def _avg_finish_on_finished(g: pd.DataFrame) -> float:
        
        finished = g[g["statusId"] == 1]
        
        if finished.empty:
            return pd.NA
        return finished["positionOrder"].mean()

    avg_finish_position = (grouped.apply(_avg_finish_on_finished).rename("avg_finish_position"))

    # Total points scored
    total_points = grouped["points"].sum().rename("total_points")

    # Gather everything into a DataFrame
    constructors_performance = pd.concat(
        [
            races_count,
            finished_races,
            finish_rate,
            win_count,
            podiums,
            top10_finishes,
            avg_finish_position,
            total_points,], axis = 1,).reset_index()

    # Save new table to 'processed' folder
    constructors_performance.to_csv(output_file, index = False)
    
    # Check
    try:
        check_df = pd.read_csv(output_file)
        expected_columns = [
            "constructorId",
            "races_count",
            "finished_races",
            "finish_rate",
            "win_count",
            "podiums",
            "top10_finishes",
            "avg_finish_position",
            "total_points",]

        all_columns_present = all(col in check_df.columns for col in expected_columns)
        
        if not all_columns_present:
            print(f"❌ Columns not found in constructors_performance file saved to: {output_file}")
        else:
            print("✅ constructors_performance successfully created and filled")
            print(f"📁 Saved to: {output_file}")
            
    except Exception as e:
        print(f"⚠️ Error while checking constructors_performance file: {e}")

    return output_file


def add_constructor_name() -> Path:
    """
    Add the 'name' column to the 'constructors_performance.csv' file
    by merging it with 'constructors_cleaned.csv' using constructorId.

    The columns is inserted between the 'constructorId' and 'races_count' columns.

    Returns:
        Path: Path to the updated 'constructors_performance.csv' file.
    """

    # Define file paths
    performance_file = Path("Final-Project-Formula1/data/processed/constructors_performance.csv")
    constructors_cleaned = Path("Final-Project-Formula1/data/processed/constructors_cleaned.csv")

    # Load data
    try:
        performance_df = pd.read_csv(performance_file)
        constructors_cleaned_df = pd.read_csv(constructors_cleaned)
    except Exception as e:
        print(f"⚠️ Error while loading files: {e}")
        return None

    # Merge to bring in constructorRef and add the new column
    merged = performance_df.merge(constructors_cleaned_df[["constructorId", "name"]], on = "constructorId", how = "left")
    columns = ["constructorId", "name"] + [c for c in merged.columns if c not in ["constructorId", "name"]]
    merged = merged[columns]

    # Save updated file
    merged.to_csv(performance_file, index = False)

    # Check
    try:
        check_df = pd.read_csv(performance_file)
        if "name" not in check_df.columns:
            print(f"❌ Column 'name' not added to file: {performance_file}")
        else:
            print("✅ 'name' successfully added to constructors_performance.csv")
            print(f"📄 Saved to: {performance_file}")
            
    except Exception as e:
        print(f"⚠️ Error verifying updated file: {e}")
        return None

    return performance_file


def build_races_drivers() -> Path:
    """
    Create the main modelling table with one row per driver and per race between 2020-2025.

    The table gather information from:
    - results_cleaned.csv          (per-driver race result)
    - races_cleaned.csv            (race date, circuit, distance…)
    - circuits_cleaned.csv         (length_km, is_night_race, track_type…)
    - drivers_performance.csv      (aggregated driver metrics 2020–2025)
    - constructors_performance.csv (aggregated constructor metrics 2020–2025)

    Targets created:
    - finished: 1 if statusId == 1, else 0
    - finish_position: positionOrder if finished, otherwise NA
    - is_top10: 1 if finished and positionOrder <= 10, else 0

    The final table is saved to 'data/processed/races_drivers_features.csv' file.

    Returns:
        Path: Path to the saved 'races_drivers_features.csv' file.
    """

    # Define file paths
    root_dir = Path("Final-Project-Formula1/data/processed")
    results_cleaned = root_dir / "results_cleaned.csv"
    races_cleaned = root_dir / "races_cleaned.csv"
    circuits_cleaned = root_dir / "circuits_cleaned.csv"
    drivers_perf_file = root_dir / "drivers_performance.csv"
    constructors_perf_file = root_dir / "constructors_performance.csv"
    output_file = root_dir / "races_drivers_features.csv"
    















