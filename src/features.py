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
from src.data_loader import processed_direction

def build_drivers_performance() -> Path:
    """
    Create an aggregated performance table for each driver between 2020–2025 using 'results_cleaned.csv'
    and 'drivers_cleaned.csv'.
    
    For each driverId, compute:
    - races_count: number of races started
    - finished_races: number of races finished (position not null)
    - finish_rate: finished_races / races_count
    - win_count: number of wins (positionOrder == 1)
    - podiums: number of podiums (positionOrder <= 3)
    - top10_finishes: number of Top 10 finishes (positionOrder <=10)
    - avg_finish_position: average finish positionOrder on finished races only
    - median_finish_position: median finish positionOrder on finished races only
    - total_points: sum of points scored
    - Add the 'surname' column by merging it with 'drivers_cleaned.csv' using driverId
    
    The result is saved to 'data/processed/drivers_performance.csv'.

    Returns:
        Path: Path to the saved 'drivers_performance.csv' file.
    """

    # Define file paths
    results_cleaned = Path("Final-Project-Formula1/data/processed/results_cleaned.csv")
    drivers_cleaned = Path("Final-Project-Formula1/data/processed/drivers_cleaned.csv")
    output_file = Path("Final-Project-Formula1/data/processed/drivers_performance.csv")
    
    # Load data
    try:
        results_df = pd.read_csv(results_cleaned)
        drivers_df = pd.read_csv(drivers_cleaned)
    except Exception as e:
        print(f"⚠️ Error while reading {results_cleaned} or {drivers_cleaned}: {e}")
        return None

    # Create helper boolean flags on results_df
    results_df["is_finished"] = (results_df["position"].notna() & (position
    results_df["is_win"] = results_df["positionOrder"] == 1
    results_df["is_podium"] = results_df["positionOrder"] <= 3
    results_df["is_top10"] = results_df["positionOrder"] <= 10
    
    # Group by driverId
    grouped = results_df.groupby("driverId")

    # Basic parameters
    races_count = grouped["raceId"].nunique().rename("races_count")
    finished_races = grouped["is_finished"].sum().rename("finished_races")

    # Rates
    finish_rate = (finished_races / races_count).rename("finish_rate")

    # Additional parameters
    win_count = grouped["is_win"].sum().rename("win_count")
    podiums = grouped["is_podium"].sum().rename("podiums")
    top10_finishes = grouped["is_top10"].sum().rename("top10_finishes")

    # Average finish position on finished races only and median finish position
    avg_finish_position = results_df[results_df["is_finished"]].groupby("driverId")["positionOrder"].mean().rename("avg_finish_position")
    med_finish_position = results_df[results_df["is_finished"]].groupby("driverId")["positionOrder"].median().rename("med_finish_position")

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
            med_finish_position,
            total_points,], axis = 1,).reset_index()

    # Add surname from drivers_cleaned and put it right after driverId
    drivers_performance = drivers_performance.merge(drivers_df[["driverId", "surname"]], on = "driverId", how = "left",)
    
    columns = ["driverId", "surname"] + [c for c in drivers_performance.columns if c not in ["driverId", "surname"]]
    drivers_performance = drivers_performance[columns]                                  

    # Save new table to 'processed' folder
    drivers_performance.to_csv(output_file, index = False)
    
    # Check
    try:
        check_df = pd.read_csv(output_file)
        expected_columns = [
            "driverId",
            "surname",
            "races_count",
            "finished_races",
            "finish_rate",
            "win_count",
            "podiums",
            "top10_finishes",
            "avg_finish_position",
            "med_finish_position",
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


def build_constructors_performance() -> Path:
    """
    Create an aggregated performance table for each constructor between 2020–2025 using
    'results_cleaned.csv' and 'constructors_cleaned.csv'.
    
    For each constructorId, compute:
    - races_count: number of different race started
    - finished_races: number of finished results (statusId == 1 and positionOrder not null)
    - finish_rate: finished_races / races_count
    - win_count: number of wins (positionOrder == 1)
    - podiums: number of podiums (positionOrder <= 3)
    - top10_finishes: number of Top 10 finishes (positionOrder <= 10)
    - avg_finish_position: average finish positionOrder on finished races only
    - median_finish_position: median finish positionOrder on finished races only
    - total_points: sum of points scored
    - Add the 'name' column by merging it with 'constructors_cleaned.csv' using constructorId
    
    The result is saved to 'data/processed/constructors_performance.csv'.

    Returns:
        Path: Path to the saved 'constructors_performance.csv' file.
    """

    # Define file paths
    results_cleaned = Path("Final-Project-Formula1/data/processed/results_cleaned.csv")
    constructors_cleaned = Path("Final-Project-Formula1/data/processed/constructors_cleaned.csv")
    output_file = Path("Final-Project-Formula1/data/processed/constructors_performance.csv")

    # Load data
    try:
        results_df = pd.read_csv(results_cleaned)
        constructors_df = pd.read_csv(constructors_cleaned)
    except Exception as e:
        print(f"⚠️ Error while reading {results_cleaned} or {constructors_cleaned}: {e}")
        return None

    # Create helper boolean flags on results_df
    results_df["is_finished"] = ((results_df["statusId"] == 1) & (results_df["positionOrder"].notna()))
    results_df["is_win"] = results_df["positionOrder"] == 1
    results_df["is_podium"] = results_df["positionOrder"] <= 3
    results_df["is_top10"] = results_df["positionOrder"] <= 10
    
    # Group by constructorId
    grouped = results_df.groupby("constructorId")

    # Basic parameters
    races_count = grouped["raceId"].nunique().rename("races_count")
    finished_races = grouped["is_finished"].sum().rename("finished_races")

    # Rates
    finish_rate = (finished_races / races_count).rename("finish_rate")

    # Additional parameters
    win_count = grouped["is_win"].sum().rename("win_count")
    podiums = grouped["is_podium"].sum().rename("podiums")
    top10_finishes = grouped["is_top10"].sum().rename("top10_finishes")
    
    # Average finish position on finished races only and median finish position
    avg_finish_position = results_df[results_df["is_finished"]].groupby("constructorId")["positionOrder"].mean().rename("avg_finish_position")
    med_finish_position = results_df[results_df["is_finished"]].groupby("constructorId")["positionOrder"].median().rename("med_finish_position")
    
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
            med_finish_position,
            total_points,], axis = 1,).reset_index()

    # Add name from constructors_cleaned and put it right after constructorId
    constructors_performance = constructors_performance.merge(constructors_df[["constructorId", "name"]], on = "constructorId", how = "left",)

    columns = ["constructorId", "name"] + [c for c in constructors_performance.columns if c not in ["constructorId", "name"]]
    constructors_performance = constructors_performance[columns]

    # Save new table to 'processed' folder
    constructors_performance.to_csv(output_file, index = False)
    
    # Check
    try:
        check_df = pd.read_csv(output_file)
        expected_columns = [
            "constructorId",
            "name",
            "races_count",
            "finished_races",
            "finish_rate",
            "win_count",
            "podiums",
            "top10_finishes",
            "avg_finish_position",
            "med_finish_position",
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


def add_constructors_reliability_features() -> Path:
    """
    Add reliability-related features to 'constructors_performance.csv':
    - total_dnf: total number of non-finishes
    - mechanical_dnf: DNFs caused by mechanical issues
    - crash_dnf: DNFs caused by crashes
    - reliability_rate: finished_races / (finished_races + total_dnf)
    
    Returns:
        Path: Path to the updated 'constructors_performance.csv' file.
    """

    # Define file paths
    results_cleaned = Path("Final-Project-Formula1/data/processed/results_cleaned.csv")
    status_cleaned = Path("Final-Project-Formula1/data/processed/status_cleaned.csv")
    performance_file = Path("Final-Project-Formula1/data/processed/constructors_performance.csv")

    # Load data
    try:
        results_df = pd.read_csv(results_cleaned)
        status_df = pd.read_csv(status_cleaned)
        perf_df = pd.read_csv(performance_file)
    except Exception as e:
        print(f"⚠️ Error while reading {results_cleaned} or {status_cleaned} or {performance_file}: {e}")
        return None

    # Merge status_cleaned and prepare other parameters
    results_df = results_df.merge(status_df[["statusId", "status"]], on="statusId", how="left")
    results_df["is_finished"] = (results_df["statusId"] == 1) & results_df["positionOrder"].notna()
    results_df["is_dnf"] = ~results_df["is_finished"]

    # Mechanical / crash detection
    status_text = results_df["status"].str.lower().fillna("")

    mechanical_keywords = "|".join([
        "engine","gearbox","hydraulic","electrical","suspension","transmission",
        "brake","fuel","oil","turbo","mechanical","power unit","overheat"])
    crash_keywords = "|".join(["accident","collision","crash","damage","spun"])

    results_df["is_mechanical_dnf"] = results_df["is_dnf"] & status_text.str.contains(mechanical_keywords, na = False)
    results_df["is_crash_dnf"] = results_df["is_dnf"] & status_text.str.contains(crash_keywords, na = False)

    # Group by constructorId
    grouped = results_df.groupby("constructorId")

    # Aggregate by constructorId
    total_dnf = grouped["is_dnf"].sum().rename("total_dnf")
    mechanical_dnf = grouped["is_mechanical_dnf"].sum().rename("mechanical_dnf")
    crash_dnf = grouped["is_crash_dnf"].sum().rename("crash_dnf")

    # Merge with existing file
    updated_df = perf_df.merge(pd.concat([total_dnf, mechanical_dnf, crash_dnf], axis = 1), on = "constructorId", how = "left",)

    # Rates
    updated_df["reliability_rate"] = (updated_df["finished_races"] / (updated_df["finished_races"] + updated_df["total_dnf"]))

    # Save new table to 'processed' folder
    updated_df.to_csv(performance_file, index = False)

    # Check
    try:
        check_df = pd.read_csv(performance_file)
        expected_columns = [
            "total_dnf",
            "mechanical_dnf",
            "crash_dnf",
            "reliability_rate",]

        all_columns_present = all(col in check_df.columns for col in expected_columns)
        
        if not all_columns_present:
            print(f"❌ Reliability columns not found in constructors_performance file saved to: {performance_file}")
        else:
            print("✅ Reliability columns successfully added to constructors_performance.csv")
            print(f"📁 Saved to: {performance_file}")
            
    except Exception as e:
        print(f"⚠️ Error while checking constructors_performance file: {e}")

    return performance_file



def build_qualifying_performance() -> Path:
    """
    Create an aggregated qualifying performance table for each driver between 2020–2025 using
    'qualifying_cleaned.csv' and 'drivers_cleaned.csv'.
    
    For each driverId, compute:
    - quali_sessions_count: number of qualifying sessions
    - pole_count: number of pole positions (position == 1)
    - avg_quali_position: average qualifying position
    - median_quali_position: median qualifying position
    - best_quali_position: best qualifying position
    - Add the 'surname' column by merging with 'drivers_cleaned.csv' using driverId
    
    The result is saved to 'data/processed/qualifying_performance.csv'.

    Returns:
        Path: Path to the saved 'qualifying_performance.csv' file.
    """

    # Define file paths
    qualifying_cleaned = Path("Final-Project-Formula1/data/processed/qualifying_cleaned.csv")
    drivers_cleaned = Path("Final-Project-Formula1/data/processed/drivers_cleaned.csv")
    output_file = Path("Final-Project-Formula1/data/processed/qualifying_performance.csv")

    # Load data
    try:
        quali_df = pd.read_csv(qualifying_cleaned)
        drivers_df = pd.read_csv(drivers_cleaned)
    except Exception as e:
        print(f"⚠️ Error while reading {qualifying_cleaned} or {drivers_cleaned}: {e}")
        return None

    # Group by driverId
    grouped = quali_df.groupby("driverId")

    # Basic parameters
    quali_sessions_count = grouped.size().rename("quali_sessions_count")
    pole_count = (quali_df["position"] == 1).groupby(quali_df["driverId"]).sum().rename("pole_count")

    # Additional parameters
    avg_quali_position = grouped["position"].mean().rename("avg_quali_position")
    med_quali_position = grouped["position"].median().rename("med_quali_position")
    best_quali_position = grouped["position"].min().rename("best_quali_position")

    # Gather everything into a DataFrame
    qualifying_performance = pd.concat(
        [
            quali_sessions_count,
            pole_count,
            avg_quali_position,
            med_quali_position,
            best_quali_position,], axis = 1,).reset_index()

    # Add surname from drivers_cleaned and put it right after driverId
    qualifying_performance = qualifying_performance.merge(drivers_df[["driverId", "surname"]], on = "driverId", how = "left")
    
    columns = ["driverId", "surname"] + [c for c in qualifying_performance.columns if c not in ["driverId", "surname"]]
    qualifying_performance = qualifying_performance[columns]

    # Save new table to 'processed' folder
    qualifying_performance.to_csv(output_file, index = False)

    # Check
    try:
        check_df = pd.read_csv(output_file)
        expected_columns = [
            "driverId",
            "surname",
            "quali_sessions_count",
            "pole_count",
            "avg_quali_position",
            "med_quali_position",
            "best_quali_position",]
        
        all_columns_present = all(col in check_df.columns for col in expected_columns)
        
        if not all_columns_present:
            print(f"❌ Columns not found in qualifying_performance file saved to: {output_file}")
        else:
            print("✅ qualifying_performance successfully created and filled")
            print(f"📁 Saved to: {output_file}")
            
    except Exception as e:
        print(f"⚠️ Error while checking qualifying_performance file: {e}")

    return output_file


def build_sprint_performance() -> Path:
    """
    Create an aggregated sprint performance table for each driver between 2020–2025 using
    'sprint_results_cleaned.csv' and 'drivers_cleaned.csv'.
    
    For each driverId, compute:
    - sprint_count: number of sprint sessions started
    - sprint_finished: number of sprints finished (statusId == 1)
    - best_sprint_position: best sprint result
    - avg_sprint_position: average sprint position on finished sprints only
    - median_sprint_position: median sprint position on finished sprints only
    - top3_sprint_finishes: number of Top-3 sprint results
    - top8_sprint_finishes: number of scoring sprint finishes (<= 8)
    - sprint_points: total sprint points scored
    - Add the 'surname' column by merging with 'drivers_cleaned.csv' using driverId
    
    The result is saved to 'data/processed/sprint_performance.csv'.

    Returns:
        Path: Path to the saved 'sprint_performance.csv' file.
    """
    
    # Define file paths
    sprint_cleaned = Path("Final-Project-Formula1/data/processed/sprint_results_cleaned.csv")
    drivers_cleaned = Path("Final-Project-Formula1/data/processed/drivers_cleaned.csv")
    output_file = Path("Final-Project-Formula1/data/processed/sprint_performance.csv")

    # Load data
    try:
        sprint_df = pd.read_csv(sprint_cleaned)
        drivers_df = pd.read_csv(drivers_cleaned)
    except Exception as e:
        print(f"⚠️ Error while reading {sprint_cleaned} or {drivers_cleaned}: {e}")
        return None

    # Create helper boolean flags on sprint_df
    sprint_df["is_finished"] = (sprint_df["statusId"] == 1) & sprint_df["position"].notna()
    sprint_df["is_top3"] = sprint_df["position"] <= 3
    sprint_df["is_top8"] = sprint_df["position"] <= 8

    # Group by driverId
    grouped = sprint_df.groupby("driverId")

    # Basic parameters
    sprint_count = grouped["raceId"].nunique().rename("sprint_count")
    sprint_finished = grouped["is_finished"].sum().rename("sprint_finished")
    best_sprint_position = grouped["position"].min().rename("best_sprint_position")
    
    # Average sprint position on finished sprints only
    finished_sprints = sprint_df[sprint_df["is_finished"]]
    avg_sprint_position = finished_sprints.groupby("driverId")["position"].mean().rename("avg_sprint_position")
    med_sprint_position = finished_sprints.groupby("driverId")["position"].median().rename("med_sprint_position")
    
    # Additional parameters
    top3_sprint_finishes = grouped["is_top3"].sum().rename("top3_sprint_finishes")
    top8_sprint_finishes = grouped["is_top8"].sum().rename("top8_sprint_finishes")
    
    # Total points scored
    sprint_points = grouped["points"].sum().rename("sprint_points")

    # Gather everything into a DataFrame
    sprint_performance = pd.concat(
        [
            sprint_count,
            sprint_finished,
            best_sprint_position,
            avg_sprint_position,
            med_sprint_position,
            top3_sprint_finishes,
            top8_sprint_finishes,
            sprint_points,], axis = 1,).reset_index()
    
    # Add surname from drivers_cleaned and put it right after driverId
    sprint_performance = sprint_performance.merge(drivers_df[["driverId", "surname"]], on="driverId", how="left")

    columns = ["driverId", "surname"] + [c for c in sprint_performance.columns if c not in ["driverId", "surname"]]
    sprint_performance = sprint_performance[columns]
    
    # Save new table to 'processed' folder
    sprint_performance.to_csv(output_file, index = False)

    # Check
    try:
        check_df = pd.read_csv(output_file)
        expected_columns = [
            "driverId",
            "sprint_count",
            "sprint_finished",
            "best_sprint_position",
            "avg_sprint_position",
            "med_sprint_position",
            "top3_sprint_finishes",
            "top8_sprint_finishes",
            "sprint_points"]
        
        all_columns_present = all(col in check_df.columns for col in expected_columns)
        
        if not all_columns_present:
            print(f"❌ Columns not found in sprint_performance file saved to: {output_file}")
        else:
            print("✅ sprint_performance successfully created and filled")
            print(f"📁 Saved to: {output_file}")
            
    except Exception as e:
        print(f"⚠️ Error while checking sprint_performance file: {e}")

    return output_file


def build_driver_circuits_performance() -> Path:
    """
    Create an aggregated circuit performance table for each driver between 2020–2025 using
    'circuits_cleaned.csv', 'results_cleaned.csv', 'races_cleaned.csv' and 'drivers_cleaned.csv'.
    
    For each driverId and circuitId combination, compute:
    - races_count: number of races on this circuit
    - finished_races: number of races finished on this circuit (statusId == 1 and positionOrder not null)
    - finish_rate: finished_races / races_count
    - win_count: number of wins on this circuit (positionOrder == 1)
    - podiums: number of podiums on this circuit (positionOrder <= 3)
    - top10_finishes: number of Top 10 finishes on this circuit (positionOrder <=10)
    - avg_finish_position: average finish positionOrder on this circuit (on finished races only)
    - median_finish_position: median finish positionOrder on this circuit (on finished races only)
    - total_points: sum of points scored on this circuit
    - dnf_count on this circuit (did not finish)
    - Add the 'surname' column by merging it with 'drivers_cleaned.csv' using driverId
    - Add the 'name' column by merging it with 'circuits_cleaned.csv' using circuitId
    
    The result is saved to 'data/processed/circuits_performance.csv'.

    Returns:
        Path: Path to the saved 'circuits_performance.csv' file.
    """

    # Define file paths
    circuits_cleaned = Path("Final-Project-Formula1/data/processed/circuits_cleaned.csv")
    results_cleaned = Path("Final-Project-Formula1/data/processed/results_cleaned.csv")
    races_cleaned = Path("Final-Project-Formula1/data/processed/races_cleaned.csv")
    drivers_cleaned = Path("Final-Project-Formula1/data/processed/drivers_cleaned.csv")
    output_file = Path("Final-Project-Formula1/data/processed/circuits_performance.csv")

    # Load data
    try:
        results_df = pd.read_csv(results_cleaned)
        circuits_df = pd.read_csv(circuits_cleaned)
        drivers_df = pd.read_csv(drivers_cleaned)
        races_df = pd.read_csv(races_cleaned)
    except Exception as e:
        print(f"⚠️ Error while loading performance files: {e}")
        return None
        
    # Add circuitId to results_df via races_cleaned
    results_df = results_df.merge(races_df[["raceId", "circuitId"]], on = "raceId", how = "left",)
    
    # Create helper boolean flags on results_df
    results_df["is_finished"] = (results_df["statusId"] == 1) & (results_df["positionOrder"].notna())
    results_df["is_win"] = results_df["positionOrder"] == 1
    results_df["is_podium"] = results_df["positionOrder"] <= 3
    results_df["is_top10"] = results_df["positionOrder"] <= 10
    results_df["is_dnf"] = results_df["statusId"] != 1
    
    # Group by driverId and circuitId
    grouped = results_df.groupby(["driverId", "circuitId"])

    # Basic parameters
    races_count = grouped["raceId"].nunique().rename("races_count")
    finished_races = grouped["is_finished"].sum().rename("finished_races")
    dnf_count = grouped["is_dnf"].sum().rename("dnf_count")

    # Rates
    finish_rate = (finished_races / races_count).rename("finish_rate")

    # Additional parameters
    win_count = grouped["is_win"].sum().rename("win_count")
    podiums = grouped["is_podium"].sum().rename("podiums")
    top10_finishes = grouped["is_top10"].sum().rename("top10_finishes")

    # Average finish position on finished races only and median finish position
    avg_finish_position = results_df[results_df["is_finished"]].groupby(["driverId", "circuitId"])["positionOrder"].mean().rename("avg_finish_position")
    med_finish_position = results_df[results_df["is_finished"]].groupby(["driverId", "circuitId"])["positionOrder"].median().rename("med_finish_position")
    
    # Total points scored
    total_points = grouped["points"].sum().rename("total_points")

    # Gather everything into a DataFrame
    driver_circuit_perf = pd.concat([
        races_count,
        finished_races,
        finish_rate,
        win_count,
        podiums,
        top10_finishes,
        avg_finish_position,
        med_finish_position,
        total_points,
        dnf_count], axis = 1).reset_index()

    # Add surname from drivers_cleaned and put it right after driverId
    driver_circuit_perf = driver_circuit_perf.merge(drivers_df[["driverId", "surname"]], on = "driverId", how = "left",)
    
    columns = ["driverId", "surname"] + [c for c in driver_circuit_perf.columns if c not in ["driverId", "surname"]]
    driver_circuit_perf = driver_circuit_perf[columns]        

    # Put name of circuits right after circuitId
    driver_circuit_perf = driver_circuit_perf.merge(circuits_df[["circuitId", "name"]].rename(columns = {"name": "name"}), on = "circuitId", how = "left")

    columns = ["driverId", "surname", "circuitId", "name"] + [c for c in driver_circuit_perf.columns if c not in ["driverId", "surname", "circuitId", "name"]]
    driver_circuit_perf = driver_circuit_perf[columns]

    # Save new table to 'processed' folder
    driver_circuit_perf.to_csv(output_file, index = False)

    # Check
    try:
        check_df = pd.read_csv(output_file)
        expected_columns = [
            "driverId",
            "surname",
            "circuitId",
            "name",
            "races_count",
            "finished_races",
            "finish_rate",
            "win_count",
            "podiums",
            "top10_finishes",
            "avg_finish_position",
            "med_finish_position",
            "total_points",
            "dnf_count",]

        all_columns_present = all(col in check_df.columns for col in expected_columns)
        
        if not all_columns_present:
            print(f"❌ Columns not found in circuits_performance file saved to: {output_file}")
        else:
            print("✅ circuits_performance successfully created and filled")
            print(f"📁 Saved to: {output_file}")
            
    except Exception as e:
        print(f"⚠️ Error while checking circuits_performance file: {e}")

    return output_file


def build_drivers_performance_summary() -> Path:
    """
    Create a clean summary table with one row per driver for seasons 2020-2025.
    
    The table gather information from:
    - drivers_performance.csv       (driver performance 2020–2025)
    - qualifying_performance.csv    (qualifying stats per driver)
    - sprint_performance.csv        (sprint stats per driver)
    - circuits_performance.csv      (circuit stats per driver)

    The purpose of this table is descriptive/analytical (for the report), not directly for modelling.
    It provides a compact view of each driver's overall performance during the 2020–2025 period.
    
    The final table is saved to 'data/results/drivers_performance_summary.csv' file.
    
    Returns:
        Path: Path to the saved 'drivers_performance_summary.csv' file.
    """

    # Define file paths
    drivers_performance = Path("Final-Project-Formula1/data/processed/drivers_performance.csv")
    qualifying_performance = Path("Final-Project-Formula1/data/processed/qualifying_performance.csv")
    sprint_performance = Path("Final-Project-Formula1/data/processed/sprint_performance.csv")
    circuits_performance = Path("Final-Project-Formula1/data/processed/circuits_performance.csv")
    output_file = Path("Final-Project-Formula1/data/processed/drivers_performance_summary.csv")
    
    # Load data
    try:
        drivers_perf_df = pd.read_csv(drivers_performance)
        quali_perf_df = pd.read_csv(qualifying_performance)
        sprint_perf_df = pd.read_csv(sprint_performance)
        circuits_perf_df = pd.read_csv(circuits_performance)
    except Exception as e:
        print(f"⚠️ Error while loading performance files: {e}")
        return None

    # Basic parameters
    # drivers_performance: prefix with drv_
    drivers_perf_stat = drivers_perf_df.rename(columns = {
        "races_count": "drv_races_count",
        "finished_races": "drv_finished_races",
        "finish_rate": "drv_finish_rate",
        "win_count": "drv_win_count",
        "podiums": "drv_podiums",
        "top10_finishes": "drv_top10_finishes",
        "avg_finish_position": "drv_avg_finish_position",
        "med_finish_position": "drv_med_finish_position",
        "total_points": "drv_total_points",})
    
    # Qualifying_performance: drop surname, prefix columns with quali_
    quali_perf_stat = quali_perf_df.copy()
    
    if "surname" in quali_perf_stat.columns:
        quali_perf_stat = quali_perf_stat.drop(columns = ["surname"])

    quali_perf_stat = quali_perf_stat.rename(columns = {
        "quali_sessions_count": "quali_sessions_count",
        "pole_count": "quali_pole_count",
        "avg_quali_position": "quali_avg_position",
        "med_quali_position": "quali_med_position",
        "best_quali_position": "quali_best_position",})

    # Sprint_performance: drop surname, prefix columns with sprint_
    sprint_perf_stat = sprint_perf_df.copy()
    if "surname" in sprint_perf_stat.columns:
        sprint_perf_stat = sprint_perf_stat.drop(columns=["surname"])

    sprint_perf_stat = sprint_perf_stat.rename(columns = {
        "sprint_count": "sprint_count",
        "sprint_finished": "sprint_finished",
        "best_sprint_position": "sprint_best_position",
        "avg_sprint_position": "sprint_avg_position",
        "med_sprint_position": "sprint_med_position",
        "top3_sprint_finishes": "sprint_top3_finishes",
        "top8_sprint_finishes": "sprint_top8_finishes",
        "sprint_points": "sprint_points",})

    # Circuits_performance: circuit where each driver has the best avg_finish_position
    columns = circuits_perf_df[["driverId", "name", "avg_finish_position"]]
    best_circuit_ever = columns.groupby("driverId")["avg_finish_position"].idxmin()
    best_circuit = columns.loc[best_circuit_ever, ["driverId", "name"]].rename(columns = {"name": "circuit_best_avg_position"})

    # Merge everything into one big race–driver table
    global_df = drivers_perf_stat.copy()
    global_df = global_df.merge(quali_perf_stat, on = "driverId", how = "left")
    global_df = global_df.merge(sprint_perf_stat, on = "driverId", how = "left")
    global_df = global_df.merge(best_circuit, on = "driverId", how = "left")
    
    # Save final table
    global_df.to_csv(output_file, index = False)

    # Check
    try:
        check_df = pd.read_csv(output_file)
        expected_columns = [
            "driverId",
            "surname",
            "drv_races_count",
            "drv_finished_races",
            "drv_finish_rate",
            "drv_win_count",
            "drv_podiums",
            "drv_top10_finishes",
            "drv_avg_finish_position",
            "drv_med_finish_position",
            "drv_total_points",
            "quali_sessions_count",
            "quali_pole_count",
            "quali_avg_position",
            "quali_med_position",
            "quali_best_position",
            "sprint_count",
            "sprint_finished",
            "sprint_best_position",
            "sprint_avg_position",
            "sprint_med_position",
            "sprint_top3_finishes",
            "sprint_top8_finishes",
            "sprint_points",
            "circuit_best_avg_position",]
        
        all_columns_present = all(col in check_df.columns for col in expected_columns)
        
        if not all_columns_present:
            print(f"❌ Columns not found in drivers_performance_summary file saved to: {output_file}")
        else:
            print("✅ drivers_performance_summary successfully created and filled")
            print(f"📁 Saved to: {output_file}")
            
    except Exception as e:
        print(f"⚠️ Error while checking drivers_performance_summary file: {e}")

    return output_file
    
    




"""
def build_races_drivers_features() -> Path:
    
    Create the main modelling table with one row per driver and per race between 2020-2025.

    The table gather information from:
    - results_cleaned.csv          (one row per driver per race)
    - races_cleaned.csv            (race year, round, circuitId, distance,...)
    - circuits_cleaned.csv         (track info: length_km, is_night_race, track_type,...)
    - drivers_performance.csv      (aggregated driver stats 2020–2025)
    - constructors_performance.csv (aggregated constructor stats 2020–2025)
    - qualifying_performance.csv   (qualifying stats per driver)
    - sprint_performance.csv       (sprint stats per driver)
    - sprint_results_cleaned.csv   (real list of races that had a sprint)

    Each row corresponds to (raceId, driverId) and contains:
    - race context                  (race year, round, circuitId, distance, grid,...)
    - driver static features        (finish rate, wins, podiums,...)
    - constructor static features   (reliability, wins, podiums,...)
    - aggregated qualifying features
    - aggregated sprint features
    - target column: positionOrder  (final ranking of the race)

    The final table is saved to 'data/processed/races_drivers_features.csv' file.

    Returns:
        Path: Path to the saved 'races_drivers_features.csv' file.
    

    # Define file paths
    results_cleaned = Path("Final-Project-Formula1/data/processed/results_cleaned.csv")
    races_cleaned = Path("Final-Project-Formula1/data/processed/races_cleaned.csv")
    circuits_cleaned = Path("Final-Project-Formula1/data/processed/circuits_cleaned.csv")
    drivers_performance = Path("Final-Project-Formula1/data/processed/drivers_performance.csv")
    constructors_performance = Path("Final-Project-Formula1/data/processed/constructors_performance.csv")
    qualifying_performance = Path("Final-Project-Formula1/data/processed/qualifying_performance.csv")
    sprint_performance = Path("Final-Project-Formula1/data/processed/sprint_performance.csv")
    sprint_results_cleaned = Path("Final-Project-Formula1/data/processed/sprint_results_cleaned.csv")
    output_file = Path("Final-Project-Formula1/data/processed/races_drivers_features.csv")

    # Load data
    try:
        results_df = pd.read_csv(results_cleaned)
        races_df = pd.read_csv(races_cleaned)
        circuits_df = pd.read_csv(circuits_cleaned)
        drivers_perf_df = pd.read_csv(drivers_performance)
        constructors_perf_df = pd.read_csv(constructors_performance)
        quali_perf_df = pd.read_csv(qualifying_performance)
        sprint_perf_df = pd.read_csv(sprint_performance)
        sprint_df = pd.read_csv(sprint_results_cleaned)
    except Exception as e:
        print(f"⚠️ Error while loading input files for races_drivers_features: {e}")
        return None
        
    # Basic parameters
    # Races_cleaned:
    races_stat = races_df[["raceId", "year", "round", "circuitId", "race_distance_km"]]

    # Circuits_cleaned:
    circuit_columns = ["circuitId", "name", "alt", "length_km", "is_night_race", "track_type"]
    circuits_stat = circuits_df[circuit_columns].rename(columns = {
        "name": "circuit_name",
        "alt": "circuit_alt",
        "length_km": "circuit_length_km",
        "is_night_race": "circuit_is_night_race",
        "track_type": "circuit_track_type",})
    
    # Drivers_performance: prefix with drv_
    drivers_perf_stat = drivers_perf_df.rename(columns = {
        "races_count": "drv_races_count",
        "finished_races": "drv_finished_races",
        "finish_rate": "drv_finish_rate",
        "win_count": "drv_win_count",
        "podiums": "drv_podiums",
        "top10_finishes": "drv_top10_finishes",
        "avg_finish_position": "drv_avg_finish_position",
        "med_finish_position": "drv_med_finish_position",
        "total_points": "drv_total_points",})

    # Constructors_performance: prefix with team_
    constructors_perf_stat = constructors_perf_df.rename(columns = {
        "name": "constructor_name",
        "races_count": "team_races_count",
        "finished_races": "team_finished_races",
        "finish_rate": "team_finish_rate",
        "win_count": "team_win_count",
        "podiums": "team_podiums",
        "top10_finishes": "team_top10_finishes",
        "avg_finish_position": "team_avg_finish_position",
        "total_points": "team_total_points",
        "total_dnf": "team_total_dnf",
        "mechanical_dnf": "team_mechanical_dnf",
        "crash_dnf": "team_crash_dnf",
        "reliability_rate": "team_reliability_rate",})

    # Qualifying_performance: drop surname, prefix columns with quali_
    quali_perf_stat = quali_perf_df.copy()
    if "surname" in quali_perf_stat.columns:
        quali_perf_stat = quali_perf_stat.drop(columns = ["surname"])

    quali_perf_stat = quali_perf_stat.rename(columns = {
        "quali_sessions_count": "quali_sessions_count",
        "pole_count": "quali_pole_count",
        "avg_quali_position": "quali_avg_position",
        "med_quali_position": "quali_med_position",
        "best_quali_position": "quali_best_position",})

    # Sprint_performance: drop surname, prefix columns with sprint_
    sprint_perf_stat = sprint_perf_df.copy()
    if "surname" in sprint_perf_stat.columns:
        sprint_perf_stat = sprint_perf_stat.drop(columns=["surname"])

    sprint_perf_stat = sprint_perf_stat.rename(columns = {
        "sprint_count": "sprint_count",
        "sprint_finished": "sprint_finished",
        "best_sprint_position": "sprint_best_position",
        "avg_sprint_position": "sprint_avg_position",
        "med_sprint_position": "sprint_med_position",
        "top3_sprint_finishes": "sprint_top3_finishes",
        "top8_sprint_finishes": "sprint_top8_finishes",
        "sprint_points": "sprint_points",})
    
    # Merge everything into one big race–driver table
    features_df = results_df.copy()
    features_df = results_df.merge(races_stat, on = "raceId", how = "left")
    features_df = features_df.merge(circuits_stat, on = "circuitId", how = "left")
    features_df = features_df.merge(drivers_perf_stat, on = "driverId", how = "left")
    features_df = features_df.merge(constructors_perf_stat, on = "constructorId", how = "left")
    features_df = features_df.merge(quali_perf_stat, on = "driverId", how = "left")
    features_df = features_df.merge(sprint_perf_stat, on = "driverId", how = "left")

    # Sprint_results_cleaned: Filter the real raceId that have a sprint
    real_sprint_races = set(sprint_df["raceId"].unique())
    sprint_columns = [c for c in features_df.columns if c.startswith("sprint_")]
    features_df.loc[~features_df["raceId"].isin(real_sprint_races), sprint_columns] = np.nan
    
    # Clean up useless columns
    columns_to_drop = [
        "resultId",
        "number",
        "positionText",
        "position",
        "time",
        "milliseconds",
        "fastestLap",
        "rank",
        "fastestLapTime",
        "fastestLapSpeed",
        "laps",
        "statusId",]

    existing_to_drop = [c for c in columns_to_drop if c in features_df.columns]
    features_df = features_df.drop(columns = existing_to_drop)
    
    # Save final table
    features_df.to_csv(output_file, index = False)

    # Check
    try:
        check_df = pd.read_csv(output_file)
        expected_columns = [
            "raceId",
            "driverId",
            "constructorId",
            "year",
            "round",
            "grid",
            "positionOrder",
            "points",
            "surname",
            "constructor_name",
            "drv_finish_rate",
            "team_finish_rate",
            "team_reliability_rate",
            "quali_pole_count",
            "quali_avg_position",
            "sprint_points",
            "circuit_name",
            "circuit_length_km",
            "circuit_is_night_race",]
        
        all_columns_present = all(col in check_df.columns for col in expected_columns)
        
        if not all_columns_present:
            print(f"❌ Columns not found in races_drivers_features file saved to: {output_file}")
        else:
            print("✅ races_drivers_features successfully created and filled")
            print(f"📁 Saved to: {output_file}")
            
    except Exception as e:
        print(f"⚠️ Error while checking races_drivers_features file: {e}")

    return output_file
"""











