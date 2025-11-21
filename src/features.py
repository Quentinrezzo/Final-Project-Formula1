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
    - finished_races: number of races finished (statusId == 1 and positionOrder not null)
    - finish_rate: finished_races / races_count
    - win_count: number of wins (positionOrder == 1)
    - podiums: number of podiums (positionOrder <= 3)
    - top10_finishes: number of Top 10 finishes (positionOrder <=10)
    - avg_finish_position: average finish positionOrder on finished races only
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
    results_df["is_finished"] = (results_df["statusId"] == 1) & (results_df["positionOrder"].notna())
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

    # Average finish position on finished races only
    avg_finish_position = results_df[results_df["is_finished"]].groupby("driverId")["positionOrder"].mean().rename("avg_finish_position")

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
    Create an aggregated performance table for each constructor between 2020–2025 using 'results_cleaned.csv'.
    
    For each constructorId, compute:
    - races_count: number of different race started
    - finished_races: number of finished results (statusId == 1 and positionOrder not null)
    - finish_rate: finished_races / races_count
    - win_count: number of wins (positionOrder == 1)
    - podiums: number of podiums (positionOrder <= 3)
    - top10_finishes: number of Top 10 finishes (positionOrder <= 10)
    - avg_finish_position: average finish positionOrder on finished races only
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
    
    # Average finish position on finished races only
    avg_finish_position = results_df[results_df["is_finished"]].groupby("constructorId")["positionOrder"].mean().rename("avg_finish_position")

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
    'qualifying_cleaned.csv'.
    
    For each driverId, compute:
    - quali_sessions_count: number of qualifying sessions
    - pole_count: number of pole positions (position == 1)
    - avg_quali_position: average qualifying position
    - best_quali_position: best qualifying position
    - q3_appearances: number of times the driver reached Q3 (non-null q3)
    - q2_appearances: number of times the driver reached Q2 (non-null q2)
    - q1_appearances: number of times the driver reached Q1 (same as quali_sessions_count)
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
        quali_df = pd.read_csv(qualifying_file)
        drivers_df = pd.read_csv(drivers_cleaned)
    except Exception as e:
        print(f"⚠️ Error while reading {qualifying_file} or {drivers_cleaned}: {e}")
        return None

    # Group by driverId
    grouped = quali_df.groupby("driverId")

    # Basic parameters
    quali_sessions_count = grouped.size().rename("quali_sessions_count")
    pole_count = grouped.apply(lambda g: (g["position"] == 1).sum()).rename("pole_count")

    # Average and best qualifying positions
    avg_quali_position = grouped["position"].mean().rename("avg_quali_position")
    best_quali_position = grouped["position"].min().rename("best_quali_position")

    # Additional parameters
    q3_appearances = grouped["q3"].count().rename("q3_appearances")
    q2_appearances = grouped["q2"].count().rename("q2_appearances")
    q1_appearances = grouped["q1"].count().rename("q1_appearances")

    # Gather everything into a DataFrame
    qualifying_performance = pd.concat(
        [
            quali_sessions_count,
            pole_count,
            avg_quali_position,
            best_quali_position,
            q1_appearances,
            q2_appearances,
            q3_appearances,], axis = 1,).reset_index()

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
            "best_quali_position",
            "q1_appearances",
            "q2_appearances",
            "q3_appearances",]
        
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
    'sprint_results_cleaned.csv'.
    
    For each driverId, compute:
    - sprint_count: number of sprint sessions started
    - sprint_finished: number of sprints finished (statusId == 1)
    - best_sprint_position: best sprint result
    - avg_sprint_position: average sprint position on finished sprints only
    - top3_sprint_finishes: number of Top-3 sprint results
    - top8_sprint_finishes: number of scoring sprint finishes (<= 8)
    - sprint_points: total sprint points scored
    
    The result is saved to 'data/processed/sprint_performance.csv'.

    Returns:
        Path: Path to the saved 'sprint_performance.csv' file.
    """

    # Define file paths
    sprint_file = Path("Final–Project–Formula1/data/processed/sprint_results_cleaned.csv")
    output_file = Path("Final–Project–Formula1/data/processed/sprint_performance.csv")

    # Load data
    try:
        sprint_df = pd.read_csv(sprint_file)
    except Exception as e:
        print(f"⚠️ Error while reading {sprint_file}: {e}")
        return None

    # Group by driverId
    grouped = sprint_df.groupby("driverId")

    # Basic parameters
    sprint_count = grouped.size().rename("sprint_count")
    sprint_finished = grouped.apply(lambda g: (g["statusId"] == 1).sum()).rename("sprint_finished")
    best_sprint_position = grouped["position"].min().rename("best_sprint_position")

    # Average sprint position on finished sprints only
    def _avg_sprint_pos(g: pd.DataFrame) -> float:
        
        finished = g[g["statusId"] == 1]
        
        if finished.empty:
            return pd.NA
        return finished["position"].mean()
        
    avg_sprint_position = grouped.apply(_avg_sprint_pos).rename("avg_sprint_position")

    # Additional parameters
    top3_sprint_finishes = grouped.apply(lambda g: (g["position"] <= 3).sum()).rename("top3_sprint_finishes")
    top8_sprint_finishes = grouped.apply(lambda g: (g["position"] <= 8).sum()).rename("top8_sprint_finishes")

    # Total points scored
    sprint_points = grouped["points"].sum().rename("sprint_points")

    # Gather everything into a DataFrame
    sprint_performance = pd.concat(
        [
            sprint_count,
            sprint_finished,
            best_sprint_position,
            avg_sprint_position,
            top3_sprint_finishes,
            top8_sprint_finishes,
            sprint_points,], axis = 1,).reset_index()

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


def build_pit_stops_performance() -> Path:
    """
    Create an aggregated pit-stop performance table for each driver between 2020–2025
    using 'pit_stops_cleaned.csv'.

    For each driverId, compute:
    - pit_stops_count: total number of pit stops
    - avg_pit_duration_ms: average pit-stop duration in milliseconds
    - best_pit_duration_ms: best (shortest) pit-stop duration in milliseconds
    - worst_pit_duration_ms: worst (longest) pit-stop duration in milliseconds

    The result is saved to 'data/processed/pit_stops_performance.csv'.

    Returns:
        Path: Path to the saved 'pit_stops_performance.csv' file.
    """

    # Define file paths
    pit_file = Path("Final-Project-Formula1/data/processed/pit_stops_cleaned.csv")
    output_file = Path("Final-Project-Formula1/data/processed/pit_stops_performance.csv")

    # Load data
    try:
        pit_df = pd.read_csv(pit_file)
    except Exception as e:
        print(f"⚠️ Error while reading {pit_file}: {e}")
        return output_file

    # Group by driverId
    grouped = pit_df.groupby("driverId")

    # Parameters
    pit_stops_count = grouped.size().rename("pit_stops_count")
    avg_pit_duration_ms = grouped["milliseconds"].mean().rename("avg_pit_duration_ms")
    best_pit_duration_ms = grouped["milliseconds"].min().rename("best_pit_duration_ms")
    worst_pit_duration_ms = grouped["milliseconds"].max().rename("worst_pit_duration_ms")

    # Gather everything into a DataFrame
    pitstops_performance = pd.concat(
        [
            pit_stops_count,
            avg_pit_duration_ms,
            best_pit_duration_ms,
            worst_pit_duration_ms,], axis = 1,).reset_index()

    # Save new table to 'processed' folder
    pitstops_performance.to_csv(output_file, index = False)

    # Check
    try:
        check_df = pd.read_csv(output_file)
        expected_columns = [
            "driverId",
            "pit_stops_count",
            "avg_pit_duration_ms",
            "best_pit_duration_ms",
            "worst_pit_duration_ms",]

        all_columns_present = all(col in check_df.columns for col in expected_columns)
        
        if not all_columns_present:
            print(f"❌ Columns not found in pitstops_performance file saved to: {output_file}")
        else:
            print("✅ pitstops_performance successfully created and filled")
            print(f"📁 Saved to: {output_file}")
            
    except Exception as e:
        print(f"⚠️ Error while checking pitstops_performance file: {e}")

    return output_file


def add_driver_surname_to_pitstops() -> Path:
    """
    Add the 'surname' column to the 'pitstops_performance.csv' file
    by merging it with 'drivers_cleaned.csv' using driverId.

    The columns is inserted between the 'driverId' and 'pit_stops_count' columns.

    Returns:
        Path: Path to the updated 'pit_stops_performance.csv' file.
    """

    # Define file paths
    performance_file = Path("Final-Project-Formula1/data/processed/pit_stops_performance.csv")
    drivers_cleaned = Path("Final-Project-Formula1/data/processed/drivers_cleaned.csv")

    # Load data
    try:
        performance_df = pd.read_csv(performance_file)
        drivers_cleaned_df = pd.read_csv(drivers_cleaned)
    except Exception as e:
        print(f"⚠️ Error while loading files: {e}")
        return None

    # Merge to bring in surname and add the new column
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
            print("✅ 'surname' successfully added to pit_stops_performance.csv")
            print(f"📄 Saved to: {performance_file}")
            
    except Exception as e:
        print(f"⚠️ Error verifying updated file: {e}")
        return None

    return performance_file



"""

def build_races_drivers_features() -> Path:
    
    Create the main modelling table with one row per driver and per race between 2020-2025.

    The table gather information from:
    - results_cleaned.csv          (per-driver race result)
    - races_cleaned.csv            (race date, circuit, distance,...)
    - circuits_cleaned.csv         (length_km, is_night_race, track_type,...)
    - drivers_performance.csv      (aggregated driver metrics 2020–2025)
    - constructors_performance.csv (aggregated constructor metrics 2020–2025)

    Targets created:
    - finished: 1 if statusId == 1, else 0
    - finish_position: positionOrder if finished, otherwise NA
    - is_top10: positionOrder <= 10, else 0

    The final table is saved to 'data/processed/races_drivers_features.csv' file.

    Returns:
        Path: Path to the saved 'races_drivers_features.csv' file.
    

    # Define file paths
    root_dir = Path("Final-Project-Formula1/data/processed")
    results_cleaned = root_dir / "results_cleaned.csv"
    races_cleaned = root_dir / "races_cleaned.csv"
    circuits_cleaned = root_dir / "circuits_cleaned.csv"
    drivers_perf_file = root_dir / "drivers_performance.csv"
    constructors_perf_file = root_dir / "constructors_performance.csv"
    output_file = root_dir / "races_drivers_features.csv"

    # Load data
    try:
        results_df = pd.read_csv(results_cleaned)
        races_df = pd.read_csv(races_cleaned)
        circuits_df = pd.read_csv(circuits_cleaned)
        drivers_perf_df = pd.read_csv(drivers_perf_file)
        constructors_perf_df = pd.read_csv(constructors_perf_file)
    except Exception as e:
        print(f"⚠️ Error while loading input files for races_drivers_features: {e}")
        return output_file

    # Per-driver race results and add race-level information (year, circuit, distance,...)
    features = results_df.copy()
    features = features.merge(races_df[["raceId", "year", "round", "circuitId", "race_distance_km"]], on = "raceId", how = "left",)

    # Add circuit characteristics and aggregated driver performance
    features = features.merge(circuits_df[["circuitId", "length_km", "alt", "is_night_race", "track_type"]], on = "circuitId", how = "left",)
    features = features.merge(drivers_perf_df,on = "driverId", how = "left",)

    # Add aggregated constructor performance
    features = features.merge(constructors_perf_df, on = "constructorId", how = "left",)

    # Create target columns
    features["finished"] = (features["statusId"] == 1).astype(int)
    features["finish_position"] = features["positionOrder"].where(features["statusId"] == 1, other = pd.NA,)
    features["is_top10"] = (features["positionOrder"] <= 10)).astype(int)

    # Save final table
    features.to_csv(output_file, index = False)

    # Check
    try:
        check_df = pd.read_csv(output_file)
        expected_columns = [
            "raceId",
            "driverId",
            "constructorId",
            "grid",
            "positionOrder",
            "statusId",
            "year",
            "circuitId",
            "length_km",
            "is_night_race",
            "track_type",
            "finished",
            "finish_position",
            "is_top10",]
        
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












