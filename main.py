"""
Main entry point for the Formula 1 race outcome prediction project.

Pipeline:
1. Downloads the Kaggle dataset into data/raw/
2. Lists the available CSV files in data/raw/
3. Ensure data/processed/ exists
4. Filter races to recent seasons (2015–2025) and all race-based tables
5. Filter dimension tables (circuits, constructors, drivers, seasons, status)
6. Enrich processed tables (circuits, races, status) with extra information
7. Create progressive feature performance tables
   (drivers/constructors/sprint/qualifying/driver-circuit)
8. Create the final modelling dataset (model_dataset.csv) from the progressive
   performance tables and prepare the model dataset predictions for future seasons
9. Modelling (Random Forest baseline)
10. Evaluation and predictions
11. Visualizations (graphs)
"""

from pathlib import Path
import pandas as pd
import time

# 1,2) Download raw data and lists the available CSV files
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

# 7,8) Progressive feature performance tables, final modelling dataset and 
#      model dataset predictions for future seasons
from src.features import (
    build_driver_race_base,
    build_driver_progressive_performance,
    build_constructor_progressive_performance,
    build_sprint_progressive_performance,
    build_qualifying_progressive_performance,
    build_driver_circuits_progressive_performance,
    build_model_dataset,
    build_future_model_dataset,)

# 9) Modelling (Random Forest baseline)
from src.models import build_and_train_gp_model, build_and_train_sprint_model

#10) Evaluation and predictions
from src.evaluation import (
    create_results_folder,
    evaluate_all_targets,
    predict_season_2025,
    comparisons_predictions,
    predict_future_season,)

# 11) Visualizations
from src.visualization import (
    create_graphs_folder,
    plot_model_performance,
    plot_comparison_metrics,
    plot_predictions_summary_2026,
    plot_predictions_heatmaps_2026,
    plot_combined_feature_importances,)


def main() -> None:
    start_time = time.time()
    print("=== 🏁 F1 Project: full data pipeline ===")

    # 1. Download raw data
    print("\n🟦 STEP 1 – Download raw dataset")
    data_raw_path: Path = download_dataset()
    print(f"✅ Dataset downloaded successfully into {data_raw_path}")
    
    # 2. Lists the available CSV files
    print("\n🟦 STEP 2 – List CSV files in data/raw/")
    csv_files = sorted(data_raw_path.glob("*.csv"))
    if not csv_files:
        print("❌ No CSV files found in data/raw/. Please check Kaggle download.")
        return
    else:
        print("✅ CSV files available in data/raw/:")
        for f in csv_files:
            print("   -", f.name)
            
    # 3. Ensure data/processed/ exists
    print("\n🟦 STEP 3 – Ensure processed directory exists")
    processed_dir: Path = create_processed_folder()
    print(f"📁 Processed data directory: {processed_dir}")

    # 4. Filter races (2015–2025) and all race-based tables
    print("\n🟦 STEP 4 – Filter races and race-based tables")

    # 4.1 Filter races by year
    races_cleaned_path: Path = filter_races_by_year(start_year = 2015, end_year = 2025)
    races_df = pd.read_csv(races_cleaned_path)
    recent_race_ids = set(races_df["raceId"].unique())
    print(f"\n✅ Number of recent races: {len(recent_race_ids)}")

    # 4.2 Filter all tables that have a raceId column
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
    print("✅ All race-based tables filtered")
    
    # 5. Filter dimension tables (no raceId column)
    print("\n🟦 STEP 5 – Filter dimension tables (circuits, constructors, drivers, seasons, status)")
    filter_circuits_by_races()
    filter_constructors_by_results()
    filter_drivers_by_results()
    filter_seasons_by_year()
    filter_status_by_results()
    print("\n✅ Cleaning step finished. Cleaned files are available in data/processed/.")
    
    # 6. Data enrichment (circuits, races, status)
    print("\n🟦 STEP 6 – Data Enrichment")

    # 6.1 Circuits enrichment
    print("\n Enriching circuits_cleaned.csv with extra info")
    circuits_file = add_extra_info_on_circuits()
    if circuits_file is None:
        print("❌ Error in add_extra_info_on_circuits()")
        return
    print("✅ circuits_cleaned enriched")

    print("\n Filling missing circuit fields")
    circuits_file = fill_circuit_extra_info()
    if circuits_file is None:
        print("❌ Error in fill_circuit_extra_info()")
        return
    print("✅ circuits_cleaned fully filled")

    # 6.2 Races enrichment
    print("\n Enriching races_cleaned.csv with extra metadata")
    races_file = add_extra_info_on_races()
    if races_file is None:
        print("❌ Error in add_extra_info_on_races()")
        return
    print("✅ races_cleaned enriched")

    print("\n Filling missing race distances (km)")
    races_file = fill_races_distance_km()
    if races_file is None:
        print("❌ Error in fill_races_distance_km()")
        return
    print("✅ races distance info filled")

    # 6.3 Status enrichment
    print("\n Adding mechanical/crash/other categories to status_cleaned.csv")
    status_file = add_status_dnf_categories()
    if status_file is None:
        print("❌ Error in add_status_dnf_categories()")
        return
    print("✅ Status categories enriched (mech/crash/other/no_dnf)")

    # 7. Create progressive feature performance tables
    print("\n🟦 STEP 7 – Build progressive feature performance tables")

    # 7.1 Base driver-race table
    print("\n Building driver_race_base.csv")
    driver_race_base_file = build_driver_race_base()
    if driver_race_base_file is None:
        print("❌ Error in build_driver_race_base()")
        return
    print(f"✅ driver_race_base created: {driver_race_base_file}")

    # 7.2 Drivers performance
    print("\n Building driver_progressive_performance.csv")
    drivers_perf_file = build_driver_progressive_performance()
    if drivers_perf_file is None:
        print("❌ Error in build_driver_progressive_performance()")
        return
    print(f"✅ driver_progressive_performance created: {drivers_perf_file}")

    # 7.3 Constructors performance
    print("\n Building constructor_progressive_performance.csv")
    constructors_perf_file = build_constructor_progressive_performance()
    if constructors_perf_file is None:
        print("❌ Error in build_constructor_progressive_performance()")
        return
    print(f"✅ constructor_progressive_performance created: {constructors_perf_file}")

    # 7.4 Sprint performance per driver
    print("\n Building sprint_progressive_performance.csv")
    sprint_perf_file = build_sprint_progressive_performance()
    if sprint_perf_file is None:
        print("❌ Error in build_sprint_progressive_performance()")
        return
    print(f"✅ sprint_progressive_performance created: {sprint_perf_file}")

    # 7.5 Qualifying performance per driver
    print("\n Building qualifying_progressive_performance.csv")
    quali_perf_file = build_qualifying_progressive_performance()
    if quali_perf_file is None:
        print("❌ Error in build_qualifying_progressive_performance()")
        return
    print(f"✅ qualifying_progressive_performance created: {quali_perf_file}")

    # 7.6 Driver x circuit performance
    print("\n Building driver_circuits_progressive_performance.csv")
    circuits_perf_file = build_driver_circuits_progressive_performance()
    if circuits_perf_file is None:
        print("❌ Error in build_driver_circuits_progressive_performance()")
        return
    print(f"✅ driver_circuits_progressive_performance created: {circuits_perf_file}")
    
    # 8. Create final modelling dataset
    print("\n🟦 STEP 8 – Build final modelling dataset and the future season dataset for predictions")
    model_file = build_model_dataset()
    if model_file is None:
        print("❌ Error while building model_dataset.csv")
        return
    else:
        print(f"✅ model_dataset.csv successfully created")
        print(f"📂 Saved to: {model_file}")

    # 8.1 Create future season dataset (2026)
    print("\n Build future model dataset for next season predictions (2026)")
    future_file = build_future_model_dataset(next_year = 2026)
    if future_file is None:
        print("❌ Error while building model_dataset_predictions_2026.csv")
    else:
        print("✅ model_dataset_predictions_2026.csv ready for future predictions")

    # 9. Modelling (Random Forest baseline)
    print("\n🟦 STEP 9 – Train Random Forest models")
    gp_targets = ["target_top10", "target_top3", "target_win"]
    for target in gp_targets:
        print(f"\n✅ Training GP model for target: {target}")
        build_and_train_gp_model(target_col = target)
        
    sprint_targets = ["target_top8_sprint", "target_top3_sprint", "target_win_sprint"]
    for target in sprint_targets:
        print(f"\n✅ Training Sprint model for target: {target}")
        build_and_train_sprint_model(target_col = target)
        
    print("\n✅ All Random Forest models successfully trained and evaluated")
    
    # 10. Evaluation and predictions
    print("\n🟦 STEP 10 – Model evaluation, season 2025 simulation, and future season predictions")
    
    print("\n Ensure results directory exists")
    results_dir: Path = create_results_folder()
    print(f"📁 Results directory: {results_dir}")

    # 10.1 Evaluate the Random Forest baseline on all targets
    print("\n Evaluating Random Forest baseline on 2025 test season")
    metrics_df = evaluate_all_targets()
    print(metrics_df)

    # 10.2 Predict the 2025 season (using data up to 2024)
    print("\n Predicting 2025 season (trained on data up to 2024)")
    pred_2025_df = predict_season_2025()
    print(f"✅ 2025 predictions generated successfully ({len(pred_2025_df)} rows)")

    # 10.3 Compare predictions vs real results
    print("\n Comparing predicted 2025 results with real results")
    comparison_df = comparisons_predictions(season_year = 2025)
    print(f"✅ Comparison metrics saved successfully ({len(comparison_df)} rows)")

    # 10.4 Predict future season (2026)
    print("\n Predicting future season (2026)")
    predictions_df = predict_future_season(next_year = 2026)
    print(f"✅ Future season predictions saved successfully ({len(predictions_df)} rows)")
    print("\n✅ All evaluation and predictions steps successfully generated")
    
    # 11. Visualizations
    print("\n🟦 STEP 11 – Generating visualizations for model evaluation and 2026 predictions")
    
    print("\n Ensure graphs directory exists")
    graphs_dir: Path = create_graphs_folder()
    print(f"📁 Figures directory: {graphs_dir}")

    # 11.1 Visualize model performance metrics
    print("\n Visualize model performance metrics")
    plot_model_performance()
    print("✅ Model performance plot saved successfully")

    # 11.2 Visualize comparison metrics (predicted vs real 2025)
    print("\n Visualize comparison metrics (2025 predicted vs actual)")
    plot_comparison_metrics()
    print("✅ Comparison metrics visualization saved successfully")

    # 11.3 Visualize 2026 predictions summary
    print("\n Visualize 2026 driver prediction summary")
    plot_predictions_summary_2026()
    print("✅ Prediction summary plot saved successfully")

    # 11.4 Visualize 2026 heatmaps (GP + Sprint)
    print("\n Visualize 2026 GP & Sprint heatmaps")
    plot_predictions_heatmaps_2026()
    print("✅ Heatmaps generated and saved successfully")

    # 11.5 Visualize
    print("\n Visualize feature importance across all models")
    plot_combined_feature_importances()
    print("✅ Combined feature importance plot generated and saved successfully")
    
    print("\n All visualization plots successfully generated and saved")

    # End
    end_time = time.time()
    elapsed = end_time - start_time
    mins, secs = divmod(elapsed, 60)
    print(f"\n🏁 Pipeline completed successfully in {int(mins)}m {secs:.1f}s")
    
if __name__ == "__main__":
    main()