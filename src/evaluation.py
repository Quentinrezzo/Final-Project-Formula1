"""
Evaluation and prediction tools for Formula 1 race outcome models.

This file reuses the training functions in models.py to:
- train and evaluate a Random Forest baseline model
- evaluate it on the test set and save metrics to the results/ folder
- compare the model predictions for a given season with the actual outcomes
- use the trained model to predict a future season (e.g., 2026)
"""

from pathlib import Path
import pandas as pd
import numpy as np
from sklearn.metrics import roc_auc_score, f1_score, accuracy_score

# Import paths and training functions
from .data_loader import processed_direction
from .models import train_val_test_split_by_year, train_rf_baseline
from .features import build_future_model_dataset
from .downloading_dataset import project_root

# Path to the results/ directory
results_direction = project_root / "results"

def create_results_folder() -> Path:
    """
    Create the results/ folder inside the Final-Project-Formula1/ folder.
    
    Returns:
        Path: path to the results/ directory.
    """
    
    # Create the results/ folder
    results_direction.mkdir(parents = True, exist_ok = True)

    # Check if results/ exists
    if results_direction.exists():
        print(f"📁 Folder created: {results_direction}")
    else:
        print(f"❌ Folder not created: {results_direction}")

    return results_direction


def evaluate_rf_for_target(target_col: str = "target_top10") -> dict:
    """
    Train and evaluate the Random Forest baseline model for a single target.

    Returns:
        a dictionary with accuracy and ROC-AUC on the test set.
    """

    # Load dataset
    df_path = processed_direction / "model_dataset.csv"
    df = pd.read_csv(df_path)

    # Detect if it's a sprint target
    is_sprint = "sprint" in target_col.lower()

    # GP vs Sprint dataset selection
    if is_sprint:
        if "has_sprint" not in df.columns:
            raise KeyError("Column 'has_sprint' missing from model_dataset.csv")

        # Keeps only rows where a sprint race took place
        df_sprint = df[df["has_sprint"] == 1].copy()
        if df_sprint.empty:
            raise ValueError("No sprint rows found in model_dataset.csv")
        
        # Determine sprint seasons automatically
        years = sorted(df_sprint["year"].unique())
        if len(years) < 3:
            raise ValueError(f"Need at least 3 different sprint seasons in model_dataset.csv for train/val/test split, found {len(years)}: {years}")
            
        test_year = years[-1]
        val_year = years[-2]
        train_years = tuple(y for y in years if y < val_year)

        test_years = (test_year,)
        val_years = (val_year,)
        df_to_use = df_sprint

    else:
        # Determine GP seasons automatically
        years = sorted(df["year"].unique())
        if len(years) < 3:
            raise ValueError(f"Need at least 3 different sprint seasons in model_dataset.csv for train/val/test split, found {len(years)}: {years}")
            
        test_year = years[-1]
        val_year = years[-2]
        train_years = tuple(y for y in years if y < val_year)

        test_years = (test_year,)
        val_years = (val_year,)
        df_to_use = df

    # Split by year (no leakage)
    X_train, y_train, X_val, y_val, X_test, y_test = train_val_test_split_by_year(
        df_to_use,
        target_col = target_col,
        train_years = train_years,
        val_years = val_years,
        test_years = test_years,)
    
    # Train Random Forest baseline model
    model = train_rf_baseline(X_train, y_train, X_val, y_val)

    # Test metrics
    y_test_proba = model.predict_proba(X_test)[:, 1]
    y_test_pred = (y_test_proba >= 0.5).astype(int)

    test_acc = accuracy_score(y_test, y_test_pred)
    test_auc = roc_auc_score(y_test, y_test_proba)

    print(f"\n=== Test set performance for {target_col} ===")
    print("Test accuracy:", test_acc)
    print("Test ROC-AUC:", test_auc)

    return {
        "target": target_col,
        "train_years": str([int(y) for y in train_years])[1:-1],
        "val_years": str([int(y) for y in val_years])[1:-1],
        "test_years": str([int(y) for y in test_years])[1:-1],
        "test_accuracy": test_acc,
        "test_roc_auc": test_auc,}


def evaluate_all_targets() -> pd.DataFrame:
    """
    Evaluate the Random Forest baseline on several targets and report accuracy and
    ROC-AUC on the 2025 test season.

    The metrics are saved as: results/rf_baseline_metrics.csv.
    
    Returns:
        pd.DataFrame: table of metrics for each target.
    """

    # Define the path
    output_file = results_direction / "rf_baseline_metrics.csv"

    all_targets = {
        "target_top10": "gp",
        "target_top3": "gp",
        "target_win": "gp",
        "target_top8_sprint": "sprint",
        "target_top3_sprint": "sprint",
        "target_win_sprint": "sprint",}
    
    all_metrics = []

    for target, race_type in all_targets.items():
        print(f"\n Evaluating target: {target}")
        metrics = evaluate_rf_for_target(target_col = target)
        metrics["race_type"] = race_type
        
        all_metrics.append(metrics)

    metrics_df = pd.DataFrame(all_metrics)
    
    # Save to results/ folder
    metrics_df.to_csv(output_file, index = False)
    print(f"\nMetrics saved to: {output_file}")

    return metrics_df


def predict_season_2025() -> pd.DataFrame:
    """
    Train the Random Forest model on data up to 2024 and generate predictions for 2025.

    This version is similar to predict_future_season(), but specifically designed
    to compare the model’s predicted 2025 season with the actual 2025 results.

    The predictions are saved as: results/predictions_2025.csv

    Returns:
        pd.DataFrame: prediction table for the 2025 season.
    """

    # Define targets
    targets = [
        "target_top10",
        "target_top3",
        "target_win",
        "target_top8_sprint",
        "target_top3_sprint",
        "target_win_sprint"]

    # Define file paths
    hist_file = processed_direction / "model_dataset.csv"
    future_file = build_future_model_dataset(next_year = 2025)
    output_file = results_direction / "predictions_2025.csv"

    # Load datasets
    try:
        hist_df = pd.read_csv(hist_file)
        future_df = pd.read_csv(future_file)
    except Exception as e:
        print(f"⚠️ Error while reading {hist_file} or {future_file}: {e}")
        return None

    # Ensure required columns
    for col in ["year", "has_sprint"]:
        if col not in hist_df.columns:
            raise KeyError(f"Missing essential column: {col}")

    results = []

    # Predict per target
    for target_col in targets:
        print(f"\n Predicting for target {target_col}")

        is_sprint = "sprint" in target_col.lower()
        df_use = hist_df[hist_df["has_sprint"] == 1] if is_sprint else hist_df.copy()

        # Train up to 2024 only
        df_use = df_use[df_use["year"] <= 2024]
        years = sorted(df_use["year"].unique())
        if len(years) < 3:
            print(f"⚠️ Not enough seasons for {target_col}. Found: {years}")
            continue

        val_year = years[-1]   # 2024
        train_years = tuple(y for y in years if y < val_year)

        X_train, y_train, X_val, y_val, _, _ = train_val_test_split_by_year(
            df_use,
            target_col = target_col,
            train_years = train_years,
            val_years = (val_year,),
            test_years = ())

        model = train_rf_baseline(X_train, y_train, X_val, y_val)

        # Prepare 2025 data
        if "has_sprint" in future_df.columns:
            future_df["race_type"] = "gp"
            sprint_weekends = future_df.loc[future_df["has_sprint"] == 1, ["raceId", "driverId"]]
            sprint_rows = future_df.merge(sprint_weekends, on = ["raceId", "driverId"], how = "inner")
            sprint_rows["race_type"] = "sprint"
            future_df = pd.concat([future_df, sprint_rows], ignore_index = True)
            future_df = future_df.drop_duplicates(subset = ["raceId", "driverId", "race_type"], keep = "first").reset_index(drop = True)

        # Filter race type
        future_use = future_df.copy()
        if is_sprint:
            future_use = future_use[future_use["race_type"] == "sprint"]
        else:
            future_use = future_use[future_use["race_type"] == "gp"]

        if future_use.empty:
            print(f"⚠️ Skipping {target_col}: no data available for this race type")
            continue

        # Predict probabilities
        feature_cols = [c for c in X_train.columns if c in future_use.columns]
        proba = model.predict_proba(future_use[feature_cols])[:, 1]

        preds = future_use[["raceId", "driverId", "constructorId", "circuitId", "race_type"]].copy()
        preds[f"proba_{target_col}"] = proba
        preds[f"pred_{target_col}"] = 0

        # Assign binary predictions
        for race_id, idx in preds.groupby("raceId").groups.items():
            race_probs = preds.loc[idx, f"proba_{target_col}"]

            if "top10" in target_col:
                top_idx = race_probs.nlargest(10).index
            elif "top3" in target_col and "sprint" not in target_col:
                top_idx = race_probs.nlargest(3).index
            elif "win" in target_col and "sprint" not in target_col:
                top_idx = [race_probs.idxmax()]
            elif "top8_sprint" in target_col:
                top_idx = race_probs.nlargest(8).index
            elif "top3_sprint" in target_col:
                top_idx = race_probs.nlargest(3).index
            elif "win_sprint" in target_col:
                top_idx = [race_probs.idxmax()]
            else:
                continue
            preds.loc[top_idx, f"pred_{target_col}"] = 1

        results.append(preds)

    # Merge all predictions
    if not results:
        print("⚠️ No predictions generated.")
        return None

    final_preds = results[0].copy()
    for df_part in results[1:]:
        final_preds = final_preds.merge(df_part, on = ["raceId", "driverId", "constructorId", "circuitId", "race_type"], how = "outer")

    # Ensure IDs are integers
    for col in ["raceId", "driverId", "constructorId", "circuitId"]:
        final_preds[col] = pd.to_numeric(final_preds[col], errors = "coerce").fillna(0).astype(int)

    # Save predictions to results/ folder
    final_preds.to_csv(output_file, index = False)
    print(f"\n✅ Predictions for 2025 saved to: {output_file}")

    return final_preds


def comparisons_predictions(season_year: int = 2025) -> pd.DataFrame:
    """
    Compare the model predictions for a given season with the actual outcomes.

    The comparisons are saved as: results/comparisons_predictions_{season_year}.csv.
    
    Returns:
        pd.DataFrame: table with accuracy per target for the given season.
    """
    
    # Define targets
    targets = [
        "target_top10", 
        "target_top3",
        "target_win",
        "target_top8_sprint",
        "target_top3_sprint",
        "target_win_sprint"]
    
    # Define file paths
    hist_file = processed_direction / "model_dataset.csv"
    pred_file = results_direction / f"predictions_{season_year}.csv"
    output_file = results_direction / f"comparisons_predictions_{season_year}.csv"

    # Load data
    try:
        hist_df = pd.read_csv(hist_file)
        pred_df = pd.read_csv(pred_file)
    except Exception as e:
        print(f"⚠️ Error while reading {hist_file} or {pred_file}: {e}")
        return None

    # Keep only the requested season in the historical dataset
    true_df = hist_df[hist_df["year"] == season_year].copy()

    # Merge on raceId and driverId
    merge_cols = ["raceId", "driverId"]
    if "race_type" in pred_df.columns and "race_type" in true_df.columns:
        merge_cols.append("race_type")

    df = pred_df.merge(true_df, on = merge_cols, how = "inner", suffixes = ("_pred", "_true"))
    
    # Compute metrics per target
    results = []
    
    for target_col in targets:
        pred_col = f"pred_{target_col}"
        proba_col = f"proba_{target_col}"

        if target_col not in df.columns or pred_col not in df.columns:
            continue

        subset = [target_col, pred_col]
        if proba_col in df.columns:
            subset.append(proba_col)

        clean_df = df.dropna(subset=subset)
        if clean_df.empty:
            continue

        y_true = clean_df[target_col]
        y_pred = clean_df[pred_col]
        y_proba = clean_df[proba_col] if proba_col in clean_df.columns else None

        acc = accuracy_score(y_true, y_pred)
        f1 = f1_score(y_true, y_pred, zero_division=0)
        auc = roc_auc_score(y_true, y_proba) if y_proba is not None else None
        
        results.append({
            "season_year": season_year,
            "target": target_col,
            "samples": len(clean_df),
            "accuracy": acc,
            "f1_score": f1,
            "roc_auc": auc})
        
    results_df = pd.DataFrame(results)
    
    # Save comparisons table to results/ folder
    results_df.to_csv(output_file, index = False)
    print(f"\n✅ Comparison metrics saved to: {output_file}")
    
    return results_df


def predict_future_season(next_year: int = 2026) -> pd.DataFrame:
    """
    Train the Random Forest model on past seasons and generate predictions
    for a future season.

    The predictions are saved as: results/predictions_{next_year}.csv.
    
    Returns:
        pd.DataFrame: future season table
    """
    
    # Define targets
    targets = [
        "target_top10", 
        "target_top3",
        "target_win",
        "target_top8_sprint",
        "target_top3_sprint",
        "target_win_sprint"]

    # Define file paths
    hist_file = processed_direction / "model_dataset.csv"
    future_file = build_future_model_dataset(next_year = next_year)
    output_file = results_direction / f"predictions_{next_year}.csv"
    
    # Load dataset
    try:
        hist_df = pd.read_csv(hist_file)
        future_df = pd.read_csv(future_file)
    except Exception as e:
        print(f"⚠️ Error while reading {hist_file} or {future_file}: {e}")
        return None

    for col in ["year", "has_sprint"]:
        if col not in hist_df.columns:
            raise KeyError(f"Missing essential column: {col}")

    results = []
    
    # Predict per target
    for target_col in targets:
        print(f"\n Predicting for target {target_col}")

        is_sprint = "sprint" in target_col.lower()
        df_use = hist_df[hist_df["has_sprint"] == 1] if is_sprint else hist_df.copy()

        years = sorted(df_use["year"].unique())
        if len(years) < 3:
            print(f"⚠️ Not enough seasons for {target_col}. Found: {years}")
            continue

        val_year = years[-1]
        train_years = tuple(y for y in years if y < val_year)

        X_train, y_train, X_val, y_val, _, _ = train_val_test_split_by_year(
            df_use,
            target_col=target_col,
            train_years=train_years,
            val_years=(val_year,),
            test_years=())
        
        model = train_rf_baseline(X_train, y_train, X_val, y_val,)

        if "has_sprint" in future_df.columns:
            future_df["race_type"] = "gp"
            sprint_weekends = future_df.loc[future_df["has_sprint"] == 1, ["raceId", "driverId"]]
            sprint_rows = future_df.merge(sprint_weekends, on = ["raceId", "driverId"], how = "inner")
            sprint_rows["race_type"] = "sprint"
            future_df = pd.concat([future_df, sprint_rows], ignore_index = True)
            
            future_df = future_df.drop_duplicates(subset = ["raceId", "driverId", "race_type"], keep = "first").reset_index(drop = True)
            
        # Filter race types
        future_use = future_df.copy()
        if is_sprint:
            future_use = future_use[future_use["race_type"] == "sprint"]
        else:
            future_use = future_use[future_use["race_type"] == "gp"]

        if future_use.empty:
            print(f"⚠️ Skipping {target_col} — no data available for this race type")
            continue

        # Predict probabilities
        feature_cols = [c for c in X_train.columns if c in future_use.columns]
        proba = model.predict_proba(future_use[feature_cols])[:, 1]

        preds = future_use[["raceId", "driverId", "constructorId", "circuitId", "race_type"]].copy()
        preds[f"proba_{target_col}"] = proba
        preds[f"pred_{target_col}"] = 0

        # Assign binary predictions
        for race_id, idx in preds.groupby("raceId").groups.items():
            race_probs = preds.loc[idx, f"proba_{target_col}"]
            
            if "top10" in target_col:
                top_idx = race_probs.nlargest(10).index
            elif "top3" in target_col and "sprint" not in target_col:
                top_idx = race_probs.nlargest(3).index
            elif "win" in target_col and "sprint" not in target_col:
                top_idx = [race_probs.idxmax()]
            elif "top8_sprint" in target_col:
                top_idx = race_probs.nlargest(8).index
            elif "top3_sprint" in target_col:
                top_idx = race_probs.nlargest(3).index
            elif "win_sprint" in target_col:
                top_idx = [race_probs.idxmax()]
            else:
                continue
            preds.loc[top_idx, f"pred_{target_col}"] = 1

        results.append(preds)

    # Merge all predictions
    if not results:
        print("⚠️ No predictions generated.")
        return None

    final_preds = results[0].copy()
    for df_part in results[1:]:
        final_preds = final_preds.merge(df_part, on = ["raceId", "driverId", "constructorId", "circuitId", "race_type"], how = "outer")

    # Ensure IDs are integers
    for col in ["raceId", "driverId", "constructorId", "circuitId"]:
        final_preds[col] = pd.to_numeric(final_preds[col], errors = "coerce").fillna(0).astype(int)

    # Fix a bug
    for (race_id, race_type), group in final_preds.groupby(["raceId", "race_type"]):
        if race_type == "gp":
            topX, top3, win = "pred_target_top10", "pred_target_top3", "pred_target_win" 
            n_topX = 10
        else:
            topX, top3, win = "pred_target_top8_sprint", "pred_target_top3_sprint", "pred_target_win_sprint"
            n_topX = 8
            
        final_preds.loc[group.index, [topX, top3, win]] = 0
        proba_topX = f"proba_{topX.replace('pred_', '')}"
        if proba_topX not in group.columns:
            continue
        
        probs = group[[proba_topX]].copy()
        top10_idx = probs.nlargest(n_topX, proba_topX).index
        top3_idx = probs.nlargest(3, proba_topX).index
        win_idx = [probs[proba_topX].idxmax()]
    
        final_preds.loc[top10_idx, topX] = 1
        final_preds.loc[top3_idx, top3] = 1
        final_preds.loc[win_idx, win] = 1
        
    # Save predictions to results/ folder
    final_preds.to_csv(output_file, index = False)
    print(f"\n✅ All predictions saved to: {output_file}")

    return final_preds