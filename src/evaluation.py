"""
Evaluation tools for Formula 1 race outcome models.

This file reuses the training functions in models.py to:
- train a Random Forest baseline model
- evaluate it on the test set and save metrics to the results/ folder
- use the trained model to predict a future season (e.g., 2026)
"""

from pathlib import Path
import pandas as pd
from sklearn.metrics import roc_auc_score, accuracy_score

# Import paths and training functions
from .data_loader import processed_direction
from .models import train_val_test_split_by_year, train_rf_baseline
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


def predict_future_season(
    season_year: int = 2025,
    targets = ("target_top10", "target_top3", "target_win"),
    train_years = (2020, 2021, 2022, 2023),
    val_years = (2024,),
    test_years = (2025,),) -> pd.DataFrame:
    """
    Train an XGBoost model on past seasons and generate predictions
    for a future season.

    The predictions are saved as: results/predictions_{season_year}.csv.
    
    Returns:
        pd.DataFrame: future season table
    """

    # Define file paths (historical data with targets and future season features 
    # without target)
    hist_file = processed_direction / "model_dataset.csv"
    future_file = processed_direction / f"model_dataset_predictions_{season_year}.csv"
    output_file = results_direction / f"predictions_{season_year}.csv"
    
    # Load data
    try:
        hist_df = pd.read_csv(hist_file)
        future_df = pd.read_csv(future_file)
    except Exception as e:
        print(f"⚠️ Error while reading {hist_file} or {future_file}: {e}")
        return None

    # Loop over targets
    for target_col in targets:
        print(f"\n Predicting {target_col} for season {season_year}")

        # Split by year (no leakage)
        X_train, y_train, X_val, y_val, X_test, _ = train_val_test_split_by_year(
            hist_df,
            target_col = target_col,
            train_years = train_years,
            val_years = val_years,
            test_years = test_years,)

        # Train XGBoost baseline model
        model = train_xgb_baseline(X_train, y_train, X_val, y_val)

        # Use exactly the same feature columns on the future season
        feature_columns = list(X_train.columns)
        X_future = future_df[feature_columns].copy()
        
        for col in feature_columns:
            X_future[col] = X_future[col].astype(X_train[col].dtype)

        # Predictions
        proba = model.predict_proba(X_future)[:, 1]
        
        # Clear columns with suffix
        if target_col.startswith("target_"):
            suffix = target_col.replace("target_", "")
        else:
            suffix = target_col

        pred_col = f"pred_{suffix}"
        proba_col = f"proba_{suffix}"

        # Save the probabilities and initialize the prediction column to 0
        future_df[proba_col] = proba
        future_df[pred_col] = 0
        
        # rank by raceId
        for race_id, idx in future_df.groupby("raceId").groups.items():
            
            # Top10: mark the 10 best probabilities
            if suffix == "top10":
                top_idx = future_df.loc[idx, proba_col].nlargest(10).index
                future_df.loc[top_idx, pred_col] = 1
            
            # Top3: mark the 3 best probabilities
            elif suffix == "top3":
                top_idx = future_df.loc[idx, proba_col].nlargest(3).index
                future_df.loc[top_idx, pred_col] = 1
            
            # Win: mark only the top probability
            elif suffix == "win":
                winner_idx = future_df.loc[idx, proba_col].idxmax()
                future_df.loc[winner_idx, pred_col] = 1

    # Save predictions table to results/ folder
    future_df.to_csv(output_file, index = False)

    print(f"\n✅ Predictions for season {season_year} saved to: {output_file}")

    return future_df


def comparisons_predictions(
    season_year: int = 2025,
    targets = ("target_top10", "target_top3", "target_win"),) -> pd.DataFrame:
    """
    Compare the model predictions for a given season with the actual outcomes.

    The comparisons are saved as: results/comparisons_predictions_{season_year}.csv.
    
    Returns:
        pd.DataFrame: table with accuracy per target for the given season.
    """

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
    merged = pred_df.merge(true_df, on = ["raceId", "driverId"], how = "inner",
                           suffixes = ("_predfile", "_truefile"),)

    predictions = []

    for target_col in targets:
        suffix = target_col.replace("target_", "")
        pred_col = f"pred_{suffix}"
        
        y_true = merged[target_col]
        y_pred = merged[pred_col]
        
        acc = accuracy_score(y_true, y_pred)
        
        predictions.append({
            "season_year": season_year,
            "target": target_col,
            "pred_column": pred_col,
            "samples": len(merged),
            "accuracy": acc,})

    predictions_df = pd.DataFrame(predictions)

    # Save comparisons table to results/ folder
    predictions_df.to_csv(output_file, index = False)

    print(f"\n✅ Comparison metrics saved to: {output_file}")

    return predictions_df

    















