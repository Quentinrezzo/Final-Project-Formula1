"""
This module contains the machine learning workflow used to train and evaluate
predictive models for Formula 1 race outcomes.

It works on model_dataset.csv, which already uses N-1 progressive features
(each row only knows the history before that season / race).

The usual pattern is:
- split the data by year into train / validation / test
- train a Random Forest baseline model
- report accuracy and ROC-AUC on the test year/season
"""

from pathlib import Path
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score, accuracy_score

# Import processed_direction
from .data_loader import processed_direction

def train_val_test_split_by_year(
    df: pd.DataFrame,
    target_col: str = "target_top10",
    train_years: tuple = (),
    val_years: tuple = (),
    test_years: tuple = (),):
    """
    Split the dataset by year/season to avoid data leakage.

    Args:
        df: pd.DataFrame, full modelling dataset (one row = driver in a race)
        target_col (str): name of the target column
        train_years (tuple): years used for training
        val_years (tuple): years used for validation
        test_years (tuple): years used for test
    
    Returns:
        (X_train, y_train, X_val, y_val, X_test, y_test)
    """
    
    # All target and IDs columns must not be used as features
    target_columns = [c for c in df.columns if c.startswith("target_")]
    id_columns = ["raceId", "driverId", "constructorId", "circuitId"]
    feature_columns = [c for c in df.columns if c not in target_columns + id_columns]
    
    # Split by years
    train_df = df[df["year"].isin(train_years)]
    val_df = df[df["year"].isin(val_years)]
    test_df = df[df["year"].isin(test_years)]
    
    X_train, y_train = train_df[feature_columns], train_df[target_col]
    X_val, y_val = val_df[feature_columns], val_df[target_col]
    X_test, y_test = test_df[feature_columns], test_df[target_col]

    return X_train, y_train, X_val, y_val, X_test, y_test


def train_rf_baseline(X_train, y_train, X_val, y_val, random_state = 42):
    """
    Train a simple Random Forest baseline model and print validation metrics.
    """
    
    model = RandomForestClassifier(
        n_estimators = 700,
        max_depth = 20,
        min_samples_split = 2,
        min_samples_leaf = 3,
        max_features = "sqrt",
        bootstrap = True,
        class_weight = "balanced",
        random_state = int(random_state),
        n_jobs =-1,)
    
    model.fit(X_train, y_train)

    # Validation metrics
    val_proba = model.predict_proba(X_val)[:, 1]
    val_pred = (val_proba >= 0.5).astype(int)

    val_acc = accuracy_score(y_val, val_pred)
    val_auc = roc_auc_score(y_val, val_proba)

    print("\n=== Validation performance (Random Forest baseline) ===")
    print("Val accuracy:", val_acc)
    print("Val ROC-AUC:", val_auc)

    return model

    
def build_and_train_gp_model(target_col: str = "target_top10",)-> RandomForestClassifier:
    """
    Full modelling pipeline for one GP target:
    - load model_dataset.csv
    - automatically splits years into train / val / test
    - trains a Random Forest baseline model
    - evaluate it on the test set

    Args:
        target_col(str): name of the target column (target_top10, target_top3, target_win)

    Returns:
        Trained RandomForestClassifier model
    """
    
    print(f"\n=== Training race model for target: {target_col} ===")

    # Load dataset
    df_path = processed_direction / "model_dataset.csv"
    df = pd.read_csv(df_path)
    
    if "year" not in df.columns:
        raise KeyError("Column year is missing from model_dataset.csv")

    # Determine seasons automatically
    years = sorted(df["year"].unique())
    if len(years) < 3:
        raise ValueError(f"Need at least 3 different seasons in model_dataset.csv for train/val/test split, found {len(years)}: {years}")
        
    test_year = years[-1]
    val_year = years[-2]
    train_years = tuple(y for y in years if y < val_year)

    test_years = (test_year,)
    val_years = (val_year,)
    
    # Split by years (no leakage)
    X_train, y_train, X_val, y_val, X_test, y_test = train_val_test_split_by_year(
        df,
        target_col = target_col,
        train_years = train_years,
        val_years = val_years,
        test_years = test_years,)

    # Train Random Forest baseline model
    model = train_rf_baseline(X_train, y_train, X_val, y_val)

    # test metrics
    y_test_proba = model.predict_proba(X_test)[:, 1]
    y_test_pred = (y_test_proba >= 0.5).astype(int)

    test_acc = accuracy_score(y_test, y_test_pred)
    test_auc = roc_auc_score(y_test, y_test_proba)
    
    print(f"\n=== Test set performance {target_col} ===")
    print("Test accuracy:", test_acc)
    print("Test ROC-AUC:", test_auc)

    return model


def build_and_train_sprint_model(target_col: str = "target_top8_sprint")-> RandomForestClassifier:
    """
    Full modelling pipeline for one sprint target:
    - load model_dataset.csv
    - keeps only rows corresponding to sprint races
    - automatically splits years into train / val / test
    - trains a Random Forest baseline model
    - evaluate it on the test set

    Args:
        target_col(str): name of the target column (target_top8_sprint, target_top3_sprint, target_win_sprint)

    Returns:
        Trained RandomForestClassifier model
    """

    print(f"\n=== Training sprint model for target: {target_col} ===")

    # Load dataset
    df_path = processed_direction / "model_dataset.csv"
    df = pd.read_csv(df_path)
    
    if "year" not in df.columns:
        raise KeyError("Column year is missing from model_dataset.csv")

    if "has_sprint" not in df.columns:
        raise KeyError("Column has_sprint is missing from model_dataset.csv")
        
    # Keeps only rows where a sprint race took place
    df_sprint = df[df["has_sprint"] == 1].copy()

    if df_sprint.empty:
        raise ValueError("No sprint rows found in model_dataset.csv")
        
    # Determine seasons automatically
    years = sorted(df_sprint["year"].unique())
    if len(years) < 3:
        raise ValueError(f"Need at least 3 different sprint seasons in model_dataset.csv for train/val/test split, found {len(years)}: {years}")

    test_year = years[-1]
    val_year = years[-2]
    train_years = tuple(y for y in years if y < val_year)

    test_years = (test_year,)
    val_years = (val_year,)

    # Split by years (no leakage)
    X_train, y_train, X_val, y_val, X_test, y_test = train_val_test_split_by_year(
        df_sprint,
        target_col = target_col,
        train_years = train_years,
        val_years = val_years,
        test_years = test_years,)
    
    # Train Random Forest baseline model
    model = train_rf_baseline(X_train, y_train, X_val, y_val)

    # test metrics
    y_test_proba = model.predict_proba(X_test)[:, 1]
    y_test_pred = (y_test_proba >= 0.5).astype(int)

    test_acc = accuracy_score(y_test, y_test_pred)
    test_auc = roc_auc_score(y_test, y_test_proba)
    
    print(f"\n=== Test set performance (sprint) {target_col} ===")
    print("Test accuracy:", test_acc)
    print("Test ROC-AUC:", test_auc)

    return model