"""
This module contains the machine learning workflow used to train and evaluate
predictive models for Formula 1 race outcomes.
"""

from pathlib import Path
import pandas as pd
import xgboost as xgb
from xgboost import XGBClassifier
from sklearn.metrics import roc_auc_score, accuracy_score

# Import processed_direction
from .data_loader import processed_direction

def train_val_test_split_by_year(
    df: pd.DataFrame,
    target_col: str = "target_top10",
    train_years = (2020, 2021, 2022,2023),
    val_years = (2024,),
    test_years = (2025,),):
    """
    Split the dataset by year to avoid data leakage.
    
    Returns (X_train, y_train, X_val, y_val, X_test, y_test).
    """
    
    target_columns = [c for c in df.columns if c.startswith("target_")]
    feature_columns = [c for c in df.columns if c not in target_columns]

    print("Nb features :", len(feature_columns))
    print("'target_points' dans features ?",
          "target_points" in feature_columns)
    
    # Split by years
    train_df = df[df["year"].isin(train_years)]
    val_df = df[df["year"].isin(val_years)]
    test_df = df[df["year"].isin(test_years)]

    X_train, y_train = train_df[feature_columns], train_df[target_col]
    X_val, y_val = val_df[feature_columns], val_df[target_col]
    X_test, y_test = test_df[feature_columns], test_df[target_col]

    return X_train, y_train, X_val, y_val, X_test, y_test


def train_xgb_baseline(
    X_train,
    y_train,
    X_val,
    y_val,
    random_state: int = 42,) -> XGBClassifier:
    """
    Train a simple XGBoost baseline model and print validation metrics.
    """
    
    model = XGBClassifier(
        max_depth = 3,
        n_estimators = 150,
        learning_rate = 0.05,
        subsample = 0.8,
        colsample_bytree = 0.8,
        objective = "binary:logistic",
        eval_metric = "logloss",
        random_state = random_state,
        n_jobs =-1,)

    model.fit(
    X_train,
    y_train,
    eval_set = [(X_val, y_val)],
    verbose = False,)
    
    # simple validation metrics
    y_val_pred_proba = model.predict_proba(X_val)[:, 1]
    y_val_pred = (y_val_pred_proba >= 0.5).astype(int)
    
    return model

def build_and_train_model(target_col: str = "target_top10") -> XGBClassifier:
    """
    Full modelling pipeline:
    - load model_dataset.csv
    - split train / val / test by year
    - train a baseline XGBoost model
    - evaluate on validation and test
    """
    print(f"\n=== Training model for target: {target_col} ===")
    
    df = pd.read_csv(processed_direction / "model_dataset.csv")
    
    X_train, y_train, X_val, y_val, X_test, y_test = train_val_test_split_by_year(df, target_col = target_col)

    model = train_xgb_baseline(X_train, y_train, X_val, y_val)

    # test metrics
    y_test_proba = model.predict_proba(X_test)[:, 1]
    y_test_pred = (y_test_proba >= 0.5).astype(int)

    print("\n=== Test set performance ===")
    print("Test accuracy:", accuracy_score(y_test, y_test_pred))
    print("Test ROC-AUC:", roc_auc_score(y_test, y_test_proba))

    return model
    








    
    
    