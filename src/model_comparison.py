"""
Model comparison module for Formula 1 race outcome prediction.

This module compares multiple machine learning algorithms (Logistic Regression,
Random Forest, Gradient Boosting, XGBoost) to identify the most effective
approach for forecasting race results.
"""

from pathlib import Path
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score, roc_auc_score

# step 1: Import the functions
from .data_loader import processed_direction
from .evaluation import results_direction
from .models import train_val_test_split_by_year

def evaluate(model, name, X_train, y_train, X_val, y_val):
    """
    Train and evaluate a model on validation set.
    
    Args:
        model: sklearn/xgboost model instance
        name (str): model name for display
        X_train, y_train: training data
        X_val, y_val: validation data
        
    Returns:
        tuple: (accuracy, roc_auc)
    """
    model.fit(X_train, y_train)
    proba = model.predict_proba(X_val)[:, 1]
    pred = (proba >= 0.5).astype(int)
    
    acc = accuracy_score(y_val, pred)
    auc = roc_auc_score(y_val, proba)
    
    print(f"\n{name}")
    print("Val accuracy:", acc)
    print("Val ROC-AUC :", auc)
    
    return acc, auc


def compare_models(target_col="target_top10", save_results=False):
    """
    Compare multiple ML algorithms on the same prediction task.
    """
    
    print(f"\n=== Model Comparison for {target_col} ===")
    
    # Load dataset
    df_path = processed_direction / "model_dataset.csv"
    df = pd.read_csv(df_path)
    
    # Split data
    X_train, y_train, X_val, y_val, X_test, y_test = train_val_test_split_by_year(
        df,
        target_col=target_col,
        train_years=(2020, 2021, 2022, 2023),
        val_years=(2024,),
        test_years=(2025,),
    )
    
    # Define models
    models = {
        "Logistic Regression": LogisticRegression(
            max_iter=5000, 
            n_jobs=-1
        ),
        "Random Forest": RandomForestClassifier(
            n_estimators=700,
            max_depth=20,
            min_samples_split=2,
            min_samples_leaf=3,
            max_features="sqrt",
            bootstrap=True,
            class_weight="balanced",
            random_state=42,
            n_jobs=-1,
        ),
        "Gradient Boosting": GradientBoostingClassifier(
            random_state=42
        ),
        "XGBoost": XGBClassifier(
            max_depth=3,
            n_estimators=150,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            objective="binary:logistic",
            eval_metric="logloss",
            random_state=42,
            n_jobs=-1,
        ),
    }
    
    # Train and evaluate all models
    results = []
    for name, model in models.items():
        acc, auc = evaluate(model, name, X_train, y_train, X_val, y_val)
        results.append([name, acc, auc])
    
    # Create comparison DataFrame
    comparison_df = pd.DataFrame(results, columns=["model", "val_accuracy", "val_roc_auc"])
    
    # Save to results/ folder
    if save_results:
        output_file = results_direction / f"model_comparison_{target_col}.csv"
        comparison_df.to_csv(output_file, index=False)
        print(f"\n✅ Comparison saved to: {output_file}")
    
    return comparison_df


def compare_all_models(save_results=True):
    """
    Compare ML algorithms across all 6 targets.
    Creates styled table visualization.
    """
    
    all_targets = [
        "target_top10",
        "target_top3",
        "target_win",
        "target_top8_sprint",
        "target_top3_sprint",
        "target_win_sprint",
    ]
    
    all_results = []
    
    for target in all_targets:
        comparison_df = compare_models(target_col=target, save_results=False)
        comparison_df.insert(0, 'target', target)
        all_results.append(comparison_df)
    
    summary_df = pd.concat(all_results, ignore_index=True)
    
    # Create pivot tables
    pivot_auc = summary_df.pivot(index='target', columns='model', values='val_roc_auc')
    pivot_acc = summary_df.pivot(index='target', columns='model', values='val_accuracy')
    
    # Display styled tables
    print("\n" + "="*90)
    print("📊 ROC-AUC COMPARISON")
    print("="*90)
    print(pivot_auc.to_string())
    print("\n" + "="*90)
    print("📊 ACCURACY COMPARISON")
    print("="*90)
    print(pivot_acc.to_string())
    print("="*90 + "\n")
    
    # Save visualization
    if save_results:
        import matplotlib.pyplot as plt
        
        fig, axes = plt.subplots(2, 1, figsize=(12, 10))
        
        # Table 1: ROC-AUC
        ax1 = axes[0]
        ax1.axis('tight')
        ax1.axis('off')
        table1 = ax1.table(
            cellText=pivot_auc.round(4).values,
            rowLabels=pivot_auc.index,
            colLabels=pivot_auc.columns,
            cellLoc='center',
            loc='center'
        )
        table1.auto_set_font_size(False)
        table1.set_fontsize(10)
        table1.scale(1, 2)
        ax1.set_title('ROC-AUC Scores by Model and Target', fontsize=14, fontweight='bold', pad=20)
        
        # Table 2: Accuracy
        ax2 = axes[1]
        ax2.axis('tight')
        ax2.axis('off')
        table2 = ax2.table(
            cellText=pivot_acc.round(4).values,
            rowLabels=pivot_acc.index,
            colLabels=pivot_acc.columns,
            cellLoc='center',
            loc='center'
        )
        table2.auto_set_font_size(False)
        table2.set_fontsize(10)
        table2.scale(1, 2)
        ax2.set_title('Accuracy Scores by Model and Target', fontsize=14, fontweight='bold', pad=20)
        
        plt.tight_layout()
        output_file = results_direction / "graphs" / "model_comparison_all_targets.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"✅ Table visualization saved to: {output_file}\n")
        plt.close()
    
    return summary_df