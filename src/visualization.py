"""
This module contains all visualization functions used in the Formula 1 prediction project.
It centralizes the generation of plots and charts that support model evaluation,
feature interpretation, and predictive analysis.
"""

from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap

# Import paths and training functions
from .data_loader import processed_direction
from .evaluation import results_direction
from .models import build_and_train_gp_model, build_and_train_sprint_model

# Path to the graphs/ directory
graphs_direction = results_direction / "graphs"

def create_graphs_folder() -> Path:
    """
    Create the graphs/ folder inside the results/ folder.
    
    Returns:
        Path: path to the graphs/ directory.
    """
    
    # Create the graphs/ folder
    graphs_direction.mkdir(parents = True, exist_ok = True)

    # Check if graphs/ exists
    if graphs_direction.exists():
        print(f"📁 Folder created: {graphs_direction}")
    else:
        print(f"❌ Folder not created: {graphs_direction}")

    return graphs_direction


def plot_model_performance():
    """
    Random Forest performance plot (accuracy & roc-auc) by target,
    separated by race type (GP vs Sprint), using rf_baseline_metrics.csv.
    
    The figure is saved as: figures/rf_model_performance_by_target.png
    """

    # Define file paths
    metrics_file = results_direction / "rf_baseline_metrics.csv"
    output_file = graphs_direction / "rf_model_performance_by_target.png"
    
    # Load data
    try:
        metrics_df = pd.read_csv(metrics_file)
    except Exception as e:
        print(f"⚠️ Error while reading {metrics_file}: {e}")
        return None

    # Split by race type
    gp_df = metrics_df[metrics_df["race_type"] == "gp"]
    sprint_df = metrics_df[metrics_df["race_type"] == "sprint"]

    fig, axes = plt.subplots(1, 2, figsize = (12, 5), sharey = True)
    bar_width = 0.35

    # GP subplot
    x1 = range(len(gp_df))
    axes[0].bar(x1, gp_df["test_accuracy"], width = bar_width, label = "Accuracy", color = "blue")
    axes[0].bar([i + bar_width for i in x1], gp_df["test_roc_auc"], width = bar_width, label = "ROC–AUC", color = "skyblue")
    axes[0].set_xticks([i + bar_width / 2 for i in x1])
    axes[0].set_xticklabels(gp_df["target"], rotation = 45, ha = "right")
    axes[0].set_title("Grand Prix (GP)")
    axes[0].set_ylabel("Score")
    axes[0].set_ylim(0, 1)
    
    # Sprint subplot
    x2 = range(len(sprint_df))
    axes[1].bar(x2, sprint_df["test_accuracy"], width = bar_width, label = "Accuracy", color = "orange")
    axes[1].bar([i + bar_width for i in x2], sprint_df["test_roc_auc"], width = bar_width, label = "ROC–AUC", color = "gold")
    axes[1].set_xticks([i + bar_width / 2 for i in x2])
    axes[1].set_xticklabels(sprint_df["target"], rotation = 45, ha = "right")
    axes[1].set_title("Sprint Races")

    for ax in axes:
        ax.legend()
        ax.grid(axis = "y", linestyle = "--")

    plt.suptitle("Random Forest Performance by Target (Test Set)", fontsize = 13)
    plt.tight_layout(rect = [0, 0, 1, 0.95])
        
    # Save to graphs/ folder
    plt.savefig(output_file, dpi = 300, bbox_inches = "tight")
    plt.show
    print(f"✅ Random Forest performance plot saved to: {output_file}")
    
    return None


def plot_comparison_metrics(season_year: int = 2025):
    """
    Comparison metrics plot (accuracy, f1-score, roc-auc) for each target using
    comparisons_predictions_{season_year}.csv.

    The figure is saved as: figures/comparison_metrics_{season_year}.png
    """
    
    # Define file paths
    comparison_file = results_direction / f"comparisons_predictions_{season_year}.csv"
    output_file = graphs_direction / f"comparison_metrics_{season_year}.png"

    # Load data
    try:
        df = pd.read_csv(comparison_file)
    except Exception as e:
        print(f"⚠️ Error while reading {comparison_file}: {e}")
        return None

    # Split GP and Sprint data
    gp_df = df[df["target"].str.contains("sprint", case = False) == False].sort_values(by = "target")
    sprint_df = df[df["target"].str.contains("sprint", case = False)].sort_values(by = "target")

    # Sort GP and Sprint targets in a logical order
    gp_order = ["target_top10", "target_top3", "target_win"]
    sprint_order = ["target_top8_sprint", "target_top3_sprint", "target_win_sprint"]

    gp_df["target"] = pd.Categorical(gp_df["target"], categories = gp_order, ordered = True)
    sprint_df["target"] = pd.Categorical(sprint_df["target"], categories = sprint_order, ordered = True)

    gp_df = gp_df.sort_values("target")
    sprint_df = sprint_df.sort_values("target")

    fig, axes = plt.subplots(1, 2, figsize = (12, 5), sharey = True)
    bar_width = 0.25

    # GP subplot
    x1 = range(len(gp_df))
    axes[0].bar([i - bar_width for i in x1], gp_df["accuracy"], width = bar_width, color = "blue", label = "Accuracy")
    axes[0].bar(x1, gp_df["f1_score"], width = bar_width, color = "green", label = "F1-Score")
    axes[0].bar([i + bar_width for i in x1], gp_df["roc_auc"], width = bar_width, color = "orange", label = "ROC-AUC")
    axes[0].set_xticks(x1)
    axes[0].set_xticklabels(gp_df["target"], rotation = 45, ha = "right")
    axes[0].set_title("Grand Prix (GP)")
    axes[0].set_ylabel("Score")
    axes[0].set_ylim(0, 1)

    # Sprint subplot
    x2 = range(len(sprint_df))
    axes[1].bar([i - bar_width for i in x2], sprint_df["accuracy"], width = bar_width, color = "blue", label = "Accuracy")
    axes[1].bar(x2, sprint_df["f1_score"], width = bar_width, color = "green", label = "F1-Score")
    axes[1].bar([i + bar_width for i in x2], sprint_df["roc_auc"], width = bar_width, color = "orange", label = "ROC-AUC")
    axes[1].set_xticks(x2)
    axes[1].set_xticklabels(sprint_df["target"], rotation = 45, ha = "right")
    axes[1].set_title("Sprint Races")
    
    for ax in axes:
        ax.legend(loc = "upper left")
        ax.grid(axis = "y", linestyle = "--")
        
    plt.suptitle(f"Model Comparison Metrics for {season_year} Season", fontsize = 13)
    plt.tight_layout(rect = [0, 0, 1, 0.95])
    
    # Save to graphs/ folder
    plt.savefig(output_file, dpi = 300, bbox_inches = "tight")
    plt.show()
    print(f"✅ Comparison metrics plot saved to: {output_file}")
    
    return None


def plot_predictions_summary_2026():
    """
    Plot a summary of the model's 2026 predictions for all drivers.
    
    Displays how many times each driver was predicted in top positions
    (top10, top3, win for GP; top8, top3, win for Sprint races).
    
    The figure is saved as: figures/predictions_summary_2026.png
    """

    # Define file paths
    pred_file = results_direction / "predictions_2026.csv"
    drivers_file = processed_direction / "drivers_cleaned.csv"
    output_file = graphs_direction / "predictions_summary_2026.png"

    # Load data
    try:
        df = pd.read_csv(pred_file)
        drivers = pd.read_csv(drivers_file)
    except Exception as e:
        print(f"⚠️ Error while reading {pred_file} or {drivers_file}: {e}")
        return None
    
    # Take surname from driverId
    driver_names = drivers.set_index("driverId")["surname"].to_dict()
    
    # Count how many times each driver is predicted
    def summarize_predictions(sub_df, targets):
        grouped = (sub_df.groupby(["raceId", "driverId", "race_type"])[[f"pred_{t}" for t in targets]].max().reset_index())
        results = grouped.groupby("driverId")[[f"pred_{t}" for t in targets]].sum()
        results.columns = [t.replace("target_", "").replace("_sprint", "") for t in targets]
        return results.astype(int)
        
    # Separate GP and Sprint
    gp_df = df[df["race_type"] == "gp"].copy()
    sprint_df = df[df["race_type"] == "sprint"].copy()
    
    # Define targets
    gp_targets = ["target_top10", "target_top3", "target_win"]
    sprint_targets = ["target_top8_sprint", "target_top3_sprint", "target_win_sprint"]

    # Aggregate predictions
    gp_counts = summarize_predictions(gp_df, gp_targets)
    sprint_counts = summarize_predictions(sprint_df, sprint_targets)
    
    # Replace driverId with surnames
    gp_counts.index = gp_counts.index.to_series().replace(driver_names)
    sprint_counts.index = sprint_counts.index.to_series().replace(driver_names)

    # Filter: only drivers with at least one Top10/Top8
    gp_counts = gp_counts[gp_counts["top10"] > 0]
    sprint_counts = sprint_counts[sprint_counts["top8"] > 0]

    # Sort descending by number of wins
    gp_counts = gp_counts.sort_values(by = ["win", "top3", "top10"], ascending = False)
    sprint_counts = sprint_counts.sort_values(by = ["win", "top3", "top8"], ascending = False)
    
    # Create the plots
    fig, axes = plt.subplots(1, 2, figsize = (14, 6))

    # GP subplot
    gp_counts.plot(kind = "barh", ax = axes[0], color = ["lightblue", "royalblue", "midnightblue"], edgecolor = "black")
    axes[0].set_title("Grand Prix Predictions (2026)")
    axes[0].set_xlabel("Number of Races Predicted")
    axes[0].legend(["Top 10", "Top 3", "Win"], title = "Target")
    axes[0].set_ylabel("Driver")
    axes[0].invert_yaxis()

    # Sprint subplot
    sprint_counts.plot(kind = "barh", ax = axes[1], color = ["gold", "orange", "#CD7F32"], edgecolor = "black")
    axes[1].set_title("Sprint Race Predictions (2026)")
    axes[1].set_xlabel("Number of Races Predicted")
    axes[1].legend(["Top 8", "Top 3", "Win"], title = "Target")
    axes[1].invert_yaxis()
    axes[1].set_ylabel("Driver")

    plt.suptitle("Predicted Driver Performance Summary — 2026 Season", fontsize = 14)
    plt.tight_layout(rect = [0, 0, 1, 0.95])
    
    # Save to graphs/ folder
    plt.savefig(output_file, dpi = 300, bbox_inches = "tight")
    plt.show()
    print(f"✅ Prediction summary plot saved to: {output_file}")

    return None


def plot_predictions_heatmaps_2026():
    """
    Plot two side-by-side heatmaps showing the best predicted performance
    per driver and per circuit for GP and Sprint races.

    The figure is saved as: figures/predictions_heatmaps_2026.png
    """
    
    # Define file paths
    pred_file = results_direction / "predictions_2026.csv"
    drivers_file = processed_direction / "drivers_cleaned.csv"
    circuits_file = processed_direction / "circuits_cleaned.csv"
    output_file = graphs_direction / "predictions_heatmaps_2026.png"

    # Load data
    try:
        df = pd.read_csv(pred_file)
        drivers = pd.read_csv(drivers_file)
        circuits = pd.read_csv(circuits_file)
    except Exception as e:
        print(f"⚠️ Error while reading {pred_file} or {drivers_file} or {circuits_file}: {e}")
        return None

    # Take surname from driverId and take name from circuitId
    driver_map = drivers.set_index("driverId")["surname"].to_dict()
    circuit_map = circuits.set_index("circuitId")["name"].to_dict()
    df["driver"] = df["driverId"].map(driver_map)
    df["circuit"] = df["circuitId"].map(circuit_map)

    # Compute performance score
    def get_score(row):
        if row["race_type"] == "gp":
            if row.get("pred_target_win", 0) == 1:
                return 3
            elif row.get("pred_target_top3", 0) == 1:
                return 2
            elif row.get("pred_target_top10", 0) == 1:
                return 1
        elif row["race_type"] == "sprint":
            if row.get("pred_target_win_sprint", 0) == 1:
                return 3
            elif row.get("pred_target_top3_sprint", 0) == 1:
                return 2
            elif row.get("pred_target_top8_sprint", 0) == 1:
                return 1
        return 0

    df["score"] = df.apply(get_score, axis = 1)

    # Separate GP and Sprint
    gp_df = df[df["race_type"] == "gp"].copy()
    sprint_df = df[df["race_type"] == "sprint"].copy()

    # Keep all drivers who appear at least once
    common_drivers = sorted(set(gp_df["driver"].dropna()) | set(sprint_df["driver"].dropna()))
    gp_df = gp_df[gp_df["driver"].isin(common_drivers)]
    sprint_df = sprint_df[sprint_df["driver"].isin(common_drivers)]
    
    # Pivot tables
    gp_pivot = gp_df.pivot_table(index = "driver", columns = "circuit", values = "score", aggfunc = "max").fillna(0)
    sprint_pivot = sprint_df.pivot_table(index = "driver", columns = "circuit", values = "score", aggfunc = "max").fillna(0)
    
    # Compute total score for sorting
    driver_total_scores = gp_pivot.sum(axis = 1)
    driver_order = driver_total_scores.sort_values(ascending = False).index.tolist()
    gp_pivot = gp_pivot.reindex(driver_order)
    sprint_pivot = sprint_pivot.reindex(driver_order)
    driver_total_scores = driver_total_scores[driver_total_scores > 0]
    
    # Define colors
    blues = ["white", "lightblue", "royalblue", "midnightblue"]
    oranges = ["white", "gold", "orange", "#CD7F32"]
    gp_cmap = LinearSegmentedColormap.from_list("gp_cmap", blues, N = 4)
    sprint_cmap = LinearSegmentedColormap.from_list("sprint_cmap", oranges, N = 4)

    # Create the Heatmaps
    fig, axes = plt.subplots(1, 2, figsize = (14, 6))

    # GP Heatmap
    im1 = axes[0].imshow(gp_pivot.values, cmap = gp_cmap, aspect = "auto", vmin = 0, vmax = 3)
    axes[0].set_title("Grand Prix Predictions (2026)", fontsize = 12)
    axes[0].set_xticks(np.arange(len(gp_pivot.columns)) + 0.5)
    axes[0].set_xticklabels(gp_pivot.columns, rotation = 75, ha = "right", fontsize = 8)
    axes[0].set_yticks(np.arange(len(gp_pivot.index)))
    axes[0].set_yticklabels(gp_pivot.index, fontsize = 9)
    axes[0].set_ylabel("Driver", fontsize = 10)

    # Sprint Heatmap
    im2 = axes[1].imshow(sprint_pivot.values, cmap = sprint_cmap, aspect = "auto", vmin = 0, vmax = 3)
    axes[1].set_title("Sprint Race Predictions (2026)", fontsize = 12)
    axes[1].set_xticks(np.arange(len(sprint_pivot.columns)) + 0.5)
    axes[1].set_xticklabels(sprint_pivot.columns, rotation = 75, ha = "right", fontsize = 8)
    axes[1].set_yticks(np.arange(len(sprint_pivot.index)))
    axes[1].set_yticklabels(sprint_pivot.index, fontsize = 9)
    axes[1].set_ylabel("Driver")

    # Add legends
    for c, label in zip(blues, ["No points", "Top 10", "Top 3", "Win"]):
        axes[0].bar(0, 0, color = c, label = label)
    for c, label in zip(oranges, ["No points", "Top 8", "Top 3", "Win"]):
        axes[1].bar(0, 0, color = c, label = label)
    axes[0].legend(title = "Target", fontsize = 8, loc = "lower right")
    axes[1].legend(title = "Target", fontsize = 8, loc = "lower right")

    plt.suptitle("Predicted Driver Performance per Race — 2026 Season", fontsize = 14)
    plt.tight_layout(rect = [0, 0, 1, 0.95])

    # Save to graphs/ folder
    plt.savefig(output_file, dpi = 300, bbox_inches = "tight")
    plt.show()
    print(f"✅ Heatmaps saved to: {output_file}")

    return None


def plot_combined_feature_importances(top_n = 20):
    """
    Collect feature importances from all trained Grand Prix and Sprint models
    and plot the most influential features across all targets.
    """

    # Define file paths
    dataset_file = processed_direction / "model_dataset.csv"
    output_file = graphs_direction / "combined_feature_importances.png"

    # Load data
    try:
        df = pd.read_csv(dataset_file)
    except Exception as e:
        print(f"⚠️ Error while reading {dataset_file}: {e}")
        return None

    importances_data = []

    # Train the Grand Prix model for each target
    for target in ("target_top10", "target_top3", "target_win"):
        model = build_and_train_gp_model(target_col = target)
        
        # Extract feature importances for each model
        feature_cols = model.feature_names_in_
        importances = pd.Series(model.feature_importances_, index = feature_cols)
        top_importances = importances.sort_values(ascending = False).head(top_n)

        for feature, value in top_importances.items():
            importances_data.append({
                "Target": target,
                "Feature": feature,
                "Importance": value,
                "Type": "Grand Prix"})

    # Train the Sprint model for each target
    for target in ("target_top8_sprint", "target_top3_sprint", "target_win_sprint"):
        model = build_and_train_sprint_model(target_col = target)
        
        # Extract feature importances for each model
        feature_cols = model.feature_names_in_
        importances = pd.Series(model.feature_importances_, index = feature_cols)
        top_importances = importances.sort_values(ascending = False).head(top_n)

        for feature, value in top_importances.items():
            importances_data.append({
                "Target": target,
                "Feature": feature,
                "Importance": value,
                "Type": "Sprint"})
    
    # Convert all results into a DataFrame
    importances_df = pd.DataFrame(importances_data)

    # Compute mean importance per feature
    mean_importances = (importances_df.groupby("Feature")["Importance"].mean().sort_values(ascending = False).head(top_n))

    # Create the plot
    plt.figure(figsize = (10, 6))
    mean_importances[::-1].plot(kind = "barh", color = "royalblue")
    plt.title(f"Top {top_n} Most Influential Features Across All Models", fontsize = 13)
    plt.xlabel("Average Feature Importance", fontsize = 11)
    plt.ylabel("Feature", fontsize = 11)
    plt.grid(axis = "x", linestyle = "--")
    plt.tight_layout()

    # Save to graphs/ folder
    plt.savefig(output_file, dpi = 300, bbox_inches = "tight")
    plt.show()
    print(f"\n✅ Combined feature importance plot saved successfully: {output_file}")

    return None


def plot_feature_correlation_heatmap(top_n = 30):

    # Define file paths
    dataset_file = processed_direction / "model_dataset.csv"
    output_file = graphs_direction / "feature_correlation_heatmap.png"

    # Load data
    try:
        df = pd.read_csv(dataset_file)
    except Exception as e:
        print(f"⚠️ Error while reading {dataset_file}: {e}")
        return None
        
    # Automatically select only relevant numeric feature columns
    cols_to_keep = [
        col for col in df.columns
        if col.startswith(("drv_", "team_", "circ_")) and df[col].dtype!= "object"]
    corr_df = df[cols_to_keep]
    
    # Compute the correlation matrix
    corr_matrix = df.corr(numeric_only = True)

    # Identify top correlated features
    mean_corr = corr_matrix.abs().mean().sort_values(ascending = False)
    top_features = mean_corr.head(top_n).index
    zoom_corr = corr_matrix.loc[top_features, top_features]
    
    # Create the Heatmap
    fig, ax = plt.subplots(figsize = (8, 6))
    cax = ax.matshow(zoom_corr, cmap="coolwarm", vmin =-1, vmax = 1)
    plt.title(f"Top {top_n} Most Correlated Features", pad = 20, fontsize = 14)
    fig.colorbar(cax)

    ticks = np.arange(len(zoom_corr.columns))
    ax.set_xticks(ticks)
    ax.set_yticks(ticks)
    ax.set_xticklabels(zoom_corr.columns, rotation = 90, fontsize = 8)
    ax.set_yticklabels(zoom_corr.columns, fontsize = 8)
    
    # Save to graphs/ folder
    plt.tight_layout()
    plt.savefig(output_file, dpi = 300, bbox_inches = "tight")
    plt.show()
    
    print(f"✅ Correlation heatmap saved at: {output_file}")

    return None