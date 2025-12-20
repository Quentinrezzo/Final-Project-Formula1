# 🏎️ Predicting Formula 1 Race Outcomes (Season‑Ahead Forecasting)

## Overview
This project predicts **Formula 1 race outcomes** specifically whether each driver will finish in the **Top-10 (GP), Top-8 (Sprint), Top-3, or Win** a race for an **upcoming season** based on past performance data.

Using the **Kaggle Formula 1 Race Data** repository, the workflow combines **data engineering**, **feature creation**, and **machine learning models** to anticipate driver results before a season begins.
All predictions follow an **N–1 logic**: every forecast for a target year (e.g., 2026) is trained only on data from the previous five seasons using a rolling window (up to 2025).

The ultimate goal is to understand which factors most influence race results such as driver consistency, constructor performance, and circuit characteristics while building a fully automated and interpretable forecasting pipeline.


## Project Pipeline
The pipeline is organized in **main.py** and executes the full end-to-end process:
1. **Load local raw dataset** the Kaggle dataset (data/raw/)
2. **List and verify** available CSV files
3. **Create processed folder** for intermediate datasets
4. **Filter races** (e.g., 2015–2025) and all race-based tables
5. **Filter dimension tables** (circuits, constructors, drivers, seasons, status)
6. **Enrich processed tables** (circuits, races, status) with extra information
7. **Feature engineering** build progressive (N–1) performance features for drivers, constructors, qualifying, and sprint sessions
8. **Create model datasets** for current and future season predictions
9. **Train and evaluate models** (Random Forest baseline for GP and Sprint targets)
10. **Evaluate and simulate** 2025 results, and **predict** the 2026 season
11. **Visualize results** with analytical plots and heatmaps

## Outputs
After running the full pipeline, the following results are generated:
- **comparisons_predictions_2025** -> comparison of predicted vs. real 2025 race results 
- **predictions_2025** -> model outputs for the 2025 season (hold-out simulation)
- **predictions_2026** -> predicted outcomes for the 2026 season (season-ahead forecast)
- **rf_baseline_metrics** -> Random Forest model evaluation metrics across all targets
- **the graphs folder** -> containing analytical visualizations as well as the most influential features across all models


## How to run

### 1. Setup Environment
Clone the repository and install dependencies:

**in a VSCode Terminal**
- git clone https://github.com/Quentinrezzo/Final-Project-Formula1.git
- cd Final-Project-Formula1
- conda env create -f environment.yml
- conda activate f1-project

### 2. Verify installation
conda list

### 3. Run the full pipeline
python main.py


### Requirements
- Python 3.11
- scikit-learn, pandas, matplotlib, seaborn, xgboost, kagglehub


**Author:** Quentin Rezzonico
**Last update:** December 2025
**Contact:** www.linkedin.com/in/quentinrezzonico