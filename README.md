# 🏎️ Predicting Formula 1 Race Outcomes (Season‑Ahead Forecasting)

## Overview
This project predicts **Formula 1 race outcomes** specifically whether each driver will finish in the **Top-10 (GP), Top-8 (Sprint), Top-3, or Win** a race for an **upcoming season** based on past performance data.

Using the **Kaggle Formula 1 Race Data** repository, the workflow combines **data engineering**, **feature creation**, and **machine learning models** to anticipate driver results before a season begins.
All predictions follow an **N–1 logic**: features are computed cumulatively 
from **2015 onwards**, ensuring each race only uses historical data available 
up to that point. For model training, an **adaptive temporal split** is used:

- **For evaluation** (e.g., predicting 2025): Train on 2021–2023 (3 years), 
  validate on 2024, test on 2025
- **For future forecasting** (e.g., predicting 2026): Train on 2021–2024 
  (4 years), validate on 2025
  
This approach balances recent competitive dynamics (rolling training window) 
with deep historical context (cumulative features from 2015).

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


## Setup

### 1. Clone the repository
```bash
git clone https://github.com/Quentinrezzo/Final-Project-Formula1.git
cd Final-Project-Formula1
```

### 2. Create and activate the Conda environment
```bash
conda env create -f environment.yml
conda activate f1-project
```

### 3. Run the full pipeline
```bash
python main.py
```

## Requirements
- Python 3.11
- pandas
- numpy
- matplotlib
- seaborn
- scikit-learn
- kagglehub
- xgboost


## Directory Structure
```
Final-Project-Formula1/
│
├── data/                             # Data directory
│   ├── processed/                    # Processed datasets (generated)
│   └── raw/                          # Raw, unprocessed data
│
├── notebooks/                        # Jupyter notebooks for exploration
│
├── results/                          # Model outputs
│   ├── graphs/                       # Figures and plots
│   ├── comparisons_predictions_2025.csv   # Model comparison results
│   ├── predictions_2025.csv          # 2025 season predictions
│   ├── predictions_2026.csv          # 2026 season predictions
│   └── rf_baseline_metrics.csv       # Random Forest baseline metrics
│
├── src/                              # Source code
│   ├── __init__.py                   # Package initializer
│   ├── downloading_dataset.py        # Dataset download from Kaggle
│   ├── data_loader.py                # Raw data loading and cleaning
│   ├── data_enrichment.py            # Feature computation and enrichment
│   ├── features.py                   # Feature engineering
│   ├── models.py                     # Model training and evaluation
│   ├── evaluation.py                 # Model performance metrics
│   └── visualization.py              # Data visualization utilities
│
├── main.py                           # Main entry point (runs full pipeline)
├── project_report.pdf                # Final project report
├── PROPOSAL.md                       # Initial project proposal
├── README.md                         # Project documentation
├── environment.yml                   # Conda environment specification
├── requirements.txt                  # pip dependencies
└── .gitignore                        # Git ignore file
```


**Author:** Quentin Rezzonico<br>
**Last update:** December 2025<br>
**Contact:** www.linkedin.com/in/quentinrezzonico