# Project Report — Predicting Formula 1 Race Outcomes (Season‑Ahead Forecasting)


**Abstract-This project explores the use of machine learning techniques to predict Formula 1 driver and team performance for upcoming Grand Prix and Sprint races. Using historical race data enriched with driver, constructor, and circuit characteristics, we built and evaluated Random Forest models to forecast key outcomes such as top-10 finishes, podiums, and race wins. Separate models were trained for traditional Grand Prix and Sprint formats to capture their different competitive dynamics.
Our approach combines predictive modeling with analytical visualization to better understand which factors most strongly influence success. Feature importance analyses highlight the relevance of consistency indices, team points per race, and circuit-specific variables in determining results. The models achieved solid validation accuracy and ROC-AUC scores across all targets, demonstrating their ability to generalize to unseen seasons.
Finally, the project provides interpretable insights rather than only raw predictions, offering a data-driven view of F1 performance patterns. These findings could support future performance forecasting, team strategy development, and fan-oriented analytics within the sport.**


## I. INTRODUCTION
Formula 1 is one of the most data-rich and analytically demanding sports in the world. Each race combines driver skill, car engineering, and team strategy under changing conditions that make performance prediction a complex task. As Formula 1 evolves into an increasingly data-driven sport, engineers are constantly seeking methods to analyze and interpret the growing volume of historical race data in order to enhance team and driver performance over time.

Machine learning provides a promising framework for uncovering patterns and trends within these complex datasets. Beyond the excitement of competition, understanding what drives success in Formula 1 is also a way to study human performance, decision-making, and technological optimization under pressure. This project explores how data science methods particularly Random Forest models can be applied to forecast race outcomes and identify the most influential variables behind them.


## II. Research Question

### A. Problem
Predicting performance in Formula 1 is a complex challenge. Race results depend on a mix of factors such as driver ability, car performance, team strategy, and race conditions like track type or weather. No single element determines success, and small differences in performance can completely change race outcomes.

Another layer of complexity comes from the financial structure of the sport. Since 2021, Formula 1 has introduced a budget cap, limiting how much teams can spend each season. However, not all teams operate at the same financial level or with the same efficiency under these constraints. This financial imbalance contributes to performance gaps that are difficult to quantify using traditional analytical methods.

These challenges make Formula 1 an ideal field for applying machine learning, which can account for multiple interacting variables and uncover relationships that are not immediately visible through classical statistics. Understanding how these technical, strategic, and human factors combine to influence race results is at the core of this project’s research question:

**Can machine learning models predict future Formula 1 race outcomes using historical performance data and contextual features such as driver consistency, team form, and circuit characteristics?**

### B. Objective
The main goal of this project is to leverage machine learning techniques to predict Formula 1 race outcomes for both Grand Prix and Sprint races. Specifically, the models aim to forecast whether a driver will finish within the top-10, top-3, or Win a race in Grand Prix or the top-8, top-3, or Win a Sprint race. These thresholds align with the FIA’s official points-scoring positions and provide a structured way to evaluate model performance across varying levels of competitiveness.

To achieve this, Random Forest classifiers are trained using data from the five most recent seasons relative to the year being predicted. For the current project, this means that models predicting the 2026 season use data from 2021 to 2025. This rolling window approach allows the models to learn from the most recent performance dynamics while avoiding information leakage from future results. Separate models are built for Grand Prix and Sprint formats to account for their different race lengths, strategies, and point systems.

The project’s goals extend beyond accurate prediction. By analyzing feature importances, the goal is to identify which elements such as driver consistency, team performance trends, and circuit characteristics most influence success. These insights are then visualized to provide a clearer understanding of what factors drive high performance in Formula 1.

### C. Scope
The scope of this project is defined by both its analytical depth and its temporal focus. The analysis covers Formula 1 race data from 2015 to 2025, including detailed information on drivers, teams, constructors, and circuits. This range ensures sufficient historical depth to capture evolving performance trends while maintaining relevance to the modern hybrid era of Formula 1.

The models are trained using a rolling five-year window, meaning that for each prediction year, the five most recent seasons are used to build the dataset. For example, predicting the 2026 season relies on data from 2021 to 2025. This structure allows the model to continuously adapt to new conditions and driver-team combinations, reflecting the sport’s dynamic nature.

A key design choice in this project is the automation of the entire workflow. The data preparation, feature engineering, model training, evaluation, and visualization steps are all coded to automatically adjust based on the chosen data range. This means that the same pipeline can be reused for future seasons without structural changes — the user only needs to specify the filtering period (e.g., 2015–2025). This design ensures scalability and long-term applicability.

However, the scope of this project is limited to predictive modeling and exploratory analysis. It does not attempt to simulate real time race conditions, weather forecasts, or intra-race strategy decisions such as pit stops or tire management. The predictions are based purely on historical data and aggregate performance indicators, providing a consistent and interpretable foundation for analyzing race outcomes.


## III. Methodology
This section outlines the methodology used to develop and evaluate the Formula 1 race prediction pipeline, covering the dataset, preprocessing, feature engineering, model selection, and performance evaluation. The objective was to build a fully reproducible workflow capable of generating accurate and interpretable predictions of race outcomes.

### A. Dataset Description
The dataset used in this project covers historical Formula 1 data from the 1950–2025 seasons. It includes detailed information about drivers, constructors, circuits, qualifying sessions, race results, and more. The data originates from Kaggle's "Formula 1 Race Data" repository, which is itself built upon the official **Ergast API**, a comprehensive and continuously updated source providing structured race results, standings, and metadata for every Formula 1 race. Using this dataset ensures both data consistency and alignment with official FIA statistics, while offering a clean, machine-learning-ready structure. The dataset is automatically downloaded and managed via the src/downloading_dataset.py file, ensuring consistent and reproducible access to data across runs.

The dataset is organized in a **relational structure**. Each CSV file represents a distinct table. For instance: drivers.csv, constructors.csv, races.csv, results.csv and circuits.csv are all connected through primary and foreign keys, such as driverId, constructorId, raceId and circuitId, which enable efficient merging and cross-referencing of information across multiple entities.

At the center of this relational structure lies the races.csv table, which serves as the core reference linking race results to specific circuits, seasons (years), and numerous other features. This architecture allows for the creation of integrated datasets that combine driver-level, team-level, and circuit-level information for each race, forming the analytical foundation of this project.

### B. Preprocessing Steps
The preprocessing phase was a crucial component of this project, transforming raw Formula 1 data into a consistent, enriched, and model-ready format. All the data preparation logic was implemented in the src/data_loader.py, src/data_enrichment.py, and src/features.py scripts, ensuring a modular and reproducible workflow.

The process began by **filtering the raw datasets** to include only the **2015–2025 seasons**, corresponding to the modern hybrid era of Formula 1. This period was selected to ensure consistency in technical regulations and competitive structure. The filtering leveraged the relational structure of the dataset, with races.csv serving as the central reference to link data across multiple tables such as drivers.csv, constructors.csv, results.csv, and circuits.csv. Cleaned datasets were stored in the data/processed/ directory, while the original Kaggle/Ergast files were preserved in data/raw/ for full traceability.

After cleaning, several datasets were enriched with new engineered features to capture additional contextual and performance-related insights.
For instance, the circuits file was completed with variables such as length_km (total track length in kilometers), is_night_race (boolean metric identifying night races), and track_type (circuit classification: high_speed, technical, balanced).
From these attributes, the total race distance (race_distance_km) was derived for each Grand Prix. Similarly, the status table, which originally listed detailed incident causes, was reworked into four categories (is_mechanical, is_crash, is_other_dnf, is_no_dnf) to enable the calculation of driver and constructor reliability metrics.

To avoid data leakage and capture evolving trends, a series of progressive performance tables were created for each entity: drivers, constructors, qualifying sessions, sprints, and driver–circuit combinations. These tables aggregated historical statistics such as average finish position, podium rate, mechanical failure rate, and consistency indices. They also incorporated measures of drivers' and constructors' experience, allowing the model to learn from recent but contextually relevant patterns rather than entire career histories.

The final step consisted of **merging all progressive performance tables** into a single integrated table named model_dataset.csv serving as the foundation for model training. This important table, generated through the features.py file, contains **2,258 rows and 113 columns**, representing a rich overview of the features influencing race outcomes in Formula 1.
A **rolling five-year window** was applied so that, for instance, when predicting the 2026 season, only data from 2021–2025 were used for training. This dynamic structure allows the model to remain temporally consistent and adaptive to new data, while preventing information leakage from future races.

### C. Models Used
The predictive modeling stage of this project focused on evaluating different machine learning algorithms to determine the best-performing model for forecasting Formula 1 race outcomes. The model development and training logic were implemented in the src/models.py file, ensuring full reproducibility and consistency across experiments.

Several classification models were initially tested, including Logistic Regression, Gradient Boosting, XGBoost, and Random Forests, to assess their ability to capture the complex, nonlinear relationships between input features and race results. (To reproduce the model comparison, refer to the models.ipynb notebook and execute the corresponding cell.) Among them, Random Forest achieved the highest overall performance with a **validation accuracy of 0.79 and a ROC–AUC score of 0.84**, outperforming the other approaches. For this reason, it was selected as the final model for the project. Training and validation were conducted separately for Grand Prix and Sprint formats, using the rolling five-year dataset described in Section B to ensure temporal consistency and adaptability to evolving race dynamics.

Although **XGBoost** was initially considered due to its strong performance on large-scale structured datasets, it proved less efficient for this project’s moderate dataset size (around 2,000 rows). It required complex parameter tuning and showed signs of overfitting on validation data. In contrast, the **Random Forest** model provided more stable and interpretable results. Its ensemble of decision trees averaged multiple predictions, reducing random fluctuations and improving consistency across different race targets.

The final Random Forest classifier was configured with the following hyperparameters:
- **n_estimators = 700** (number of trees in the forest)
- **max_depth = 20** (limiting tree depth to avoid overfitting)
- **min_samples_split = 2** and **min_samples_leaf = 3** (controlling node splitting for smoother decision boundaries)
- **max_features = "sqrt"** (random feature selection to enhance decorrelation between trees)
- **bootstrap = True** (enabling bootstrapped sampling for robust ensemble learning)
- **class_weight = "balanced"** (compensating for class imbalance across race outcome categories)
- **random_state = 42** (ensuring reproducibility of results)
- **n_jobs = -1** (enables parallel computation by using all available CPU cores, significantly speeding up the training process)

### D. Evaluation Metrics
Model performance was evaluated using complementary metrics to capture both overall accuracy and class-specific quality. The evaluation process, implemented in src/evaluation.py, generated detailed reports and visualizations stored in the results/ directory.

**Accuracy** was used as the primary metric, representing the proportion of correct predictions. However, since Formula 1 outcomes are highly imbalanced (only one driver wins out of twenty), additional metrics were included for balance: the **F1-score**, reflecting the trade-off between precision and recall for rare events such as wins or podiums, and **ROC–AUC**, which measures the model’s ability to distinguish between positive and negative classes across thresholds.

Evaluations were performed separately for Grand Prix and Sprint formats across all targets Top-10, Top-3, and Win for Grands Prix, and Top-8, Top-3, and Win for Sprints. This multi-target analysis provided a comprehensive view of model robustness and predictive reliability. Final results and comparative plots were generated using the src/visualization.py module, providing a clear overview of model performance across all metrics and targets.

#### Directory Structure

Final-Project-Formula1/
│
├── data/
│ ├── processed/
│ └── raw/
│
├── notebooks/
│
├── results/
│ ├── graphs/
│ ├── comparisons_predictions_2025.csv
│ ├── predictions_2025.csv
│ ├── predictions_2026.csv
│ └── rf_baseline_metrics.csv
│
├── src/
│ ├── init.py
│ ├── downloading_dataset.py
│ ├── data_loader.py
│ ├── data_enrichment.py
│ ├── features.py
│ ├── models.py
│ ├── evaluation.py
│ └── visualization.py
│
├── main.py
├── project_report.md
├── PROPOSAL.md
├── README.md
├── environment.yml
├── requirements.txt
└── .gitignore


## IV. Results





5.Results - Tables, figures, findings
-Tables comparing models
-Figures (learning curves, confusion matrices)






Random Forest Baseline Metrics
| Target             | Train Years      | Val Years | Test Years | Test Accuracy | Test ROC–AUC | Race Type |
| ------------------ | ---------------- | --------- | ---------- | ------------- | ------------ | --------- |
| target_top10       | 2021, 2022, 2023 | 2024      | 2025       | 0.7211        | 0.7152       | gp        |
| target_top3        | 2021, 2022, 2023 | 2024      | 2025       | 0.8039        | 0.8037       | gp        |
| target_win         | 2021, 2022, 2023 | 2024      | 2025       | 0.9303        | 0.7985       | gp        |
| target_top8_sprint | 2021, 2022, 2023 | 2024      | 2025       | 0.7833        | 0.8076       | sprint    |
| target_top3_sprint | 2021, 2022, 2023 | 2024      | 2025       | 0.7917        | 0.8290       | sprint    |
| target_win_sprint  | 2021, 2022, 2023 | 2024      | 2025       | 0.9583        | 0.8275       | sprint    |
> Source: results/rf_baseline_metrics.csv
> (To get the graph, see results/graphs/rf_model_performance_by_target.png)


![](results/graphs/comparison_metrics_2025.png)








6.Discussion - Interpretation and limitations
-Why did one model outperform others?
-What surprised you?
-What are the limitations?
-How do results compare to expectations?

7.Conclusion - Summary and future work
-Summary of key findings
-Recommendations (which model to use when)
-Future work (what's next?)

8.References - Citations
-datasets used
-Libraries/frameworks