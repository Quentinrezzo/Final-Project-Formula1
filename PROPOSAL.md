# Predicting Formula 1 Race Outcomes (Season‑Ahead Forecasting)

# Problem statement or motivation
Formula 1 is a sport driven by precision, performance, and data. Each race outcome depends on numerous factors such as driver skill, team performance, car reliability, track characteristics, and race strategy. Predicting results is notoriously difficult because even small variations such as weather conditions, drivers' psychological state, or tyre degradation can completely alter the final results.

The aim of this project is to apply data science and machine learning techniques to analyze how historical performance affects future race outcomes and to build a predictive model capable of forecasting an entire upcoming season. Following an N‑1 temporal logic, the model for a given target year (e.g., 2026) will only use data from previous seasons (up to 2025).

Using this framework, the model will estimate the probability that drivers will finish in the Top-10, top-8, Top-3, or Win each race in the next season. Beyond prediction accuracy, the project will aim to understand the relative influence of different performance indicators and will assess whether a data-driven model can anticipate outcomes before they occur.

# Planned approach and technologies
I will use the **Kaggle Formula 1 Race Data** repository which is itself built upon the official **Ergast API**, a comprehensive and continuously updated source providing structured information on race results, standings, drivers, constructors, circuits, qualifying sessions, and sprint events.

The project will include:
- **Downloading and organising the dataset from Kaggle**
- **Data cleaning and enrichment** using pandas, pathlib and numpy. Exploratory data analysis and
  establishment of the project foundation
- **Feature engineering** based on progressive (N-1) performance metrics (e.g., driver and
  team progressive performance)
- **Building a modelling dataset** where each row is a driver‑race-circuit combinations for a given
  season, and all features represent what was known before that race
- **Training and evaluating machine-learning models** such as Logistic Regression, Random Forest,
  Gradient Boosting and XGBoost, to compare their predictive accuracy and ROC-AUC
- **Producing of a forecast for a future season** (e.g., predicting all races
  of 2026 using results up to 2025)
- **Evaluating predictions** by comparing model outputs with actual 2025 results
  (when predicting 2025 as hold‑out)
- **Visualizing model outputs** through graphs (e.g., prediction summaries importance of metrics,
  and comparison plots)

# Expected challenges and how I will address them
- **Data imbalance:** Only a few drivers consistently finish in the Top-3 or Win races.
  This will be handled using appropriate evaluation metrics (ROC-AUC)
- **Complex dependencies:** Race outcomes depend on many interacting variables
  (driver skill, car skill, etc). Non‑linear models will help capture these relationships
- **Real-world variability:** External factors such as weather conditions, poor race starts, or
  strategic decisions cannot be perfectly modeled. These will be treated as inherent noise and will be
  acknowledged as limitations of the project

# Success criteria
The project will be considered successful if:
1. The model achieves an important predictive accuracy on the 2025 hold‑out season (e.g., ROC-AUC > 0.7)
2. The model produces coherent and meaningful season‑ahead predictions
3. The project delivers clear and informative visualizations to communicate driver and model performance