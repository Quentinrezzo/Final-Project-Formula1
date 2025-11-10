"""
Data management module for the formula 1 Race Outcome Prediction project.
"""

from pathlib import Path
import shutil
import pandas as pd
import kagglehub

def download_dataset(destination: str = "Final-Project-Formula1/data/raw"):
    """
    Download the dataset named Formula 1 World Championship (1950 - 2024) from Kaggle using KaggleHub.

    Arg:
       destination (str): Directory to save the dataset.
    Returns:
       str: Local path to the dataset.
    """

    # Define paths
    destination_path = Path(destination)
    project_root = destination_path.parents[1]
    
    # Check if parent folder exists (This is not essential because the line below creates the parent folder if it
    # does not already exist, but it is useful for greater transparency)
    if project_root.exists():
        print(f"📂 Parent folder exists: {project_root}")
    else:
        print(f"❌ Parent folder not found: {project_root}")
    
    # Create the folder Final-Project-Formula1/data/raw if it does not already exist
    destination_path.mkdir(parents = True, exist_ok = True)

    # Check if dataset already exists (this avoid unnecessary downloads every time you run the script)
    if any(destination_path.iterdir()):
        print(f"✅ Dataset already exists in {destination_path}")
        return destination_path

    # Copy the download files from Kaggle into your Final-Project-Formula1/data/raw directory
    try:
        print("📦 Downloading the dataset named Formula 1 World Championship (1950 - 2024) from Kaggle")
        # Download latest version
        path = kagglehub.dataset_download("rohanrao/formula-1-world-championship-1950-2020")
        src_path = Path(path)
        shutil.copytree(src_path, destination_path, dirs_exist_ok = True)
        print(f"✅ Dataset download and available at: {destination_path}")
    except Exception as e:
        print("⚠️ Kaggle download failed:", e)

    return destination_path

def copy_raw_to_processed(raw_dir: str = "Final-Project-Formula1/data/raw", processed_dir: str = "Final-Project-Formula1/data/processed"):
    """
    Copy all CSV files from the raw folder to the processed folder under the data parent folder named data. This
    preserves the original raw data for safety.

    Arg:
       raw_dir (str): Path to the folder containing the raw data.
       processed_dir (str): Path to the folder where processed data will be stored.
    """
    
    # Define paths
    raw_path = Path(raw_dir)
    processed_path = Path(processed_dir)

    # Create processed folder if it doesn't exist
    processed_path.mkdir(parents = True, exist_ok = True)

    # Copy each CSV from raw to processed
    files = list(raw_path.glob("*.csv"))
    success, failed = 0, 0
    
    for f in files:
        try:
            (processed_path / f.name).write_bytes(f.read_bytes())
            print(f"📄 {f.name} copied")
            success += 1
        except Exception as e:
            print(f"⚠️ Cannot copy {f.name}: {e}")
            failed += 1
    
    print(f"✅ {success} files copied successfully to {processed_path}")
    if failed > 0:
        print(f"❌ {failed} files failed to copy")

    return processed_path
    
    
def load_csv(file_name: str, data_dir:str = "data/raw") -> pd.DataFrame:
    """
    Load a CSV file from the Formula 1 dataset

    Arg:
        file_name (str): Name of the CSV file (e.g. 'circuits.csv')
        data_dir (str): Path where the dataset is stored
    Returns:
        pd.DataFrame: Loaded dataset as a pandas DataFrame
    """