"""
File allowing you to download the Kaggle base dataset to your Nuvolos environment and preview each raw file downloaded.
"""

from pathlib import Path
import shutil
import pandas as pd
import kagglehub

def download_dataset(destination: str = "Final-Project-Formula1/data/raw"):
    """
    Download the dataset named Formula 1 World Championship (1950 - 2024) from Kaggle using
    KaggleHub.

    Arg:
       destination (str): Directory to save the dataset.
    Returns:
       str: Local path to the dataset.
    """

    # Define paths
    destination_path = Path(destination)
    project_root = destination_path.parents[1]
    
    # Check if parent folder exists (This is not essential because the line below creates
    # the parent folder if it does not already exist, but it is useful for greater
    # transparency)
    if project_root.exists():
        print(f"📂 Parent folder exists: {project_root}")
    else:
        print(f"❌ Parent folder not found: {project_root}")
    
    # Create the folder Final-Project-Formula1/data/raw if it does not already exist
    destination_path.mkdir(parents = True, exist_ok = True)

    # Check if dataset already exists (this avoid unnecessary downloads every time you run
    # the script)
    if any(destination_path.iterdir()):
        print(f"✅ Formula 1 World Championship (1950 - 2024) Dataset already exists in {destination_path}")
        return destination_path

    # Copy the download files from Kaggle into your Final-Project-Formula1/data/raw
    # directory
    try:
        print("📦 Downloading the dataset named Formula 1 World Championship (1950 - 2024) from Kaggle")
        # Download latest version
        path = kagglehub.dataset_download("rohanrao/formula-1-world-championship-1950-2020")
        src_path = Path(path)
        shutil.copytree(src_path, destination_path, dirs_exist_ok = True)
        print(f"✅ Formula 1 World Championship (1950 - 2024) Dataset download and available at: {destination_path}")
    except Exception as e:
        print("⚠️ Kaggle download failed:", e)

    return destination_path


def load_csv() -> pd.DataFrame:
    """
    Ask the user for a CSV file name and load it from the raw folder. Then show the
    first 5 rows of this specific csv file.
    
    Arg:
        file_name (input): Ask the user which file their wants to load.
    Returns:
        pd.DataFrame: Loaded CSV file as a pandas DataFrame.
    """

    # Folder where your data is stored
    raw_dir = "Final-Project-Formula1/data/raw"
    
    # Show all CSV files in the raw folder
    for file in Path(raw_dir).glob("*.csv"):
        print("•", file.name)

    # Ask the user which CSV file to load
    file_name = str(input("type the name of the csv file you want to preview (e.g., circuits.csv): ")).strip()

    file_path = Path(raw_dir) / file_name
    
    # Check if the CSV file exists
    if not file_path.exists():
        print(f"❌ CSV file not found in the list above (check if the spelling is right)")
        return None

    # Read the CSV file using pandas and show the first 5 rows as a preview
    df = pd.read_csv(file_path)
    print(f"✅ {file_name} loaded")
    print(df.head())

    return df