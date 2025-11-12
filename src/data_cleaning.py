"""
From now on, each CSV file will be taken individually to be modified and cleaned up so that we can work properly afterwards. When a CSV file is touched, it will go directly to a new folder called processed located under the main data folder, allowing the original files to remain in raw and the modified files in processed.
"""

from pathlib import Path
import pandas as pd

# Create the new folder called 'processed' in order to store my modified CSV files
processed_dir = Path("Final-Project-Formula1/data/processed")
processed_dir.mkdir(parents = True, exist_ok = True)
print(f"📂 Folder created (or already exists): {processed_dir}")

# Load the first CSV file from data/raw
df = pd.read_csv("Final-Project-Formula1/data/processed/circuits.csv")

# Create a new column titled 'lenght' between 'alt' and 'url'
df["lenght"] = "N/A"
cols = list(df.columns)
alt_index = cols.index("alt")
cols.insert(alt_index + 1, cols.pop(cols.index("length")))
df = df[cols]

# Save it back to CSV into the new processed folder
df.to_csv("Final-Project-Formula1/data/processed/circuits_cleaned.csv", index=False)
print("✅ New column 'lenght' added between 'alt' and 'url'")


        

def filter_recent_years_to_processed(raw_dir: str = "Final-Project-Formula1/data/raw", processed_dir: str = "Final-Project-Formula1/data/processed"):
    """
    Copy all CSV files from the raw folder to the processed folder under the data parent
    folder named data. This preserves the original raw data for safety.

    Arg:
       raw_path (str): Path to the folder containing the raw data.
       processed_path (str): Path to the folder where processed data will be stored.
    Returns:
       str: Local path to the processed folder.
    """
    
    # Define paths
    raw_path = Path(raw_dir)
    processed_path = Path(processed_dir)

    # Create processed folder if it doesn't exist
    processed_path.mkdir(parents = True, exist_ok = True)

    # Check if all csv files already exist (this avoid unnecessary downloads every time you run
    # the script)
    if any(processed_path.glob("*.csv")):
        print(f"✅ all CSV files already exist in {processed_path}")
        return processed_path
        
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