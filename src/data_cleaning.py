"""
Data cleaning and preprocessing functions for the Formula 1
"""

from pathlib import Path
import pandas as pd


def export_all_csv_to_excel():
    """
    Export all CSV files from the raw folder into a subfolder called raw_excel_data inside the main docs
    directory

    Arg:
       docs_dir (str): directory for saving all CSV files that have been exported to Excel.
    returns:
       str: Local path to the new raw_excel_data folder.
    """
    # Define paths
    raw_dir = Path("Final-Project-Formula1/data/raw")
    excel_dir = Path("Final-Project-Formula1/docs/raw_excel_data")

    # Create the output folder if it does not exist
    excel_dir.mkdir(parents = True, exist_ok = True)

    # Get all CSV files from raw and make a loop through each file and export to Excel
    csv_files = list(raw_dir.glob("*csv"))

    success = 0
    failed = 0
    for i, f in enumerate(csv_files, start = 1):
        try:
            df = pd.read_csv(f)
            excel_path = excel_dir / (f.stem + ".xlsx")
            df.to_excel(excel_path, index = False)
            success += 1
            print(f"✅ ({i}/{len(csv_files)}) {f.name} → {excel_path.name}")
        except Exception as e:
            failed += 1
            print(f"⚠️ ({i}/{len(csv_files)}) Could not export {f.name}: {e}")

    if failed == 0:
        print(f"📂 All Excel files saved in: {excel_dir}")
        print("You can now open them directly from your Nuvolos environment")
    else:
        print(f"⚠️ Some files failed to export. Please check the messages above.")

    return excel_dir


        

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