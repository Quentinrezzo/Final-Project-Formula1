"""
Main entry point for the Formula 1 race outcome prediction project.
"""

from pathlib import Path

"""
1. Downloads the Kaggle dataset into data/raw/.
2. Lists the available CSV files.
"""

from src.downloading_dataset import download_dataset

def main() -> None:
    print("=== F1 Project: setup and data download ===")

    # 1. Download dataset into data/raw/
    data_raw_path: Path = download_dataset()

    # 2. List CSV files in data/raw/
    csv_files = sorted(data_raw_path.glob("*.csv"))
    if not csv_files:
        print("❌ No CSV files found in data/raw after download.")
    else:
        print("✅ CSV files available in data/raw/:")
        for f in csv_files:
            print("  -", f.name)

    print("=== End of main.py (pipeline to be extended later) ===")


if __name__ == "__main__":
    main()