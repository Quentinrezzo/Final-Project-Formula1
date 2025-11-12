"""
Simple pytest file for downloading_dataset.py
"""

from pathlib import Path
import pandas as pd
import builtins

# step 1: Allow notebook to see the src folder
import sys
sys.path.append("/files/Final-Project-Formula1") #project root

# Import the functions stored in the downloading_dataset.py file under the src folder
from src.downloading_dataset import download_dataset, load_csv

def test_download_dataset_copies_files(monkeypatch, tmp_path):
    """
    Simulate a Kaggle download by pointing dataset_download to a temp folder
    that contains a tiny CSV. Check the file is copied into data/raw.
    """
    # 1) Create a fake 'Kaggle cache' folder with one small file
    fake_kaggle_cache = tmp_path /"kaggle_cache"
    fake_kaggle_cache.mkdir(parents=True, exist_ok=True)
    (fake_kaggle_cache /"circuits.csv").write_text("a,b\n1,2\n")

    # 2) Replace kagglehub.dataset_download with a simple fake function
    monkeypatch.setattr("src.downloading_dataset.kagglehub.dataset_download", lambda *_a, **_k:
    str(fake_kaggle_cache), raising = False,)

    # 3) Create a fake project structure
    dest_raw = tmp_path /"Final-Project-Formula1"/"data"/"raw"

    # 4) Run the function
    out_path = download_dataset(destination = str(dest_raw))

    # 5) Assertions
    assert Path(out_path).exists()
    assert (dest_raw /"circuits.csv").exists()
    assert (dest_raw /"circuits.csv").read_text().startswith("a,b")


def test_load_csv_reads_dataframe(monkeypatch, tmp_path):
    """
    This test checks that load_csv() can read a small CSV file
    from data/raw/. If your function asks for user input, we fake it
    using monkeypatch.
    """
    # 1) Create a fake project folder with data/raw/circuits.csv
    raw_dir = tmp_path /"Final-Project-Formula1"/"data"/"raw"
    raw_dir.mkdir(parents = True, exist_ok = True)
    pd.DataFrame({"col1": [1, 2], "col2": [3, 4]}).to_csv(raw_dir /"circuits.csv", index = False)

    # 2) Set current working directory so your relative paths work
    monkeypatch.chdir(tmp_path)

    # 3) If load_csv() uses input(), we fake the user typing "circuits.csv"
    monkeypatch.setattr(builtins, "input", lambda prompt = "": "circuits.csv")

    # 4) Run the function
    df = load_csv()

    # 5) Assertions
    assert df is not None
    assert not df.empty
    assert list(df.columns) == ["col1", "col2"]