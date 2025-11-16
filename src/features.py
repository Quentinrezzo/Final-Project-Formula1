"""
def filter


def fill_circuit_lengths() -> None:
    
    Read the circuits_cleaned.csv file and fill in the “length_km” column with the values corresponding to
    each circuit using a dictionary of circuitId:length_km pairs.
    
    
    # Define paths and load the right file
    file_path = Path("Final-Project-Formula1/data/processed/circuits_cleaned.csv")
    df = pd.read_csv(file_path)

    # Dictionary of circuitId:length_km pairs
    circuit_lengths = {
        "1":
"""