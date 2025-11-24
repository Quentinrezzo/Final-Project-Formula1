"""
Helper file to make Jupyter notebooks aware of the project root and src/ package
"""

from pathlib import Path
import sys

def setup_notebooks() -> Path:
    """
    Configure sys.path so that Jupyter notebooks can import from the src folder.

    Returns:
        Path: absolute path to the project root directory (the folder that contains src/)
    """

    # Project root = parent folder of src/
    project_root = Path(__file__).resolve().parents[1]
    
    # Add project root to sys.path if it is not already there
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
        print(f" setup_notebooks: Added project root to sys.path: {project_root}")
    else:
        print(f" setup_notebooks: Project root already in sys.path: {project_root}")


    return project_root