"""Project-wide settings: package set, analysis window, and paths."""

import os
import tempfile
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

# Everything the flow writes lives under RUN_DIR: the project folder normally, or a temp
# folder when the code runs from a read-only checkout (the Prefect Kubernetes worker).
RUN_DIR = BASE_DIR if os.access(BASE_DIR, os.W_OK) else Path(tempfile.gettempdir()) / "package-health"
DATA_DIR = RUN_DIR / "data" / "raw"
LOADED_MARKER = DATA_DIR / "_LOADED"
DB_PATH = RUN_DIR / "packages.duckdb"
LOG_DIR = RUN_DIR / "logs"
PLOTS_DIR = RUN_DIR / "plots"
DBT_TARGET_DIR = RUN_DIR / "target"

# Package set A: one large project and two smaller ones.
REPOS = ["numpy/numpy", "networkx/networkx", "statsmodels/statsmodels"]

# The window starts on a fixed date and runs through the time of each run.
# PH_WINDOW_START / PH_REPOS only exist for quick development runs (see README).
WINDOW_START = os.environ.get("PH_WINDOW_START", "2024-01-01")
if os.environ.get("PH_REPOS"):
    REPOS = os.environ["PH_REPOS"].split(",")

GITHUB_API = "https://api.github.com"
PER_PAGE = 100

# One color per repo, used in every plot (the first three slots of a palette validated
# for color-vision deficiency when all three appear together).
REPO_COLORS = {"numpy/numpy": "#2a78d6", "networkx/networkx": "#eb6834", "statsmodels/statsmodels": "#1baf7a"}
