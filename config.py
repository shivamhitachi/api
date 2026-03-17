# import os
# import yaml
# from pathlib import Path
#
# CONFIG_FILE = Path(__file__).parent / "config.yaml"
#
# def load_config() -> dict:
#     if CONFIG_FILE.exists():
#         try:
#             with open(CONFIG_FILE, "r") as f:
#                 return yaml.safe_load(f) or {}
#         except yaml.YAMLError as e:
#             print(f"Warning: Failed to parse config.yaml. Error: {e}")
#     else:
#         print(f"Warning: {CONFIG_FILE.name} not found. Falling back to defaults.")
#     return {}
#
# _config_data = load_config()
#
# _paths = _config_data.get("paths", {})
# OUTPUTS_DIR = _paths.get("outputs_dir") or os.getenv("E2_OUTPUTS_DIR", "outputs_new")
# RUN_FOLDER = _paths.get("run_folder", "fcn3_stormcast")
#
# _server = _config_data.get("server", {})
# ALLOWED_ORIGINS = _server.get("allowed_origins", [
#     "http://localhost:3000",
#     "http://127.0.0.1:3000"
# ])
#
# _geo = _config_data.get("geo", {})
# HRRR_PROJ_STRING = _geo.get(
#     "hrrr_proj_string",
#     "+proj=lcc +lat_1=38.5 +lat_2=38.5 +lat_0=38.5 +lon_0=-97.5 +a=6371229 +b=6371229 +units=m +no_defs"
# )



import yaml
from pathlib import Path


config_path = Path("config.yaml")
with open(config_path, "r") as f:
    config = yaml.safe_load(f)


BASE_DATA_DIR = config["paths"]["base_data_dir"]
RUN_FOLDER = config["paths"]["run_folder"]

ALLOWED_ORIGINS = config["server"]["allowed_origins"]

HRRR_PROJ_STRING = config["geo"]["hrrr_proj_string"]