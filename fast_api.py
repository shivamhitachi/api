# #
# # import os
# # import re
# # import hashlib
# # from pathlib import Path
# # from glob import glob
# # from typing import Literal, Optional, List, Dict, Tuple
# #
# # import numpy as np
# # import tensorstore as ts
# # from pyproj import Proj
# #
# # from fastapi import FastAPI, Response, Request
# # from fastapi.middleware.cors import CORSMiddleware
# # from fastapi.middleware.gzip import GZipMiddleware
# # from fastapi.responses import JSONResponse
# #
# #
# # from config import OUTPUTS_DIR, RUN_FOLDER, ALLOWED_ORIGINS, HRRR_PROJ_STRING
# #
# #
# # app = FastAPI(title="Stormcast Weather API")
# #
# # app.add_middleware(GZipMiddleware, minimum_size=1000)
# #
# # app.add_middleware(
# #     CORSMiddleware,
# #     allow_origins=ALLOWED_ORIGINS,  # Loaded from YAML
# #     allow_credentials=True,
# #     allow_methods=["GET"],
# #     allow_headers=["*"],
# # )
# #
# #
# #
# # class CustomError(Exception):
# #     def __init__(self, status_code: int, error: str, details: str = None):
# #         self.status_code = status_code
# #         self.error = error
# #         self.details = details
# #
# #
# # @app.exception_handler(CustomError)
# # async def custom_error_handler(request: Request, exc: CustomError):
# #     content = {"error": exc.error}
# #     if exc.details:
# #         content["details"] = exc.details
# #     return JSONResponse(status_code=exc.status_code, content=content)
# #
# #
# # DATE_FOLDER_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
# #
# # def resolve_zarr_path(model: str, date: str, ensemble_id: int) -> Path:
# #     model = model.lower()
# #
# #     if model not in ("stormcast", "fcn3"):
# #         raise CustomError(400, "Invalid parameters", f"Unknown model: {model}")
# #
# #     if not DATE_FOLDER_RE.match(date):
# #         raise CustomError(400, "Invalid parameters", "date must be in YYYY-MM-DD format")
# #
# #     if ensemble_id < 0:
# #         raise CustomError(400, "Invalid parameters", "ensemble_id must be a non-negative integer")
# #
# #     date_folder = f"{date}_24h"
# #
# #     zarr_dir = Path(OUTPUTS_DIR) / RUN_FOLDER / date_folder / f"{model}_member_{ensemble_id}.zarr"
# #
# #     return zarr_dir
# #
# #
# # def list_available_ensembles(model: str, date: str) -> List[int]:
# #     model = model.lower()
# #     base = Path(OUTPUTS_DIR) / RUN_FOLDER / f"{date}_24h"
# #     pattern = str(base / f"{model}_member_*.zarr")
# #
# #     ids: List[int] = []
# #     for p in glob(pattern):
# #         m = re.search(r"_member_(\d+)\.zarr$", p)
# #         if m:
# #             ids.append(int(m.group(1)))
# #     return sorted(set(ids))
# #
# #
# # def list_available_variables(zarr_path: Path) -> List[str]:
# #     if not zarr_path.exists():
# #         return []
# #
# #     ignore = {"lat", "lon", "hrrr_x", "hrrr_y"}
# #     vars_ = []
# #     for child in zarr_path.iterdir():
# #         if child.is_dir():
# #             name = child.name
# #             if name not in ignore:
# #                 vars_.append(name)
# #     return sorted(vars_)
# #
# #
# #
# # async def read_zarr_array(zarr_path: str, folder_name: str) -> np.ndarray:
# #     path = os.path.join(zarr_path, folder_name)
# #     if not os.path.exists(path):
# #         raise FileNotFoundError(f"Folder '{folder_name}' not found at {path}")
# #
# #     try:
# #         dataset = await ts.open({
# #             "driver": "zarr3",
# #             "kvstore": {"driver": "file", "path": path}
# #         })
# #     except Exception:
# #         dataset = await ts.open({
# #             "driver": "zarr",
# #             "kvstore": {"driver": "file", "path": path}
# #         })
# #
# #     data = await dataset.read()
# #     return np.squeeze(data)
# #
# #
# # async def fetch_base_data(model: str, date: str, variable: str, ensemble_id: int):
# #     zarr_path = resolve_zarr_path(model, date, ensemble_id)
# #
# #     if not zarr_path.exists():
# #         raise CustomError(503, "Model run not available", f"Missing: {str(zarr_path)}")
# #
# #     try:
# #         raw_data = await read_zarr_array(str(zarr_path), variable)
# #     except FileNotFoundError:
# #         raise CustomError(404, "Resource not found", "Invalid variable or ensemble_id not available")
# #     except Exception as e:
# #         raise CustomError(500, "Internal Server Error", f"Failed to process data arrays: {e}")
# #
# #     coords: Dict[str, List[float]] = {}
# #     for coord_name in ["lat", "lon", "hrrr_x", "hrrr_y"]:
# #         try:
# #             arr = await read_zarr_array(str(zarr_path), coord_name)
# #             if arr.ndim > 1 and coord_name in ["lat", "lon"]:
# #                 arr = arr[:, 0] if coord_name == "lat" else arr[0, :]
# #             coords[coord_name] = np.round(arr, 2).tolist()
# #         except FileNotFoundError:
# #             pass
# #
# #     raw_data = np.nan_to_num(raw_data, nan=0.0)
# #     raw_data = np.round(raw_data, decimals=2)
# #
# #     return raw_data, coords
# #
# #
# # def generate_etag(ident_string: str) -> str:
# #     return hashlib.md5(ident_string.encode()).hexdigest()
# #
# #
# # def get_1d_lat_lon(coords: dict):
# #     if "hrrr_x" in coords and "hrrr_y" in coords:
# #
# #         hrrr_proj = Proj(HRRR_PROJ_STRING)
# #
# #         x_arr = np.array(coords["hrrr_x"])
# #         y_arr = np.array(coords["hrrr_y"])
# #
# #         mid_y = y_arr[len(y_arr) // 2] if len(y_arr) > 0 else 0
# #         mid_x = x_arr[len(x_arr) // 2] if len(x_arr) > 0 else 0
# #
# #         lons, _ = hrrr_proj(x_arr, np.full_like(x_arr, mid_y), inverse=True)
# #         _, lats = hrrr_proj(np.full_like(y_arr, mid_x), y_arr, inverse=True)
# #
# #         return np.round(lats, 2).tolist(), np.round(lons, 2).tolist()
# #
# #     return coords.get("lat", []), coords.get("lon", [])
# #
# #
# # def bilinear_interpolate(data_3d, y_array, x_array, target_y, target_x):
# #     y_arr = np.array(y_array)
# #     x_arr = np.array(x_array)
# #
# #     y_idx = np.abs(y_arr - target_y).argmin()
# #     x_idx = np.abs(x_arr - target_x).argmin()
# #
# #     y1_idx = max(0, y_idx - 1) if target_y < y_arr[y_idx] else y_idx
# #     y2_idx = min(len(y_arr) - 1, y1_idx + 1)
# #
# #     x1_idx = max(0, x_idx - 1) if target_x < x_arr[x_idx] else x_idx
# #     x2_idx = min(len(x_arr) - 1, x1_idx + 1)
# #
# #     y1, y2 = y_arr[y1_idx], y_arr[y2_idx]
# #     x1, x2 = x_arr[x1_idx], x_arr[x2_idx]
# #
# #     if y1 == y2:
# #         wy1, wy2 = 1.0, 0.0
# #     else:
# #         wy1 = (y2 - target_y) / (y2 - y1)
# #         wy2 = (target_y - y1) / (y2 - y1)
# #
# #     if x1 == x2:
# #         wx1, wx2 = 1.0, 0.0
# #     else:
# #         wx1 = (x2 - target_x) / (x2 - x1)
# #         wx2 = (target_x - x1) / (x2 - x1)
# #
# #     val_q11 = data_3d[:, y1_idx, x1_idx] if data_3d.ndim >= 3 else data_3d[y1_idx, x1_idx]
# #     val_q12 = data_3d[:, y1_idx, x2_idx] if data_3d.ndim >= 3 else data_3d[y1_idx, x2_idx]
# #     val_q21 = data_3d[:, y2_idx, x1_idx] if data_3d.ndim >= 3 else data_3d[y2_idx, x1_idx]
# #     val_q22 = data_3d[:, y2_idx, x2_idx] if data_3d.ndim >= 3 else data_3d[y2_idx, x2_idx]
# #
# #     interpolated_values = (
# #             val_q11 * wy1 * wx1 +
# #             val_q21 * wy2 * wx1 +
# #             val_q12 * wy1 * wx2 +
# #             val_q22 * wy2 * wx2
# #     )
# #
# #     if np.isscalar(interpolated_values):
# #         return [round(float(interpolated_values), 2)]
# #     return np.round(interpolated_values, 2).tolist()
# #
# #
# #
# # @app.get("/api/{model}/{date}/available")
# # async def available(model: Literal["stormcast", "fcn3"], date: str, ensemble_probe: int = 0):
# #     ens = list_available_ensembles(model, date)
# #     variables: List[str] = []
# #
# #     probe_id = ensemble_probe if ensemble_probe in ens else (ens[0] if ens else None)
# #     if probe_id is not None:
# #         zarr_path = resolve_zarr_path(model, date, probe_id)
# #         variables = list_available_variables(zarr_path)
# #
# #     return {
# #         "model": model,
# #         "date": date,
# #         "run_folder": RUN_FOLDER,
# #         "available_ensembles": ens,
# #         "variables_probe_ensemble": probe_id,
# #         "available_variables": variables
# #     }
# #
# #
# # @app.get("/api/{model}/{date}/{variable}/timeseries")
# # async def get_timeseries(
# #         model: Literal["stormcast", "fcn3"],
# #         date: str,
# #         variable: str,
# #         lat: float,
# #         lon: float,
# #         ensemble_id: int = 0,
# #         preview: bool = False,
# #         response: Response = None,
# # ):
# #     if response is None:
# #         response = Response()
# #
# #     raw_data, coords = await fetch_base_data(model, date, variable, ensemble_id)
# #
# #     y_array = coords.get("lat") or coords.get("hrrr_y")
# #     x_array = coords.get("lon") or coords.get("hrrr_x")
# #
# #     if not y_array or not x_array:
# #         raise CustomError(404, "Resource not found", "Coordinate data missing from file for interpolation.")
# #
# #     target_y, target_x = lat, lon
# #     if "hrrr_y" in coords and "hrrr_x" in coords and "lat" not in coords:
# #         # Uses HRRR_PROJ_STRING from YAML
# #         hrrr_proj = Proj(HRRR_PROJ_STRING)
# #         target_x, target_y = hrrr_proj(lon, lat)
# #
# #     max_hours = min(25, raw_data.shape[0] if raw_data.ndim >= 3 else 1)
# #     data_slice = raw_data[:max_hours] if raw_data.ndim >= 3 else raw_data
# #
# #     timeseries_values = bilinear_interpolate(data_slice, y_array, x_array, target_y, target_x)
# #     lead_times = list(range(len(timeseries_values)))
# #
# #     if preview:
# #         timeseries_values = timeseries_values[:5]
# #         lead_times = lead_times[:5]
# #
# #     payload = {
# #         "date": date,
# #         "variable": variable,
# #         "model": model,
# #         "ensemble_id": ensemble_id,
# #         "lat": lat,
# #         "lon": lon,
# #         "lead_time": lead_times,
# #         "values": timeseries_values
# #     }
# #
# #     response.headers["Cache-Control"] = "public, max-age=3600"
# #     response.headers["ETag"] = f'W/"{generate_etag(f"ts-{model}-{date}-{variable}-{lat}-{lon}-{ensemble_id}")}"'
# #     return payload
# #
# #
# # @app.get("/api/{model}/{date}/{variable}/{hours}")
# # async def get_specific_hours(
# #         model: Literal["stormcast", "fcn3"],
# #         date: str,
# #         variable: str,
# #         hours: str,
# #         ensemble_id: int = 0,
# #         preview: bool = False,
# #         response: Response = None,
# # ):
# #     if response is None:
# #         response = Response()
# #
# #     try:
# #         hour_list = [int(h.strip()) for h in hours.split(",")]
# #     except ValueError:
# #         raise CustomError(
# #             400,
# #             "Invalid parameters",
# #             "Hours must be an integer or comma-separated integers (e.g., '1' or '1,2,3')"
# #         )
# #
# #     raw_data, coords = await fetch_base_data(model, date, variable, ensemble_id)
# #     lat_1d, lon_1d = get_1d_lat_lon(coords)
# #
# #     max_valid_hour = raw_data.shape[0] - 1 if raw_data.ndim >= 3 else 0
# #     for h in hour_list:
# #         if h < 0 or h > 24 or h > max_valid_hour:
# #             raise CustomError(404, "Resource not found", f"Hour {h} is out of bounds")
# #
# #     if raw_data.ndim >= 3:
# #         data_slice = raw_data[hour_list, :, :]
# #     else:
# #         data_slice = np.array([raw_data])
# #
# #     if preview:
# #         lat_1d = lat_1d[:10]
# #         lon_1d = lon_1d[:10]
# #         if data_slice.ndim >= 3:
# #             data_slice = data_slice[:, :10, :10]
# #         else:
# #             data_slice = data_slice[:10, :10]
# #
# #     flat_values = data_slice.flatten().tolist()
# #
# #     payload = {
# #         "date": date,
# #         "variable": variable,
# #         "model": model,
# #         "ensemble_id": ensemble_id,
# #         "lead_time": hour_list,
# #         "lat": lat_1d,
# #         "lon": lon_1d,
# #         "values": flat_values
# #     }
# #
# #     response.headers["Cache-Control"] = "public, max-age=3600"
# #     response.headers["ETag"] = f'W/"{generate_etag(f"sh-{model}-{date}-{variable}-{hours}-{ensemble_id}")}"'
# #     return payload
# #
# #
# # @app.get("/api/{model}/{date}/{variable}")
# # async def get_all_hours(
# #         model: Literal["stormcast", "fcn3"],
# #         date: str,
# #         variable: str,
# #         ensemble_id: int = 0,
# #         preview: bool = False,
# #         response: Response = None,
# # ):
# #     if response is None:
# #         response = Response()
# #
# #     raw_data, coords = await fetch_base_data(model, date, variable, ensemble_id)
# #     lat_1d, lon_1d = get_1d_lat_lon(coords)
# #
# #     max_hours = min(25, raw_data.shape[0] if raw_data.ndim >= 3 else 1)
# #     data_slice = raw_data[:max_hours, :, :] if raw_data.ndim >= 3 else raw_data
# #
# #     lead_times = list(range(data_slice.shape[0] if data_slice.ndim >= 3 else 1))
# #
# #     if preview:
# #         lat_1d = lat_1d[:10]
# #         lon_1d = lon_1d[:10]
# #         lead_times = lead_times[:2]
# #         if data_slice.ndim >= 3:
# #             data_slice = data_slice[:2, :10, :10]
# #         else:
# #             data_slice = data_slice[:10, :10]
# #
# #     flat_values = data_slice.flatten().tolist()
# #
# #     payload = {
# #         "date": date,
# #         "variable": variable,
# #         "model": model,
# #         "ensemble_id": ensemble_id,
# #         "lead_time": lead_times,
# #         "lat": lat_1d,
# #         "lon": lon_1d,
# #         "values": flat_values
# #     }
# #
# #     response.headers["Cache-Control"] = "public, max-age=3600"
# #     response.headers["ETag"] = f'W/"{generate_etag(f"all-{model}-{date}-{variable}-{ensemble_id}")}"'
# #     return payload
# 
# 
# 
# 
# 
# 
# import os
# import re
# import hashlib
# from pathlib import Path
# from glob import glob
# from typing import Literal, Optional, List, Dict, Tuple
# 
# import numpy as np
# import tensorstore as ts
# from pyproj import Proj
# 
# from fastapi import FastAPI, Response, Request
# from fastapi.middleware.cors import CORSMiddleware
# from fastapi.middleware.gzip import GZipMiddleware
# from fastapi.responses import JSONResponse
# 
# from config import BASE_DATA_DIR, RUN_FOLDER, ALLOWED_ORIGINS, HRRR_PROJ_STRING
# 
# app = FastAPI(title="Stormcast Weather API")
# 
# app.add_middleware(GZipMiddleware, minimum_size=1000)
# 
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=ALLOWED_ORIGINS,
#     allow_credentials=True,
#     allow_methods=["GET"],
#     allow_headers=["*"],
# )
# 
# 
# class CustomError(Exception):
#     def __init__(self, status_code: int, error: str, details: str = None):
#         self.status_code = status_code
#         self.error = error
#         self.details = details
# 
# 
# @app.exception_handler(CustomError)
# async def custom_error_handler(request: Request, exc: CustomError):
#     content = {"error": exc.error}
#     if exc.details:
#         content["details"] = exc.details
#     return JSONResponse(status_code=exc.status_code, content=content)
# 
# 
# DATE_FOLDER_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
# 
# 
# DATE_FOLDER_CACHE: Dict[str, Path] = {}
# 
# 
# def find_date_folder_path(date: str) -> Path:
# 
#     date_folder_name = f"{date}_24h"
# 
#     if date in DATE_FOLDER_CACHE:
#         cached_path = DATE_FOLDER_CACHE[date]
#         if cached_path.exists():
#             return cached_path
#         else:
#             del DATE_FOLDER_CACHE[date]
# 
#     base_dir = Path(BASE_DATA_DIR)
# 
#     if not base_dir.exists():
#         raise CustomError(500, "Server Configuration Error", f"Base directory {BASE_DATA_DIR} does not exist.")
# 
#     for folder in base_dir.iterdir():
#         if folder.is_dir() and "stormcast" in folder.name.lower():
#             target_date_path = folder / RUN_FOLDER / date_folder_name
# 
#             if target_date_path.exists():
#                 DATE_FOLDER_CACHE[date] = target_date_path
#                 return target_date_path
# 
#     raise CustomError(404, "Data not found", f"No stormcast data found for date: {date}")
# 
# 
# def resolve_zarr_path(model: str, date: str, ensemble_id: int) -> Path:
#     model = model.lower()
# 
#     if model not in ("stormcast", "fcn3"):
#         raise CustomError(400, "Invalid parameters", f"Unknown model: {model}")
# 
#     if not DATE_FOLDER_RE.match(date):
#         raise CustomError(400, "Invalid parameters", "date must be in YYYY-MM-DD format")
# 
#     if ensemble_id < 0:
#         raise CustomError(400, "Invalid parameters", "ensemble_id must be a non-negative integer")
# 
#     date_dir = find_date_folder_path(date)
#     zarr_dir = date_dir / f"{model}_member_{ensemble_id}.zarr"
# 
#     return zarr_dir
# 
# 
# def list_available_ensembles(model: str, date: str) -> List[int]:
#     model = model.lower()
# 
#     try:
#         date_dir = find_date_folder_path(date)
#     except CustomError:
#         return []
# 
#     pattern = str(date_dir / f"{model}_member_*.zarr")
# 
#     ids: List[int] = []
#     for p in glob(pattern):
#         m = re.search(r"_member_(\d+)\.zarr$", p)
#         if m:
#             ids.append(int(m.group(1)))
#     return sorted(set(ids))
# 
# 
# def list_available_variables(zarr_path: Path) -> List[str]:
#     if not zarr_path.exists():
#         return []
# 
#     ignore = {"lat", "lon", "hrrr_x", "hrrr_y"}
#     vars_ = []
#     for child in zarr_path.iterdir():
#         if child.is_dir():
#             name = child.name
#             if name not in ignore:
#                 vars_.append(name)
#     return sorted(vars_)
# 
# 
# async def read_zarr_array(zarr_path: str, folder_name: str) -> np.ndarray:
#     path = os.path.join(zarr_path, folder_name)
#     if not os.path.exists(path):
#         raise FileNotFoundError(f"Folder '{folder_name}' not found at {path}")
# 
#     try:
#         dataset = await ts.open({
#             "driver": "zarr3",
#             "kvstore": {"driver": "file", "path": path}
#         })
#     except Exception:
#         dataset = await ts.open({
#             "driver": "zarr",
#             "kvstore": {"driver": "file", "path": path}
#         })
# 
#     data = await dataset.read()
#     return np.squeeze(data)
# 
# 
# async def fetch_base_data(model: str, date: str, variable: str, ensemble_id: int):
#     zarr_path = resolve_zarr_path(model, date, ensemble_id)
# 
#     if not zarr_path.exists():
#         raise CustomError(503, "Model run not available", f"Missing: {str(zarr_path)}")
# 
#     try:
#         raw_data = await read_zarr_array(str(zarr_path), variable)
#     except FileNotFoundError:
#         raise CustomError(404, "Resource not found", "Invalid variable or ensemble_id not available")
#     except Exception as e:
#         raise CustomError(500, "Internal Server Error", f"Failed to process data arrays: {e}")
# 
#     coords: Dict[str, List[float]] = {}
#     for coord_name in ["lat", "lon", "hrrr_x", "hrrr_y"]:
#         try:
#             arr = await read_zarr_array(str(zarr_path), coord_name)
#             if arr.ndim > 1 and coord_name in ["lat", "lon"]:
#                 arr = arr[:, 0] if coord_name == "lat" else arr[0, :]
#             coords[coord_name] = np.round(arr, 2).tolist()
#         except FileNotFoundError:
#             pass
# 
#     raw_data = np.nan_to_num(raw_data, nan=0.0)
#     raw_data = np.round(raw_data, decimals=2)
# 
#     return raw_data, coords
# 
# 
# def generate_etag(ident_string: str) -> str:
#     return hashlib.md5(ident_string.encode()).hexdigest()
# 
# 
# def get_1d_lat_lon(coords: dict):
#     if "hrrr_x" in coords and "hrrr_y" in coords:
#         hrrr_proj = Proj(HRRR_PROJ_STRING)
# 
#         x_arr = np.array(coords["hrrr_x"])
#         y_arr = np.array(coords["hrrr_y"])
# 
#         mid_y = y_arr[len(y_arr) // 2] if len(y_arr) > 0 else 0
#         mid_x = x_arr[len(x_arr) // 2] if len(x_arr) > 0 else 0
# 
#         lons, _ = hrrr_proj(x_arr, np.full_like(x_arr, mid_y), inverse=True)
#         _, lats = hrrr_proj(np.full_like(y_arr, mid_x), y_arr, inverse=True)
# 
#         return np.round(lats, 2).tolist(), np.round(lons, 2).tolist()
# 
#     return coords.get("lat", []), coords.get("lon", [])
# 
# 
# def nearest_neighbor_lookup(data_3d, y_array, x_array, target_y, target_x):
#     """Replaces bilinear interpolation to grab the raw grid cell value."""
#     y_arr = np.array(y_array)
#     x_arr = np.array(x_array)
# 
#     y_idx = np.abs(y_arr - target_y).argmin()
#     x_idx = np.abs(x_arr - target_x).argmin()
# 
#     if data_3d.ndim >= 3:
#         raw_values = data_3d[:, y_idx, x_idx]
#     else:
#         raw_values = data_3d[y_idx, x_idx]
# 
# 
#     if np.isscalar(raw_values):
#         return [round(float(raw_values), 2)]
#     return np.round(raw_values, 2).tolist()
# 
# 
# @app.get("/api/{model}/{date}/available")
# async def available(model: Literal["stormcast", "fcn3"], date: str, ensemble_probe: int = 0):
#     ens = list_available_ensembles(model, date)
#     variables: List[str] = []
# 
#     probe_id = ensemble_probe if ensemble_probe in ens else (ens[0] if ens else None)
#     if probe_id is not None:
#         zarr_path = resolve_zarr_path(model, date, probe_id)
#         variables = list_available_variables(zarr_path)
# 
#     return {
#         "model": model,
#         "date": date,
#         "run_folder": RUN_FOLDER,
#         "available_ensembles": ens,
#         "variables_probe_ensemble": probe_id,
#         "available_variables": variables
#     }
# 
# 
# @app.get("/api/{model}/{date}/{variable}/timeseries")
# async def get_timeseries(
#         model: Literal["stormcast", "fcn3"],
#         date: str,
#         variable: str,
#         lat: float,
#         lon: float,
#         ensemble_id: int = 0,
#         preview: bool = False,
#         response: Response = None,
# ):
#     if response is None:
#         response = Response()
# 
#     zarr_path = resolve_zarr_path(model, date, ensemble_id)
# 
#     if not zarr_path.exists():
#         raise CustomError(503, "Model run not available", f"Missing: {str(zarr_path)}")
# 
#     # 1. Safely fetch the main variable data
#     try:
#         raw_data = await read_zarr_array(str(zarr_path), variable)
#     except FileNotFoundError:
#         raise CustomError(404, "Resource not found", f"Variable '{variable}' not found in zarr.")
# 
#     lat_arr, lon_arr = None, None
#     hrrr_x, hrrr_y = None, None
# 
#     try:
#         lat_arr = await read_zarr_array(str(zarr_path), "lat")
#         lon_arr = await read_zarr_array(str(zarr_path), "lon")
#     except FileNotFoundError:
#         pass
# 
#     try:
#         hrrr_x = await read_zarr_array(str(zarr_path), "hrrr_x")
#         hrrr_y = await read_zarr_array(str(zarr_path), "hrrr_y")
#     except FileNotFoundError:
#         pass
# 
#     if lat_arr is None and hrrr_x is None:
#         raise CustomError(500, "Internal Server Error", "No coordinate variables found in Zarr file.")
# 
#     y_idx, x_idx = 0, 0
# 
#     if hrrr_x is not None and hrrr_y is not None and hrrr_x.ndim == 1:
# 
#         hrrr_proj = Proj(HRRR_PROJ_STRING)
#         target_x, target_y = hrrr_proj(lon, lat)
# 
#         y_idx = np.abs(hrrr_y - target_y).argmin()
#         x_idx = np.abs(hrrr_x - target_x).argmin()
# 
#     elif lat_arr is not None and lon_arr is not None:
# 
#         if lat_arr.ndim >= 2 and lon_arr.ndim >= 2:
#             dist_sq = (lat_arr - lat)**2 + (lon_arr - lon)**2
#             y_idx, x_idx = np.unravel_index(np.argmin(dist_sq), lat_arr.shape)
#         else:
# 
#             y_idx = np.abs(lat_arr - lat).argmin()
#             x_idx = np.abs(lon_arr - lon).argmin()
# 
#     raw_data = np.nan_to_num(raw_data, nan=0.0)
#     max_hours = min(25, raw_data.shape[0] if raw_data.ndim >= 3 else 1)
# 
# 
#     if raw_data.ndim >= 3:
#         timeseries_values = raw_data[:max_hours, y_idx, x_idx]
#     else:
#         timeseries_values = np.array([raw_data[y_idx, x_idx]])
# 
#     timeseries_values = np.round(timeseries_values, 2).tolist()
#     lead_times = list(range(len(timeseries_values)))
# 
#     if preview:
#         timeseries_values = timeseries_values[:5]
#         lead_times = lead_times[:5]
# 
#     payload = {
#         "date": date,
#         "variable": variable,
#         "model": model,
#         "ensemble_id": ensemble_id,
#         "lat": lat,
#         "lon": lon,
#         "lead_time": lead_times,
#         "values": timeseries_values
#     }
# 
#     response.headers["Cache-Control"] = "public, max-age=3600"
#     response.headers["ETag"] = f'W/"{generate_etag(f"ts-{model}-{date}-{variable}-{lat}-{lon}-{ensemble_id}")}"'
#     return payload
# 
# 
# @app.get("/api/{model}/{date}/{variable}/{hours}")
# async def get_specific_hours(
#         model: Literal["stormcast", "fcn3"],
#         date: str,
#         variable: str,
#         hours: str,
#         ensemble_id: int = 0,
#         preview: bool = False,
#         response: Response = None,
# ):
#     if response is None:
#         response = Response()
# 
#     try:
#         hour_list = [int(h.strip()) for h in hours.split(",")]
#     except ValueError:
#         raise CustomError(
#             400,
#             "Invalid parameters",
#             "Hours must be an integer or comma-separated integers (e.g., '1' or '1,2,3')"
#         )
# 
#     raw_data, coords = await fetch_base_data(model, date, variable, ensemble_id)
#     lat_1d, lon_1d = get_1d_lat_lon(coords)
# 
#     max_valid_hour = raw_data.shape[0] - 1 if raw_data.ndim >= 3 else 0
#     for h in hour_list:
#         if h < 0 or h > 24 or h > max_valid_hour:
#             raise CustomError(404, "Resource not found", f"Hour {h} is out of bounds")
# 
#     if raw_data.ndim >= 3:
#         data_slice = raw_data[hour_list, :, :]
#     else:
#         data_slice = np.array([raw_data])
# 
#     if preview:
#         lat_1d = lat_1d[:10]
#         lon_1d = lon_1d[:10]
#         if data_slice.ndim >= 3:
#             data_slice = data_slice[:, :10, :10]
#         else:
#             data_slice = data_slice[:10, :10]
# 
#     flat_values = data_slice.flatten().tolist()
# 
#     payload = {
#         "date": date,
#         "variable": variable,
#         "model": model,
#         "ensemble_id": ensemble_id,
#         "lead_time": hour_list,
#         "lat": lat_1d,
#         "lon": lon_1d,
#         "values": flat_values
#     }
# 
#     response.headers["Cache-Control"] = "public, max-age=3600"
#     response.headers["ETag"] = f'W/"{generate_etag(f"sh-{model}-{date}-{variable}-{hours}-{ensemble_id}")}"'
#     return payload
# 
# 
# @app.get("/api/{model}/{date}/{variable}")
# async def get_all_hours(
#         model: Literal["stormcast", "fcn3"],
#         date: str,
#         variable: str,
#         ensemble_id: int = 0,
#         preview: bool = False,
#         response: Response = None,
# ):
#     if response is None:
#         response = Response()
# 
#     raw_data, coords = await fetch_base_data(model, date, variable, ensemble_id)
#     lat_1d, lon_1d = get_1d_lat_lon(coords)
# 
#     max_hours = min(25, raw_data.shape[0] if raw_data.ndim >= 3 else 1)
#     data_slice = raw_data[:max_hours, :, :] if raw_data.ndim >= 3 else raw_data
# 
#     lead_times = list(range(data_slice.shape[0] if data_slice.ndim >= 3 else 1))
# 
#     if preview:
#         lat_1d = lat_1d[:10]
#         lon_1d = lon_1d[:10]
#         lead_times = lead_times[:2]
#         if data_slice.ndim >= 3:
#             data_slice = data_slice[:2, :10, :10]
#         else:
#             data_slice = data_slice[:10, :10]
# 
#     flat_values = data_slice.flatten().tolist()
# 
#     payload = {
#         "date": date,
#         "variable": variable,
#         "model": model,
#         "ensemble_id": ensemble_id,
#         "lead_time": lead_times,
#         "lat": lat_1d,
#         "lon": lon_1d,
#         "values": flat_values
#     }
# 
#     response.headers["Cache-Control"] = "public, max-age=3600"
#     response.headers["ETag"] = f'W/"{generate_etag(f"all-{model}-{date}-{variable}-{ensemble_id}")}"'
#     return payload



import os
import re
import hashlib
from pathlib import Path
from glob import glob
from typing import Literal, Optional, List, Dict, Tuple

import numpy as np
import tensorstore as ts
from pyproj import Proj

from fastapi import FastAPI, Response, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse

from config import BASE_DATA_DIR, RUN_FOLDER, ALLOWED_ORIGINS, HRRR_PROJ_STRING

app = FastAPI(title="Stormcast Weather API")

app.add_middleware(GZipMiddleware, minimum_size=1000)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)


class CustomError(Exception):
    def __init__(self, status_code: int, error: str, details: str = None):
        self.status_code = status_code
        self.error = error
        self.details = details


@app.exception_handler(CustomError)
async def custom_error_handler(request: Request, exc: CustomError):
    content = {"error": exc.error}
    if exc.details:
        content["details"] = exc.details
    return JSONResponse(status_code=exc.status_code, content=content)


DATE_FOLDER_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


DATE_FOLDER_CACHE: Dict[str, Path] = {}


def find_date_folder_path(date: str) -> Path:
    date_folder_name = f"{date}_24h"

    if date in DATE_FOLDER_CACHE:
        cached_path = DATE_FOLDER_CACHE[date]
        if cached_path.exists():
            return cached_path
        else:
            del DATE_FOLDER_CACHE[date]

    base_dir = Path(BASE_DATA_DIR)

    if not base_dir.exists():
        raise CustomError(500, "Server Configuration Error", f"Base directory {BASE_DATA_DIR} does not exist.")

    for folder in base_dir.iterdir():
        if folder.is_dir() and "stormcast" in folder.name.lower():
            target_date_path = folder / RUN_FOLDER / date_folder_name

            if target_date_path.exists():
                DATE_FOLDER_CACHE[date] = target_date_path
                return target_date_path

    raise CustomError(404, "Data not found", f"No stormcast data found for date: {date}")


def resolve_zarr_path(model: str, date: str, ensemble_id: int) -> Path:
    model = model.lower()

    if model not in ("stormcast", "fcn3"):
        raise CustomError(400, "Invalid parameters", f"Unknown model: {model}")

    if not DATE_FOLDER_RE.match(date):
        raise CustomError(400, "Invalid parameters", "date must be in YYYY-MM-DD format")

    if ensemble_id < 0:
        raise CustomError(400, "Invalid parameters", "ensemble_id must be a non-negative integer")

    date_dir = find_date_folder_path(date)
    zarr_dir = date_dir / f"{model}_member_{ensemble_id}.zarr"

    return zarr_dir


def list_available_ensembles(model: str, date: str) -> List[int]:
    model = model.lower()

    try:
        date_dir = find_date_folder_path(date)
    except CustomError:
        return []

    pattern = str(date_dir / f"{model}_member_*.zarr")

    ids: List[int] = []
    for p in glob(pattern):
        m = re.search(r"_member_(\d+)\.zarr$", p)
        if m:
            ids.append(int(m.group(1)))
    return sorted(set(ids))


def list_available_variables(zarr_path: Path) -> List[str]:
    if not zarr_path.exists():
        return []

    ignore = {"lat", "lon", "hrrr_x", "hrrr_y"}
    vars_ = []
    for child in zarr_path.iterdir():
        if child.is_dir():
            name = child.name
            if name not in ignore:
                vars_.append(name)
    return sorted(vars_)


async def read_zarr_array(zarr_path: str, folder_name: str) -> np.ndarray:
    path = os.path.join(zarr_path, folder_name)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Folder '{folder_name}' not found at {path}")

    try:
        dataset = await ts.open({
            "driver": "zarr3",
            "kvstore": {"driver": "file", "path": path}
        })
    except Exception:
        dataset = await ts.open({
            "driver": "zarr",
            "kvstore": {"driver": "file", "path": path}
        })

    data = await dataset.read()
    return np.squeeze(data)


async def fetch_base_data(model: str, date: str, variable: str, ensemble_id: int):

    zarr_path = resolve_zarr_path(model, date, ensemble_id)

    if not zarr_path.exists():
        raise CustomError(503, "Model run not available", f"Missing: {str(zarr_path)}")

    try:
        raw_data = await read_zarr_array(str(zarr_path), variable)
    except FileNotFoundError:
        raise CustomError(404, "Resource not found", "Invalid variable or ensemble_id not available")
    except Exception as e:
        raise CustomError(500, "Internal Server Error", f"Failed to process data arrays: {e}")

    coords: Dict[str, List[float]] = {}
    for coord_name in ["lat", "lon", "hrrr_x", "hrrr_y"]:
        try:
            arr = await read_zarr_array(str(zarr_path), coord_name)
            if arr.ndim > 1 and coord_name in ["lat", "lon"]:
                arr = arr[:, 0] if coord_name == "lat" else arr[0, :]
            coords[coord_name] = np.round(arr, 2).tolist()
        except FileNotFoundError:
            pass

    raw_data = np.nan_to_num(raw_data, nan=0.0)
    raw_data = np.round(raw_data, decimals=2)

    return raw_data, coords


def generate_etag(ident_string: str) -> str:
    return hashlib.md5(ident_string.encode()).hexdigest()


def get_1d_lat_lon(coords: dict):
    if "hrrr_x" in coords and "hrrr_y" in coords:
        hrrr_proj = Proj(HRRR_PROJ_STRING)

        x_arr = np.array(coords["hrrr_x"])
        y_arr = np.array(coords["hrrr_y"])

        mid_y = y_arr[len(y_arr) // 2] if len(y_arr) > 0 else 0
        mid_x = x_arr[len(x_arr) // 2] if len(x_arr) > 0 else 0

        lons, _ = hrrr_proj(x_arr, np.full_like(x_arr, mid_y), inverse=True)
        _, lats = hrrr_proj(np.full_like(y_arr, mid_x), y_arr, inverse=True)

        return np.round(lats, 2).tolist(), np.round(lons, 2).tolist()

    return coords.get("lat", []), coords.get("lon", [])


@app.get("/api/{model}/{date}/available")
async def available(model: Literal["stormcast", "fcn3"], date: str, ensemble_probe: int = 0):
    ens = list_available_ensembles(model, date)
    variables: List[str] = []

    probe_id = ensemble_probe if ensemble_probe in ens else (ens[0] if ens else None)
    if probe_id is not None:
        zarr_path = resolve_zarr_path(model, date, probe_id)
        variables = list_available_variables(zarr_path)

    return {
        "model": model,
        "date": date,
        "run_folder": RUN_FOLDER,
        "available_ensembles": ens,
        "variables_probe_ensemble": probe_id,
        "available_variables": variables
    }


@app.get("/api/{model}/{date}/{variable}/timeseries")
async def get_timeseries(
        model: Literal["stormcast", "fcn3"],
        date: str,
        variable: str,
        lat: float,
        lon: float,
        ensemble_id: int = 0,
        preview: bool = False,
        response: Response = None,
):

    if response is None:
        response = Response()

    zarr_path = resolve_zarr_path(model, date, ensemble_id)

    if not zarr_path.exists():
        raise CustomError(503, "Model run not available", f"Missing: {str(zarr_path)}")


    try:
        raw_data = await read_zarr_array(str(zarr_path), variable)
    except FileNotFoundError:
        raise CustomError(404, "Resource not found", f"Variable '{variable}' not found in zarr.")

    lat_arr, lon_arr = None, None
    hrrr_x, hrrr_y = None, None

    try:
        lat_arr = await read_zarr_array(str(zarr_path), "lat")
        lon_arr = await read_zarr_array(str(zarr_path), "lon")
    except FileNotFoundError:
        pass

    try:
        hrrr_x = await read_zarr_array(str(zarr_path), "hrrr_x")
        hrrr_y = await read_zarr_array(str(zarr_path), "hrrr_y")
    except FileNotFoundError:
        pass

    if lat_arr is None and hrrr_x is None:
        raise CustomError(500, "Internal Server Error", "No coordinate variables found in Zarr file.")

    y_idx, x_idx = 0, 0

    if hrrr_x is not None and hrrr_y is not None and hrrr_x.ndim == 1:
        hrrr_proj = Proj(HRRR_PROJ_STRING)
        target_x, target_y = hrrr_proj(lon, lat)

        y_idx = np.abs(hrrr_y - target_y).argmin()
        x_idx = np.abs(hrrr_x - target_x).argmin()

    elif lat_arr is not None and lon_arr is not None:
        if lat_arr.ndim >= 2 and lon_arr.ndim >= 2:
            dist_sq = (lat_arr - lat)**2 + (lon_arr - lon)**2
            y_idx, x_idx = np.unravel_index(np.argmin(dist_sq), lat_arr.shape)
        else:
            y_idx = np.abs(lat_arr - lat).argmin()
            x_idx = np.abs(lon_arr - lon).argmin()

    raw_data = np.nan_to_num(raw_data, nan=0.0)
    max_hours = min(25, raw_data.shape[0] if raw_data.ndim >= 3 else 1)

    if raw_data.ndim >= 3:
        timeseries_values = raw_data[:max_hours, y_idx, x_idx]
    else:
        timeseries_values = np.array([raw_data[y_idx, x_idx]])

    timeseries_values = np.round(timeseries_values, 2).tolist()
    lead_times = list(range(len(timeseries_values)))

    if preview:
        timeseries_values = timeseries_values[:5]
        lead_times = lead_times[:5]

    payload = {
        "date": date,
        "variable": variable,
        "model": model,
        "ensemble_id": ensemble_id,
        "lat": lat,
        "lon": lon,
        "lead_time": lead_times,
        "values": timeseries_values
    }

    response.headers["Cache-Control"] = "public, max-age=3600"
    response.headers["ETag"] = f'W/"{generate_etag(f"ts-{model}-{date}-{variable}-{lat}-{lon}-{ensemble_id}")}"'
    return payload


@app.get("/api/{model}/{date}/{variable}/{hours}")
async def get_specific_hours(
        model: Literal["stormcast", "fcn3"],
        date: str,
        variable: str,
        hours: str,
        ensemble_id: int = 0,
        zoom: Optional[float] = None,
        preview: bool = False,
        response: Response = None,
):

    if response is None:
        response = Response()

    try:
        hour_list = [int(h.strip()) for h in hours.split(",")]
    except ValueError:
        raise CustomError(
            400,
            "Invalid parameters",
            "Hours must be an integer or comma-separated integers (e.g., '1' or '1,2,3')"
        )

    raw_data, coords = await fetch_base_data(model, date, variable, ensemble_id)
    lat_1d, lon_1d = get_1d_lat_lon(coords)

    max_valid_hour = raw_data.shape[0] - 1 if raw_data.ndim >= 3 else 0
    for h in hour_list:
        if h < 0 or h > 24 or h > max_valid_hour:
            raise CustomError(404, "Resource not found", f"Hour {h} is out of bounds")


    step = 1
    if zoom is not None:
        if zoom <= 3.0: step = 16
        elif zoom <= 5.0: step = 8
        elif zoom <= 7.0: step = 4
        elif zoom <= 9.0: step = 2
        else: step = 1

    lat_1d = lat_1d[::step]
    lon_1d = lon_1d[::step]

    if raw_data.ndim >= 3:
        data_slice = raw_data[hour_list, ::step, ::step]
    else:

        data_slice = np.array([raw_data[::step, ::step]])

    if preview:
        lat_1d = lat_1d[:10]
        lon_1d = lon_1d[:10]
        if data_slice.ndim >= 3:
            data_slice = data_slice[:, :10, :10]
        else:
            data_slice = data_slice[:10, :10]

    flat_values = data_slice.flatten().tolist()

    payload = {
        "date": date,
        "variable": variable,
        "model": model,
        "ensemble_id": ensemble_id,
        "lead_time": hour_list,
        "zoom_applied": zoom,
        "step_used": step,
        "lat": lat_1d,
        "lon": lon_1d,
        "values": flat_values
    }

    response.headers["Cache-Control"] = "public, max-age=3600"
    response.headers["ETag"] = f'W/"{generate_etag(f"sh-{model}-{date}-{variable}-{hours}-{ensemble_id}-z{zoom}")}"'
    return payload


@app.get("/api/{model}/{date}/{variable}")
async def get_all_hours(
        model: Literal["stormcast", "fcn3"],
        date: str,
        variable: str,
        ensemble_id: int = 0,
        zoom: Optional[float] = None,
        preview: bool = False,
        response: Response = None,
):

    if response is None:
        response = Response()

    raw_data, coords = await fetch_base_data(model, date, variable, ensemble_id)
    lat_1d, lon_1d = get_1d_lat_lon(coords)

    max_hours = min(25, raw_data.shape[0] if raw_data.ndim >= 3 else 1)


    step = 1
    if zoom is not None:
        if zoom <= 3.0: step = 16
        elif zoom <= 5.0: step = 8
        elif zoom <= 7.0: step = 4
        elif zoom <= 9.0: step = 2
        else: step = 1

    lat_1d = lat_1d[::step]
    lon_1d = lon_1d[::step]

    if raw_data.ndim >= 3:
        data_slice = raw_data[:max_hours, ::step, ::step]
    else:
        data_slice = raw_data[::step, ::step]

    lead_times = list(range(data_slice.shape[0] if data_slice.ndim >= 3 else 1))

    if preview:
        lat_1d = lat_1d[:10]
        lon_1d = lon_1d[:10]
        lead_times = lead_times[:2]
        if data_slice.ndim >= 3:
            data_slice = data_slice[:2, :10, :10]
        else:
            data_slice = data_slice[:10, :10]

    flat_values = data_slice.flatten().tolist()

    payload = {
        "date": date,
        "variable": variable,
        "model": model,
        "ensemble_id": ensemble_id,
        "lead_time": lead_times,
        "zoom_applied": zoom,
        "step_used": step,
        "lat": lat_1d,
        "lon": lon_1d,
        "values": flat_values
    }

    response.headers["Cache-Control"] = "public, max-age=3600"
    response.headers["ETag"] = f'W/"{generate_etag(f"all-{model}-{date}-{variable}-{ensemble_id}-z{zoom}")}"'
    return payload