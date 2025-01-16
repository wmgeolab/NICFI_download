import os
import requests
import geopandas as gpd
from shapely.geometry import shape
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import logging
import json
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


TEST_MODE = False
TEST_MOSAIC_NAME = "planet_medres_normalized_analytic_2020-11_mosaic"

# Paths and parameters
API_KEY_PATH = "/sciclone/geograd/.keys/NICFI_planet.key"
LOG_DIR = "/sciclone/geograd/satellite_data/NICFI/LOGS"
OUTPUT_DIR = "/sciclone/geograd/satellite_data/NICFI/MX_TX_SOUTHERN_US_BORDER"
GEOJSON_PATH = "/sciclone/geograd/satellite_data/NICFI/NICFI_download/region.geojson"
CACHE_FILE = os.path.join(LOG_DIR, "quad_cache.json")
NICFI_URL = "https://api.planet.com/basemaps/v1/mosaics"

# Load Planet API key
with open(API_KEY_PATH, "r") as f:
    PLANET_API_KEY = f.read().strip()

# Logging setup
os.makedirs(LOG_DIR, exist_ok=True)
log_file = os.path.join(LOG_DIR, "nicfi_download.log")
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(processName)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Load or initialize cache
if os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, "r") as cache_file:
        quad_cache = json.load(cache_file)
else:
    quad_cache = {}

# Function to save cache
def save_cache():
    with open(CACHE_FILE, "w") as cache_file:
        json.dump(quad_cache, cache_file)

# Function to fetch NICFI mosaics metadata
def fetch_nicfi_mosaics():
    headers = {"Authorization": f"api-key {PLANET_API_KEY}"}
    mosaics = []
    next_page_url = NICFI_URL

    try:
        while next_page_url:
            response = requests.get(next_page_url, headers=headers)
            response.raise_for_status()
            response_json = response.json()

            mosaics += [mosaic for mosaic in response_json.get("mosaics", [])
                        if mosaic["name"].startswith("planet_medres_normalized")]

            next_page_url = response_json["_links"].get("_next")

        logger.info(f"Total mosaics found: {len(mosaics)}")
        return mosaics
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch mosaics: {e}")
        raise

# Function to fetch quad download links with caching
def fetch_quad_links(mosaic_id, bbox):
    headers = {"Authorization": f"api-key {PLANET_API_KEY}"}
    quads_url = f"{NICFI_URL}/{mosaic_id}/quads"
    bbox_str = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"
    logger.info(f"Requesting quads for mosaic {mosaic_id} with bbox: {bbox_str}")

    params = {"bbox": bbox_str, "_page_size": 50}
    all_quads = quad_cache.get(mosaic_id, [])

    try:
        # Track already cached quads for uniqueness
        seen_quads = set([(mosaic_id, quad["id"]) for quad in all_quads])
        
        while quads_url:
            response = requests.get(quads_url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()

            for quad in data.get("items", []):
                quad_id = quad["id"]
                unique_id = (mosaic_id, quad_id)  # Combine mosaic_id and quad_id
                download_url = quad["_links"].get("download")

                if unique_id not in seen_quads and download_url:
                    # Append complete quad metadata to ensure uniqueness
                    all_quads.append({
                        "mosaic_id": mosaic_id,  # Include mosaic_id explicitly
                        "id": quad_id,
                        "bbox": quad["bbox"],
                        "percent_covered": quad["percent_covered"],
                        "download_url": download_url
                    })
                    seen_quads.add(unique_id)

            quads_url = data["_links"].get("_next", None)
            params = None  # Clear params for subsequent paginated requests

        quad_cache[mosaic_id] = all_quads  # Cache results for this mosaic
        save_cache()

        logger.info(f"Total unique quads found for mosaic {mosaic_id}: {len(all_quads)}")
        return all_quads
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch quads for mosaic {mosaic_id}: {e}")
        raise


# Function to download a single quad
def download_quad(download_url, output_dir):
    quad_name = download_url.split("/")[-2]
    filepath = os.path.join(output_dir, f"{quad_name}.tif")
    if os.path.exists(filepath):
        logger.info(f"Already downloaded: {filepath}")
        return

    try:
        response = requests.get(download_url, stream=True)
        response.raise_for_status()
        with open(filepath, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024):
                f.write(chunk)
        logger.info(f"Downloaded: {filepath}")
    except Exception as e:
        logger.error(f"Failed to download {quad_name}: {e}")

# Parallel downloading with progress bar
def download_all_quads(quads, mosaic_name, base_output_dir):
    mosaic_output_dir = os.path.join(base_output_dir, mosaic_name)
    os.makedirs(mosaic_output_dir, exist_ok=True)  # Ensure mosaic-specific folder exists
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        with tqdm(total=len(quads)) as pbar:
            for quad_url in quads:
                future = executor.submit(download_quad, quad_url, mosaic_output_dir)
                future.add_done_callback(lambda _: pbar.update())
                futures.append(future)
            for future in futures:
                future.result()

# Main process
def download_nicfi_tiles():
    gdf = gpd.read_file(GEOJSON_PATH, engine="pyogrio")
    geojson_geometry = gdf.geometry.unary_union
    bbox = geojson_geometry.bounds

    logger.info(f"Computed bounding box: {bbox}")
    mosaics = fetch_nicfi_mosaics()
    logger.info(f"Found {len(mosaics)} NICFI mosaics.")

    for mosaic in mosaics:
        mosaic_id = mosaic["id"]
        mosaic_name = mosaic["name"]

        if TEST_MODE and mosaic_name != TEST_MOSAIC_NAME:
            continue

        logger.info(f"Processing mosaic: {mosaic_name}")
        try:
            quads = fetch_quad_links(mosaic_id, bbox)
            logger.info(f"Found {len(quads)} quads for mosaic {mosaic_name}")
            download_all_quads(quads, mosaic_name, OUTPUT_DIR)  # Pass mosaic name and base directory
        except Exception as e:
            logger.error(f"Failed to process mosaic {mosaic_name}: {e}")

if __name__ == "__main__":
    try:
        download_nicfi_tiles()
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
