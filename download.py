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
import time
from requests.exceptions import RequestException


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
import time

def fetch_nicfi_mosaics():
    headers = {"Authorization": f"api-key {PLANET_API_KEY}"}
    mosaics = []
    next_page_url = NICFI_URL
    retries = 5  # Number of retries for network issues
    delay = 10   # Delay between retries (seconds)

    try:
        while next_page_url:
            for attempt in range(retries):
                try:
                    response = requests.get(next_page_url, headers=headers)
                    response.raise_for_status()
                    response_json = response.json()

                    mosaics += [mosaic for mosaic in response_json.get("mosaics", [])
                                if mosaic["name"].startswith("planet_medres_normalized")]

                    next_page_url = response_json["_links"].get("_next")
                    break  # Exit retry loop if successful
                except requests.exceptions.RequestException as e:
                    if attempt < retries - 1:
                        logger.warning(f"Retrying request to {next_page_url} after error: {e}")
                        time.sleep(delay)
                    else:
                        logger.error(f"Max retries reached for {next_page_url}: {e}")
                        raise

        logger.info(f"Total mosaics found: {len(mosaics)}")
        return mosaics
    except Exception as e:
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
    seen_quads = set([(mosaic_id, quad["id"]) for quad in all_quads])

    max_retries = 5  # Maximum retries for transient errors
    retry_delay = 2  # Initial delay between retries
    retry_multiplier = 2  # Exponential backoff multiplier

    try:
        while quads_url:
            retries = 0
            while retries < max_retries:
                try:
                    response = requests.get(quads_url, headers=headers, params=params, timeout=30)
                    response.raise_for_status()
                    data = response.json()
                    break  # Exit retry loop if successful
                except RequestException as e:
                    retries += 1
                    logger.warning(f"Request failed (attempt {retries}/{max_retries}) for URL: {quads_url} - {e}")
                    time.sleep(retry_delay)
                    retry_delay *= retry_multiplier  # Exponential backoff
                    if retries == max_retries:
                        logger.error(f"Skipping problematic page: {quads_url} after {max_retries} retries.")
                        return all_quads  # Return quads collected so far
            
            for quad in data.get("items", []):
                quad_id = quad["id"]
                unique_id = (mosaic_id, quad_id)
                download_url = quad["_links"].get("download")

                if unique_id not in seen_quads and download_url:
                    all_quads.append({
                        "mosaic_id": mosaic_id,
                        "id": quad_id,
                        "bbox": quad["bbox"],
                        "percent_covered": quad["percent_covered"],
                        "download_url": download_url
                    })
                    seen_quads.add(unique_id)

            quads_url = data["_links"].get("_next", None)
            params = None

        quad_cache[mosaic_id] = all_quads
        save_cache()
        logger.info(f"Total unique quads found for mosaic {mosaic_id}: {len(all_quads)}")
        return all_quads
    except Exception as e:
        logger.error(f"Failed to fetch quads for mosaic {mosaic_id}: {e}")
        raise


# Function to download a single quad
def download_quad(quad, output_dir):
    # Validate that the quad contains the required fields
    download_url = quad.get("download_url")
    mosaic_name = quad.get("mosaic_id", "unknown_mosaic")
    quad_id = quad.get("id", "unknown_id")

    if not isinstance(download_url, str):
        logger.error(f"Invalid download URL for quad {quad_id} in mosaic {mosaic_name}: {download_url}")
        return

    # Ensure unique file naming with mosaic ID and quad ID
    quad_name = f"{mosaic_name}_{quad_id}.tif"
    filepath = os.path.join(output_dir, quad_name)

    if os.path.exists(filepath):
        logger.info(f"Already downloaded: {filepath}")
        return

    # Attempt to download the quad
    try:
        response = requests.get(download_url, stream=True, timeout=30)  # Added timeout
        response.raise_for_status()
        with open(filepath, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024):
                f.write(chunk)
        logger.info(f"Downloaded: {filepath}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download quad {quad_id} from {download_url}: {e}")



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
