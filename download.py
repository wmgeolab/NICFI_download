import os
import requests
import geopandas as gpd
from shapely.geometry import shape
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import logging

# Paths and parameters
API_KEY_PATH = "/sciclone/geograd/.keys/NICFI_planet.key"
LOG_DIR = "/sciclone/geograd/satellite_data/NICFI/LOGS"
OUTPUT_DIR = "/sciclone/geograd/satellite_data/NICFI/MX_TX_SOUTHERN_US_BORDER"
GEOJSON_PATH = "/sciclone/geograd/satellite_data/NICFI/NICFI_download/region.geojson"
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
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Function to fetch NICFI mosaics metadata
def fetch_nicfi_mosaics():
    headers = {"Authorization": f"api-key {PLANET_API_KEY}"}
    response = requests.get(NICFI_URL, headers=headers)
    response.raise_for_status()
    mosaics = response.json()["mosaics"]
    return [mosaic for mosaic in mosaics if mosaic["name"].startswith("nicfi")]

# Function to fetch quad download links
def fetch_quad_links(mosaic_id, geojson_geometry):
    headers = {"Authorization": f"api-key {PLANET_API_KEY}"}
    quads_url = f"{NICFI_URL}/{mosaic_id}/quads"
    params = {
        "intersects": geojson_geometry
    }
    response = requests.get(quads_url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()["quads"]

# Function to download a single quad
def download_quad(quad, output_dir):
    download_url = quad["_links"]["download"]
    quad_name = quad["id"]
    filepath = os.path.join(output_dir, f"{quad_name}.tif")
    if not os.path.exists(filepath):
        try:
            response = requests.get(download_url, stream=True)
            response.raise_for_status()
            with open(filepath, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024):
                    f.write(chunk)
            logger.info(f"Downloaded: {filepath}")
        except Exception as e:
            logger.error(f"Failed to download {quad_name}: {e}")
    else:
        logger.info(f"Already downloaded: {filepath}")

# Parallel downloading with progress bar
def download_all_quads(quads, output_dir):
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        with tqdm(total=len(quads)) as pbar:
            for quad in quads:
                future = executor.submit(download_quad, quad, output_dir)
                future.add_done_callback(lambda _: pbar.update())
                futures.append(future)
            for future in futures:
                future.result()

# Main process
def download_nicfi_tiles():
    # Load GeoJSON geometry
    gdf = gpd.read_file(GEOJSON_PATH)
    geojson_geometry = gdf.geometry.unary_union.__geo_interface__

    # Fetch NICFI mosaics
    mosaics = fetch_nicfi_mosaics()
    logger.info(f"Found {len(mosaics)} NICFI mosaics.")

    #For testing
    mosaics = mosaics[0:4]

    # Iterate over mosaics and download tiles
    for mosaic in mosaics:
        mosaic_id = mosaic["id"]
        logger.info(f"Processing mosaic: {mosaic['name']}")
        quads = fetch_quad_links(mosaic_id, geojson_geometry)
        logger.info(f"Found {len(quads)} quads for mosaic {mosaic['name']}")
        download_all_quads(quads, OUTPUT_DIR)

if __name__ == "__main__":
    try:
        download_nicfi_tiles()
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
