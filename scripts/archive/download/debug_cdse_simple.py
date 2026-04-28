"""
Debug CDSE Process API with a simple, single request.
"""
import os
import sys
import json
import requests
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

# Path setup
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
sys.path.append(str(BASE_DIR / "backend"))

# Load Env
load_dotenv(BASE_DIR / "backend/.env")

from app.services.s1_cdse_client import S1CDSEClient

PROCESS_API_URL = "https://sh.dataspace.copernicus.eu/api/v1/process"

# Evalscript
EVALSCRIPT = """
//VERSION=3
function setup() {
  return {
    input: ["VV", "VH", "dataMask"],
    output: [
      { id: "VV", bands: 1, sampleType: "FLOAT32" },
      { id: "VH", bands: 1, sampleType: "FLOAT32" }
    ]
  };
}

function evaluatePixel(sample) {
  return {
    VV: [sample.VV],
    VH: [sample.VH]
  };
}
"""

def main():
    print("--- Starting Debug Request ---")
    
    # 1. Get Token
    print("1. Acquiring Token...")
    client = S1CDSEClient()
    token = client._get_token()
    
    if not token:
        print("ERROR: Failed to get token.")
        return
        
    print(f"Token acquired. Length: {len(token)}")
    print(f"Token prefix: {token[:20]}...")

    # 2. Prepare Payload
    # Target: N03585E14045 (Lat 35.85, Lon 140.45) approx
    # Date: 2022-09-01 (Arbitrary test date, need to ensure coverage? 
    # Let's use a wide range or known event if possible. 
    # Or just try a generic request that should return *something* or empty, but not 400 error.)
    
    # Sentinel-1 passes over Japan every few days.
    # Let's try to query a known existing scene from valid candidates if possible, 
    # but for simple debugging, just a valid request format is enough.
    
    bbox = [140.40, 35.80, 140.50, 35.90]
    time_range = ("2023-01-01T00:00:00Z", "2023-01-14T23:59:59Z")
    
    payload = {
        "input": {
            "bounds": {
                "bbox": bbox,
                "properties": { "crs": "http://www.opengis.net/def/crs/EPSG/0/4326" }
            },
            "data": [
                {
                    "type": "SENTINEL-1-GRD",
                    "dataFilter": {
                        "timeRange": {
                            "from": time_range[0],
                            "to": time_range[1]
                        },
                        "acquisitionMode": "IW",
                        "polarization": "DV",
                        "resolution": "HIGH"
                    },
                    "processing": {
                        "backCoeff": "SIGMA0_ELLIPSOID",
                        "orthorectification": False
                    }
                }
            ]
        },
        "output": {
            "responses": [
                { "identifier": "VV", "format": { "type": "image/tiff" } },
                { "identifier": "VH", "format": { "type": "image/tiff" } }
            ]
        },
        "evalscript": EVALSCRIPT
    }
    
    print("\n2. Sending Request...")
    print("Payload JSON:", json.dumps(payload, indent=2))
    
    # headers = client._auth_headers() # This might be getting OData specific headers?
    # Let's constuct manually to be safe
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/tar"
    }
    
    response = requests.post(PROCESS_API_URL, headers=headers, json=payload, timeout=60)
    
    print(f"\nStatus Code: {response.status_code}")
    print(f"Response Headers: {response.headers}")
    
    if response.status_code == 200:
        print("\nSUCCESS! Content-Length:", len(response.content))
        with open("debug_output.tar", "wb") as f:
            f.write(response.content)
        print("Saved to debug_output.tar")
    else:
        print("\nFAILURE!")
        print("Response Text:")
        try:
            print(json.dumps(response.json(), indent=2))
        except:
            print(response.text)

if __name__ == "__main__":
    main()
