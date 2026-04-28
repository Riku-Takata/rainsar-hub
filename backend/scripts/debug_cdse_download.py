#!/usr/bin/env python3
import requests
import json
from app.core.config import settings

def debug():
    data = {
        'grant_type': 'client_credentials',
        'client_id': settings.CDSE_CLIENT_ID,
        'client_secret': settings.CDSE_CLIENT_SECRET,
    }
    print(f"Requesting token for Client ID: {settings.CDSE_CLIENT_ID[:5]}...")
    resp = requests.post(settings.CDSE_TOKEN_URL, data=data)
    if not resp.ok:
        print("TOKEN FAILED:", resp.text)
        return
    token = resp.json().get('access_token')
    headers = {"Authorization": f"Bearer {token}"}
    print("Token fetched successfully.")

    # Search for an arbitrary product
    url_meta = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
    params = {"$top": 1, "$filter": "contains(Name, 'S1A_IW_GRDH')"}
    print("Fetching product UUID...")
    r_meta = requests.get(url_meta, headers=headers, params=params)
    
    if r_meta.status_code != 200:
        print("MetaData Fetch Failed:", r_meta.status_code, r_meta.text)
        return
        
    rj = r_meta.json()
    if 'value' not in rj or len(rj['value']) == 0:
        print("No product found.")
        return
        
    uuid = rj['value'][0]['Id']
    print(f"Found Product UUID: {uuid}")
    
    # Try catalogue download url
    url_dl = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products({uuid})/$value"
    print(f"Trying download via {url_dl}")
    
    r_dl = requests.get(url_dl, headers=headers)
    print("Status Code:", r_dl.status_code)
    try:
        err_json = r_dl.json()
        print("Error JSON Response:")
        print(json.dumps(err_json, indent=2))
    except Exception:
        print("Error Text Response:")
        print(r_dl.text[:500])

if __name__ == "__main__":
    debug()
