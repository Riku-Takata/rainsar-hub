import zipfile
import fiona
import logging
from pathlib import Path

FUDE_DIR = Path("D:/sotsuron/fude-polygon")

def main():
    if not FUDE_DIR.exists():
        print("Fude dir not found")
        return
        
    zips = list(FUDE_DIR.glob("*.zip"))
    if not zips:
        print("No zips found")
        return
        
    target = zips[0]
    print(f"Inspecting {target}...")
    
    with zipfile.ZipFile(target, 'r') as z:
        names = [n for n in z.namelist() if n.lower().endswith('.geojson') or n.lower().endswith('.json')]
        if not names:
            print("No geojson found in zip")
            return
            
        vsi_path = f"/vsizip/{str(target).replace('\\', '/')}/{names[0]}"
        print(f"Opening {vsi_path}...")
        
        try:
            with fiona.open(vsi_path) as src:
                print(f"Schema: {src.schema}")
                print("First 5 features:")
                for i, feat in enumerate(src):
                    if i >= 5: break
                    print(f"Feature {i}: Properties = {feat['properties']}")
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    main()
