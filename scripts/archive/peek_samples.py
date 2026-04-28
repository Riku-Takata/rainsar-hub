import os
from pathlib import Path

path = Path(r"D:\sotsuron\rainsar-hub\data\expanded\samples")
if not path.exists():
    print("Samples dir does not exist.")
else:
    print(f"Items in samples: {len(list(path.iterdir()))}")
    for i, p in enumerate(path.iterdir()):
        if i > 5: break
        print(f"{p.name} (Dir: {p.is_dir()})")
        if p.is_dir():
            print("  Contents:")
            for j, sub in enumerate(p.iterdir()):
                if j > 3: break
                print(f"    {sub.name}")
