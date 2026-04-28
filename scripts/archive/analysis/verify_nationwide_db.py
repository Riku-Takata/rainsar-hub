import os
import sys
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Setup paths
BACKEND_DIR = Path(__file__).resolve().parents[2] / "backend"
sys.path.append(str(BACKEND_DIR))

# Load Env
load_dotenv(BACKEND_DIR / ".env")

DB_USER = os.getenv("DB_USER", "rainsar")
DB_PASSWORD = os.getenv("DB_PASSWORD", "rainsar_pw")
DB_HOST = "127.0.0.1"
DB_PORT = os.getenv("DB_PORT_HOST", "3307")
DB_NAME = os.getenv("DB_NAME", "rainsar_hub")

DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def main():
    print(f"Connecting to {DB_HOST}:{DB_PORT}/{DB_NAME}...")
    engine = create_engine(DATABASE_URL)

    query = text("SELECT COUNT(*) FROM s1_pairs WHERE source = 'cdse_nationwide_search'")
    
    try:
        with engine.connect() as conn:
            count = conn.execute(query).scalar()
            print(f"Total pairs with source='cdse_nationwide_search': {count}")
            
            # Additional verify: count unique grids
            q2 = text("SELECT COUNT(DISTINCT grid_id) FROM s1_pairs WHERE source = 'cdse_nationwide_search'")
            grid_count = conn.execute(q2).scalar()
            print(f"Unique Grids found: {grid_count}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
