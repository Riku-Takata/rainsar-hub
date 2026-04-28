
import os
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pathlib import Path

BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
env_path = BASE_DIR / "backend" / ".env"
load_dotenv(env_path)

DB_USER = os.getenv("DB_USER", "rainsar")
DB_PASSWORD = os.getenv("DB_PASSWORD", "rainsar_pw")
DB_HOST = "127.0.0.1"
DB_PORT = os.getenv("DB_PORT_HOST", "3307")
DB_NAME = os.getenv("DB_NAME", "rainsar_hub")
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def main():
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        print("--- s1_pairs columns ---")
        try:
            res = conn.execute(text("DESCRIBE s1_pairs")).fetchall()
            for r in res:
                print(r[0])
        except Exception as e:
            print(e)
            
        print("\n--- gsmap_events columns ---")
        try:
            res = conn.execute(text("DESCRIBE gsmap_events")).fetchall()
            for r in res:
                print(r[0])
        except Exception as e:
            print(e)

if __name__ == "__main__":
    main()
