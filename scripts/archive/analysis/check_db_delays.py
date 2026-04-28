import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

env_path = r"D:\sotsuron\rainsar-hub\backend\.env"
load_dotenv(env_path)

DB_USER = os.getenv("DB_USER", "rainsar")
DB_PASSWORD = os.getenv("DB_PASSWORD", "rainsar_pw")
DB_HOST = "127.0.0.1"
DB_PORT = os.getenv("DB_PORT_HOST", "3307")
DB_NAME = os.getenv("DB_NAME", "rainsar_hub")
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def main():
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            # Check for events with delay < 1.0
            query = text("""
                SELECT count(*) as count_under_1h
                FROM s1_pairs
                WHERE source = 'cdse_nationwide_search'
                AND delay_h < 1.0
                AND delay_h >= 0
                AND max_gauge_mm_h >= 10.0
                AND before_scene_id IS NOT NULL
            """)
            result = conn.execute(query).fetchone()
            print(f"Events with Delay < 1h in DB: {result[0]}")

            # Check min delay
            query_min = text("""
                SELECT min(delay_h) as min_delay
                FROM s1_pairs
                WHERE source = 'cdse_nationwide_search'
                AND max_gauge_mm_h >= 10.0
                AND before_scene_id IS NOT NULL
            """)
            result_min = conn.execute(query_min).fetchone()
            print(f"Minimum Delay in DB: {result_min[0]}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
