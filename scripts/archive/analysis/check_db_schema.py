import os
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

try:
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        print("Connected.")
        # Describe table
        try:
            result = conn.execute(text("DESCRIBE gsmap"))
            print("\nColumns in gsmap:")
            for row in result:
                print(row)
        except Exception as e:
            print(f"gsmap table error: {e}")
            
        # Also check s1_pairs just in case
        result = conn.execute(text("DESCRIBE s1_pairs"))
        print("\nColumns in s1_pairs:")
        for row in result:
            print(row)

except Exception as e:
    print(f"Error: {e}")
