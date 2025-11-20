#!/usr/bin/env python3
import logging
from sqlalchemy import text
from app.db.session import SessionLocal

logger = logging.getLogger(__name__)

def main():
    logging.basicConfig(level=logging.INFO)
    logger.info("Resetting s1_pairs table...")
    
    db = SessionLocal()
    try:
        # s1_pairs テーブルの中身を全て削除
        db.execute(text("TRUNCATE TABLE s1_pairs"))
        db.commit()
        logger.info("Successfully truncated s1_pairs table.")
    except Exception as e:
        logger.error(f"Error resetting table: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    main()