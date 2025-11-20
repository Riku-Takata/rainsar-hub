#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
watch_process.py
D:\\sotsuron\\s1_samples\\_triggers フォルダを監視し、
ダウンロード完了を検知して preprocess_s1_cog.py を実行する。
"""

import time
import sys
import subprocess
import logging
import os
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# パス設定
TRIGGER_DIR = Path(r"D:\sotsuron\s1_samples\_triggers")
SAFE_DIR = Path(r"D:\sotsuron\s1_safe")
OUTPUT_DIR = Path(r"D:\sotsuron\s1_samples")
PREPROCESS_SCRIPT = Path(r"D:\sotsuron\rainsar-hub\scripts\preprocess_s1_cog.py")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%H:%M:%S')

class TriggerHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory: return
        path = Path(event.src_path)
        if path.suffix != ".req": return
        
        filename = path.stem # {grid_id}___{zip_stem}
        try:
            if "___" not in filename:
                logging.warning(f"Invalid format: {filename}")
                return
                
            grid_id, zip_stem = filename.split("___", 1)
            zip_filename = f"{zip_stem}.zip"
            
            logging.info(f"[TRIGGER] Grid: {grid_id}, File: {zip_filename}")
            self.run_preprocess(grid_id, zip_filename)
            
            # 完了後トリガー削除
            try:
                os.remove(path)
            except Exception as e:
                logging.error(f"Failed to delete trigger: {e}")

        except Exception as e:
            logging.error(f"Error: {e}")

    def run_preprocess(self, grid_id: str, zip_filename: str):
        cmd = [
            sys.executable,
            str(PREPROCESS_SCRIPT),
            "--grid-id", grid_id,
            "--target-file", zip_filename,
            "--in-root", str(SAFE_DIR),
            "--out-root", str(OUTPUT_DIR),
            "--pol", "VH"
        ]
        logging.info("Running SNAP...")
        try:
            subprocess.run(cmd, check=True)
            logging.info("SUCCESS")
        except subprocess.CalledProcessError:
            logging.error("FAILED")

if __name__ == "__main__":
    if not TRIGGER_DIR.exists():
        TRIGGER_DIR.mkdir(parents=True, exist_ok=True)
    
    logging.info(f"Watching: {TRIGGER_DIR}")
    observer = Observer()
    observer.schedule(TriggerHandler(), str(TRIGGER_DIR), recursive=False)
    observer.start()
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()