#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
watch_process.py

トリガー({grid_id}___{stem}.req)を監視し、前処理を実行する。
処理中は _status フォルダにステータスJSONを出力し、完了したら削除する。
"""

import time
import sys
import subprocess
import logging
import os
import json
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# パス設定 (Windows環境)
TRIGGER_DIR = Path(r"D:\sotsuron\s1_samples\_triggers")
STATUS_DIR = Path(r"D:\sotsuron\s1_samples\_status")
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
                return
                
            grid_id, zip_stem = filename.split("___", 1)
            zip_filename = f"{zip_stem}.zip"
            
            logging.info(f"[TRIGGER] Grid: {grid_id}, File: {zip_filename}")
            
            self.run_preprocess(grid_id, zip_filename, filename)
            
            # トリガー削除
            try:
                os.remove(path)
            except: pass

        except Exception as e:
            logging.error(f"Error: {e}")

    def update_status(self, base_name: str, status: str, error: str = None):
        """ _status/{base_name}.json を更新 """
        status_file = STATUS_DIR / f"{base_name}.json"
        data = {"status": status, "updated": time.time()}
        if error: data["error"] = str(error)
        
        try:
            with open(status_file, "w") as f:
                json.dump(data, f)
        except Exception as e:
            logging.error(f"Failed to write status: {e}")

    def run_preprocess(self, grid_id: str, zip_filename: str, base_name: str):
        # 処理開始ステータス
        self.update_status(base_name, "processing")

        cmd = [
            sys.executable,
            str(PREPROCESS_SCRIPT),
            "--grid-id", grid_id,
            "--target-file", zip_filename,
            "--in-root", str(SAFE_DIR),
            "--out-root", str(OUTPUT_DIR),
            "--pol", "VH"
        ]
        
        logging.info(f"Running SNAP for {zip_filename}...")
        try:
            subprocess.run(cmd, check=True)
            logging.info("SUCCESS")
            # 成功したらステータスファイルは削除 (processed判定はファイル実体で行うため)
            status_file = STATUS_DIR / f"{base_name}.json"
            if status_file.exists():
                os.remove(status_file)
        except subprocess.CalledProcessError as e:
            logging.error("FAILED")
            self.update_status(base_name, "failed", error=str(e))

if __name__ == "__main__":
    for d in [TRIGGER_DIR, STATUS_DIR]:
        if not d.exists(): d.mkdir(parents=True, exist_ok=True)
    
    logging.info(f"Watching: {TRIGGER_DIR}")
    observer = Observer()
    observer.schedule(TriggerHandler(), str(TRIGGER_DIR), recursive=False)
    observer.start()
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()