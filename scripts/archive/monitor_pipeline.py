"""
RainSAR Thesis Pipeline Monitor
前処理、後方散乱強度分析、差分分析の進捗を一括表示
"""
import sys
import time
from pathlib import Path
import psutil
import datetime

BASE_DIR = Path("d:/sotsuron/rainsar-hub")
DATA_DIR = BASE_DIR / "data"
EXPANDED_DIR = DATA_DIR / "expanded" / "samples"
RESULT_SIGMA_DIR = DATA_DIR / "result" / "sigma"
RESULT_DIFF_DIR = DATA_DIR / "result" / "diff"
GRID_LIST = DATA_DIR / "thesis_grids_with_masks.txt"

def count_files(directory, pattern):
    return len(list(directory.rglob(pattern)))

def get_process_status(script_name):
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if proc.info['cmdline'] and script_name in ' '.join(proc.info['cmdline']):
                return f"RUNNING (PID: {proc.info['pid']})"
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return "STOPPED"

def main():
    while True:
        try:
            # Clear screen (simple way)
            print("\033[H\033[J", end="")
            
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"=== RainSAR Thesis Pipeline Monitor [{now}] ===\n")
            
            # 1. Process Status
            print("--- Process Status ---")
            print(f"Preprocess (01_preprocess_thesis.py):       {get_process_status('01_preprocess_thesis.py')}")
            print(f"Backscatter (analyze_backscatter_thesis.py): {get_process_status('analyze_backscatter_thesis.py')}")
            print(f"Difference (analyze_difference_thesis.py):   {get_process_status('analyze_difference_thesis.py')}")
            print()
            
            # 2. File Statistics
            # Load grid count
            if GRID_LIST.exists():
                with open(GRID_LIST) as f:
                    total_grids = len([l for l in f if l.strip()])
            else:
                total_grids = 300
                
            # Count outputs
            total_tiffs = count_files(EXPANDED_DIR, "*.tif")
            processed_events = total_tiffs // 4
            
            sigma_stats = count_files(RESULT_SIGMA_DIR, "stats.csv")
            diff_stats = count_files(RESULT_DIFF_DIR, "diff_stats.csv")
            
            # Estimate total events (approx 9.7 per grid)
            est_total_events = int(total_grids * 9.7)
            
            print("--- Progress Statistics ---")
            print(f"Target Grids: {total_grids}")
            print(f"Est. Total Events: ~{est_total_events}")
            print()
            
            p_bar = "█" * int(processed_events / est_total_events * 20)
            p_pct = processed_events / est_total_events * 100
            print(f"[Preprocess] {processed_events} events ({p_pct:.1f}%) | {p_bar:<20}")
            
            s_bar = "█" * int(sigma_stats / processed_events * 20) if processed_events > 0 else ""
            s_pct = sigma_stats / processed_events * 100 if processed_events > 0 else 0
            print(f"[Backscatter] {sigma_stats} events ({s_pct:.1f}%) | {s_bar:<20} (of preprocessed)")
            
            d_bar = "█" * int(diff_stats / processed_events * 20) if processed_events > 0 else ""
            d_pct = diff_stats / processed_events * 100 if processed_events > 0 else 0
            print(f"[Difference]  {diff_stats} events ({d_pct:.1f}%) | {d_bar:<20} (of preprocessed)")
            
            print("\nPress Ctrl+C to exit monitor (processes will continue running)")
            
            time.sleep(10)
            
        except KeyboardInterrupt:
            print("\nMonitor stopped.")
            break
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()
