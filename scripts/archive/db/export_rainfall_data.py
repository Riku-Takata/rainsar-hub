import subprocess
import pandas as pd
import io
import os

def run_query(query, output_file):
    """Runs a MySQL query via docker exec and saves the output to a CSV file."""
    command = [
        "docker", "exec", "-i", "rainsarhub-db",
        "mysql", "-u", "rainsar", "-prainsar_pw", "rainsar_hub",
        "-B", "-e", query
    ]
    
    print(f"Running query: {query}")
    try:
        # Run command and capture output
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        
        # MySQL -B output is tab-separated
        tsv_data = result.stdout
        
        # Read into pandas DataFrame
        df = pd.read_csv(io.StringIO(tsv_data), sep='\t')
        
        # Save to CSV
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        df.to_csv(output_file, index=False)
        print(f"Saved {len(df)} rows to {output_file}")
        
    except subprocess.CalledProcessError as e:
        print(f"Error running query: {e}")
        print("Stderr:", e.stderr)
    except Exception as e:
        print(f"An error occurred: {e}")

def main():
    # 1. Export gsmap_events (excluding large JSON blob to avoid potential parsing errors, 
    #    we only need grid_id, start/end/sum for now)
    query_gsmap = "SELECT id, grid_id, start_ts_utc, end_ts_utc, sum_gauge_mm_h, max_gauge_mm_h FROM gsmap_events;"
    output_gsmap = "data/analysis/gsmap_events.csv"
    run_query(query_gsmap, output_gsmap)

    # 2. Export s1_pairs
    query_s1 = "SELECT id, grid_id, event_start_ts_utc, delay_h, after_scene_id, before_scene_id FROM s1_pairs;"
    output_s1 = "data/analysis/s1_pairs.csv"
    run_query(query_s1, output_s1)

if __name__ == "__main__":
    main()
