
import pandas as pd
from pathlib import Path

# Path setup
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
EXISTING_CSV = BASE_DIR / "data/result/vv/diff/all_events_diff_vv.csv"

def main():
    if not EXISTING_CSV.exists():
        print(f"Error: {EXISTING_CSV} not found.")
        return

    print(f"Loading {EXISTING_CSV}...")
    df = pd.read_csv(EXISTING_CSV)
    
    # event_name format: delay_10.0h_20240801
    # Extract date and delay
    
    def parse_event(event_name):
        try:
            parts = event_name.split('_')
            # delay
            delay_str = parts[1].replace('h', '')
            delay = float(delay_str)
            
            # date
            date_str = parts[2]
            year = int(date_str[:4])
            month = int(date_str[4:6])
            
            return delay, month
        except:
            return None, None

    # Apply parsing
    df[['delay_h', 'month']] = df['event_name'].apply(lambda x: pd.Series(parse_event(x)))
    
    # Filter for August (month == 8)
    df_aug = df[df['month'] == 8].copy()
    
    print(f"\nTotal Events in CSV: {len(df)}")
    print(f"August Events in CSV: {len(df_aug)}")
    
    if len(df_aug) == 0:
        return

    # Distribution by Delay (1h bins)
    bins = list(range(0, 13))
    labels = [f"{i}-{i+1}h" for i in range(0, 12)]
    df_aug['Delay_Bin'] = pd.cut(df_aug['delay_h'], bins=bins, labels=labels, include_lowest=True)
    
    summary = df_aug['Delay_Bin'].value_counts().sort_index()
    
    print("\n--- Existing August Data Distribution (1h bins) ---")
    print(summary)
    
    # Show detail for 7-8h if any
    missing_range = df_aug[(df_aug['delay_h'] >= 7) & (df_aug['delay_h'] <= 8)]
    if not missing_range.empty:
        print("\nEvents in 7-8h range:")
        print(missing_range[['grid_id', 'event_name', 'delay_h']])
    else:
        print("\nNo events found in 7-8h range.")

if __name__ == "__main__":
    main()
