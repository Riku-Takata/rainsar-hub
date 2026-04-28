import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

INPUT_CSV = r"D:\sotsuron\rainsar-hub\data\result\visualization\event_parameters.csv"
OUTPUT_DIR = r"D:\sotsuron\rainsar-hub\data\result\visualization"

def analyze_separability():
    if not os.path.exists(INPUT_CSV):
        print("Input CSV not found.")
        return

    df = pd.read_csv(INPUT_CSV)
    
    # Filter for valid data
    df = df.dropna(subset=['paddy_diff_mean', 'road_diff_mean', 'total_precip_mm'])
    
    # Calculate Separability (Road - Paddy)
    # Ideally, if Road rises more than Paddy, this value is positive.
    # The magnitude of this difference represents how "distinct" they are in that event (on average).
    df['separability'] = df['road_diff_mean'] - df['paddy_diff_mean']
    df['abs_separability'] = df['separability'].abs()
    
    # Filter for fresh events (Delay <= 3h) to see immediate rain impact
    fresh_df = df[df['delay_hours'] <= 3]
    
    print(f"Analyzing {len(fresh_df)} events with Delay <= 3h...")
    
    # Correlation Analysis
    cols = ['total_precip_mm', 'max_intensity_mm_h', 'duration_hours', 'separability']
    
    print("\n--- Correlation with Separability (Road - Paddy) ---")
    corr_matrix = fresh_df[cols].corr()
    print(corr_matrix[['separability']])
    
    # Visualization
    plt.figure(figsize=(15, 5))
    
    # 1. Separability vs Total Precip
    plt.subplot(1, 3, 1)
    sns.scatterplot(data=fresh_df, x='total_precip_mm', y='separability', hue='polarization', alpha=0.6)
    plt.axhline(0, color='gray', linestyle='--')
    plt.title('Separability vs Total Precip')
    plt.xlabel('Total Precip (mm)')
    plt.ylabel('Separability (Road - Paddy) [dB]')
    
    # 2. Separability vs Max Intensity
    plt.subplot(1, 3, 2)
    sns.scatterplot(data=fresh_df, x='max_intensity_mm_h', y='separability', hue='polarization', alpha=0.6)
    plt.axhline(0, color='gray', linestyle='--')
    plt.title('Separability vs Max Intensity')
    plt.xlabel('Max Intensity (mm/h)')
    
    # 3. Separability vs Duration
    plt.subplot(1, 3, 3)
    sns.scatterplot(data=fresh_df, x='duration_hours', y='separability', hue='polarization', alpha=0.6)
    plt.axhline(0, color='gray', linestyle='--')
    plt.title('Separability vs Duration')
    plt.xlabel('Duration (h)')
    
    plt.tight_layout()
    plot_path = os.path.join(OUTPUT_DIR, 'separability_vs_rain_params.png')
    plt.savefig(plot_path)
    print(f"Saved plot to {plot_path}")
    
    # Statistical check: significant correlation?
    from scipy.stats import pearsonr
    
    print("\n--- P-values for Correlation ---")
    for col in ['total_precip_mm', 'max_intensity_mm_h', 'duration_hours']:
        r, p = pearsonr(fresh_df[col], fresh_df['separability'])
        print(f"{col}: r={r:.3f}, p={p:.4f}")

if __name__ == "__main__":
    analyze_separability()
