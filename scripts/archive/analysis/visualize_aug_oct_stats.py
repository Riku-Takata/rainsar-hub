import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path

# Paths
DATA_DIR = Path(r"D:\sotsuron\rainsar-hub\data\analysis")
INPUT_CSV = DATA_DIR / "monthly_delay_pixel_counts_detailed.csv"
OUTPUT_PIXEL_PLOT = DATA_DIR / "aug_oct_pixel_counts.png"
OUTPUT_GRID_PLOT = DATA_DIR / "aug_oct_grid_distribution.png"

def main():
    print("Starting visualization...")
    
    if not INPUT_CSV.exists():
        print(f"Error: {INPUT_CSV} not found.")
        return
        
    df = pd.read_csv(INPUT_CSV)
    
    # Filter Aug (8) and Oct (10)
    df = df[df['month'].isin([8, 10])]
    
    # --- 1. Pixel Counts Visualization ---
    # Aggregate pixels by delay and month
    pixel_agg = df.groupby(['month', 'delay_int'])[['road_pixels', 'paddy_pixels']].sum().reset_index()
    
    # Melt for plotting
    pixel_melt = pixel_agg.melt(id_vars=['month', 'delay_int'], 
                                value_vars=['road_pixels', 'paddy_pixels'],
                                var_name='Type', value_name='Count')
    
    # Rename for display
    pixel_melt['Type'] = pixel_melt['Type'].replace({'road_pixels': '道路 (Road)', 'paddy_pixels': '田んぼ (Paddy)'})
    
    # Ensure full range 0-11
    full_idx = pd.MultiIndex.from_product([[8, 10], range(12)], names=['month', 'delay_int'])
    
    # Set Japanese Font
    plt.rcParams['font.family'] = 'Meiryo'
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 6), sharey=False) # Separate Y scales might be better as counts differ
    
    colors = {'道路 (Road)': 'salmon', '田んぼ (Paddy)': 'lightgreen'}
    
    for i, month in enumerate([8, 10]):
        ax = axes[i]
        month_data = pixel_melt[pixel_melt['month'] == month]
        
        # Ensure range
        # (Using a trick to show 0s: merge with full range or let seaborn handle it if data is missing, 
        # but better to show 0 explicitly)
        
        sns.barplot(data=month_data, x='delay_int', y='Count', hue='Type', ax=ax, palette=colors, edgecolor='black')
        
        ax.set_title(f"{month}月: 種類別ピクセル数", fontsize=14)
        ax.set_xlabel("経過時間 (時間)", fontsize=12)
        ax.set_ylabel("ピクセル数", fontsize=12)
        ax.grid(axis='y', linestyle='--', alpha=0.7)
        ax.legend(title=None)
        
        # Format Y axis with commas
        import matplotlib.ticker as ticker
        ax.get_yaxis().set_major_formatter(ticker.FuncFormatter(lambda x, p: format(int(x), ',')))

    plt.tight_layout()
    plt.savefig(OUTPUT_PIXEL_PLOT, dpi=300)
    print(f"Saved pixel plot to {OUTPUT_PIXEL_PLOT}")
    
    # --- 2. Grid Distribution Visualization ---
    # Count unique grids per delay and month
    grid_agg = df.groupby(['month', 'delay_int'])['grid_id'].nunique().reset_index(name='unique_grids')
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 6), sharey=True)
    
    for i, month in enumerate([8, 10]):
        ax = axes[i]
        month_data = grid_agg[grid_agg['month'] == month]
        
        # Pad with 0 for missing delays
        full_df = pd.DataFrame({'delay_int': range(12)})
        month_data = full_df.merge(month_data, on='delay_int', how='left').fillna(0)
        
        sns.barplot(data=month_data, x='delay_int', y='unique_grids', ax=ax, color='skyblue', edgecolor='black')
        
        ax.set_title(f"{month}月: 経過時間別グリッド数", fontsize=14)
        ax.set_xlabel("経過時間 (時間)", fontsize=12)
        ax.set_ylabel("グリッド数 (箇所)", fontsize=12)
        ax.grid(axis='y', linestyle='--', alpha=0.7)
        
        # Add values on top
        for index, row in month_data.iterrows():
            if row['unique_grids'] > 0:
                ax.text(index, row['unique_grids'] + 0.1, str(int(row['unique_grids'])), ha='center', fontsize=10)

    plt.tight_layout()
    plt.savefig(OUTPUT_GRID_PLOT, dpi=300)
    print(f"Saved grid plot to {OUTPUT_GRID_PLOT}")

if __name__ == "__main__":
    main()
