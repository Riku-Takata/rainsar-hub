import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from scipy.stats import linregress

# Config
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data" / "expanded" / "analysis"
CSV_PATH = DATA_DIR / "evolution" / "evolution_data_final.csv"
OUT_DIR = DATA_DIR / "drainage_analysis"

OUT_DIR.mkdir(parents=True, exist_ok=True)

def parse_month(event_str):
    import re
    m = re.search(r"delay_\d+h_(\d{8})", event_str)
    if m:
        date_str = m.group(1)
        return int(date_str[4:6])
    return None

def calculate_drainage_parameters(df):
    """
    Calculate 3-axis drainage parameters for each grid-event:
    1. Decay Rate (時定数): How fast the signal returns to baseline
    2. Saturation Threshold (飽和閾値): Response to heavy rainfall
    3. Seasonal Sensitivity (季節感度): Baseline-corrected responsiveness
    """
    
    # Add month column
    df['month'] = df['event'].apply(parse_month)
    
    # 1. DECAY RATE (時定数): Slope of Road_Diff vs Delay
    # Group by grid and calculate decay rate for each grid
    decay_rates = []
    
    for grid_id in df['grid_id'].unique():
        grid_data = df[df['grid_id'] == grid_id].copy()
        
        # Road Decay
        road_delays = grid_data['delay_h'].values
        road_diffs = grid_data['road_diff_mean'].values
        
        valid_mask = ~np.isnan(road_diffs)
        if valid_mask.sum() >= 3:  # Need at least 3 points
            slope_r, intercept_r, r_r, _, _ = linregress(
                road_delays[valid_mask], 
                road_diffs[valid_mask]
            )
        else:
            slope_r, r_r = np.nan, np.nan
            
        # Paddy Decay
        paddy_diffs = grid_data['paddy_diff_mean'].values
        valid_mask_p = ~np.isnan(paddy_diffs)
        if valid_mask_p.sum() >= 3:
            slope_p, intercept_p, r_p, _, _ = linregress(
                road_delays[valid_mask_p], 
                paddy_diffs[valid_mask_p]
            )
        else:
            slope_p, r_p = np.nan, np.nan
            
        decay_rates.append({
            'grid_id': grid_id,
            'road_decay_rate': slope_r,  # Negative = faster decay
            'paddy_decay_rate': slope_p,
            'decay_contrast': abs(slope_r) - abs(slope_p),  # Higher = Road drains faster
            'road_r2': r_r**2 if not np.isnan(r_r) else np.nan,
            'paddy_r2': r_p**2 if not np.isnan(r_p) else np.nan
        })
    
    df_decay = pd.DataFrame(decay_rates)
    
    # 2. SATURATION THRESHOLD (飽和閾値): Response to heavy rain
    df['rain_cat'] = pd.cut(
        df['rain_total_est_mm'], 
        bins=[0, 20, 50, 200], 
        labels=['Light', 'Moderate', 'Heavy']
    )
    
    saturation = []
    for grid_id in df['grid_id'].unique():
        grid_data = df[df['grid_id'] == grid_id].copy()
        
        # Contrast in Moderate vs Heavy
        mod_contrast = grid_data[grid_data['rain_cat'] == 'Moderate']['road_diff_mean'].mean() - \
                       grid_data[grid_data['rain_cat'] == 'Moderate']['paddy_diff_mean'].mean()
        
        heavy_contrast = grid_data[grid_data['rain_cat'] == 'Heavy']['road_diff_mean'].mean() - \
                         grid_data[grid_data['rain_cat'] == 'Heavy']['paddy_diff_mean'].mean()
        
        saturation.append({
            'grid_id': grid_id,
            'moderate_contrast': mod_contrast,
            'heavy_contrast': heavy_contrast,
            'saturation_index': mod_contrast - heavy_contrast  # Positive = Road saturates under heavy rain
        })
    
    df_saturation = pd.DataFrame(saturation)
    
    # 3. SEASONAL SENSITIVITY (季節感度): April baseline correction
    seasonal = []
    for grid_id in df['grid_id'].unique():
        grid_data = df[df['grid_id'] == grid_id].copy()
        
        # April (Puddling) sensitivity
        april_road = grid_data[grid_data['month'] == 4]['road_diff_mean'].mean()
        april_paddy = grid_data[grid_data['month'] == 4]['paddy_diff_mean'].mean()
        april_sensitivity = april_road - april_paddy
        
        # May (Growing) sensitivity
        may_road = grid_data[grid_data['month'] == 5]['road_diff_mean'].mean()
        may_paddy = grid_data[grid_data['month'] == 5]['paddy_diff_mean'].mean()
        may_sensitivity = may_road - may_paddy
        
        seasonal.append({
            'grid_id': grid_id,
            'april_sensitivity': april_sensitivity,
            'may_sensitivity': may_sensitivity,
            'seasonal_contrast': april_sensitivity - may_sensitivity  # Positive = April is best
        })
    
    df_seasonal = pd.DataFrame(seasonal)
    
    # Merge all parameters
    df_params = df_decay.merge(df_saturation, on='grid_id').merge(df_seasonal, on='grid_id')
    
    return df_params

def create_drainage_score(df_params):
    """
    Create a composite drainage score from the 3-axis parameters.
    Score > 0 indicates Road-like behavior (high drainage)
    Score < 0 indicates Paddy-like behavior (low drainage, water retention)
    """
    
    # Normalize each parameter to [-1, 1] range
    params_to_norm = ['decay_contrast', 'saturation_index', 'april_sensitivity']
    
    for param in params_to_norm:
        values = df_params[param].values
        valid = ~np.isnan(values)
        if valid.sum() > 0:
            # Robust normalization using percentiles
            p25, p75 = np.nanpercentile(values, [25, 75])
            iqr = p75 - p25
            if iqr > 0:
                df_params[f'{param}_norm'] = (values - np.nanmedian(values)) / (1.5 * iqr)
                df_params[f'{param}_norm'] = np.clip(df_params[f'{param}_norm'], -1, 1)
            else:
                df_params[f'{param}_norm'] = 0
    
    # Composite Score (Equal weights for now)
    df_params['drainage_score'] = (
        df_params['decay_contrast_norm'].fillna(0) * 0.4 +  # Time constant (40%)
        df_params['saturation_index_norm'].fillna(0) * 0.3 +  # Saturation (30%)
        df_params['april_sensitivity_norm'].fillna(0) * 0.3   # Seasonal (30%)
    )
    
    return df_params

def visualize_results(df_params, out_dir):
    """
    Generate visualizations of the drainage parameters.
    """
    
    # 1. 3D Scatter Plot
    fig = plt.figure(figsize=(12, 8))
    ax = fig.add_subplot(111, projection='3d')
    
    scatter = ax.scatter(
        df_params['decay_contrast'],
        df_params['saturation_index'],
        df_params['april_sensitivity'],
        c=df_params['drainage_score'],
        cmap='RdYlGn',
        s=50,
        alpha=0.6
    )
    
    ax.set_xlabel('Decay Contrast (Time Constant)', fontsize=10)
    ax.set_ylabel('Saturation Index (Capacity)', fontsize=10)
    ax.set_zlabel('April Sensitivity (Baseline)', fontsize=10)
    ax.set_title('3-Axis Drainage Parameter Space', fontsize=12, fontweight='bold')
    
    cbar = plt.colorbar(scatter, ax=ax, pad=0.1, shrink=0.8)
    cbar.set_label('Drainage Score', fontsize=10)
    
    plt.tight_layout()
    plt.savefig(out_dir / 'drainage_3d_scatter.png', dpi=150)
    plt.close()
    
    # 2. Score Distribution
    fig, axes = plt.subplots(2, 2, figsize=(12, 10))
    
    # Score Histogram
    axes[0, 0].hist(df_params['drainage_score'].dropna(), bins=30, color='steelblue', edgecolor='black')
    axes[0, 0].axvline(0, color='red', linestyle='--', linewidth=2, label='Neutral (0)')
    axes[0, 0].set_xlabel('Drainage Score')
    axes[0, 0].set_ylabel('Frequency')
    axes[0, 0].set_title('Drainage Score Distribution')
    axes[0, 0].legend()
    axes[0, 0].grid(alpha=0.3)
    
    # Decay vs Saturation
    axes[0, 1].scatter(
        df_params['decay_contrast'],
        df_params['saturation_index'],
        c=df_params['drainage_score'],
        cmap='RdYlGn',
        s=50,
        alpha=0.6
    )
    axes[0, 1].axhline(0, color='gray', linestyle=':', alpha=0.5)
    axes[0, 1].axvline(0, color='gray', linestyle=':', alpha=0.5)
    axes[0, 1].set_xlabel('Decay Contrast')
    axes[0, 1].set_ylabel('Saturation Index')
    axes[0, 1].set_title('Time Constant vs Capacity')
    axes[0, 1].grid(alpha=0.3)
    
    # Decay vs April Sensitivity
    axes[1, 0].scatter(
        df_params['decay_contrast'],
        df_params['april_sensitivity'],
        c=df_params['drainage_score'],
        cmap='RdYlGn',
        s=50,
        alpha=0.6
    )
    axes[1, 0].axhline(0, color='gray', linestyle=':', alpha=0.5)
    axes[1, 0].axvline(0, color='gray', linestyle=':', alpha=0.5)
    axes[1, 0].set_xlabel('Decay Contrast')
    axes[1, 0].set_ylabel('April Sensitivity')
    axes[1, 0].set_title('Time Constant vs Seasonal Baseline')
    axes[1, 0].grid(alpha=0.3)
    
    # Saturation vs April
    axes[1, 1].scatter(
        df_params['saturation_index'],
        df_params['april_sensitivity'],
        c=df_params['drainage_score'],
        cmap='RdYlGn',
        s=50,
        alpha=0.6
    )
    axes[1, 1].axhline(0, color='gray', linestyle=':', alpha=0.5)
    axes[1, 1].axvline(0, color='gray', linestyle=':', alpha=0.5)
    axes[1, 1].set_xlabel('Saturation Index')
    axes[1, 1].set_ylabel('April Sensitivity')
    axes[1, 1].set_title('Capacity vs Seasonal Baseline')
    axes[1, 1].grid(alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(out_dir / 'drainage_parameter_matrix.png', dpi=150)
    plt.close()

def generate_summary_report(df_params, out_dir):
    """
    Generate a summary report of the drainage parameters.
    """
    
    with open(out_dir / 'drainage_analysis_report.md', 'w', encoding='utf-8') as f:
        f.write("# 透水特性の定量的評価レポート\n\n")
        
        f.write("## 1. 分析概要\n\n")
        f.write(f"- **対象グリッド数**: {len(df_params)}\n")
        f.write(f"- **評価軸**: 3軸（時定数・飽和閾値・季節感度）\n\n")
        
        f.write("## 2. 排水パラメータの統計\n\n")
        
        stats = df_params[['decay_contrast', 'saturation_index', 'april_sensitivity', 'drainage_score']].describe()
        f.write(stats.to_markdown())
        f.write("\n\n")
        
        f.write("## 3. 判別性能の評価\n\n")
        
        # Score distribution
        positive = (df_params['drainage_score'] > 0).sum()
        negative = (df_params['drainage_score'] < 0).sum()
        neutral = (df_params['drainage_score'] == 0).sum()
        
        f.write(f"- **高排水性（Score > 0）**: {positive}グリッド ({positive/len(df_params)*100:.1f}%)\n")
        f.write(f"- **低排水性（Score < 0）**: {negative}グリッド ({negative/len(df_params)*100:.1f}%)\n")
        f.write(f"- **中立（Score = 0）**: {neutral}グリッド\n\n")
        
        f.write("## 4. 主要な発見\n\n")
        
        # Top 5 high drainage
        top5_high = df_params.nlargest(5, 'drainage_score')[['grid_id', 'drainage_score', 'decay_contrast', 'saturation_index', 'april_sensitivity']]
        f.write("### 高排水性グリッド Top 5\n\n")
        f.write(top5_high.to_markdown(index=False))
        f.write("\n\n")
        
        # Top 5 low drainage
        top5_low = df_params.nsmallest(5, 'drainage_score')[['grid_id', 'drainage_score', 'decay_contrast', 'saturation_index', 'april_sensitivity']]
        f.write("### 低排水性グリッド Top 5\n\n")
        f.write(top5_low.to_markdown(index=False))
        f.write("\n\n")
        
        f.write("## 5. 結論\n\n")
        f.write("- 3軸パラメータ（時定数・飽和閾値・季節感度）を統合することで、各グリッドの透水特性を定量化できました。\n")
        f.write("- Drainage Scoreの分布から、道路的挙動と田んぼ的挙動の分離が可能であることが示唆されます。\n")
        f.write("- 今後、Ground Truthラベル（実際の土地利用）との照合により、判別精度を検証する必要があります。\n")

def main():
    print("Loading data...")
    df = pd.read_csv(CSV_PATH)
    
    print(f"Loaded {len(df)} events from {len(df['grid_id'].unique())} grids.")
    
    print("\nCalculating drainage parameters...")
    df_params = calculate_drainage_parameters(df)
    
    print("\nCreating drainage score...")
    df_params = create_drainage_score(df_params)
    
    # Save results
    out_csv = OUT_DIR / 'drainage_parameters.csv'
    df_params.to_csv(out_csv, index=False)
    print(f"\nSaved parameters to {out_csv}")
    
    print("\nGenerating visualizations...")
    visualize_results(df_params, OUT_DIR)
    
    print("\nGenerating summary report...")
    generate_summary_report(df_params, OUT_DIR)
    
    print(f"\nDone! Results saved to {OUT_DIR}")
    
    # Print quick stats
    print("\n" + "="*60)
    print("QUICK SUMMARY")
    print("="*60)
    print(f"Mean Drainage Score: {df_params['drainage_score'].mean():.3f}")
    print(f"Std Drainage Score: {df_params['drainage_score'].std():.3f}")
    print(f"High Drainage (>0): {(df_params['drainage_score'] > 0).sum()} grids")
    print(f"Low Drainage (<0): {(df_params['drainage_score'] < 0).sum()} grids")
    print("="*60)

if __name__ == "__main__":
    main()
