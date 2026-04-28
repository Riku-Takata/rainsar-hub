import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Config
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
RIVER_CSV = BASE_DIR / "data/result/River_vs_Road_Aug/metrics_aug_refined.csv"
PADDY_CSV = BASE_DIR / "data/analysis/rf_classification_metrics.csv"
OUTPUT_DIR = BASE_DIR / "data/result/distribution_analysis"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ARTIFACT_DIR = Path(r"C:\Users\riku_\.gemini\antigravity\brain\b329ce41-a43c-48c1-b77b-c2a6700a3f1f")

# Baselines (Before-Before Stability Accuracy)
BASELINE_RIVER = 0.751
BASELINE_PADDY = 0.594  # Based on previous analysis (~60%)

# Font setup
plt.rcParams['font.family'] = ['Meiryo', 'MS Gothic', 'sans-serif']

def calculate_slope_to_baseline(df, class_name, baseline):
    """
    Find Peak, Find point close to Baseline (or Last point), Calculate Slope.
    Slope = (Acc_end - Acc_peak) / (Time_end - Time_peak)
    """
    # Sort by delay
    df = df.sort_values('delay')
    
    # 1. Find Peak
    peak_idx = df['accuracy'].idxmax()
    peak_row = df.loc[peak_idx]
    peak_acc = peak_row['accuracy']
    peak_time = peak_row['delay']
    
    # 2. Find End Point (Convergence)
    # We look for the first point after peak where accuracy drops near baseline + tolerance?
    # Or just use the last point if it doesn't drop enough?
    # User said: "compare with baseline... analyse data point close to baseline in graph"
    # Let's find the point closest to baseline *after* peak.
    
    after_peak = df[df['delay'] > peak_time]
    
    if after_peak.empty:
        return None
        
    # Find point closest to baseline (assuming monotonic-ish decrease)
    # Actually, we want the point where it *reaches* baseline levels.
    # Let's pick the point with minimum absolute difference to baseline
    # But strictly speaking, recovery means Acc -> Baseline.
    
    # Let's use the LAST point for robust slope if it hasn't fully converged,
    # OR the first point that touches baseline range.
    
    # Strategy: Find first point where Acc <= Baseline + 0.05 (5% margin)
    # If not found, use last point.
    
    target_acc = baseline + 0.05
    candidates = after_peak[after_peak['accuracy'] <= target_acc]
    
    if not candidates.empty:
        end_row = candidates.iloc[0] # First point to hit baseline range
        status = "Baseline Reached"
    else:
        end_row = after_peak.iloc[-1] # Last point available
        status = "End of Data"
        
    end_acc = end_row['accuracy']
    end_time = end_row['delay']
    
    slope = (end_acc - peak_acc) / (end_time - peak_time)
    
    return {
        'class': class_name,
        'peak_time': peak_time,
        'peak_acc': peak_acc,
        'end_time': end_time,
        'end_acc': end_acc,
        'slope': slope,
        'status': status,
        'baseline': baseline
    }

def main():
    print("=== Analyzing Recovery Slopes (River vs Paddy) ===")
    
    # 1. Load River Data (Aug)
    # columns: delay, accuracy, n_samples
    df_river = pd.read_csv(RIVER_CSV)
    # Ensure columns match
    if 'delay' not in df_river.columns and 'delay_int' in df_river.columns:
        df_river = df_river.rename(columns={'delay_int': 'delay'})
        
    # Filter only Aug (metrics_aug_refined is already Aug)
    # Sort
    df_river = df_river.sort_values('delay')
    
    # 2. Load Paddy Data (Oct)
    # columns: month, delay_int, accuracy...
    df_paddy_raw = pd.read_csv(PADDY_CSV)
    df_paddy = df_paddy_raw[df_paddy_raw['month'] == 10].copy()
    df_paddy = df_paddy.rename(columns={'delay_int': 'delay'})
    df_paddy = df_paddy.sort_values('delay')
    
    # 3. Analyze Slopes
    res_river = calculate_slope_to_baseline(df_river, '河川 (8月)', BASELINE_RIVER)
    res_paddy = calculate_slope_to_baseline(df_paddy, '水田 (10月)', BASELINE_PADDY)
    
    results = [r for r in [res_river, res_paddy] if r is not None]
    
    # 4. Visualization
    plt.figure(figsize=(10, 7))
    
    # Plot River
    plt.plot(df_river['delay'], df_river['accuracy'], 'o-', label='河川 (8月)', color='tab:blue')
    plt.axhline(BASELINE_RIVER, color='tab:blue', linestyle='--', alpha=0.5, label='河川 ベースライン (75.1%)')
    
    # Plot Paddy
    plt.plot(df_paddy['delay'], df_paddy['accuracy'], 's-', label='水田 (10月)', color='tab:green')
    plt.axhline(BASELINE_PADDY, color='tab:green', linestyle='--', alpha=0.5, label='水田 ベースライン (59.4%)')
    
    # Draw Slopes
    for res in results:
        p_t, p_a = res['peak_time'], res['peak_acc']
        e_t, e_a = res['end_time'], res['end_acc']
        color = 'blue' if '河川' in res['class'] else 'green'
        
        # Arrow
        plt.annotate('', xy=(e_t, e_a), xytext=(p_t, p_a),
                     arrowprops=dict(arrowstyle="->", color=color, lw=2, linestyle='-'))
        
        # Text
        mid_t = (p_t + e_t) / 2
        mid_a = (p_a + e_a) / 2
        plt.text(mid_t, mid_a + 0.02, f"傾き: {res['slope']:.4f}/h", color=color, fontweight='bold', ha='center')
        print(f"{res['class']}: Peak({p_t}h, {p_a:.3f}) -> End({e_t}h, {e_a:.3f}) | Slope = {res['slope']:.4f}")

    plt.title("河川 vs 水田: 精度回復過程の傾き分析", fontsize=16)
    plt.xlabel("降雨後経過時間 (h)", fontsize=14)
    plt.ylabel("分類精度 (Accuracy)", fontsize=14)
    plt.legend()
    plt.grid(True)
    
    out_path = OUTPUT_DIR / "recovery_slope_analysis.png"
    plt.savefig(out_path, dpi=300)
    plt.savefig(ARTIFACT_DIR / "recovery_slope_analysis.png", dpi=300)
    plt.close()
    print(f"Saved analysis plot to {out_path}")

if __name__ == "__main__":
    main()
