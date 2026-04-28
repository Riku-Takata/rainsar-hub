import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from sklearn.metrics import confusion_matrix, accuracy_score, classification_report
from scipy.stats import linregress
import rasterio
import re

# Config
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"
EVO_CSV = DATA_DIR / "expanded" / "analysis" / "evolution" / "evolution_data_combined.csv"  # UPDATED: use consolidated metadata
OUT_DIR = DATA_DIR / "expanded" / "analysis" / "permeability_classification"

# Sample directories (in priority order)
EXPANDED_SAMPLES = DATA_DIR / "expanded" / "samples"
FINAL_SAMPLES = DATA_DIR / "final" / "samples"
VV_SAMPLES = BASE_DIR / "data_vv" / "samples"  # NEW: for new 100 grids

OUT_DIR.mkdir(parents=True, exist_ok=True)

print("="*80)
print("PERMEABILITY-BASED PIXEL CLASSIFICATION")
print("Comparing Multi-Temporal vs Single-Event Approaches")
print("="*80)

def parse_month(event_str):
    m = re.search(r"delay_\d+h_(\d{8})", event_str)
    if m:
        date_str = m.group(1)
        return int(date_str[4:6])
    return None

def load_pixel_data(grid_id, event_name):
    """
    Load pixel-level data for a specific event.
    Returns: dict with arrays (after, before, diff, road_mask, paddy_mask)
    """
    # Search in all sample directories
    for samples_dir in [EXPANDED_SAMPLES, FINAL_SAMPLES, VV_SAMPLES]:
        event_path = samples_dir / grid_id / event_name
        
        if not event_path.exists():
            continue
        
        paths = {
            'after': event_path / 'after.tif',
            'before': event_path / 'before.tif',
            'road': event_path / 'mask_road.tif',
            'paddy': event_path / 'mask_paddy.tif'
        }
        
        if not all(p.exists() for p in paths.values()):
            continue
        
        try:
            data = {}
            for key, path in paths.items():
                with rasterio.open(path) as src:
                    data[key] = src.read(1)
            
            # Ensure all arrays have the same shape (crop to minimum)
            shapes = [arr.shape for arr in data.values()]
            min_h = min(s[0] for s in shapes)
            min_w = min(s[1] for s in shapes)
            
            for key in data.keys():
                data[key] = data[key][:min_h, :min_w]
            
            data['diff'] = data['after'] - data['before']
            
            # Valid pixels
            valid = (~np.isnan(data['diff']) & 
                    (data['after'] > -50) & 
                    (data['before'] > -50) & 
                    (data['after'] < 20))
            
            data['valid'] = valid
            
            return data
            
        except Exception as e:
            print(f"Error loading {grid_id}/{event_name}: {e}")
            continue
    
    return None

# ============================================================================
# METHOD A: MULTI-TEMPORAL PERMEABILITY ESTIMATION
# ============================================================================

def method_a_multitemporal(grid_id, df_events, n_pixels_sample=5000):
    """
    Method A: Multi-temporal permeability estimation.
    For each pixel, use multiple events to calculate drainage parameters.
    """
    
    print(f"\n  [Method A] Processing {grid_id}...")
    
    # Get all events for this grid
    grid_events = df_events[df_events['grid_id'] == grid_id].copy()
    
    if len(grid_events) < 3:
        print(f"    Skipped: Only {len(grid_events)} events (need >=3)")
        return None
    
    # Load all event data
    event_data = []
    for _, row in grid_events.iterrows():
        data = load_pixel_data(grid_id, row['event'])
        if data is not None:
            event_data.append({
                'data': data,
                'delay': row['delay_h'],
                'rain_intensity': row['rain_max_mm_h'],
                'rain_total': row['rain_total_est_mm'],
                'month': parse_month(row['event'])
            })
    
    if len(event_data) < 3:
        print(f"    Skipped: Only {len(event_data)} valid event data")
        return None
    
    # Get minimum common shape across all events
    all_shapes = [ev['data']['diff'].shape for ev in event_data]
    min_h = min(s[0] for s in all_shapes)
    min_w = min(s[1] for s in all_shapes)
    
    ref_shape = (min_h, min_w)
    
    # Sample pixels (Relaxed strategy)
    # Instead of checking all pixels for 'valid_in_all' (too strict),
    # we randomly sample candidate pixels and check if they have enough valid events.
    
    height, width = ref_shape
    n_candidates = n_pixels_sample * 5  # Sample more candidates
    
    candidate_indices = np.random.choice(height * width, n_candidates, replace=False)
    candidate_pixels = [(idx // width, idx % width) for idx in candidate_indices]
    
    sampled_pixels = []
    
    for i, j in candidate_pixels:
        valid_events_count = sum(ev['data']['valid'][i, j] for ev in event_data)
        if valid_events_count >= 3:
            sampled_pixels.append((i, j))
            if len(sampled_pixels) >= n_pixels_sample:
                break
                
    if len(sampled_pixels) == 0:
        print(f"    WARNING: No pixels found with >=3 valid events")
        return None
    
    print(f"    Sampled {len(sampled_pixels)} pixels (from {len(candidate_pixels)} candidates)")
    
    # Calculate permeability parameters for each pixel
    results = []
    
    for i, j in sampled_pixels:
        # Collect time series for this pixel
        pixel_series = []
        for ev in event_data:
            # ONLY use valid events for this pixel
            if ev['data']['valid'][i, j]:
                pixel_series.append({
                    'diff': ev['data']['diff'][i, j],
                    'before': ev['data']['before'][i, j],
                    'after': ev['data']['after'][i, j],
                    'delay': ev['delay'],
                    'rain_intensity': ev['rain_intensity'],
                    'rain_total': ev['rain_total'],
                    'month': ev['month']
                })
        
        # Calculate drainage parameters
        delays = np.array([p['delay'] for p in pixel_series])
        diffs = np.array([p['diff'] for p in pixel_series])
        rain_totals = np.array([p['rain_total'] for p in pixel_series if not np.isnan(p['rain_total'])])
        months = [p['month'] for p in pixel_series if p['month'] is not None]
        
        # 1. Decay rate (time constant) WITH R² VALUE
        decay_r2 = np.nan
        if len(delays) >= 3 and np.std(delays) > 0:
            slope, _, r, _, _ = linregress(delays, diffs)
            decay_rate = -slope  # Positive = faster decay
            decay_r2 = r ** 2  # ★ NEW: R² value for reliability assessment
        else:
            decay_rate = np.nan
        
        # 2. Saturation response WITH DIVERSITY FLAGS
        has_moderate_rain = False
        has_heavy_rain = False
        
        if len(rain_totals) >= 2:
            moderate_mask = (rain_totals >= 20) & (rain_totals < 50)
            heavy_mask = rain_totals >= 50
            
            has_moderate_rain = moderate_mask.sum() > 0  # ★ NEW
            has_heavy_rain = heavy_mask.sum() > 0  # ★ NEW
            
            if moderate_mask.sum() > 0 and heavy_mask.sum() > 0:
                moderate_diff = np.mean([pixel_series[i]['diff'] for i in range(len(pixel_series)) 
                                        if not np.isnan(pixel_series[i]['rain_total']) and 
                                        20 <= pixel_series[i]['rain_total'] < 50])
                heavy_diff = np.mean([pixel_series[i]['diff'] for i in range(len(pixel_series)) 
                                     if not np.isnan(pixel_series[i]['rain_total']) and 
                                     pixel_series[i]['rain_total'] >= 50])
                saturation_response = moderate_diff - heavy_diff
            else:
                saturation_response = np.nan
        else:
            saturation_response = np.nan
        
        # 3. April sensitivity WITH MONTH TRACKING
        april_events = [p for p in pixel_series if p['month'] == 4]
        other_events = [p for p in pixel_series if p['month'] != 4 and p['month'] is not None]
        march_events = [p for p in pixel_series if p['month'] == 3]  # ★ NEW
        
        has_april = len(april_events) > 0  # ★ NEW
        has_march = len(march_events) > 0  # ★ NEW
        
        if len(april_events) > 0 and len(other_events) > 0:
            april_diff = np.mean([p['diff'] for p in april_events])
            other_diff = np.mean([p['diff'] for p in other_events])
            april_sensitivity = other_diff - april_diff  # Positive = April shows less change
        else:
            april_sensitivity = np.nan
        
        # Drainage score (normalized combination)
        valid_params = []
        if not np.isnan(decay_rate):
            valid_params.append(decay_rate)
        if not np.isnan(saturation_response):
            valid_params.append(saturation_response * 0.5)  # Scale down
        if not np.isnan(april_sensitivity):
            valid_params.append(april_sensitivity * 0.5)  # Scale down
        
        if len(valid_params) > 0:
            drainage_score = np.mean(valid_params)
        else:
            drainage_score = np.nan
        
        # Ground truth
        ref_event = event_data[0]['data']
        is_road = ref_event['road'][i, j] == 1
        is_paddy = ref_event['paddy'][i, j] == 1
        
        if is_road:
            ground_truth = 1
        elif is_paddy:
            ground_truth = 0
        else:
            ground_truth = np.nan
        
        # Prediction
        if not np.isnan(drainage_score):
            predicted = 1 if drainage_score > 0 else 0
        else:
            predicted = np.nan
        
        # ★ NEW: Collect month string for seasonal filtering
        months_str = ','.join([str(m) for m in sorted(set(months)) if m is not None])
        
        results.append({
            'pixel_i': i,
            'pixel_j': j,
            'decay_rate': decay_rate,
            'decay_r2': decay_r2,  # ★ NEW: Reliability of decay estimate
            'saturation_response': saturation_response,
            'april_sensitivity': april_sensitivity,
            'drainage_score': drainage_score,
            'ground_truth': ground_truth,
            'predicted': predicted,
            'n_events': len(pixel_series),
            'months': months_str,  # ★ NEW: e.g., "3,4,5,7"
            'has_march': has_march,  # ★ NEW: For paddy dry season
            'has_april': has_april,  # ★ NEW: For paddy dry season
            'has_moderate_rain': has_moderate_rain,  # ★ NEW: Rainfall diversity
            'has_heavy_rain': has_heavy_rain  # ★ NEW: Rainfall diversity
        })
    
    return pd.DataFrame(results)

# ============================================================================
# METHOD B: SINGLE-EVENT PERMEABILITY PROXY
# ============================================================================

def method_b_single_event(grid_id, event_name, df_events):
    """
    Method B: Single-event permeability proxy.
    Use rainfall intensity, season, and absolute values as proxies.
    """
    
    print(f"\n  [Method B] Processing {grid_id}/{event_name}...")
    
    # Get event metadata
    event_info = df_events[(df_events['grid_id'] == grid_id) & 
                          (df_events['event'] == event_name)]
    
    if len(event_info) == 0:
        return None
    
    event_info = event_info.iloc[0]
    
    # Load pixel data
    data = load_pixel_data(grid_id, event_name)
    
    if data is None:
        return None
    
    # Extract features
    diff = data['diff']
    before = data['before']
    after = data['after']
    valid = data['valid']
    
    delay = event_info['delay_h']
    rain_intensity = event_info['rain_max_mm_h']
    rain_total = event_info.get('rain_total_est_mm', np.nan)
    month = parse_month(event_name)
    
    # Calculate permeability proxies for each pixel
    results = []
    
    # Sample pixels
    valid_coords = np.argwhere(valid & ((data['road'] == 1) | (data['paddy'] == 1)))
    
    if len(valid_coords) > 10000:
        indices = np.random.choice(len(valid_coords), 10000, replace=False)
        valid_coords = valid_coords[indices]
    
    print(f"    Processing {len(valid_coords)} pixels...")
    
    for i, j in valid_coords:
        pixel_diff = diff[i, j]
        pixel_before = before[i, j]
        pixel_after = after[i, j]
        
        # Feature engineering for permeability proxies
        
        # 1. Time-constant proxy: Diff adjusted by delay
        # Road should show high diff at short delay, low diff at long delay
        decay_proxy = pixel_diff / (delay + 1)  # Normalize by delay
        
        # 2. Saturation proxy: Response to rainfall intensity
        # Road shows moderate response to moderate rain, less to heavy rain
        if not np.isnan(rain_total):
            if 20 <= rain_total < 50:
                saturation_proxy = pixel_diff * 1.5  # Boost moderate rain response
            elif rain_total >= 50:
                saturation_proxy = pixel_diff * 0.5  # Penalize heavy rain response
            else:
                saturation_proxy = pixel_diff
        else:
            saturation_proxy = pixel_diff
        
        # 3. Seasonal proxy: Response in April vs other months
        if month == 4:
            # In April, road should still show change, paddy should not
            seasonal_proxy = pixel_diff * 2.0  # Boost April diff
        else:
            seasonal_proxy = pixel_diff
        
        # 4. Baseline proxy: Dry before state indicates road
        baseline_proxy = -pixel_before  # More negative before = drier = road-like
        
        # Combine proxies into permeability score
        permeability_score = (
            decay_proxy * 0.3 +
            saturation_proxy * 0.3 +
            seasonal_proxy * 0.2 +
            baseline_proxy * 0.2
        )
        
        # Ground truth
        is_road = data['road'][i, j] == 1
        is_paddy = data['paddy'][i, j] == 1
        
        if is_road:
            ground_truth = 1
        elif is_paddy:
            ground_truth = 0
        else:
            continue  # Skip unlabeled pixels
        
        # Prediction (threshold = 0)
        predicted = 1 if permeability_score > 0 else 0
        
        results.append({
            'pixel_i': i,
            'pixel_j': j,
            'decay_proxy': decay_proxy,
            'saturation_proxy': saturation_proxy,
            'seasonal_proxy': seasonal_proxy,
            'baseline_proxy': baseline_proxy,
            'permeability_score': permeability_score,
            'ground_truth': ground_truth,
            'predicted': predicted
        })
    
    return pd.DataFrame(results)

# ============================================================================
# COMPARISON AND EVALUATION
# ============================================================================

def evaluate_method(df_results, method_name):
    """
    Evaluate classification performance.
    """
    
    df_valid = df_results.dropna(subset=['ground_truth', 'predicted']).copy()
    
    if len(df_valid) == 0:
        print(f"\n{method_name}: No valid predictions")
        return None
    
    y_true = df_valid['ground_truth'].astype(int).values
    y_pred = df_valid['predicted'].astype(int).values
    
    accuracy = accuracy_score(y_true, y_pred)
    cm = confusion_matrix(y_true, y_pred)
    report = classification_report(y_true, y_pred, target_names=['Paddy', 'Road'], 
                                   output_dict=True, zero_division=0)
    
    print(f"\n{method_name} Results:")
    print(f"  Samples: {len(df_valid)}")
    print(f"  Accuracy: {accuracy:.3f}")
    print(f"  Road Precision: {report['Road']['precision']:.3f}")
    print(f"  Road Recall: {report['Road']['recall']:.3f}")
    print(f"  Paddy Precision: {report['Paddy']['precision']:.3f}")
    print(f"  Paddy Recall: {report['Paddy']['recall']:.3f}")
    
    return {
        'method': method_name,
        'n_samples': len(df_valid),
        'accuracy': accuracy,
        'road_precision': report['Road']['precision'],
        'road_recall': report['Road']['recall'],
        'paddy_precision': report['Paddy']['precision'],
        'paddy_recall': report['Paddy']['recall'],
        'confusion_matrix': cm
    }

def visualize_comparison(results_a, results_b):
    """
    Visualize comparison between methods.
    """
    
    if results_a is None or results_b is None:
        print("Cannot visualize: Missing results")
        return
    
    fig, axes = plt.subplots(2, 2, figsize=(14, 12))
    
    # Accuracy comparison
    methods = ['Method A\n(Multi-Temporal)', 'Method B\n(Single-Event)']
    accuracies = [results_a['accuracy'], results_b['accuracy']]
    colors = ['steelblue', 'darkorange']
    
    axes[0, 0].bar(methods, accuracies, color=colors, alpha=0.7, edgecolor='black')
    axes[0, 0].set_ylabel('Accuracy', fontsize=12)
    axes[0, 0].set_title('Overall Accuracy Comparison', fontsize=13, fontweight='bold')
    axes[0, 0].set_ylim([0, 1])
    axes[0, 0].grid(alpha=0.3, axis='y')
    
    for i, v in enumerate(accuracies):
        axes[0, 0].text(i, v + 0.02, f'{v:.3f}', ha='center', fontsize=11, fontweight='bold')
    
    # Precision-Recall comparison
    metrics = ['Road\nPrecision', 'Road\nRecall', 'Paddy\nPrecision', 'Paddy\nRecall']
    values_a = [results_a['road_precision'], results_a['road_recall'], 
                results_a['paddy_precision'], results_a['paddy_recall']]
    values_b = [results_b['road_precision'], results_b['road_recall'], 
                results_b['paddy_precision'], results_b['paddy_recall']]
    
    x = np.arange(len(metrics))
    width = 0.35
    
    axes[0, 1].bar(x - width/2, values_a, width, label='Method A', color='steelblue', alpha=0.7)
    axes[0, 1].bar(x + width/2, values_b, width, label='Method B', color='darkorange', alpha=0.7)
    
    axes[0, 1].set_ylabel('Score', fontsize=12)
    axes[0, 1].set_title('Precision and Recall by Class', fontsize=13, fontweight='bold')
    axes[0, 1].set_xticks(x)
    axes[0, 1].set_xticklabels(metrics)
    axes[0, 1].legend()
    axes[0, 1].set_ylim([0, 1])
    axes[0, 1].grid(alpha=0.3, axis='y')
    
    # Confusion matrices
    for idx, (result, title, ax) in enumerate([
        (results_a, 'Method A: Multi-Temporal', axes[1, 0]),
        (results_b, 'Method B: Single-Event', axes[1, 1])
    ]):
        sns.heatmap(result['confusion_matrix'], annot=True, fmt='d', cmap='Blues',
                   xticklabels=['Paddy', 'Road'],
                   yticklabels=['Paddy', 'Road'],
                   ax=ax, cbar_kws={'label': 'Count'})
        ax.set_xlabel('Predicted', fontsize=11)
        ax.set_ylabel('True', fontsize=11)
        ax.set_title(title, fontsize=12, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(OUT_DIR / 'method_comparison.png', dpi=150)
    plt.close()
    
    print(f"\nSaved: {OUT_DIR / 'method_comparison.png'}")

def main():
    # Load evolution data
    print("\nLoading evolution data...")
    df_events = pd.read_csv(EVO_CSV)
    print(f"Loaded {len(df_events)} events from {df_events['grid_id'].nunique()} grids")
    
    # Select sample grids with sufficient events
    grid_counts = df_events.groupby('grid_id').size()
    suitable_grids = grid_counts[grid_counts >= 5].index.tolist()
    
    print(f"\nFound {len(suitable_grids)} grids with >=5 events")
    
    # Use ALL available grids (no sampling)
    sample_grids = suitable_grids
    
    print(f"Processing ALL {len(sample_grids)} grids for comprehensive analysis...")
    
    # Method A: Multi-temporal
    print("\n" + "="*80)
    print("METHOD A: MULTI-TEMPORAL PERMEABILITY ESTIMATION")
    print("="*80)
    
    results_a_list = []
    for grid_id in sample_grids:
        result = method_a_multitemporal(grid_id, df_events, n_pixels_sample=5000)
        if result is not None:
            results_a_list.append(result)
    
    if results_a_list:
        df_results_a = pd.concat(results_a_list, ignore_index=True)
        df_results_a.to_csv(OUT_DIR / 'results_method_a.csv', index=False)
        print(f"\nMethod A: Processed {len(df_results_a)} pixels")
    else:
        df_results_a = None
        print("\nMethod A: No results")
    
    # Method B: Single-event
    print("\n" + "="*80)
    print("METHOD B: SINGLE-EVENT PERMEABILITY PROXY")
    print("="*80)
    
    results_b_list = []
    for grid_id in sample_grids:
        grid_events = df_events[df_events['grid_id'] == grid_id]
        # Sample 2 events per grid
        sample_events = grid_events.sample(n=min(2, len(grid_events)))
        
        for _, event_info in sample_events.iterrows():
            result = method_b_single_event(grid_id, event_info['event'], df_events)
            if result is not None:
                results_b_list.append(result)
    
    if results_b_list:
        df_results_b = pd.concat(results_b_list, ignore_index=True)
        df_results_b.to_csv(OUT_DIR / 'results_method_b.csv', index=False)
        print(f"\nMethod B: Processed {len(df_results_b)} pixels")
    else:
        df_results_b = None
        print("\nMethod B: No results")
    
    # Evaluation
    print("\n" + "="*80)
    print("EVALUATION AND COMPARISON")
    print("="*80)
    
    eval_a = evaluate_method(df_results_a, "Method A (Multi-Temporal)") if df_results_a is not None else None
    eval_b = evaluate_method(df_results_b, "Method B (Single-Event)") if df_results_b is not None else None
    
    # Visualization
    if eval_a and eval_b:
        visualize_comparison(eval_a, eval_b)
        
        # Generate comparison report
        with open(OUT_DIR / 'comparison_report.md', 'w', encoding='utf-8') as f:
            f.write("# 透水性ベースピクセル判別の比較レポート\n\n")
            
            f.write("## Method A: Multi-Temporal Permeability Estimation\n\n")
            f.write("- **手法**: 複数イベントから透水パラメータ（減衰率・飽和応答・季節感度）を算出\n")
            f.write(f"- **サンプル数**: {eval_a['n_samples']} pixels\n")
            f.write(f"- **Overall Accuracy**: {eval_a['accuracy']:.3f}\n")
            f.write(f"- **Road Precision/Recall**: {eval_a['road_precision']:.3f} / {eval_a['road_recall']:.3f}\n")
            f.write(f"- **Paddy Precision/Recall**: {eval_a['paddy_precision']:.3f} / {eval_a['paddy_recall']:.3f}\n\n")
            
            f.write("## Method B: Single-Event Permeability Proxy\n\n")
            f.write("- **手法**: 降雨強度・時期・絶対値から即時透水性を推定\n")
            f.write(f"- **サンプル数**: {eval_b['n_samples']} pixels\n")
            f.write(f"- **Overall Accuracy**: {eval_b['accuracy']:.3f}\n")
            f.write(f"- **Road Precision/Recall**: {eval_b['road_precision']:.3f} / {eval_b['road_recall']:.3f}\n")
            f.write(f"- **Paddy Precision/Recall**: {eval_b['paddy_precision']:.3f} / {eval_b['paddy_recall']:.3f}\n\n")
            
            f.write("## 結論\n\n")
            
            if eval_a['accuracy'] > eval_b['accuracy']:
                winner = "Method A (Multi-Temporal)"
                diff = eval_a['accuracy'] - eval_b['accuracy']
            else:
                winner = "Method B (Single-Event)"
                diff = eval_b['accuracy'] - eval_a['accuracy']
            
            f.write(f"- **精度勝者**: {winner} (+{diff:.3f})\n")
            f.write("- **実用性**: Method Bは単一イベントで推定可能（計算コスト低）\n")
            f.write("- **理論性**: Method Aは透水性を直接推定（物理的妥当性高）\n")
        
        print(f"\nSaved: {OUT_DIR / 'comparison_report.md'}")
    
    print("\n" + "="*80)
    print("ANALYSIS COMPLETE")
    print("="*80)
    print(f"\nResults saved to: {OUT_DIR}")

if __name__ == "__main__":
    main()
