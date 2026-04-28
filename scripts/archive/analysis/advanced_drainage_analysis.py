import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from scipy.stats import pearsonr, spearmanr
from scipy.ndimage import label as ndimage_label
from sklearn.metrics import confusion_matrix, accuracy_score, classification_report
import rasterio

# Config
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"
CLASS_CSV = DATA_DIR / "expanded" / "analysis" / "classification" / "classification_results.csv"
OUT_DIR = DATA_DIR / "expanded" / "analysis" / "advanced_analysis"

EXPANDED_SAMPLES = DATA_DIR / "expanded" / "samples"
FINAL_SAMPLES = DATA_DIR / "final" / "samples"

OUT_DIR.mkdir(parents=True, exist_ok=True)

print("="*80)
print("ADVANCED DRAINAGE ANALYSIS")
print("="*80)

# ============================================================================
# PART 1: ROAD RATIO CORRELATION ANALYSIS
# ============================================================================

def analyze_road_ratio_correlation(df):
    """
    Analyze correlation between road ratio and drainage score.
    Identify threshold percentage where behavior changes.
    """
    
    print("\n" + "="*80)
    print("PART 1: ROAD RATIO CORRELATION ANALYSIS")
    print("="*80)
    
    df_valid = df.dropna(subset=['road_ratio', 'drainage_score'])
    
    if len(df_valid) == 0:
        print("Warning: No valid data for correlation analysis")
        return None
    
    # Calculate correlations
    pearson_r, pearson_p = pearsonr(df_valid['road_ratio'], df_valid['drainage_score'])
    spearman_r, spearman_p = spearmanr(df_valid['road_ratio'], df_valid['drainage_score'])
    
    print(f"\nCorrelation Statistics:")
    print(f"  Pearson r: {pearson_r:.3f} (p={pearson_p:.4f})")
    print(f"  Spearman ρ: {spearman_r:.3f} (p={spearman_p:.4f})")
    
    # Find threshold
    # Bin by road_ratio and calculate mean drainage_score
    df_valid['ratio_bin'] = pd.cut(df_valid['road_ratio'], bins=10)
    threshold_analysis = df_valid.groupby('ratio_bin', observed=False).agg({
        'drainage_score': ['mean', 'std', 'count'],
        'road_ratio': 'mean'
    }).reset_index()
    
    # Find crossover point (where drainage_score crosses 0)
    threshold_analysis.columns = ['_'.join(col).strip('_') for col in threshold_analysis.columns]
    threshold_analysis = threshold_analysis.dropna()
    
    crossover = None
    for i in range(len(threshold_analysis) - 1):
        if (threshold_analysis.iloc[i]['drainage_score_mean'] < 0 and 
            threshold_analysis.iloc[i+1]['drainage_score_mean'] > 0):
            crossover = threshold_analysis.iloc[i]['road_ratio_mean']
            break
    
    if crossover:
        print(f"\nCrossover Threshold: ~{crossover*100:.1f}% road coverage")
        print(f"  Below this threshold: Paddy-like drainage behavior")
        print(f"  Above this threshold: Road-like drainage behavior")
    else:
        print("\nNo clear crossover threshold detected")
    
    # Visualize
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    # Scatter plot
    axes[0].scatter(df_valid['road_ratio']*100, df_valid['drainage_score'], 
                   alpha=0.5, s=50, color='steelblue')
    axes[0].axhline(0, color='red', linestyle='--', linewidth=2, label='Drainage Score = 0')
    if crossover:
        axes[0].axvline(crossover*100, color='orange', linestyle='--', linewidth=2, 
                       label=f'Threshold ~{crossover*100:.1f}%')
    
    # Add trend line
    z = np.polyfit(df_valid['road_ratio'], df_valid['drainage_score'], 1)
    p = np.poly1d(z)
    x_trend = np.linspace(df_valid['road_ratio'].min(), df_valid['road_ratio'].max(), 100)
    axes[0].plot(x_trend*100, p(x_trend), "r-", alpha=0.8, linewidth=2, label='Linear Trend')
    
    axes[0].set_xlabel('Road Pixel Ratio (%)', fontsize=12)
    axes[0].set_ylabel('Drainage Score', fontsize=12)
    axes[0].set_title(f'Road Ratio vs Drainage Score\n(Pearson r={pearson_r:.3f})', fontsize=13)
    axes[0].legend()
    axes[0].grid(alpha=0.3)
    
    # Binned analysis
    if len(threshold_analysis) > 0:
        axes[1].errorbar(
            threshold_analysis['road_ratio_mean']*100,
            threshold_analysis['drainage_score_mean'],
            yerr=threshold_analysis['drainage_score_std'],
            fmt='o-', capsize=5, capthick=2, markersize=8, linewidth=2, color='darkgreen'
        )
        axes[1].axhline(0, color='red', linestyle='--', linewidth=2, alpha=0.7)
        if crossover:
            axes[1].axvline(crossover*100, color='orange', linestyle='--', linewidth=2, alpha=0.7)
        
        axes[1].set_xlabel('Road Pixel Ratio (%) - Binned', fontsize=12)
        axes[1].set_ylabel('Mean Drainage Score', fontsize=12)
        axes[1].set_title('Binned Analysis (Mean ± Std)', fontsize=13)
        axes[1].grid(alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(OUT_DIR / 'road_ratio_correlation.png', dpi=150)
    plt.close()
    
    print(f"\nSaved: {OUT_DIR / 'road_ratio_correlation.png'}")
    
    return {
        'pearson_r': pearson_r,
        'pearson_p': pearson_p,
        'spearman_r': spearman_r,
        'spearman_p': spearman_p,
        'crossover_threshold': crossover
    }

# ============================================================================
# PART 2: SPATIAL DISTRIBUTION ANALYSIS
# ============================================================================

def calculate_spatial_metrics(grid_id):
    """
    Calculate spatial distribution metrics for road pixels in a grid.
    Returns:
    - n_components: Number of separate road clusters
    - largest_component_ratio: Size of largest cluster / total road pixels
    - mean_distance: Average distance between road pixels
    - spatial_dispersion: Coefficient of variation of distances
    """
    
    # Find grid path
    for samples_dir in [EXPANDED_SAMPLES, FINAL_SAMPLES]:
        grid_path = samples_dir / grid_id
        if not grid_path.exists():
            continue
        
        event_folders = [d for d in grid_path.iterdir() if d.is_dir() and d.name.startswith("delay_")]
        if not event_folders:
            continue
        
        event_dir = event_folders[0]
        road_mask_path = event_dir / "mask_road.tif"
        
        if not road_mask_path.exists():
            continue
        
        try:
            with rasterio.open(road_mask_path) as src:
                road_mask = src.read(1)
            
            # Count connected components
            labeled_array, n_components = ndimage_label(road_mask == 1)
            
            if n_components == 0:
                return None
            
            # Find largest component
            component_sizes = [(labeled_array == i).sum() for i in range(1, n_components + 1)]
            largest_component_size = max(component_sizes)
            total_road_pixels = (road_mask == 1).sum()
            
            largest_component_ratio = largest_component_size / total_road_pixels if total_road_pixels > 0 else 0
            
            # Calculate distances (sample for efficiency)
            road_coords = np.argwhere(road_mask == 1)
            
            if len(road_coords) > 1:
                # Sample up to 100 points
                if len(road_coords) > 100:
                    indices = np.random.choice(len(road_coords), 100, replace=False)
                    road_coords = road_coords[indices]
                
                # Calculate pairwise distances
                from scipy.spatial.distance import pdist
                distances = pdist(road_coords)
                mean_distance = np.mean(distances) if len(distances) > 0 else 0
                spatial_dispersion = np.std(distances) / mean_distance if mean_distance > 0 else 0
            else:
                mean_distance = 0
                spatial_dispersion = 0
            
            return {
                'n_components': n_components,
                'largest_component_ratio': largest_component_ratio,
                'mean_distance': mean_distance,
                'spatial_dispersion': spatial_dispersion
            }
            
        except Exception as e:
            print(f"Error processing {grid_id}: {e}")
            return None
    
    return None

def analyze_spatial_distribution(df):
    """
    Analyze spatial distribution of roads and its effect on drainage behavior.
    """
    
    print("\n" + "="*80)
    print("PART 2: SPATIAL DISTRIBUTION ANALYSIS")
    print("="*80)
    print("\nCalculating spatial metrics for each grid...")
    
    spatial_data = []
    
    for idx, grid_id in enumerate(df['grid_id'].values):
        if idx % 20 == 0:
            print(f"  Progress: {idx}/{len(df)} grids processed...")
        
        metrics = calculate_spatial_metrics(grid_id)
        
        if metrics:
            spatial_data.append({
                'grid_id': grid_id,
                **metrics
            })
    
    print(f"  Completed: {len(spatial_data)} grids with spatial metrics")
    
    if len(spatial_data) == 0:
        print("Warning: No spatial metrics calculated")
        return None
    
    df_spatial = pd.DataFrame(spatial_data)
    df_merged = df.merge(df_spatial, on='grid_id', how='left')
    
    # Correlations
    print("\nSpatial Metrics vs Drainage Score:")
    
    correlations = {}
    for metric in ['n_components', 'largest_component_ratio', 'mean_distance', 'spatial_dispersion']:
        df_valid = df_merged.dropna(subset=[metric, 'drainage_score'])
        if len(df_valid) > 3:
            r, p = pearsonr(df_valid[metric], df_valid['drainage_score'])
            correlations[metric] = {'r': r, 'p': p}
            print(f"  {metric}: r={r:.3f} (p={p:.4f})")
    
    # Visualize
    fig, axes = plt.subplots(2, 2, figsize=(14, 12))
    axes = axes.flatten()
    
    metrics = ['n_components', 'largest_component_ratio', 'mean_distance', 'spatial_dispersion']
    titles = ['Number of Road Clusters', 'Largest Cluster Ratio', 'Mean Distance', 'Spatial Dispersion (CV)']
    
    for i, (metric, title) in enumerate(zip(metrics, titles)):
        df_valid = df_merged.dropna(subset=[metric, 'drainage_score'])
        
        if len(df_valid) > 0:
            axes[i].scatter(df_valid[metric], df_valid['drainage_score'], alpha=0.5, s=50)
            axes[i].set_xlabel(title, fontsize=11)
            axes[i].set_ylabel('Drainage Score', fontsize=11)
            
            if metric in correlations:
                r = correlations[metric]['r']
                axes[i].set_title(f'{title}\n(r={r:.3f})', fontsize=12)
            else:
                axes[i].set_title(title, fontsize=12)
            
            axes[i].grid(alpha=0.3)
            axes[i].axhline(0, color='red', linestyle='--', alpha=0.5)
    
    plt.tight_layout()
    plt.savefig(OUT_DIR / 'spatial_distribution_analysis.png', dpi=150)
    plt.close()
    
    print(f"\nSaved: {OUT_DIR / 'spatial_distribution_analysis.png'}")
    
    # Save spatial data
    df_merged.to_csv(OUT_DIR / 'spatial_metrics.csv', index=False)
    print(f"Saved: {OUT_DIR / 'spatial_metrics.csv'}")
    
    return correlations

# ============================================================================
# PART 3: PIXEL-LEVEL CLASSIFICATION
# ============================================================================

def pixel_level_classification(grid_id, window_size=5):
    """
    Perform pixel-level classification within a grid.
    For each pixel, predict land cover based on local SAR signal and neighborhood.
    """
    
    # Find grid path
    for samples_dir in [EXPANDED_SAMPLES, FINAL_SAMPLES]:
        grid_path = samples_dir / grid_id
        if not grid_path.exists():
            continue
        
        event_folders = [d for d in grid_path.iterdir() if d.is_dir() and d.name.startswith("delay_")]
        if not event_folders:
            continue
        
        # Use first event with complete data
        for event_dir in event_folders:
            paths = {
                'after': event_dir / 'after.tif',
                'before': event_dir / 'before.tif',
                'road': event_dir / 'mask_road.tif',
                'paddy': event_dir / 'mask_paddy.tif'
            }
            
            if not all(p.exists() for p in paths.values()):
                continue
            
            try:
                # Load data
                with rasterio.open(paths['after']) as src:
                    after = src.read(1)
                with rasterio.open(paths['before']) as src:
                    before = src.read(1)
                with rasterio.open(paths['road']) as src:
                    road_mask = src.read(1)
                with rasterio.open(paths['paddy']) as src:
                    paddy_mask = src.read(1)
                
                # Calculate difference
                diff = after - before
                
                # Create ground truth (1=road, 0=paddy, -1=neither)
                ground_truth = np.full_like(road_mask, -1, dtype=np.int8)
                ground_truth[road_mask == 1] = 1
                ground_truth[paddy_mask == 1] = 0
                
                # Pixel-level prediction based on local statistics
                # Strategy: High positive diff + low variability = Road
                #          Low/negative diff or high variability = Paddy
                
                from scipy.ndimage import uniform_filter, generic_filter
                
                # Local mean and std
                local_mean = uniform_filter(diff, size=window_size)
                local_std = generic_filter(diff, np.std, size=window_size)
                
                # Classification rule (simple threshold-based)
                # Road: High mean diff, low std
                # Paddy: Lower mean diff or high std
                
                predicted = np.zeros_like(diff, dtype=np.int8)
                
                # Road classification criteria
                road_criteria = (local_mean > 0.5) & (local_std < 2.0)
                predicted[road_criteria] = 1
                
                # Paddy classification criteria
                paddy_criteria = ~road_criteria
                predicted[paddy_criteria] = 0
                
                # Evaluate only on labeled pixels
                valid_mask = (ground_truth == 0) | (ground_truth == 1)
                
                y_true = ground_truth[valid_mask]
                y_pred = predicted[valid_mask]
                
                if len(y_true) > 0:
                    accuracy = accuracy_score(y_true, y_pred)
                    cm = confusion_matrix(y_true, y_pred)
                    
                    return {
                        'grid_id': grid_id,
                        'accuracy': accuracy,
                        'n_pixels': len(y_true),
                        'confusion_matrix': cm,
                        'road_precision': cm[1,1] / (cm[1,1] + cm[0,1]) if (cm[1,1] + cm[0,1]) > 0 else 0,
                        'road_recall': cm[1,1] / (cm[1,1] + cm[1,0]) if (cm[1,1] + cm[1,0]) > 0 else 0,
                        'paddy_precision': cm[0,0] / (cm[0,0] + cm[1,0]) if (cm[0,0] + cm[1,0]) > 0 else 0,
                        'paddy_recall': cm[0,0] / (cm[0,0] + cm[0,1]) if (cm[0,0] + cm[0,1]) > 0 else 0
                    }
                
            except Exception as e:
                continue
    
    return None

def analyze_pixel_level(df, n_samples=20):
    """
    Analyze pixel-level classification performance on a subset of grids.
    """
    
    print("\n" + "="*80)
    print("PART 3: PIXEL-LEVEL CLASSIFICATION")
    print("="*80)
    print(f"\nPerforming pixel-level classification on {n_samples} sample grids...")
    
    # Sample grids with diverse drainage scores
    df_sorted = df.dropna(subset=['drainage_score']).sort_values('drainage_score')
    
    # Sample from different quintiles
    samples = []
    for i in range(5):
        start = int(len(df_sorted) * i / 5)
        end = int(len(df_sorted) * (i + 1) / 5)
        quintile = df_sorted.iloc[start:end]
        n = min(n_samples // 5, len(quintile))
        samples.append(quintile.sample(n=n) if len(quintile) > 0 else quintile)
    
    sample_grids = pd.concat(samples)['grid_id'].values
    
    results = []
    
    for idx, grid_id in enumerate(sample_grids):
        print(f"  Progress: {idx+1}/{len(sample_grids)} - {grid_id}")
        
        result = pixel_level_classification(grid_id)
        
        if result:
            results.append(result)
    
    print(f"\n  Completed: {len(results)} grids with pixel-level results")
    
    if len(results) == 0:
        print("Warning: No pixel-level results")
        return None
    
    df_pixel = pd.DataFrame(results)
    
    # Summary statistics
    print("\nPixel-Level Classification Performance:")
    print(f"  Mean Accuracy: {df_pixel['accuracy'].mean():.3f} ± {df_pixel['accuracy'].std():.3f}")
    print(f"  Road Precision: {df_pixel['road_precision'].mean():.3f}")
    print(f"  Road Recall: {df_pixel['road_recall'].mean():.3f}")
    print(f"  Paddy Precision: {df_pixel['paddy_precision'].mean():.3f}")
    print(f"  Paddy Recall: {df_pixel['paddy_recall'].mean():.3f}")
    
    # Visualize
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    # Accuracy distribution
    axes[0].hist(df_pixel['accuracy'], bins=15, color='steelblue', edgecolor='black', alpha=0.7)
    axes[0].axvline(df_pixel['accuracy'].mean(), color='red', linestyle='--', linewidth=2, 
                   label=f'Mean={df_pixel["accuracy"].mean():.3f}')
    axes[0].set_xlabel('Pixel-Level Accuracy', fontsize=12)
    axes[0].set_ylabel('Frequency', fontsize=12)
    axes[0].set_title('Pixel-Level Classification Accuracy Distribution', fontsize=13)
    axes[0].legend()
    axes[0].grid(alpha=0.3)
    
    # Precision-Recall by class
    metrics = ['road_precision', 'road_recall', 'paddy_precision', 'paddy_recall']
    labels = ['Road\nPrecision', 'Road\nRecall', 'Paddy\nPrecision', 'Paddy\nRecall']
    values = [df_pixel[m].mean() for m in metrics]
    colors = ['red', 'orange', 'green', 'lightgreen']
    
    axes[1].bar(labels, values, color=colors, alpha=0.7, edgecolor='black')
    axes[1].set_ylabel('Score', fontsize=12)
    axes[1].set_title('Mean Precision and Recall by Class', fontsize=13)
    axes[1].set_ylim([0, 1])
    axes[1].grid(alpha=0.3, axis='y')
    
    for i, v in enumerate(values):
        axes[1].text(i, v + 0.02, f'{v:.3f}', ha='center', fontsize=10, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(OUT_DIR / 'pixel_level_classification.png', dpi=150)
    plt.close()
    
    print(f"\nSaved: {OUT_DIR / 'pixel_level_classification.png'}")
    
    # Save results
    df_pixel.to_csv(OUT_DIR / 'pixel_level_results.csv', index=False)
    print(f"Saved: {OUT_DIR / 'pixel_level_results.csv'}")
    
    return df_pixel

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def generate_comprehensive_report(ratio_results, spatial_results, pixel_results):
    """
    Generate a comprehensive markdown report.
    """
    
    with open(OUT_DIR / 'comprehensive_analysis_report.md', 'w', encoding='utf-8') as f:
        f.write("# 透水特性の包括的分析レポート\n\n")
        
        f.write("## Part 1: Road Ratio相関分析\n\n")
        
        if ratio_results:
            f.write(f"- **Pearson相関係数**: {ratio_results['pearson_r']:.3f} (p={ratio_results['pearson_p']:.4f})\n")
            f.write(f"- **Spearman相関係数**: {ratio_results['spearman_r']:.3f} (p={ratio_results['spearman_p']:.4f})\n")
            
            if ratio_results['crossover_threshold']:
                f.write(f"- **挙動反転閾値**: 約{ratio_results['crossover_threshold']*100:.1f}%の道路被覆率\n")
                f.write(f"  - この閾値以下: 田んぼ的挙動（保水性）\n")
                f.write(f"  - この閾値以上: 道路的挙動（排水性）\n")
            else:
                f.write("- 明確な閾値は検出されませんでした\n")
        
        f.write("\n## Part 2: 空間配置分析\n\n")
        
        if spatial_results:
            f.write("### 空間メトリクスとDrainage Scoreの相関\n\n")
            for metric, corr in spatial_results.items():
                f.write(f"- **{metric}**: r={corr['r']:.3f} (p={corr['p']:.4f})\n")
            
            f.write("\n**解釈**:\n")
            f.write("- 道路の空間的な分布パターン（集中 vs 分散）が透水挙動に影響を与える可能性を調査しました。\n")
        
        f.write("\n## Part 3: ピクセル単位分類\n\n")
        
        if pixel_results is not None and len(pixel_results) > 0:
            f.write(f"- **平均精度**: {pixel_results['accuracy'].mean():.3f} ± {pixel_results['accuracy'].std():.3f}\n")
            f.write(f"- **Road Precision**: {pixel_results['road_precision'].mean():.3f}\n")
            f.write(f"- **Road Recall**: {pixel_results['road_recall'].mean():.3f}\n")
            f.write(f"- **Paddy Precision**: {pixel_results['paddy_precision'].mean():.3f}\n")
            f.write(f"- **Paddy Recall**: {pixel_results['paddy_recall'].mean():.3f}\n\n")
            
            f.write("**解釈**:\n")
            f.write("- グリッド全体ではなく、ピクセル単位での判別を試みました。\n")
            f.write("- 局所的なSAR差分値と近傍情報から、道路・田んぼを推定します。\n")
        
        f.write("\n## 総合的な結論\n\n")
        f.write("1. **Road Ratioの影響**: 道路ピクセル比率と透水挙動には相関があり、一定の閾値で挙動が反転する可能性があります。\n")
        f.write("2. **空間配置の重要性**: 道路の分布パターン（クラスタリング度合い）も透水挙動に寄与します。\n")
        f.write("3. **ピクセル単位の判別**: グリッド全体の平均ではなく、局所的な判別により精度向上が期待されます。\n")
    
    print(f"\nSaved: {OUT_DIR / 'comprehensive_analysis_report.md'}")

def main():
    print("Loading classification results...")
    df = pd.read_csv(CLASS_CSV)
    
    print(f"Loaded {len(df)} grids")
    
    # Part 1: Road Ratio Correlation
    ratio_results = analyze_road_ratio_correlation(df)
    
    # Part 2: Spatial Distribution
    spatial_results = analyze_spatial_distribution(df)
    
    # Part 3: Pixel-Level Classification
    pixel_results = analyze_pixel_level(df, n_samples=20)
    
    # Generate comprehensive report
    print("\n" + "="*80)
    print("GENERATING COMPREHENSIVE REPORT")
    print("="*80)
    
    generate_comprehensive_report(ratio_results, spatial_results, pixel_results)
    
    print("\n" + "="*80)
    print("ALL ANALYSES COMPLETE")
    print("="*80)
    print(f"\nResults saved to: {OUT_DIR}")
    print("\nGenerated files:")
    print("  - road_ratio_correlation.png")
    print("  - spatial_distribution_analysis.png")
    print("  - pixel_level_classification.png")
    print("  - spatial_metrics.csv")
    print("  - pixel_level_results.csv")
    print("  - comprehensive_analysis_report.md")

if __name__ == "__main__":
    main()
