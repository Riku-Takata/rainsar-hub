import numpy as np
import pandas as pd
from pathlib import Path
from common_utils import (
    setup_logger, parse_summary_txt, read_linear_values, linear_to_db,
    S1_SAMPLES_DIR, RESULT_DIR, TARGET_GRIDS
)

logger = setup_logger("raster_analysis")

SUFFIX_HIGHWAY = "_highway_mask.tif"
SUFFIX_PADDY = "_paddy_mask.tif"

class RasterAnalyzer:
    def __init__(self, output_dir=RESULT_DIR):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def sigmaclip(self, a, low=4.0, high=4.0):
        """SciPy's sigmaclip implementation using only NumPy."""
        c = np.asarray(a).ravel()
        delta = 1
        while delta:
            c_std = c.std()
            c_mean = c.mean()
            size = c.size
            critlower = c_mean - c_std * low
            critupper = c_mean + c_std * high
            c = c[(c >= critlower) & (c <= critupper)]
            delta = size - c.size
        return c

    def analyze_negative_pixels(self, max_delay=1.0, target_grids=None):
        """負の差分（降雨による減少）に着目した解析"""
        logger.info(f"Starting Negative Pixel Analysis (Delay < {max_delay}h)...")
        all_stats = []
        
        grids = target_grids if target_grids else TARGET_GRIDS
        
        for grid_id in grids:
            events = parse_summary_txt(grid_id)
            short_events = [e for e in events if e['delay'] is not None and e['delay'] < max_delay]
            
            if not short_events:
                continue
                
            logger.info(f"Processing Grid: {grid_id} ({len(short_events)} events)")
            grid_dir = S1_SAMPLES_DIR / grid_id
            
            for evt in short_events:
                stem_after = f"{evt['after_scene']}_proc"
                stem_before = f"{evt['before_scene']}_proc"
                
                for type_name, suffix in [('道路', SUFFIX_HIGHWAY), ('田んぼ', SUFFIX_PADDY)]:
                    path_after = grid_dir / f"{stem_after}{suffix}"
                    path_before = grid_dir / f"{stem_before}{suffix}"
                    
                    arr_a = read_linear_values(path_after)
                    arr_b = read_linear_values(path_before)
                    
                    if arr_a is not None and arr_b is not None:
                        min_len = min(len(arr_a), len(arr_b))
                        arr_a = arr_a[:min_len]
                        arr_b = arr_b[:min_len]
                        diff = arr_a - arr_b
                        
                        # Negative Diff Only
                        neg_mask = diff < 0
                        neg_diff = diff[neg_mask]
                        
                        if len(neg_diff) > 0:
                            stats = {
                                'Grid': grid_id,
                                'Delay': evt['delay'],
                                'Type': type_name,
                                'Total_Pixels': len(diff),
                                'Negative_Pixels': len(neg_diff),
                                'Negative_Ratio': len(neg_diff) / len(diff),
                                'Mean_Diff': np.mean(neg_diff),
                                'Median_Diff': np.median(neg_diff),
                                'Std_Diff': np.std(neg_diff)
                            }
                            all_stats.append(stats)

        if all_stats:
            df = pd.DataFrame(all_stats)
            # If single grid, maybe append or save separate? 
            # For now, just overwrite/append to the main file is fine, or maybe we should be careful.
            # If we are running for a single grid, we might not want to overwrite the global file with just one grid's data.
            # But the user asked for "detailed analysis".
            # Let's just save it. If the user runs "all", it will regenerate everything.
            
            out_csv = self.output_dir / "negative_pixel_stats.csv"
            
            # If target_grids is specified (subset), we should probably append or warn.
            # But for simplicity, let's just write.
            if target_grids and len(target_grids) < len(TARGET_GRIDS):
                 # Maybe save to a different file or just log it?
                 # Let's save to a specific file for this run if single grid?
                 pass

            df.to_csv(out_csv, index=False, encoding='utf-8-sig')
            logger.info(f"Saved negative stats to {out_csv}")
            
            # Summary
            logger.info("\n=== Negative Pixel Analysis Summary ===")
            logger.info(df.groupby('Type')[['Negative_Ratio', 'Mean_Diff', 'Median_Diff', 'Std_Diff']].mean())
        else:
            logger.warning("No data found for negative analysis.")

    def analyze_sigma_clipped(self, sigma_low=3.0, sigma_high=3.0, target_grids=None):
        """シグマクリッピングを用いた統計解析"""
        logger.info("Starting Sigma-clipped Analysis...")
        out_dir = self.output_dir / "sigma_clipped_stats"
        out_dir.mkdir(parents=True, exist_ok=True)
        
        grids = target_grids if target_grids else TARGET_GRIDS
        
        for grid_id in grids:
            events = parse_summary_txt(grid_id)
            if not events: continue
            
            grid_stats = []
            grid_dir = S1_SAMPLES_DIR / grid_id
            
            for evt in events:
                stem_after = f"{evt['after_scene']}_proc"
                stem_before = f"{evt['before_scene']}_proc"
                
                for type_name, suffix in [('道路', SUFFIX_HIGHWAY), ('田んぼ', SUFFIX_PADDY)]:
                    path_after = grid_dir / f"{stem_after}{suffix}"
                    path_before = grid_dir / f"{stem_before}{suffix}"
                    
                    arr_a = read_linear_values(path_after)
                    arr_b = read_linear_values(path_before)
                    
                    if arr_a is not None and arr_b is not None:
                        min_len = min(len(arr_a), len(arr_b))
                        arr_a = arr_a[:min_len]
                        arr_b = arr_b[:min_len]
                        diff = arr_a - arr_b
                        
                        c = self.sigmaclip(diff, sigma_low, sigma_high)
                        
                        if len(c) > 0:
                            row = {
                                'grid_id': grid_id,
                                'date': evt['date'],
                                'type': type_name,
                                'original_count': len(diff),
                                'clipped_count': len(c),
                                'mean': np.mean(c),
                                'median': np.median(c),
                                'std': np.std(c),
                                'discarded_ratio': 1.0 - (len(c) / len(diff))
                            }
                            grid_stats.append(row)
            
            if grid_stats:
                df = pd.DataFrame(grid_stats)
                grid_out_dir = out_dir / grid_id
                grid_out_dir.mkdir(parents=True, exist_ok=True)
                df.to_csv(grid_out_dir / "sigma_clipped_stats.csv", index=False, encoding='utf-8-sig')
    def analyze_pixel_variations(self, target_grid):
        """指定Gridの全ピクセルデータを抽出して詳細解析（分布・時系列変化）を行う"""
        logger.info(f"Starting Pixel Variation Analysis for {target_grid}...")
        import matplotlib.pyplot as plt
        import seaborn as sns
        
        # Set font for Japanese support
        plt.rcParams['font.family'] = 'MS Gothic'
        
        out_dir = self.output_dir / "pixel_variations" / target_grid
        out_dir.mkdir(parents=True, exist_ok=True)
        
        events = parse_summary_txt(target_grid)
        if not events:
            logger.warning(f"No events found for {target_grid}")
            return

        grid_dir = S1_SAMPLES_DIR / target_grid
        all_pixels = []
        
        logger.info(f"Extracting pixel data from {len(events)} events...")
        
        for evt in events:
            stem_after = f"{evt['after_scene']}_proc"
            stem_before = f"{evt['before_scene']}_proc"
            delay = evt['delay']
            
            for type_name, suffix in [('道路', SUFFIX_HIGHWAY), ('田んぼ', SUFFIX_PADDY)]:
                path_after = grid_dir / f"{stem_after}{suffix}"
                path_before = grid_dir / f"{stem_before}{suffix}"
                
                # Read absolute values (Linear)
                val_after = read_linear_values(path_after)
                val_before = read_linear_values(path_before)
                
                if val_after is not None and val_before is not None:
                    min_len = min(len(val_after), len(val_before))
                    val_after = val_after[:min_len]
                    val_before = val_before[:min_len]
                    
                    # Convert to dB for absolute value analysis (easier to interpret)
                    db_after = linear_to_db(val_after)
                    db_before = linear_to_db(val_before)
                    diff_linear = val_after - val_before
                    
                    # Sampling to avoid memory issues if too large (e.g. max 10k pixels per event/type)
                    # But user asked for "all pixels". Let's try to keep all if possible, 
                    # or sample if it's huge. 200k pixels * 10 events is fine.
                    
                    # Create a DataFrame for this batch
                    df_batch = pd.DataFrame({
                        'Delay': delay,
                        'Type': type_name,
                        'Before_dB': db_before,
                        'After_dB': db_after,
                        'Diff_Linear': diff_linear
                    })
                    all_pixels.append(df_batch)

        if not all_pixels:
            logger.warning("No pixel data extracted.")
            return
            
        df_all = pd.concat(all_pixels, ignore_index=True)
        logger.info(f"Total pixels extracted: {len(df_all)}")
        
        # 1. Delay vs Difference (Boxplot)
        logger.info("Generating Delay vs Difference plot...")
        plt.figure(figsize=(12, 6))
        sns.boxplot(x='Delay', y='Diff_Linear', hue='Type', data=df_all, showfliers=False)
        plt.title(f"Delay vs Backscatter Difference (Linear) - {target_grid}")
        plt.grid(True, axis='y')
        plt.savefig(out_dir / "delay_vs_diff_boxplot.png")
        plt.close()
        
        # 2. Absolute Intensity Distributions (Before vs After)
        logger.info("Generating Absolute Intensity Distributions...")
        g = sns.FacetGrid(df_all, col="Type", hue="Delay", height=5, aspect=1.5)
        g.map(sns.kdeplot, "Before_dB", warn_singular=False)
        g.add_legend()
        g.fig.suptitle(f"Before Intensity (dB) Distribution - {target_grid}", y=1.02)
        plt.savefig(out_dir / "dist_before_db.png")
        plt.close()
        
        g = sns.FacetGrid(df_all, col="Type", hue="Delay", height=5, aspect=1.5)
        g.map(sns.kdeplot, "After_dB", warn_singular=False)
        g.add_legend()
        g.fig.suptitle(f"After Intensity (dB) Distribution - {target_grid}", y=1.02)
        plt.savefig(out_dir / "dist_after_db.png")
        plt.close()

        # 3. Scatter Plot (Delay vs Diff) - heavily subsampled for visualization
        logger.info("Generating Scatter Plot...")
        sample_n = min(10000, len(df_all))
        df_sample = df_all.sample(n=sample_n, random_state=42)
        
        plt.figure(figsize=(10, 6))
        sns.scatterplot(x='Delay', y='Diff_Linear', hue='Type', data=df_sample, alpha=0.3)
        plt.title(f"Delay vs Difference (Sampled {sample_n} pts) - {target_grid}")
        plt.axhline(0, color='black', linestyle='--')
        plt.grid(True)
        plt.savefig(out_dir / "delay_vs_diff_scatter.png")
        plt.close()
        
        # 4. Difference Histogram (Stacked by Delay)
        logger.info("Generating Difference Histogram...")
        g = sns.FacetGrid(df_all, col="Type", hue="Delay", height=5, aspect=1.5)
        g.map(sns.kdeplot, "Diff_Linear", warn_singular=False, fill=True, alpha=0.3)
        g.add_legend()
        g.fig.suptitle(f"Difference Distribution (Linear) - {target_grid}", y=1.02)
        
        # Add reference lines for potential noise thresholds (e.g. +/- 3 sigma of road)
        road_data = df_all[df_all['Type'] == '道路']
        if not road_data.empty:
            road_std = road_data['Diff_Linear'].std()
            for ax in g.axes.flat:
                ax.axvline(3 * road_std, color='r', linestyle=':', label='Road 3$\sigma$')
                ax.axvline(-3 * road_std, color='r', linestyle=':')
        
        plt.savefig(out_dir / "dist_diff_linear.png")
        plt.close()
        
        # --- Noise Filtering & Re-analysis ---
        logger.info("=== Noise Filtering & Re-analysis ===")
        
        # Calculate Threshold (3-sigma of Road)
        road_data = df_all[df_all['Type'] == '道路']
        if road_data.empty:
            logger.warning("No road data for noise estimation. Skipping filtering.")
            return

        road_mean = road_data['Diff_Linear'].mean()
        road_std = road_data['Diff_Linear'].std()
        threshold = 3 * road_std
        
        logger.info(f"Noise Threshold (Road 3-sigma): +/- {threshold:.5f} (Mean: {road_mean:.5f})")
        
        # Filter Data
        # Keep pixels where |Diff - RoadMean| < Threshold
        # Or just |Diff| < Threshold if we assume mean is 0? 
        # Road mean might be slightly non-zero due to calibration/atmosphere.
        # Let's use |Diff - RoadMean| < Threshold for robust filtering.
        
        mask_valid = (df_all['Diff_Linear'] - road_mean).abs() < threshold
        df_clean = df_all[mask_valid].copy()
        
        removed_count = len(df_all) - len(df_clean)
        removed_ratio = removed_count / len(df_all) * 100
        logger.info(f"Removed {removed_count} pixels ({removed_ratio:.2f}%) as noise.")
        
        # Save Filtered Stats
        logger.info("Generating Filtered Plots...")
        
        # 1. Filtered Delay vs Difference (Boxplot)
        plt.figure(figsize=(12, 6))
        sns.boxplot(x='Delay', y='Diff_Linear', hue='Type', data=df_clean, showfliers=False)
        plt.title(f"[Filtered] Delay vs Backscatter Difference (Linear) - {target_grid}\n(Threshold: +/- {threshold:.4f})")
        plt.grid(True, axis='y')
        plt.savefig(out_dir / "filtered_delay_vs_diff_boxplot.png")
        plt.close()
        
        # 2. Filtered Difference Histogram
        g = sns.FacetGrid(df_clean, col="Type", hue="Delay", height=5, aspect=1.5)
        g.map(sns.kdeplot, "Diff_Linear", warn_singular=False, fill=True, alpha=0.3)
        g.add_legend()
        g.fig.suptitle(f"[Filtered] Difference Distribution (Linear) - {target_grid}", y=1.02)
        plt.savefig(out_dir / "filtered_dist_diff_linear.png")
        plt.close()
        
        # 3. Filtered Scatter Plot
        sample_n = min(10000, len(df_clean))
        df_sample = df_clean.sample(n=sample_n, random_state=42)
        
        plt.figure(figsize=(10, 6))
        sns.scatterplot(x='Delay', y='Diff_Linear', hue='Type', data=df_sample, alpha=0.3)
        plt.title(f"[Filtered] Delay vs Difference (Sampled {sample_n} pts) - {target_grid}")
        plt.axhline(0, color='black', linestyle='--')
        plt.grid(True)
        plt.savefig(out_dir / "filtered_delay_vs_diff_scatter.png")
        plt.close()
        
        logger.info(f"Filtered analysis completed. Results saved to {out_dir}")

    def analyze_time_evolution(self, target_grid):
        """Short Delay vs Long Delayの時系列変化を解析する"""
        logger.info(f"Starting Time Evolution Analysis for {target_grid}...")
        import matplotlib.pyplot as plt
        import seaborn as sns
        plt.rcParams['font.family'] = 'MS Gothic'
        
        out_dir = self.output_dir / "pixel_variations" / target_grid / "evolution"
        out_dir.mkdir(parents=True, exist_ok=True)
        
        events = parse_summary_txt(target_grid)
        if not events: return

        # Identify Short (< 1.5h) and Long (> 6h) events
        short_events = [e for e in events if e['delay'] < 1.5]
        long_events = [e for e in events if e['delay'] > 6.0]
        
        if not short_events or not long_events:
            logger.warning(f"Skipping evolution analysis: Short or Long event missing for {target_grid}")
            return
            
        # Use the first available event for each category
        evt_short = short_events[0]
        evt_long = long_events[0]
        
        logger.info(f"Comparing Short: {evt_short['delay']}h vs Long: {evt_long['delay']}h")
        
        grid_dir = S1_SAMPLES_DIR / target_grid
        
        combined_data = []
        
        for type_name, suffix in [('道路', SUFFIX_HIGHWAY), ('田んぼ', SUFFIX_PADDY)]:
            # Load Short Data
            path_s_after = grid_dir / f"{evt_short['after_scene']}_proc{suffix}"
            path_s_before = grid_dir / f"{evt_short['before_scene']}_proc{suffix}"
            val_s_a = read_linear_values(path_s_after)
            val_s_b = read_linear_values(path_s_before)
            
            # Load Long Data
            path_l_after = grid_dir / f"{evt_long['after_scene']}_proc{suffix}"
            path_l_before = grid_dir / f"{evt_long['before_scene']}_proc{suffix}"
            val_l_a = read_linear_values(path_l_after)
            val_l_b = read_linear_values(path_l_before)
            
            if all(v is not None for v in [val_s_a, val_s_b, val_l_a, val_l_b]):
                # Align lengths (just in case, though they should be same grid)
                min_len = min(len(val_s_a), len(val_l_a))
                
                # Calculate Differences
                diff_s = (val_s_a[:min_len] - val_s_b[:min_len])
                diff_l = (val_l_a[:min_len] - val_l_b[:min_len])
                
                # Create DataFrame
                df = pd.DataFrame({
                    'Type': type_name,
                    'Diff_Short': diff_s,
                    'Diff_Long': diff_l
                })
                
                # Filter Noise based on Short Delay (Road 3-sigma)
                # We need to calculate threshold from Road Short data first
                # But here we are iterating types. Let's store raw first.
                combined_data.append(df)
        
        if not combined_data:
            logger.warning("No valid data for evolution analysis.")
            return
            
        df_all = pd.concat(combined_data, ignore_index=True)
        
        # Calculate Threshold from Road Short Diff
        road_short = df_all[df_all['Type'] == '道路']['Diff_Short']
        if road_short.empty:
            logger.warning("No road data for threshold.")
            return
            
        threshold = 3 * road_short.std()
        road_mean = road_short.mean()
        logger.info(f"Noise Threshold (Short Road 3-sigma): {threshold:.5f}")
        
        # Apply Filter
        mask_valid = (df_all['Diff_Short'] - road_mean).abs() < threshold
        df_clean = df_all[mask_valid].copy()
        
        removed = len(df_all) - len(df_clean)
        logger.info(f"Removed {removed} pixels ({removed/len(df_all)*100:.2f}%) based on Short Delay noise.")
        
        # Calculate Evolution (Long - Short)
        df_clean['Evolution'] = df_clean['Diff_Long'] - df_clean['Diff_Short']
        
        # Save Data to CSV for inspection
        csv_path = out_dir / "evolution_data.csv"
        df_clean.to_csv(csv_path, index=False, encoding='utf-8-sig')
        logger.info(f"Saved evolution data to {csv_path}")
        
        # Calculate Summary Stats
        summary = df_clean.groupby('Type')[['Diff_Short', 'Diff_Long', 'Evolution']].describe()
        logger.info("\n=== Evolution Summary Stats ===")
        logger.info(summary)
        
        # Plot 1: Scatter (Short vs Long)
        plt.figure(figsize=(8, 8))
        sns.scatterplot(x='Diff_Short', y='Diff_Long', hue='Type', data=df_clean.sample(min(10000, len(df_clean))), alpha=0.3)
        plt.plot([-0.1, 0.1], [-0.1, 0.1], 'k--', label='y=x (No Change)')
        plt.title(f"Short ({evt_short['delay']}h) vs Long ({evt_long['delay']}h) Difference\n{target_grid}")
        plt.xlabel("Short Delay Difference")
        plt.ylabel("Long Delay Difference")
        plt.grid(True)
        plt.axis('equal')
        plt.savefig(out_dir / "scatter_short_vs_long.png")
        plt.close()
        
        # Plot 2: Evolution Histogram
        plt.figure(figsize=(10, 6))
        sns.histplot(data=df_clean, x='Evolution', hue='Type', kde=True, element="step")
        plt.title(f"Evolution (Long - Short) Distribution\n{target_grid}")
        plt.xlabel("Evolution (Diff_Long - Diff_Short)")
        plt.grid(True)
        plt.savefig(out_dir / "hist_evolution.png")
        plt.close()
        
        logger.info(f"Evolution analysis saved to {out_dir}")

    def analyze_comprehensive_statistics(self, target_grid):
        """多角的な視点（カテゴリ分類、遷移、初期状態依存性）での解析を行う"""
        logger.info(f"Starting Comprehensive Analysis for {target_grid}...")
        import matplotlib.pyplot as plt
        import seaborn as sns
        plt.rcParams['font.family'] = 'MS Gothic'
        
        out_dir = self.output_dir / "pixel_variations" / target_grid / "comprehensive"
        out_dir.mkdir(parents=True, exist_ok=True)
        
        # Reuse evolution data if available, otherwise we need to re-extract
        # Ideally we should refactor extraction logic, but for now let's re-use the logic from analyze_time_evolution
        # Or better, read the CSV if it exists?
        # Let's re-implement extraction to be safe and self-contained, or call a helper if I had one.
        # I'll copy the extraction logic for robustness.
        
        events = parse_summary_txt(target_grid)
        if not events: return
        short_events = [e for e in events if e['delay'] < 1.5]
        long_events = [e for e in events if e['delay'] > 6.0]
        if not short_events or not long_events:
            logger.warning("Missing Short or Long events.")
            return
        evt_short = short_events[0]
        evt_long = long_events[0]
        
        grid_dir = S1_SAMPLES_DIR / target_grid
        combined_data = []
        
        for type_name, suffix in [('道路', SUFFIX_HIGHWAY), ('田んぼ', SUFFIX_PADDY)]:
            # Load Short
            path_s_after = grid_dir / f"{evt_short['after_scene']}_proc{suffix}"
            path_s_before = grid_dir / f"{evt_short['before_scene']}_proc{suffix}"
            val_s_a = read_linear_values(path_s_after)
            val_s_b = read_linear_values(path_s_before)
            
            # Load Long
            path_l_after = grid_dir / f"{evt_long['after_scene']}_proc{suffix}"
            path_l_before = grid_dir / f"{evt_long['before_scene']}_proc{suffix}"
            val_l_a = read_linear_values(path_l_after)
            val_l_b = read_linear_values(path_l_before)
            
            if all(v is not None for v in [val_s_a, val_s_b, val_l_a, val_l_b]):
                min_len = min(len(val_s_a), len(val_l_a))
                
                # Calculate Differences & Absolute Values (for dependency analysis)
                diff_s = (val_s_a[:min_len] - val_s_b[:min_len])
                diff_l = (val_l_a[:min_len] - val_l_b[:min_len])
                before_db_s = linear_to_db(val_s_b[:min_len]) # Use Short Before as baseline
                
                df = pd.DataFrame({
                    'Type': type_name,
                    'Diff_Short': diff_s,
                    'Diff_Long': diff_l,
                    'Before_dB': before_db_s
                })
                combined_data.append(df)
        
        if not combined_data: return
        df_all = pd.concat(combined_data, ignore_index=True)
        
        # Define Threshold (Road 2-sigma for "Significant Change")
        # User requested 2-sigma in plan? Or I proposed it. Let's use 2-sigma.
        road_short = df_all[df_all['Type'] == '道路']['Diff_Short']
        sigma = road_short.std()
        threshold = 2 * sigma
        logger.info(f"Significance Threshold (Road 2-sigma): +/- {threshold:.5f}")
        
        # 1. Categorization
        def categorize(diff):
            if diff > threshold: return 'Increase'
            if diff < -threshold: return 'Decrease'
            return 'Stable'
            
        df_all['Cat_Short'] = df_all['Diff_Short'].apply(categorize)
        df_all['Cat_Long'] = df_all['Diff_Long'].apply(categorize)
        
        # Calculate Percentages
        stats_list = []
        for t in ['道路', '田んぼ']:
            sub = df_all[df_all['Type'] == t]
            total = len(sub)
            for period, col in [('Short', 'Cat_Short'), ('Long', 'Cat_Long')]:
                counts = sub[col].value_counts()
                for cat in ['Increase', 'Decrease', 'Stable']:
                    count = counts.get(cat, 0)
                    stats_list.append({
                        'Type': t,
                        'Period': period,
                        'Category': cat,
                        'Count': count,
                        'Percentage': count / total * 100
                    })
        
        df_stats = pd.DataFrame(stats_list)
        df_stats.to_csv(out_dir / "category_stats.csv", index=False, encoding='utf-8-sig')
        logger.info("Saved category stats.")
        
        # Plot Categories
        plt.figure(figsize=(10, 6))
        sns.barplot(x='Period', y='Percentage', hue='Category', data=df_stats[df_stats['Type']=='田んぼ'], palette='viridis')
        plt.title(f"Pixel Categories (Paddy) - {target_grid}\nThreshold: +/- {threshold:.4f}")
        plt.grid(True, axis='y')
        plt.savefig(out_dir / "category_barplot_paddy.png")
        plt.close()
        
        # 2. Transition Matrix (Short -> Long) for Paddy
        paddy_data = df_all[df_all['Type'] == '田んぼ']
        transition = pd.crosstab(paddy_data['Cat_Short'], paddy_data['Cat_Long'], normalize='index') * 100
        
        plt.figure(figsize=(8, 6))
        sns.heatmap(transition, annot=True, fmt=".1f", cmap="YlGnBu")
        plt.title(f"Transition Matrix (Short -> Long) [Paddy] (%)\nRow: Short, Col: Long")
        plt.ylabel("Short Category")
        plt.xlabel("Long Category")
        plt.savefig(out_dir / "transition_matrix_paddy.png")
        plt.close()
        
        # Save Transition CSV
        transition.to_csv(out_dir / "transition_matrix_paddy.csv", encoding='utf-8-sig')
        
        # 3. Intensity Dependence (Before dB vs Diff Short)
        # Sample for plotting
        sample = paddy_data.sample(min(10000, len(paddy_data)))
        
        plt.figure(figsize=(10, 6))
        sns.scatterplot(x='Before_dB', y='Diff_Short', data=sample, alpha=0.3)
        plt.axhline(0, color='k', linestyle='--')
        plt.axhline(threshold, color='r', linestyle=':', label='+2sigma')
        plt.axhline(-threshold, color='r', linestyle=':', label='-2sigma')
        plt.title(f"Before Intensity vs Short Difference (Paddy)\n{target_grid}")
        plt.xlabel("Before Intensity (dB)")
        plt.ylabel("Short Difference (Linear)")
        plt.legend()
        plt.grid(True)
        plt.savefig(out_dir / "dependence_before_vs_diff.png")
        plt.close()
        
        logger.info(f"Comprehensive analysis saved to {out_dir}")

    def analyze_outlier_evolution(self, target_grid):
        """分布の裾（外れ値）に焦点を当てた時系列変化解析"""
        logger.info(f"Starting Outlier Evolution Analysis for {target_grid}...")
        import matplotlib.pyplot as plt
        import seaborn as sns
        plt.rcParams['font.family'] = 'MS Gothic'
        
        out_dir = self.output_dir / "pixel_variations" / target_grid / "outlier_evolution"
        out_dir.mkdir(parents=True, exist_ok=True)
        
        # --- Data Extraction (Copying logic for robustness) ---
        events = parse_summary_txt(target_grid)
        if not events: return
        short_events = [e for e in events if e['delay'] < 1.5]
        long_events = [e for e in events if e['delay'] > 6.0]
        if not short_events or not long_events:
            logger.warning("Missing Short or Long events.")
            return
        evt_short = short_events[0]
        evt_long = long_events[0]
        
        grid_dir = S1_SAMPLES_DIR / target_grid
        combined_data = []
        
        for type_name, suffix in [('道路', SUFFIX_HIGHWAY), ('田んぼ', SUFFIX_PADDY)]:
            path_s_after = grid_dir / f"{evt_short['after_scene']}_proc{suffix}"
            path_s_before = grid_dir / f"{evt_short['before_scene']}_proc{suffix}"
            val_s_a = read_linear_values(path_s_after)
            val_s_b = read_linear_values(path_s_before)
            
            path_l_after = grid_dir / f"{evt_long['after_scene']}_proc{suffix}"
            path_l_before = grid_dir / f"{evt_long['before_scene']}_proc{suffix}"
            val_l_a = read_linear_values(path_l_after)
            val_l_b = read_linear_values(path_l_before)
            
            if all(v is not None for v in [val_s_a, val_s_b, val_l_a, val_l_b]):
                min_len = min(len(val_s_a), len(val_s_b), len(val_l_a), len(val_l_b))
                diff_s = (val_s_a[:min_len] - val_s_b[:min_len])
                diff_l = (val_l_a[:min_len] - val_l_b[:min_len])
                
                df = pd.DataFrame({
                    'Type': type_name,
                    'Diff_Short': diff_s,
                    'Diff_Long': diff_l
                })
                combined_data.append(df)
        
        if not combined_data: return
        df_all = pd.concat(combined_data, ignore_index=True)
        
        # --- Outlier Definition ---
        # Threshold: Road 2-sigma
        road_data = df_all[df_all['Type'] == '道路']
        threshold = 2 * road_data['Diff_Short'].std()
        logger.info(f"Threshold (Road Short 2-sigma): {threshold:.5f}")
        
        for target_type in ['道路', '田んぼ']:
            logger.info(f"--- Analyzing {target_type} ---")
            type_data = df_all[df_all['Type'] == target_type].copy()
            
            # Identify Groups based on LONG Delay
            type_data['Group'] = 'Stable'
            type_data.loc[type_data['Diff_Long'] < -threshold, 'Group'] = 'Decreased'
            type_data.loc[type_data['Diff_Long'] > threshold, 'Group'] = 'Increased'
            
            # Save Group Stats
            group_counts = type_data['Group'].value_counts()
            logger.info(f"{target_type} Groups (Long Delay Status):\n{group_counts}")
            
            # Save detailed CSV
            outlier_csv = out_dir / f"outlier_groups_{target_type}.csv"
            type_data.to_csv(outlier_csv, index=False, encoding='utf-8-sig')
            
            # --- Visualization ---
            
            # 1. Trajectory Plot
            plt.figure(figsize=(10, 10))
            
            # Plot Stable first
            stable = type_data[type_data['Group'] == 'Stable']
            if not stable.empty:
                sns.scatterplot(x='Diff_Short', y='Diff_Long', data=stable.sample(min(5000, len(stable))), 
                                color='lightgray', alpha=0.1, label='Stable')
            
            # Plot Decreased
            decreased = type_data[type_data['Group'] == 'Decreased']
            if not decreased.empty:
                sns.scatterplot(x='Diff_Short', y='Diff_Long', data=decreased.sample(min(5000, len(decreased))), 
                                color='blue', alpha=0.3, label='Decreased (Long)')
            
            # Plot Increased
            increased = type_data[type_data['Group'] == 'Increased']
            if not increased.empty:
                sns.scatterplot(x='Diff_Short', y='Diff_Long', data=increased.sample(min(5000, len(increased))), 
                                color='red', alpha=0.3, label='Increased (Long)')
            
            plt.plot([-0.5, 0.5], [-0.5, 0.5], 'k--', linewidth=1)
            plt.axhline(threshold, color='k', linestyle=':')
            plt.axhline(-threshold, color='k', linestyle=':')
            plt.axvline(threshold, color='k', linestyle=':')
            plt.axvline(-threshold, color='k', linestyle=':')
            
            plt.title(f"Outlier Trajectory (Short -> Long) [{target_type}]\n{target_grid}")
            plt.xlabel("Short Delay Difference")
            plt.ylabel("Long Delay Difference")
            plt.xlim(-0.3, 0.3)
            plt.ylim(-0.3, 0.3)
            plt.legend()
            plt.grid(True)
            plt.savefig(out_dir / f"outlier_trajectory_{target_type}.png")
            plt.close()
            
            # 2. Evolution Stats per Group
            type_data['Evolution'] = type_data['Diff_Long'] - type_data['Diff_Short']
            
            plt.figure(figsize=(10, 6))
            sns.boxplot(x='Group', y='Evolution', data=type_data, order=['Decreased', 'Stable', 'Increased'], palette='coolwarm')
            plt.title(f"Evolution (Long - Short) by Final Group [{target_type}]")
            plt.grid(True, axis='y')
            plt.savefig(out_dir / f"group_evolution_boxplot_{target_type}.png")
            plt.close()
            
            # Calculate Summary Stats
            summary = type_data.groupby('Group')[['Diff_Short', 'Diff_Long', 'Evolution']].mean()
            logger.info(f"{target_type} Group Summary (Mean):\n{summary}")
            summary.to_csv(out_dir / f"group_summary_stats_{target_type}.csv", encoding='utf-8-sig')
            
        logger.info(f"Outlier analysis saved to {out_dir}")

    def analyze_intensity_distribution(self, target_grid):
        """後方散乱強度の生データ（dB）の分布解析"""
        logger.info(f"Starting Intensity Distribution Analysis for {target_grid}...")
        import matplotlib.pyplot as plt
        import seaborn as sns
        plt.rcParams['font.family'] = 'MS Gothic'
        
        out_dir = self.output_dir / "pixel_variations" / target_grid / "intensity_dist"
        out_dir.mkdir(parents=True, exist_ok=True)
        
        events = parse_summary_txt(target_grid)
        if not events: return

        # Select Short and Long events
        short_events = [e for e in events if e['delay'] < 1.5]
        long_events = [e for e in events if e['delay'] > 6.0]
        
        target_events = []
        if short_events: target_events.append(('Short', short_events[0]))
        if long_events: target_events.append(('Long', long_events[0]))
        
        if not target_events:
            logger.warning("No suitable Short or Long events found.")
            return

        stats_list = []

        for label, evt in target_events:
            logger.info(f"Analyzing {label} Event: Delay {evt['delay']}h")
            
            grid_dir = S1_SAMPLES_DIR / target_grid
            
            for type_name, suffix in [('道路', SUFFIX_HIGHWAY), ('田んぼ', SUFFIX_PADDY)]:
                # Paths
                path_after = grid_dir / f"{evt['after_scene']}_proc{suffix}"
                path_before = grid_dir / f"{evt['before_scene']}_proc{suffix}"
                
                # Read Linear Values
                val_after_lin = read_linear_values(path_after)
                val_before_lin = read_linear_values(path_before)
                
                if val_after_lin is None or val_before_lin is None:
                    continue
                    
                # Convert to dB
                val_after_db = linear_to_db(val_after_lin)
                val_before_db = linear_to_db(val_before_lin)
                
                # Plot Histogram
                plt.figure(figsize=(10, 6))
                sns.histplot(val_before_db, color='blue', alpha=0.5, label='Before (晴天)', kde=True, stat="density")
                sns.histplot(val_after_db, color='red', alpha=0.5, label='After (降雨)', kde=True, stat="density")
                plt.title(f"Intensity Distribution (dB) - {label} / {type_name}\nDelay: {evt['delay']}h")
                plt.xlabel("Backscatter Intensity (dB)")
                plt.ylabel("Density")
                plt.legend()
                plt.grid(True)
                plt.savefig(out_dir / f"dist_{label}_{type_name}.png")
                plt.close()
                
                # Calculate Stats
                for timing, data in [('Before', val_before_db), ('After', val_after_db)]:
                    stats_list.append({
                        'Event_Label': label,
                        'Type': type_name,
                        'Timing': timing,
                        'Mean_dB': np.mean(data),
                        'Median_dB': np.median(data),
                        'Std_dB': np.std(data),
                        'Min_dB': np.min(data),
                        'Max_dB': np.max(data),
                        'Count': len(data)
                    })

        # Save Stats
        if stats_list:
            df_stats = pd.DataFrame(stats_list)
            df_stats.to_csv(out_dir / "intensity_stats.csv", index=False, encoding='utf-8-sig')
            logger.info(f"Saved intensity stats to {out_dir}")
