import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from common_utils import setup_logger, RESULT_DIR

logger = setup_logger("stats_analysis")

# Set font for Japanese support
plt.rcParams['font.family'] = 'MS Gothic'

class StatsAnalyzer:
    def __init__(self, csv_path):
        self.csv_path = Path(csv_path)
        self.output_dir = self.csv_path.parent / "analysis_output"
        self.output_dir.mkdir(exist_ok=True)
        if self.csv_path.exists():
            self.df = pd.read_csv(self.csv_path)
            if 'Median' not in self.df.columns and '50%' in self.df.columns:
                self.df['Median'] = self.df['50%']
        else:
            self.df = None

    def aggregate_data(self, base_dir, target_grids=None):
        """個別の統計ファイルを収集して結合する"""
        logger.info(f"Aggregating stats from {base_dir}...")
        base_dir = Path(base_dir)
        all_stats = []

        for grid_dir in base_dir.iterdir():
            if not grid_dir.is_dir(): continue
            grid_id = grid_dir.name
            
            if target_grids and grid_id not in target_grids:
                continue
            
            for event_dir in grid_dir.iterdir():
                if not event_dir.is_dir(): continue
                delay_str = event_dir.name.replace('h', '')
                try:
                    delay = float(delay_str)
                except ValueError: continue
                    
                stats_file = event_dir / "stats_difference.csv"
                if not stats_file.exists(): continue
                
                try:
                    df = pd.read_csv(stats_file)
                    for _, row in df.iterrows():
                        entry = {
                            'Grid': grid_id,
                            'Delay': delay,
                            'Type': row['type'],
                            'Count': row['count'],
                            'Mean': row['mean'],
                            'Std': row['std'],
                            'Min': row['min'],
                            'Max': row['max'],
                            '25%': row['25%'],
                            '50%': row['50%'],
                            '75%': row['75%']
                        }
                        all_stats.append(entry)
                except Exception as e:
                    logger.error(f"Error reading {stats_file}: {e}")

        if all_stats:
            self.df = pd.DataFrame(all_stats)
            self.df = self.df.sort_values(by=['Delay', 'Grid'])
            if 'Median' not in self.df.columns and '50%' in self.df.columns:
                self.df['Median'] = self.df['50%']
            
            if target_grids and len(target_grids) == 1:
                # Single grid mode: don't overwrite the main aggregated csv
                # Just keep it in memory or save to a temporary location?
                # Or maybe we just want to analyze it.
                logger.info(f"Aggregated {len(self.df)} records for {target_grids[0]}")
            else:
                self.df.to_csv(self.csv_path, index=False, encoding='utf-8-sig')
                logger.info(f"Saved aggregated stats to {self.csv_path}")
        else:
            logger.warning("No stats found to aggregate.")

    def analyze_single_grid(self, grid_id):
        """特定Gridの詳細解析"""
        logger.info(f"=== DETAILED ANALYSIS FOR {grid_id} ===")
        if self.df is None or self.df.empty:
            logger.warning("No data available.")
            return

        sub = self.df[self.df['Grid'] == grid_id].copy()
        if sub.empty:
            logger.warning(f"No data found for grid {grid_id}")
            return
            
        sub = sub.sort_values('Delay')
        
        print(f"\n--- Statistics for {grid_id} ---")
        print(sub[['Delay', 'Type', 'Mean', 'Median', 'Std', 'Count']].to_string(index=False))
        
        # Plot for single grid
        fig, axes = plt.subplots(1, 2, figsize=(12, 5))
        
        for t in ['道路', '田んぼ']:
            t_sub = sub[sub['Type'] == t]
            if t_sub.empty: continue
            
            axes[0].plot(t_sub['Delay'], t_sub['Mean'], marker='o', label=t)
            axes[1].plot(t_sub['Delay'], t_sub['Std'], marker='o', label=t)
            
        axes[0].set_title(f"Mean vs Delay ({grid_id})")
        axes[0].set_xlabel("Delay (h)")
        axes[0].set_ylabel("Mean Difference")
        axes[0].legend()
        axes[0].grid(True)
        
        axes[1].set_title(f"Std vs Delay ({grid_id})")
        axes[1].set_xlabel("Delay (h)")
        axes[1].set_ylabel("Std Dev")
        axes[1].legend()
        axes[1].grid(True)
        
        out_path = self.output_dir / f"analysis_{grid_id}.png"
        plt.tight_layout()
        plt.savefig(out_path)
        logger.info(f"Saved plot to {out_path}")


    def analyze_std_distribution(self):
        """標準偏差（ばらつき）の分布を詳細に分析して、ノイズ除去の閾値を検討する"""
        logger.info("=== STD DEV DISTRIBUTION ANALYSIS ===")
        df = self.df
        
        fig, axes = plt.subplots(2, 1, figsize=(10, 12))
        
        for i, t in enumerate(['道路', '田んぼ']):
            sub = df[df['Type'] == t]
            if sub.empty: continue
            
            # Histogram
            sns.histplot(sub['Std'], kde=True, ax=axes[i], bins=40)
            axes[i].set_title(f"{t} - Standard Deviation Distribution")
            axes[i].set_xlabel("Standard Deviation (Std)")
            
            # Calculate Percentiles
            percs = [50, 75, 90, 95, 99]
            values = np.percentile(sub['Std'], percs)
            
            logger.info(f"\n--- {t} Std Percentiles ---")
            for p, v in zip(percs, values):
                logger.info(f"{p}%: {v:.5f}")
                axes[i].axvline(v, color='g', linestyle='--', alpha=0.7, label=f'{p}%: {v:.3f}')
            
            # Current Threshold (IQR based)
            Q1 = sub['Std'].quantile(0.25)
            Q3 = sub['Std'].quantile(0.75)
            IQR = Q3 - Q1
            thresh = Q3 + 1.5 * IQR
            axes[i].axvline(thresh, color='r', linestyle='-', linewidth=2, label=f'IQR Thresh: {thresh:.3f}')
            
            logger.info(f"IQR Threshold (Q3 + 1.5*IQR): {thresh:.5f}")
            
            axes[i].legend()
            
        plt.tight_layout()
        out_path = self.output_dir / "std_distribution_analysis.png"
        plt.savefig(out_path)
        logger.info(f"Saved Std analysis plot to {out_path}")

    def analyze_noise(self):
        """ノイズデータの特定（標準偏差や平均値の異常値）"""
        logger.info("=== NOISE ANALYSIS ===")
        df = self.df
        
        # Call the detailed distribution analysis
        self.analyze_std_distribution()
        
        # High Std Dev
        for t in df['Type'].unique():
            sub = df[df['Type'] == t]
            mean_std = sub['Std'].mean()
            std_std = sub['Std'].std()
            thresh = mean_std + 2 * std_std
            
            logger.info(f"\n--- High Std Dev for {t} (Threshold: >{thresh:.4f}) ---")
            noisy_std = sub[sub['Std'] > thresh]
            if not noisy_std.empty:
                logger.info(noisy_std[['Grid', 'Delay', 'Std']])
            else:
                logger.info("None")

        # Extreme Mean
        for t in df['Type'].unique():
            sub = df[df['Type'] == t]
            mean_mean = sub['Mean'].mean()
            std_mean = sub['Mean'].std()
            upper = mean_mean + 2 * std_mean
            lower = mean_mean - 2 * std_mean
            
            logger.info(f"\n--- Extreme Mean for {t} (Range: {lower:.4f} ~ {upper:.4f}) ---")
            noisy_mean = sub[(sub['Mean'] > upper) | (sub['Mean'] < lower)]
            if not noisy_mean.empty:
                logger.info(noisy_mean[['Grid', 'Delay', 'Mean']])
            else:
                logger.info("None")

    def analyze_trends(self, df=None):
        """時間経過に伴うトレンド分析"""
        if df is None:
            df = self.df
            
        logger.info("\n=== TREND ANALYSIS (Delay vs Std) ===")
        bins = [0, 1, 3, 6, 12]
        labels = ['0-1h', '1-3h', '3-6h', '6-12h']
        
        for t in df['Type'].unique():
            sub = df[df['Type'] == t].copy()
            sub['DelayBin'] = pd.cut(sub['Delay'], bins=bins, labels=labels)
            
            logger.info(f"\n--- Type: {t} ---")
            grouped = sub.groupby('DelayBin', observed=True)['Std'].agg(['mean', 'count', 'std'])
            logger.info(grouped)

    def analyze_filtered(self, blacklist_grids=None, outliers=None):
        """ノイズを除去して再解析"""
        logger.info("\n=== FILTERED ANALYSIS ===")
        df_clean = self.df.copy()
        
        if blacklist_grids:
            logger.info(f"Blacklisted Grids: {blacklist_grids}")
            df_clean = df_clean[~df_clean['Grid'].isin(blacklist_grids)]
            
        if outliers:
            logger.info("Removing specific outliers...")
            for g, d, t in outliers:
                mask = (df_clean['Grid'] == g) & (df_clean['Delay'] == d) & (df_clean['Type'] == t)
                df_clean = df_clean[~mask]

        logger.info(f"Original Count: {len(self.df)} -> Clean Count: {len(df_clean)}")
        
        self.analyze_trends(df_clean)
        
        # Signal vs Noise Ratio
        logger.info("\n=== SIGNAL (Paddy) vs NOISE (Road) RATIO ===")
        bins = [0, 1, 3, 6, 12]
        labels = ['0-1h', '1-3h', '3-6h', '6-12h']
        
        paddy_stats = df_clean[df_clean['Type'] == '田んぼ'].copy()
        paddy_stats['DelayBin'] = pd.cut(paddy_stats['Delay'], bins=bins, labels=labels)
        p_grp = paddy_stats.groupby('DelayBin', observed=True)['Std'].mean()
        
        road_stats = df_clean[df_clean['Type'] == '道路'].copy()
        road_stats['DelayBin'] = pd.cut(road_stats['Delay'], bins=bins, labels=labels)
        r_grp = road_stats.groupby('DelayBin', observed=True)['Std'].mean()
        
        ratio = p_grp / r_grp
        logger.info("\nRatio of Paddy Std / Road Std:")
        logger.info(ratio)

    def plot_distributions(self):
        """分布の可視化"""
        logger.info("Plotting distributions...")
        df = self.df.copy()
        
        # Ensure Median column exists (sometimes it's named '50%')
        if 'Median' not in df.columns and '50%' in df.columns:
            df['Median'] = df['50%']
            
        fig, axes = plt.subplots(3, 2, figsize=(15, 15))
        metrics = ['Mean', 'Median', 'Std']
        
        for i, metric in enumerate(metrics):
            if metric not in df.columns: continue
            for j, t in enumerate(['道路', '田んぼ']):
                sub = df[df['Type'] == t]
                sns.histplot(sub[metric], kde=True, ax=axes[i, j], bins=30)
                axes[i, j].set_title(f"{t} - {metric} Distribution")
                
                Q1 = sub[metric].quantile(0.25)
                Q3 = sub[metric].quantile(0.75)
                IQR = Q3 - Q1
                lower = Q1 - 1.5 * IQR
                upper = Q3 + 1.5 * IQR
                
                axes[i, j].axvline(lower, color='r', linestyle='--', label='IQR Lower')
                axes[i, j].axvline(upper, color='r', linestyle='--', label='IQR Upper')
                axes[i, j].legend()

        plt.tight_layout()
        out_path = self.output_dir / "stats_distribution.png"
        plt.savefig(out_path)
        logger.info(f"Saved plot to {out_path}")

if __name__ == "__main__":
    # Example usage
    csv_path = RESULT_DIR / "distributions" / "aggregated_stats_difference.csv"
    if csv_path.exists():
        analyzer = StatsAnalyzer(csv_path)
        analyzer.analyze_noise()
        analyzer.plot_distributions()
        
        # Filtered run
        blacklist = ['N03355E13125']
        outliers = [
            ('N03335E13095', 9.36, '道路'),
            ('N03295E13185', 0.15, '田んぼ'),
            ('N03375E13095', 10.23, '田んぼ')
        ]
        analyzer.analyze_filtered(blacklist, outliers)
