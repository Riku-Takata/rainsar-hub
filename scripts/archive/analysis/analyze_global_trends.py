import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import re
from common_utils import setup_logger, RESULT_DIR

logger = setup_logger("global_trends")

class GlobalTrendAnalyzer:
    def __init__(self, csv_path):
        self.csv_path = Path(csv_path)
        self.output_dir = RESULT_DIR / "global_trends"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        # Set font for Japanese support
        plt.rcParams['font.family'] = 'MS Gothic'

    def parse_rain_info(self, row):
        """Parse 'rain_info' column to extract duration and max intensity."""
        info = str(row['rain_info'])
        # Example: "5h duration, Max 14.8 mm/h"
        duration = None
        intensity = None
        
        dur_match = re.search(r'(\d+)h duration', info)
        if dur_match:
            duration = int(dur_match.group(1))
            
        int_match = re.search(r'Max ([\d\.]+) mm/h', info)
        if int_match:
            intensity = float(int_match.group(1))
            
        return pd.Series([duration, intensity], index=['Rain_Duration_h', 'Max_Intensity_mmh'])

    def load_and_parse_data(self):
        if not self.csv_path.exists():
            logger.error(f"CSV file not found: {self.csv_path}")
            return None

        df = pd.read_csv(self.csv_path)
        
        # Parse Rain Info
        rain_stats = df.apply(self.parse_rain_info, axis=1)
        df = pd.concat([df, rain_stats], axis=1)
        
        # Parse Date
        df['Date'] = pd.to_datetime(df['date'])
        df['Month'] = df['Date'].dt.month
        
        # Filter for Paddy data availability
        # We focus on 'paddy_dec_mean_diff' (Mean difference of decreasing pixels)
        # or 'paddy_dec_ratio' (Ratio of decreasing pixels)
        # Let's use 'paddy_dec_mean_diff' as a proxy for "Magnitude of Decrease"
        # Note: 'paddy_dec_mean_diff' is usually negative.
        
        return df

    def plot_trends(self):
        df = self.load_and_parse_data()
        if df is None or df.empty:
            logger.warning("No data to plot.")
            return

        logger.info(f"Loaded {len(df)} records for trend analysis.")
        
        # 1. Rain Duration vs Paddy Decrease (Mean Diff)
        plt.figure(figsize=(10, 6))
        sns.scatterplot(x='Rain_Duration_h', y='paddy_dec_mean_diff', data=df, s=100, alpha=0.7)
        plt.title("Rain Duration vs Paddy Decrease Magnitude")
        plt.xlabel("Rain Duration (hours)")
        plt.ylabel("Mean Difference of Decreasing Pixels (dB)")
        plt.grid(True)
        plt.savefig(self.output_dir / "duration_vs_paddy_diff.png")
        plt.close()

        # 2. Max Intensity vs Paddy Decrease
        plt.figure(figsize=(10, 6))
        sns.scatterplot(x='Max_Intensity_mmh', y='paddy_dec_mean_diff', data=df, s=100, alpha=0.7)
        plt.title("Max Rain Intensity vs Paddy Decrease Magnitude")
        plt.xlabel("Max Intensity (mm/h)")
        plt.ylabel("Mean Difference of Decreasing Pixels (dB)")
        plt.grid(True)
        plt.savefig(self.output_dir / "intensity_vs_paddy_diff.png")
        plt.close()

        # 3. Seasonality (Month) vs Paddy Decrease
        plt.figure(figsize=(10, 6))
        sns.boxplot(x='Month', y='paddy_dec_mean_diff', data=df, palette='coolwarm')
        sns.stripplot(x='Month', y='paddy_dec_mean_diff', data=df, color='black', alpha=0.5)
        plt.title("Seasonality (Month) vs Paddy Decrease Magnitude")
        plt.xlabel("Month")
        plt.ylabel("Mean Difference of Decreasing Pixels (dB)")
        plt.grid(True)
        plt.savefig(self.output_dir / "season_vs_paddy_diff.png")
        plt.close()
        
        # 4. Delay vs Paddy Decrease (Colored by Duration)
        plt.figure(figsize=(10, 6))
        sns.scatterplot(x='delay', y='paddy_dec_mean_diff', hue='Rain_Duration_h', data=df, palette='viridis', s=100)
        plt.title("Delay vs Paddy Decrease (Colored by Rain Duration)")
        plt.xlabel("Delay (hours)")
        plt.ylabel("Mean Difference of Decreasing Pixels (dB)")
        plt.grid(True)
        plt.legend(title='Duration (h)')
        plt.savefig(self.output_dir / "delay_vs_paddy_diff_duration.png")
        plt.close()

        logger.info(f"Trend plots saved to {self.output_dir}")

if __name__ == "__main__":
    # Default path to analysis_result.csv
    csv_path = Path("analysis_result.csv").resolve()
    analyzer = GlobalTrendAnalyzer(csv_path)
    analyzer.plot_trends()
