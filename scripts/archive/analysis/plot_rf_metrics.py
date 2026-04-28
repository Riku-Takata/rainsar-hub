import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib as mpl
from pathlib import Path

# Set Japanese font
# Try typical fonts found in Windows/Linux environments
# For Windows, 'Meiryo' or 'MS Gothic' is common.
mpl.rcParams['font.family'] = ['Meiryo', 'MS Gothic', 'sans-serif']

OUTPUT_DIR = Path(r"D:\sotsuron\rainsar-hub\data\analysis")
METRICS_CSV = OUTPUT_DIR / "rf_classification_metrics.csv"
PLOT_DIR = OUTPUT_DIR / "rf_plots"
PLOT_DIR.mkdir(exist_ok=True)

def plot_metrics():
    if not METRICS_CSV.exists():
        print(f"Metrics file not found: {METRICS_CSV}")
        return

    df = pd.read_csv(METRICS_CSV)
    
    # Exclude August (8月除外)
    df = df[df['month'] != 8]
    
    # Ensure all remaining target months are plotted
    months = df['month'].unique()
    
    # Convert to Percentage for plotting
    df['accuracy_pct'] = df['accuracy'] * 100
    df['paddy_f1_pct'] = df['paddy_f1'] * 100

    # Plot Accuracy
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=df, x='delay_int', y='accuracy_pct', hue='month', palette='tab10', marker='o')
    
    # Annotate values
    for i, row in df.iterrows():
        plt.text(row['delay_int'], row['accuracy_pct'] + 0.3, f"{row['accuracy_pct']:.1f}", 
                 ha='center', va='bottom', fontsize=9)

    plt.title("Random Forest 分類精度の推移")
    plt.xlabel("降雨後経過時間 [時間後]")
    plt.ylabel("分類精度 [%]")
    plt.ylim(50, 80) 
    plt.xticks(range(0, 13)) # Hourly ticks
    plt.legend(title='月')
    plt.grid(True)
    plt.savefig(PLOT_DIR / "rf_accuracy_trend.png")
    plt.close()
    
    # Plot F1-Score
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=df, x='delay_int', y='paddy_f1_pct', hue='month', palette='tab10', marker='o')
    
    # Annotate values
    for i, row in df.iterrows():
        plt.text(row['delay_int'], row['paddy_f1_pct'] + 0.3, f"{row['paddy_f1_pct']:.1f}", 
                 ha='center', va='bottom', fontsize=9)

    plt.title("Random Forest F1-Scoreの推移")
    plt.xlabel("降雨後経過時間 [時間後]")
    plt.ylabel("F1-Score [%]")
    plt.ylim(50, 80)
    plt.xticks(range(0, 13)) # Hourly ticks
    plt.legend(title='月')
    plt.grid(True)
    plt.savefig(PLOT_DIR / "rf_f1_score_trend.png")
    plt.close()
    
    print(f"Plots saved to {PLOT_DIR}")

if __name__ == "__main__":
    plot_metrics()
