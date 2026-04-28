import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.rcParams['font.family'] = 'MS Gothic'

df = pd.read_csv(r'D:\sotsuron\rainsar-hub\data\result\seasonal\rf_results\vv_vh_comparison.csv')
m8 = df[df['month'] == 8].sort_values('delay')

plt.figure(figsize=(10, 6))
plt.plot(m8['delay'], m8['acc_vv'], 'o-', linewidth=2, markersize=8, color='blue')
plt.title('8月 VV偏波による分類精度', fontsize=14)
plt.xlabel('経過時間 (h)', fontsize=12)
plt.ylabel('テスト精度', fontsize=12)
plt.ylim(0.4, 1.0)
plt.xlim(-0.5, 11.5)
plt.xticks(range(0, 12))
plt.grid(True, alpha=0.3)

# Annotate peak
peak_idx = m8['acc_vv'].idxmax()
peak_row = m8.loc[peak_idx]
plt.annotate(f"{peak_row['acc_vv']:.3f}", 
             xy=(peak_row['delay'], peak_row['acc_vv']),
             xytext=(peak_row['delay']+0.5, peak_row['acc_vv']+0.03),
             fontsize=10, color='red')

plt.tight_layout()
plt.savefig(r'D:\sotsuron\rainsar-hub\data\result\seasonal\rf_results\plot_august_vv_only.png', dpi=150)
print('Saved: plot_august_vv_only.png')
