import pandas as pd

df = pd.read_csv(r"D:\sotsuron\rainsar-hub\data\result\seasonal\rf_data\rf_dataset_balanced.csv")

print("=== Balanced Dataset Class Counts ===")
print(df['label'].value_counts())
print()

print("=== By Month/Delay ===")
for (m, d), g in df.groupby(['month', 'delay_bin']):
    r = len(g[g['label'] == 0])
    p = len(g[g['label'] == 1])
    print(f"M{m} D{d}: Road={r}, Paddy={p}, Match={r==p}")
