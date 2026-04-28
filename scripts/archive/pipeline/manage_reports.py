import pandas as pd
from pathlib import Path
from common_utils import RESULT_DIR

class ReportManager:
    def __init__(self, result_dir=RESULT_DIR):
        self.result_dir = result_dir
        self.result_md_path = self.result_dir / "result.md"
        self.summary_md_path = self.result_dir / "summary.md"

    def write_initial_report(self):
        """result.md を初期化・作成する"""
        content = """# 後方散乱強度解析結果まとめ

本ドキュメントは、Sentinel-1衛星画像を用いた降雨イベント前後の後方散乱強度変化に関する解析結果をまとめたものです。

## 1. 解析手法の概要

### データ処理プロセス
1.  **後方散乱強度の算出**: 各衛星画像（S1_Safe）から、1ピクセルごとの後方散乱強度（リニア値）を算出。
2.  **差分算出**: 降雨イベントごとのペア画像（After - Before）について、ピクセル単位で強度の差分を計算。
3.  **統計量の算出**: 
    *   対象領域: **道路（Road）** および **田んぼ（Paddy）**
    *   算出指標: 平均値 (Mean)、中央値 (Median)、標準偏差 (Std/Variance)、四分位範囲 (IQR)
    *   結果格納場所: `result/distributions/{GridID}/{Delay}h/` 内のCSVファイル

## 2. 全体データの解析 (Analysis V1)

全Grid・全イベントの統計データを集約し、全体的な傾向とデータの質を評価しました。

### ノイズデータの特定
統計的分布（特に標準偏差の四分位範囲）から大きく外れるデータを「ノイズ（異常値）」として特定しました。

*   **Grid N03355E13125**: 
    *   **理由**: 道路領域の標準偏差（ばらつき）が常に高い値（> 0.24）を示しました。
    *   **考察**: 道路の形状が複雑、あるいは位置合わせのズレなどが原因で、参照データとして不適当と判断しました。
*   **Grid N03335E13095 (Delay 9.36h)**:
    *   **理由**: 平均値が極端に低い異常値を示しました。

### データ全体の傾向
*   **ばらつきの違い**: 田んぼは道路に比べて、後方散乱強度のばらつき（標準偏差）が約2.5〜3倍大きいことが判明しました。
*   **時間依存性**: 遅延時間（Delay）が長くなるほど、特に田んぼのばらつきが増大する傾向が見られました。

## 3. 有効データ選別と指標の再検討 (Analysis V2)

ノイズデータを除外し、より堅牢な解析を行うために「有効データ（Valid Data）」のみを用いた再解析を行いました。また、解析指標として「平均値」と「中央値」のどちらが適切かを検証しました。

### 平均値 vs 中央値
SARデータにはスペックルノイズや強い反射点（コーナーリフレクタ等）による外れ値が含まれやすいため、平均値はそれらに引きずられる傾向があります。

*   **検証結果**: 
    *   **平均値 (Mean)**: 外れ値の影響で、道路の変化量が実際よりも大きく見積もられる傾向がありました。
    *   **中央値 (Median)**: 分布の中央を見るため外れ値に強く、道路と田んぼの変化の違い（シグナル）をより明確に分離できました。
*   **結論**: 本解析においては、**「中央値」**を主要な指標として採用すべきであると結論付けました。

## 4. 詳細分析結果 (中央値ベース)

有効データを用い、中央値を指標として「道路」と「田んぼ」の挙動、および時間経過による変化を詳細に分析しました。

### 道路と田んぼの傾向の違い
*   **道路 (Road)**:
    *   **特徴**: 降雨後も変化はわずか（微増）で、ばらつきも非常に小さい。
    *   **役割**: 「降雨による表面の濡れ」や「大気減衰」などの環境要因を表す**ベースライン（基準）**として機能します。
*   **田んぼ (Paddy)**:
    *   **特徴**: 道路よりも明確に大きな増加を示し、ばらつきも大きい。
    *   **役割**: 道路の変化分を差し引いた「上乗せ分」が、土壌への浸透（透水）による誘電率変化を示唆します。

### 短時間 vs 長時間の変化特性

| 時間区分 | 道路の変化 (中央値) | 田んぼの変化 (中央値) | 差分 (透水シグナル) | 状態の解釈 |
| :--- | :--- | :--- | :--- | :--- |
| **短時間 (≦ 1.5h)** | **+0.0021** (微増) | **+0.0053** (増加) | **+0.0032** | **透水・浸透が進行中**。<br>シグナルが最も強く、ノイズも最小。 |
| **長時間 (> 6h)** | ほぼ 0 | 微増 | 縮小 | **乾燥・排水**。<br>田んぼのばらつきが増大し、シグナル検出は困難。 |

### 知見のまとめ
1.  **透水シグナルの抽出**: 短時間（1.5時間以内）のデータにおいて、田んぼの変化量から道路の変化量を差し引くことで、約 **+0.0032 (Linear)** の透水由来と思われるシグナルが検出されました。
2.  **解析の適正条件**:
    *   **データ選別**: 道路のばらつきが大きいGridは除外する。
    *   **指標**: 中央値を使用する。
    *   **時間枠**: 降雨直後〜1.5時間以内のデータが最も信頼性が高い。長時間のデータは環境ノイズ（風など）の影響が大きいため、透水性推定には不向きである。
"""
        with open(self.result_md_path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"Successfully wrote to {self.result_md_path}")

    def append_negative_analysis_section(self):
        """負の差分に関する追加解析セクションを追記する"""
        append_content = """
## 5. 追加解析: 負の差分（Negative Diff）に着目した分析

「降雨により後方散乱強度が低下する（差分がマイナスになる）」という仮説に基づき、短時間（<1時間）のイベントにおいて、差分が負（マイナス）になったピクセルのみを抽出して解析しました。

### 解析結果 (<1時間, 負のピクセルのみ)

| 対象 | 負ピクセルの割合 | 平均差分 (Mean) | 中央値差分 (Median) | 標準偏差 (Std) |
| :--- | :--- | :--- | :--- | :--- |
| **道路** | 30.5% | -0.029 | **-0.008** | 0.069 |
| **田んぼ** | 24.2% | -0.128 | **-0.096** | 0.175 |

### 考察
1.  **道路の負ピクセル**: 中央値が **-0.008** と非常にゼロに近く、これは単なる測定ノイズ（ゼロ付近のゆらぎ）である可能性が高いです。
2.  **田んぼの負ピクセル**: 中央値が **-0.096** と、道路に比べて**桁違いに大きな減少**を示しています。
    *   **物理的解釈**: 田んぼ全体の約24%のピクセルで、降雨により後方散乱強度が劇的に低下しています。これは、田んぼに水が溜まり、鏡面反射（Specular Reflection）が起きて電波が前方に散乱してしまった（レーダーに戻ってこない）可能性を示唆しています。
3.  **透水性推定への示唆**:
    *   従来の「全体の中央値（正の増加）」は土壌水分の増加（誘電率上昇）を捉えていますが、この「負のピクセル群」は**冠水・浸水**を捉えている可能性があります。
    *   透水性（水が染み込む速度）を見るなら「正の変化」を、冠水リスクを見るなら「負の変化」を見るという使い分けが有効かもしれません。
"""
        if self.result_md_path.exists():
            with open(self.result_md_path, "a", encoding="utf-8") as f:
                f.write(append_content)
            print(f"Appended negative analysis to {self.result_md_path}")
        else:
            print(f"File not found: {self.result_md_path}")

    def add_csv_table_to_summary(self, csv_path, target_md_path=None):
        """CSVデータをテーブルとしてMarkdownに追加する"""
        if target_md_path is None:
            target_md_path = self.summary_md_path
            
        csv_p = Path(csv_path)
        if not csv_p.exists():
            print(f"CSV not found: {csv_p}")
            return

        df = pd.read_csv(csv_p)
        
        # Select and rename columns for display
        cols_to_show = ['Delay', 'Type', 'Time', 'Mean', 'Median', 'Variance']
        # Ensure columns exist
        available_cols = [c for c in cols_to_show if c in df.columns]
        
        if not available_cols:
            print("No matching columns found in CSV")
            return

        df = df[available_cols]

        # Sort if possible
        if 'Delay' in df.columns and 'Type' in df.columns:
            df.sort_values(by=['Delay', 'Type'], inplace=True)
        
        # Format values
        for col in ['Mean', 'Median', 'Variance']:
            if col in df.columns:
                df[col] = df[col].map('{:.5f}'.format)
        
        # Create Markdown Table
        table_md = "\n### 数値データ (Statistics Table)\n\n"
        table_md += "| " + " | ".join(available_cols) + " |\n"
        table_md += "| " + " | ".join([":---"] * len(available_cols)) + " |\n"
        
        for _, row in df.iterrows():
            table_md += "| " + " | ".join([str(row[c]) for c in available_cols]) + " |\n"
        
        if not target_md_path.exists():
            print(f"MD file not found: {target_md_path}")
            return
            
        with open(target_md_path, "r", encoding="utf-8") as f:
            content = f.read()
            
        marker = "## 2. 全体解析"
        if marker in content:
            parts = content.split(marker)
            new_content = parts[0] + table_md + "\n" + marker + parts[1]
        else:
            new_content = content + table_md

        with open(target_md_path, "w", encoding="utf-8") as f:
            f.write(new_content)
            
        print(f"Added table to {target_md_path}")

if __name__ == "__main__":
    # Example usage
    manager = ReportManager()
    # manager.write_initial_report()
    # manager.append_negative_analysis_section()
