import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from sklearn.metrics import confusion_matrix, classification_report, roc_curve, auc
import rasterio

# Config
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"
PARAMS_CSV = DATA_DIR / "expanded" / "analysis" / "drainage_analysis" / "drainage_parameters.csv"
OUT_DIR = DATA_DIR / "expanded" / "analysis" / "classification"

# Mask directories
EXPANDED_SAMPLES = DATA_DIR / "expanded" / "samples"
FINAL_SAMPLES = DATA_DIR / "final" / "samples"

OUT_DIR.mkdir(parents=True, exist_ok=True)

def get_ground_truth_labels(grid_ids):
    """
    Extract ground truth labels from actual mask pixel counts.
    Label = 1 (Road-dominant) if road_pixels > paddy_pixels
    Label = 0 (Paddy-dominant) if paddy_pixels > road_pixels
    """
    
    labels = []
    
    for grid_id in grid_ids:
        # Search in both expanded and final datasets
        found = False
        
        for samples_dir in [EXPANDED_SAMPLES, FINAL_SAMPLES]:
            grid_path = samples_dir / grid_id
            if not grid_path.exists():
                continue
                
            # Find any event folder (all should have the same masks)
            event_folders = [d for d in grid_path.iterdir() if d.is_dir() and d.name.startswith("delay_")]
            
            if not event_folders:
                continue
            
            # Use first event folder
            event_dir = event_folders[0]
            road_mask_path = event_dir / "mask_road.tif"
            paddy_mask_path = event_dir / "mask_paddy.tif"
            
            if not (road_mask_path.exists() and paddy_mask_path.exists()):
                continue
            
            try:
                with rasterio.open(road_mask_path) as src:
                    road_mask = src.read(1)
                    road_pixels = (road_mask == 1).sum()
                
                with rasterio.open(paddy_mask_path) as src:
                    paddy_mask = src.read(1)
                    paddy_pixels = (paddy_mask == 1).sum()
                
                # Determine label based on pixel majority
                if road_pixels > paddy_pixels:
                    label = 1  # Road-dominant
                elif paddy_pixels > road_pixels:
                    label = 0  # Paddy-dominant
                else:
                    label = np.nan  # Equal (neutral)
                
                labels.append({
                    'grid_id': grid_id,
                    'road_pixels': road_pixels,
                    'paddy_pixels': paddy_pixels,
                    'total_pixels': road_pixels + paddy_pixels,
                    'road_ratio': road_pixels / (road_pixels + paddy_pixels) if (road_pixels + paddy_pixels) > 0 else 0,
                    'ground_truth': label
                })
                
                found = True
                break
                
            except Exception as e:
                print(f"Error reading masks for {grid_id}: {e}")
                continue
        
        if not found:
            labels.append({
                'grid_id': grid_id,
                'road_pixels': np.nan,
                'paddy_pixels': np.nan,
                'total_pixels': np.nan,
                'road_ratio': np.nan,
                'ground_truth': np.nan
            })
    
    return pd.DataFrame(labels)

def classify_by_drainage_score(df_params, threshold=0.0):
    """
    Classify grids based on drainage score.
    Predicted = 1 (Road-dominant) if drainage_score > threshold
    Predicted = 0 (Paddy-dominant) if drainage_score < threshold
    """
    
    df_params['predicted'] = np.where(
        df_params['drainage_score'] > threshold,
        1,  # Road-dominant
        0   # Paddy-dominant
    )
    
    # Handle neutrals (score == threshold)
    df_params.loc[df_params['drainage_score'] == threshold, 'predicted'] = np.nan
    
    return df_params

def evaluate_classification(df_merged):
    """
    Evaluate classification performance.
    """
    
    # Remove NaN values
    df_valid = df_merged.dropna(subset=['ground_truth', 'predicted'])
    
    if len(df_valid) == 0:
        print("Warning: No valid samples for evaluation!")
        return None
    
    y_true = df_valid['ground_truth'].astype(int).values
    y_pred = df_valid['predicted'].astype(int).values
    
    # Confusion Matrix
    cm = confusion_matrix(y_true, y_pred)
    
    # Classification Report
    report = classification_report(
        y_true, y_pred,
        target_names=['Paddy-dominant', 'Road-dominant'],
        output_dict=True
    )
    
    # Overall accuracy
    accuracy = (y_true == y_pred).sum() / len(y_true)
    
    return {
        'confusion_matrix': cm,
        'classification_report': report,
        'accuracy': accuracy,
        'n_samples': len(df_valid)
    }

def plot_confusion_matrix(cm, out_dir):
    """
    Plot confusion matrix.
    """
    
    fig, ax = plt.subplots(figsize=(8, 6))
    
    sns.heatmap(
        cm, annot=True, fmt='d', cmap='Blues',
        xticklabels=['Paddy-dominant', 'Road-dominant'],
        yticklabels=['Paddy-dominant', 'Road-dominant'],
        ax=ax,
        cbar_kws={'label': 'Count'}
    )
    
    ax.set_xlabel('Predicted Label', fontsize=12)
    ax.set_ylabel('True Label', fontsize=12)
    ax.set_title('Confusion Matrix: Drainage-Based Classification', fontsize=14, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(out_dir / 'confusion_matrix.png', dpi=150)
    plt.close()

def plot_score_distribution(df_merged, out_dir):
    """
    Plot drainage score distribution by ground truth label.
    """
    
    df_valid = df_merged.dropna(subset=['ground_truth'])
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    # Histogram
    axes[0].hist(
        df_valid[df_valid['ground_truth'] == 0]['drainage_score'],
        bins=20, alpha=0.6, label='Paddy-dominant (GT)', color='green', edgecolor='black'
    )
    axes[0].hist(
        df_valid[df_valid['ground_truth'] == 1]['drainage_score'],
        bins=20, alpha=0.6, label='Road-dominant (GT)', color='red', edgecolor='black'
    )
    axes[0].axvline(0, color='black', linestyle='--', linewidth=2, label='Classification Threshold')
    axes[0].set_xlabel('Drainage Score')
    axes[0].set_ylabel('Frequency')
    axes[0].set_title('Drainage Score Distribution by Ground Truth')
    axes[0].legend()
    axes[0].grid(alpha=0.3)
    
    # Box Plot
    df_valid['label_name'] = df_valid['ground_truth'].map({0: 'Paddy-dominant', 1: 'Road-dominant'})
    
    sns.boxplot(
        data=df_valid, x='label_name', y='drainage_score',
        palette=['green', 'red'],
        ax=axes[1]
    )
    axes[1].axhline(0, color='black', linestyle='--', linewidth=2, alpha=0.5)
    axes[1].set_xlabel('Ground Truth Label')
    axes[1].set_ylabel('Drainage Score')
    axes[1].set_title('Drainage Score by Ground Truth')
    axes[1].grid(alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig(out_dir / 'score_distribution_by_label.png', dpi=150)
    plt.close()

def plot_roc_curve(df_merged, out_dir):
    """
    Plot ROC curve for drainage score as classifier.
    """
    
    df_valid = df_merged.dropna(subset=['ground_truth', 'drainage_score'])
    
    if len(df_valid) == 0:
        print("Warning: No valid samples for ROC curve!")
        return
    
    y_true = df_valid['ground_truth'].astype(int).values
    y_score = df_valid['drainage_score'].values
    
    fpr, tpr, thresholds = roc_curve(y_true, y_score)
    roc_auc = auc(fpr, tpr)
    
    fig, ax = plt.subplots(figsize=(8, 6))
    
    ax.plot(fpr, tpr, color='darkorange', lw=2, label=f'ROC curve (AUC = {roc_auc:.3f})')
    ax.plot([0, 1], [0, 1], color='navy', lw=2, linestyle='--', label='Random Classifier')
    ax.set_xlim([0.0, 1.0])
    ax.set_ylim([0.0, 1.05])
    ax.set_xlabel('False Positive Rate', fontsize=12)
    ax.set_ylabel('True Positive Rate', fontsize=12)
    ax.set_title('ROC Curve: Drainage Score as Classifier', fontsize=14, fontweight='bold')
    ax.legend(loc="lower right", fontsize=10)
    ax.grid(alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(out_dir / 'roc_curve.png', dpi=150)
    plt.close()
    
    return roc_auc

def generate_classification_report(df_merged, eval_results, roc_auc, out_dir):
    """
    Generate a comprehensive classification report.
    """
    
    with open(out_dir / 'classification_report.md', 'w', encoding='utf-8') as f:
        f.write("# 透水特性による土地被覆判別レポート\n\n")
        
        f.write("## 1. 判別手法\n\n")
        f.write("- **分類基準**: Drainage Score (透水スコア)\n")
        f.write("- **閾値**: 0.0\n")
        f.write("  - Score > 0 → **道路優勢** (高排水性)\n")
        f.write("  - Score < 0 → **田んぼ優勢** (低排水性・保水性)\n\n")
        
        f.write("## 2. Ground Truth（正解ラベル）の定義\n\n")
        f.write("- 各グリッドの実際のマスクピクセル数を集計\n")
        f.write("- Road Pixels > Paddy Pixels → **道路優勢**\n")
        f.write("- Paddy Pixels > Road Pixels → **田んぼ優勢**\n\n")
        
        if eval_results:
            f.write("## 3. 判別精度\n\n")
            f.write(f"- **Overall Accuracy**: {eval_results['accuracy']:.3f} ({eval_results['accuracy']*100:.1f}%)\n")
            f.write(f"- **有効サンプル数**: {eval_results['n_samples']}グリッド\n")
            f.write(f"- **ROC AUC**: {roc_auc:.3f}\n\n")
            
            f.write("### 混同行列\n\n")
            cm = eval_results['confusion_matrix']
            f.write(f"|  | Predicted: Paddy | Predicted: Road |\n")
            f.write(f"|---|---|---|\n")
            f.write(f"| **True: Paddy** | {cm[0,0]} (TN) | {cm[0,1]} (FP) |\n")
            f.write(f"| **True: Road** | {cm[1,0]} (FN) | {cm[1,1]} (TP) |\n\n")
            
            f.write("### クラス別精度\n\n")
            report = eval_results['classification_report']
            
            f.write("| Class | Precision | Recall | F1-Score | Support |\n")
            f.write("|---|---|---|---|---|\n")
            
            for label, metrics in report.items():
                if label in ['Paddy-dominant', 'Road-dominant']:
                    f.write(f"| {label} | {metrics['precision']:.3f} | {metrics['recall']:.3f} | {metrics['f1-score']:.3f} | {int(metrics['support'])} |\n")
            
            f.write("\n")
            
        f.write("## 4. 誤分類の分析\n\n")
        
        df_valid = df_merged.dropna(subset=['ground_truth', 'predicted'])
        df_misclassified = df_valid[df_valid['ground_truth'] != df_valid['predicted']]
        
        f.write(f"- **誤分類数**: {len(df_misclassified)}グリッド\n\n")
        
        if len(df_misclassified) > 0:
            f.write("### 誤分類グリッド（上位5件）\n\n")
            
            # Calculate misclassification severity (absolute distance from threshold)
            df_misclassified['severity'] = abs(df_misclassified['drainage_score'])
            top_errors = df_misclassified.nlargest(5, 'severity')[
                ['grid_id', 'drainage_score', 'ground_truth', 'predicted', 'road_ratio']
            ]
            
            f.write(top_errors.to_markdown(index=False))
            f.write("\n\n")
        
        f.write("## 5. 結論\n\n")
        
        if eval_results and eval_results['accuracy'] >= 0.7:
            f.write(f"- 透水特性パラメータによる判別は **有効** です（精度 {eval_results['accuracy']*100:.1f}%）。\n")
        elif eval_results:
            f.write(f"- 透水特性パラメータによる判別は **部分的に有効** です（精度 {eval_results['accuracy']*100:.1f}%）。\n")
        
        f.write("- 各グリッドには道路と田んぼが混在しているため、「優勢な土地被覆」の推定として解釈すべきです。\n")
        f.write("- 今後、ピクセル単位の分類や、より詳細な地物分類への拡張が期待されます。\n")

def main():
    print("Loading drainage parameters...")
    df_params = pd.read_csv(PARAMS_CSV)
    
    print(f"Loaded {len(df_params)} grids.")
    
    print("\nExtracting ground truth labels from masks...")
    df_truth = get_ground_truth_labels(df_params['grid_id'].values)
    
    print(f"Ground truth extracted for {df_truth['ground_truth'].notna().sum()} grids.")
    
    print("\nClassifying grids by drainage score...")
    df_params = classify_by_drainage_score(df_params, threshold=0.0)
    
    # Merge
    df_merged = df_params.merge(df_truth, on='grid_id', how='left')
    
    # Save merged data
    df_merged.to_csv(OUT_DIR / 'classification_results.csv', index=False)
    print(f"\nSaved classification results to {OUT_DIR / 'classification_results.csv'}")
    
    print("\nEvaluating classification performance...")
    eval_results = evaluate_classification(df_merged)
    
    if eval_results:
        print("\n" + "="*60)
        print("CLASSIFICATION PERFORMANCE")
        print("="*60)
        print(f"Accuracy: {eval_results['accuracy']:.3f} ({eval_results['accuracy']*100:.1f}%)")
        print(f"Valid Samples: {eval_results['n_samples']}")
        print("\nConfusion Matrix:")
        print(eval_results['confusion_matrix'])
        print("="*60)
        
        print("\nGenerating visualizations...")
        plot_confusion_matrix(eval_results['confusion_matrix'], OUT_DIR)
        plot_score_distribution(df_merged, OUT_DIR)
        roc_auc = plot_roc_curve(df_merged, OUT_DIR)
        
        print("\nGenerating classification report...")
        generate_classification_report(df_merged, eval_results, roc_auc, OUT_DIR)
    else:
        print("\nWarning: Could not evaluate classification (insufficient data).")
        generate_classification_report(df_merged, None, 0, OUT_DIR)
    
    print(f"\nDone! Results saved to {OUT_DIR}")

if __name__ == "__main__":
    main()
