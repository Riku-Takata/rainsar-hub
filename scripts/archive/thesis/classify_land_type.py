import pandas as pd
import numpy as np
import argparse
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
import seaborn as sns
import matplotlib.pyplot as plt
import common

logger = common.setup_logger("classify_land")

def prepare_dataset(polarization="vv"):
    diff_path = common.RESULT_DIR / polarization.lower() / "diff" / f"diff_stats_{polarization.lower()}.csv"
    if not diff_path.exists():
        logger.error(f"Diff stats not found at {diff_path}")
        return None
    
    df = pd.read_csv(diff_path)
    
    # Transform to [Feature1, Feature2, ..., Label]
    # Rows in df are Events. Columns include "paddy_diff_mean", "road_diff_mean", etc.
    # We want to classify based on observed features if it's road or paddy.
    # So we create two samples from each event:
    # 1. Features: Paddy_Diff_Mean, Rain, ... Label: PADDY
    # 2. Features: Road_Diff_Mean, Rain, ... Label: ROAD
    
    samples = []
    
    for _, row in df.iterrows():
        # Common features
        total_precip = row.get("total_precip", 0)
        duration = row.get("duration", 0)
        rain_intensity = total_precip / duration if duration > 0 else 0
        
        # Parse Month
        try:
            date_str = row["event_name"].split('_')[-1]
            month = int(date_str[4:6])
        except:
            month = 0
            
        # PADDY Sample
        if "paddy_diff_mean" in row:
            samples.append({
                "diff_mean": row["paddy_diff_mean"],
                "diff_std": row.get("paddy_diff_std", 0),
                "total_precip": total_precip,
                "rain_intensity": rain_intensity,
                "month": month,
                "label": 1 # Paddy
            })
            
        # ROAD Sample
        if "road_diff_mean" in row:
            samples.append({
                "diff_mean": row["road_diff_mean"],
                "diff_std": row.get("road_diff_std", 0),
                "total_precip": total_precip,
                "rain_intensity": rain_intensity,
                "month": month,
                "label": 0 # Road
            })
            
    return pd.DataFrame(samples)

def train_and_evaluate(data):
    if data is None or data.empty:
        logger.error("No data to train.")
        return

    # Features and Target
    X = data[["diff_mean", "diff_std", "total_precip", "rain_intensity", "month"]]
    y = data["label"]
    
    # Split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
    
    # Train
    clf = RandomForestClassifier(n_estimators=100, random_state=42)
    clf.fit(X_train, y_train)
    
    # Predict
    y_pred = clf.predict(X_test)
    
    # Evaluate
    acc = accuracy_score(y_test, y_pred)
    logger.info(f"Accuracy: {acc:.4f}")
    logger.info("\n" + classification_report(y_test, y_pred, target_names=["Road", "Paddy"]))
    
    # Feature Importance
    importances = clf.feature_importances_
    indices = np.argsort(importances)[::-1]
    logger.info("Feature Importances:")
    for f in range(X.shape[1]):
        logger.info(f"{X.columns[indices[f]]}: {importances[indices[f]]:.4f}")
        
    return clf

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--polarization", default="vv")
    args = parser.parse_args()
    
    logger.info(f"Preparing dataset for {args.polarization}...")
    data = prepare_dataset(args.polarization)
    
    logger.info("Training Classifier...")
    train_and_evaluate(data)

if __name__ == "__main__":
    main()
