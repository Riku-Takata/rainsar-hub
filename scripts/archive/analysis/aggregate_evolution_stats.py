import pandas as pd
from pathlib import Path
from common_utils import setup_logger, RESULT_DIR

logger = setup_logger("aggregate_evolution")

def main():
    # List of all 13 candidate grids
    grids = [
        "N03285E13005", "N03285E13075", "N03285E13085", "N03285E13115", "N03285E13165",
        "N03295E12995", "N03295E13075", "N03295E13185",
        "N03335E13095", "N03355E13085", "N03355E13125",
        "N03375E13095", "N03385E13065"
    ]
    
    results = []
    
    for grid in grids:
        base_dir = RESULT_DIR / "pixel_variations" / grid / "outlier_evolution"
        path_paddy = base_dir / "outlier_groups_田んぼ.csv"
        path_road = base_dir / "outlier_groups_道路.csv"
        
        if not path_paddy.exists() or not path_road.exists():
            logger.warning(f"Missing data for {grid}")
            continue
            
        try:
            # 1. Calculate Threshold from Road Short
            df_road = pd.read_csv(path_road)
            if df_road.empty:
                logger.warning(f"Empty Road data for {grid}")
                continue
                
            threshold = 2 * df_road['Diff_Short'].std()
            
            # 2. Calculate Road Stability
            road_stable_count = len(df_road[abs(df_road['Diff_Long']) <= threshold]) 
            road_stability = road_stable_count / len(df_road) * 100
            
            # 3. Calculate Paddy Stats
            df_paddy = pd.read_csv(path_paddy)
            if df_paddy.empty:
                logger.warning(f"Empty Paddy data for {grid}")
                continue
                
            total_paddy = len(df_paddy)
            
            short_outliers = len(df_paddy[abs(df_paddy['Diff_Short']) > threshold])
            long_outliers = len(df_paddy[abs(df_paddy['Diff_Long']) > threshold])
            
            short_pct = short_outliers / total_paddy * 100
            long_pct = long_outliers / total_paddy * 100
            
            # 4. Classification
            # Valid: Road Stability > 90% AND Short % < 5%
            # Noisy: Road Stability < 90% OR Short % > 5%
            status = "Valid"
            if road_stability < 90 or short_pct > 5:
                status = "Noisy"
            elif long_pct < 1.0:
                status = "No Change" # Valid but no rain effect observed
            
            results.append({
                'Grid': grid,
                'Road_Stability': road_stability,
                'Paddy_Short_Outlier_Pct': short_pct,
                'Paddy_Long_Outlier_Pct': long_pct,
                'Status': status,
                'Threshold': threshold
            })
            
        except Exception as e:
            logger.error(f"Error processing {grid}: {e}")
            
    df_res = pd.DataFrame(results)
    out_path = RESULT_DIR / "large_scale_verification.csv"
    df_res.to_csv(out_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved aggregation to {out_path}")
    
    print(df_res.to_string())

if __name__ == "__main__":
    main()
