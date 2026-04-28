import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import argparse
import common

logger = common.setup_logger("compare_pol")

def load_stats(polarization):
    # Load sigma stats
    sigma_path = common.RESULT_DIR / polarization.lower() / "sigma" / f"all_grids_sigma_stats_{polarization.lower()}.csv"
    # Load diff stats
    diff_path = common.RESULT_DIR / polarization.lower() / "diff" / f"diff_stats_{polarization.lower()}.csv"
    
    sigma_df = pd.read_csv(sigma_path) if sigma_path.exists() else None
    diff_df = pd.read_csv(diff_path) if diff_path.exists() else None
    
    return sigma_df, diff_df

def compare_polarizations():
    logger.info("Loading VV Data...")
    vv_sigma, vv_diff = load_stats("vv")
    logger.info("Loading VH Data...")
    vh_sigma, vh_diff = load_stats("vh")
    
    if vv_diff is None or vh_diff is None:
        logger.error("Missing diff stats for one or both polarizations.")
        return

    # Merge on Grid + Event
    merged = pd.merge(
        vv_diff, 
        vh_diff, 
        on=["grid_id", "event_name"], 
        suffixes=("_vv", "_vh")
    )
    
    if merged.empty:
        logger.warning("No overlapping events found between VV and VH.")
        return

    output_dir = common.RESULT_DIR / "comparison"
    output_dir.mkdir(exist_ok=True, parents=True)
    
    # 1. Scatter Plot: VV Diff vs VH Diff (Paddy)
    plt.figure(figsize=(8,8))
    if "paddy_diff_mean_vv" in merged.columns and "paddy_diff_mean_vh" in merged.columns:
        sns.scatterplot(data=merged, x="paddy_diff_mean_vv", y="paddy_diff_mean_vh", alpha=0.6)
        plt.plot([-5, 5], [-5, 5], 'r--') # 1:1 line
        plt.title("Paddy Diff Mean: VV vs VH")
        plt.xlabel("VV Diff (dB)")
        plt.ylabel("VH Diff (dB)")
        plt.savefig(output_dir / "paddy_diff_vv_vs_vh.png")
        plt.close()
    
    # 2. Road Diff Comparison
    plt.figure(figsize=(8,8))
    if "road_diff_mean_vv" in merged.columns and "road_diff_mean_vh" in merged.columns:
        sns.scatterplot(data=merged, x="road_diff_mean_vv", y="road_diff_mean_vh", color="gray", alpha=0.6)
        plt.plot([-5, 5], [-5, 5], 'r--')
        plt.title("Road Diff Mean: VV vs VH")
        plt.xlabel("VV Diff (dB)")
        plt.ylabel("VH Diff (dB)")
        plt.savefig(output_dir / "road_diff_vv_vs_vh.png")
        plt.close()

    # 3. Correlation Stats
    corr_paddy = merged["paddy_diff_mean_vv"].corr(merged["paddy_diff_mean_vh"])
    logger.info(f"Paddy Diff Correlation (VV-VH): {corr_paddy:.4f}")
    
    # Save Merged
    merged.to_csv(output_dir / "vv_vh_comparison.csv", index=False)
    logger.info(f"Saved comparison to {output_dir / 'vv_vh_comparison.csv'}")

if __name__ == "__main__":
    compare_polarizations()
