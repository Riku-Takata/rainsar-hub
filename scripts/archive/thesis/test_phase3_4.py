
import unittest
import sys
import pandas as pd
import shutil
from pathlib import Path

sys.path.append(str(Path(r"D:\sotsuron\rainsar-hub\scripts\thesis")))

import compare_polarization
import classify_land_type
import common

class TestClassification(unittest.TestCase):
    
    def setUp(self):
        # Create Dummy CSVs for testing
        self.vv_dir = common.RESULT_DIR / "vv" / "diff"
        self.vv_dir.mkdir(parents=True, exist_ok=True)
        self.csv_path = self.vv_dir / "diff_stats_vv.csv"
        
        # Mock Data
        data = {
            "grid_id": ["N001", "N001", "N002", "N002"],
            "event_name": ["delay_1h_20200101", "delay_1h_20200201", "delay_1h_20200101", "delay_1h_20200201"],
            "paddy_diff_mean": [2.5, 3.0, 1.5, 2.0],
            "paddy_diff_std": [0.5, 0.6, 0.4, 0.5],
            "road_diff_mean": [0.1, 0.2, 0.0, 0.1],
            "road_diff_std": [0.2, 0.2, 0.1, 0.1],
            "total_precip": [50, 100, 40, 90],
            "duration": [5, 10, 4, 9]
        }
        pd.DataFrame(data).to_csv(self.csv_path, index=False)
        
    def tearDown(self):
        # Cleanup mocked file
        if self.csv_path.exists():
            self.csv_path.unlink()
            
    def test_01_prepare_dataset(self):
        print("\n[Test] Preparing Dataset...")
        df = classify_land_type.prepare_dataset("vv")
        print(df.head())
        self.assertFalse(df.empty)
        # Check label creation
        self.assertIn("label", df.columns)
        self.assertEqual(len(df[df["label"]==1]), 4) # 4 paddy rows
        self.assertEqual(len(df[df["label"]==0]), 4) # 4 road rows
        
    def test_02_train(self):
        print("\n[Test] Training Mock Model...")
        df = classify_land_type.prepare_dataset("vv")
        clf = classify_land_type.train_and_evaluate(df)
        self.assertIsNotNone(clf)

if __name__ == '__main__':
    unittest.main()
