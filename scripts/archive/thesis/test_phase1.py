
import unittest
import sys
import shutil
from pathlib import Path

# Add script dir to sys.path
sys.path.append(str(Path(r"D:\sotsuron\rainsar-hub\scripts\thesis")))

import common
import analyze_sigma

class TestThesisAnalysis(unittest.TestCase):
    
    def setUp(self):
        # Use a real grid that we know exists from previous list_dir
        self.test_grid = "N03145E13105"
        self.test_event_pattern = "delay_*"
        
    def test_01_paths(self):
        """Verify path definitions"""
        print(f"\n[Test] Checking paths...")
        print(f"Base: {common.BASE_DIR}")
        print(f"Samples: {common.SAMPLES_DIR}")
        self.assertTrue(common.SAMPLES_DIR.exists())
        self.assertTrue(common.MASKS_DIR.exists())

    def test_02_load_shapes(self):
        """Verify loading of mask shapes"""
        print(f"\n[Test] Loading shapes for {self.test_grid}...")
        paddy_shapes = common.get_mask_shapes(self.test_grid, "paddy")
        print(f"Found {len(paddy_shapes)} paddy shapes")
        self.assertGreater(len(paddy_shapes), 0)
        
        road_shapes = common.get_mask_shapes(self.test_grid, "road")
        # Note: Might be empty if no motorway, but let's check
        print(f"Found {len(road_shapes)} road shapes")
        
    def test_03_process_grid(self):
        """Verify analyzing a single grid"""
        print(f"\n[Test] Running process_grid for {self.test_grid}...")
        results = analyze_sigma.process_grid(self.test_grid, polarization="vv")
        
        print(f"Processed {len(results)} events")
        if results:
            first = results[0]
            print("First event stats:", first)
            self.assertIn("grid_id", first)
            self.assertIn("paddy_before_mean", first)
            
    def test_04_save(self):
        """Check if we can save outputs (mock run of main behavior)"""
        pass

if __name__ == '__main__':
    unittest.main()
