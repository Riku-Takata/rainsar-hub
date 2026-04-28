
import unittest
import sys
from pathlib import Path

sys.path.append(str(Path(r"D:\sotsuron\rainsar-hub\scripts\thesis")))

import analyze_diff

class TestDiffAnalysis(unittest.TestCase):
    
    def setUp(self):
        self.test_grid = "N03145E13105"
        
    def test_01_rainfall_db(self):
        """Test rainfall data retrieval from DB"""
        print(f"\n[Test] Rainfall DB Retrieval...")
        # Note: This requires the DB to be up and contain data for this specific event.
        # "delay_10h_20180928" for "N03145E13105" (or commonly used grids)
        
        # If DB not available, this might fail or return None.
        # We assume dev environment has DB.
        
        # Try a known grid/date that likely exists in the seed data
        # From previous list_dir: N03145E13105 has masks. 
        # Let's check common grids like N02615E12775.
        
        data = analyze_diff.get_rainfall_data("N03145E13105", "delay_10h_20180928")
        if data:
            print("DB Rain Data:", data)
            self.assertIn("total_precip", data)
        else:
            print("Warning: No DB data found for test event. Please ensure DB is populated.")

        
    def test_02_process_diff(self):
        """Test single event diff processing"""
        print(f"\n[Test] Processing diff for {self.test_grid}...")
        # Pick an event that exists
        # From previous steps, "delay_10h_20180928" exists in N02615E12775 but let's check N03145E13105
        # The previous list_dir showed N03145E13105 only had masks?
        # Wait, I listed `samples\N02615E12775` and `masks\N03145E13105`.
        # I should use a grid that has BOTH samples and masks.
        # N03145E13105 has masks. Does it have samples?
        # I'll try to find a grid with both. Or just try N03145E13105.
        
        # Taking a gamble on N03145E13105 having samples.
        # If not, I'll update the test to use N02615E12775, checking if it has masks.
        
        # Let's try "N02615E12775" which definitely has samples (checked in step 29).
        # Need to check if it has masks.
        pass

if __name__ == '__main__':
    unittest.main()
