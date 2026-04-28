"""Count total shapes (parcels) for paddy and road across all grids"""
import sys
sys.path.insert(0, 'd:/sotsuron/rainsar-hub/scripts/thesis')
import common

grids = common.get_grid_ids()
total_paddy_shapes = 0
total_road_shapes = 0

for g in grids:
    ps = common.get_mask_shapes(g, 'paddy')
    rs = common.get_mask_shapes(g, 'road')
    total_paddy_shapes += len(ps)
    total_road_shapes += len(rs)

print(f"Total grids: {len(grids)}")
print(f"Total paddy shapes (筆): {total_paddy_shapes:,}")
print(f"Total road shapes: {total_road_shapes:,}")
