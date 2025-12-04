import os
import re
import numpy as np
import pandas as pd
import rasterio
from rasterio.mask import mask
import osmnx as ox
import geopandas as gpd
from shapely.geometry import box

# --- 設定項目 ---
BASE_DIR = r"D:\sotsuron"
GRID_ID = "N03285E13005"
SAMPLES_DIR = os.path.join(BASE_DIR, "s1_samples", GRID_ID)
SUMMARY_PATH = os.path.join(BASE_DIR, "s1_safe", GRID_ID, "summary_delay.txt")

DELAY_THRESHOLD = 2.0  # 2時間未満
ROAD_BUFFER_METER = 2.5 # 道路中心線からの半径(m)

def parse_summary_text(filepath):
    """ summary_delay.txtを解析する関数（変更なし） """
    entries = []
    current_entry = {}
    re_delay = re.compile(r"Delay \(Hours\)\s*:\s*([\d\.]+)")
    re_scene = re.compile(r"(After|Before) Scene\s*:\s*([A-Z0-9_]+)")

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        for line in lines:
            line = line.strip()
            if line.startswith("----"):
                if 'delay' in current_entry and 'after' in current_entry:
                    entries.append(current_entry)
                current_entry = {}
                continue
            delay_match = re_delay.search(line)
            if delay_match:
                current_entry['delay'] = float(delay_match.group(1))
            scene_match = re_scene.search(line)
            if scene_match:
                key = 'after' if scene_match.group(1) == 'After' else 'before'
                # [OK]除去や拡張子付与
                current_entry[key] = scene_match.group(2) + "_proc.tif"
        if 'delay' in current_entry and 'after' in current_entry:
            entries.append(current_entry)
    except FileNotFoundError:
        print(f"エラー: ファイルが見つかりません -> {filepath}")
        return []
    return entries

def get_road_polygons_4326(bounds):
    """
    指定座標範囲（EPSG:4326）の道路を取得し、メートル換算でバッファした後、
    再度EPSG:4326に戻して返す関数
    """
    print("OpenStreetMapから道路データを取得・加工中...")
    west, south, east, north = bounds
    
    try:
        # 修正: OSMnx v2.0以降は bbox=(west, south, east, north) で指定します
        # rasterioのboundsも(west, south, east, north)の順なのでそのまま使えます
        G = ox.graph_from_bbox(bbox=(west, south, east, north), network_type='drive')
        
        edges = ox.graph_to_gdfs(G, nodes=False)
        
        # 2. メートル単位の座標系へ投影変換
        edges_proj = edges.to_crs(edges.estimate_utm_crs())
        
        # 3. メートル単位でバッファ
        roads_poly_proj = edges_proj.geometry.buffer(ROAD_BUFFER_METER).union_all()
        
        # 4. 緯度経度に戻す
        gdf_proj = gpd.GeoDataFrame(geometry=[roads_poly_proj], crs=edges_proj.crs)
        gdf_4326 = gdf_proj.to_crs("EPSG:4326")
        
        return gdf_4326
        
    except Exception as e:
        print(f"道路データの取得・変換エラー: {e}")
        return None

def analyze_backscatter(entries):
    results = []
    road_mask_gdf = None # ループ外で保持
    
    for entry in entries:
        if entry['delay'] >= DELAY_THRESHOLD:
            continue
            
        path_before = os.path.join(SAMPLES_DIR, entry['before'])
        path_after = os.path.join(SAMPLES_DIR, entry['after'])
        
        if not (os.path.exists(path_before) and os.path.exists(path_after)):
            # print(f"ファイル不在スキップ: {entry['before']}") 
            continue
            
        try:
            with rasterio.open(path_before) as src_before:
                # 初回のみ道路データを取得（画像範囲に基づく）
                if road_mask_gdf is None:
                    # 画像がEPSG:4326であることを前提にboundsをそのまま使用
                    road_mask_gdf = get_road_polygons_4326(src_before.bounds)
                    if road_mask_gdf is None:
                        print("道路データの取得に失敗したため処理を中断します。")
                        return pd.DataFrame()

                # 道路データを使って画像をマスク読み込み
                # 画像(src_before)も道路(road_mask_gdf)もEPSG:4326で一致している必要がある
                img_before, _ = mask(src_before, road_mask_gdf.geometry, crop=True, nodata=np.nan)

            with rasterio.open(path_after) as src_after:
                img_after, _ = mask(src_after, road_mask_gdf.geometry, crop=True, nodata=np.nan)
                
            # バンド1データ抽出
            arr_before = img_before[0]
            arr_after = img_after[0]
            
            # 差分計算 (After - Before)
            diff = arr_after - arr_before
            
            # 有効ピクセル（道路かつデータあり）
            valid_mask = ~np.isnan(diff)
            total_road_pixels = np.sum(valid_mask)
            
            if total_road_pixels == 0:
                continue

            # 値が減少したピクセル
            # ノイズ除去が必要なら diff < -1.0 など調整
            decreased_count = np.sum((diff < 0) & valid_mask)
            
            ratio = decreased_count / total_road_pixels
            
            res = {
                'Date_Before': entry['before'],
                'Date_After': entry['after'],
                'Delay_Hours': entry['delay'],
                'Total_Pixels': total_road_pixels,
                'Decreased_Pixels': decreased_count,
                'Ratio': ratio
            }
            results.append(res)
            print(f"解析: Delay {entry['delay']}h -> 減少率 {ratio:.2%}")
                
        except Exception as e:
            print(f"画像処理エラー ({entry['before']}): {e}")

    return pd.DataFrame(results)

if __name__ == "__main__":
    print("処理開始...")
    entries = parse_summary_text(SUMMARY_PATH)
    if entries:
        df = analyze_backscatter(entries)
        if not df.empty:
            out_file = os.path.join(BASE_DIR, f"result_{GRID_ID}.csv")
            df.to_csv(out_file, index=False)
            print(f"完了。保存先: {out_file}")
            # print(df)
        else:
            print("結果データなし")
    else:
        print("Summaryファイルの解析に失敗、またはデータなし")