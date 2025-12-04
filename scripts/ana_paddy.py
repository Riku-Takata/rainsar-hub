import os
import re
import zipfile
import tempfile
import shutil
import glob
import numpy as np
import pandas as pd
import rasterio
from rasterio.mask import mask
import geopandas as gpd
from shapely.geometry import box

# --- 設定項目 ---
BASE_DIR = r"D:\sotsuron"
GRID_ID = "N03285E13005"
SAMPLES_DIR = os.path.join(BASE_DIR, "s1_samples", GRID_ID)
SUMMARY_PATH = os.path.join(BASE_DIR, "s1_safe", GRID_ID, "summary_delay.txt")

# ★筆ポリゴンのZIPファイルが入っている「フォルダ」のパス
FUDE_POLYGON_DIR = r"D:\sotsuron\fude-polygon"

DELAY_THRESHOLD = 2.0
PADDY_EROSION_METER = -2.0

class FudeMultiLoader:
    """
    指定ディレクトリ内のすべてのZIPファイルを探索し、
    対象エリアに含まれる田んぼポリゴンを統合して返すクラス
    """
    def __init__(self, fude_dir):
        self.fude_dir = fude_dir
        self.zip_files = glob.glob(os.path.join(fude_dir, "*.zip"))
        print(f"フォルダ参照: {fude_dir}")
        print(f"検出されたZIPファイル: {len(self.zip_files)} 個")

    def get_paddy_gdf(self, bounds):
        """
        全ZIPファイルを順に解凍・探索し、指定範囲(bounds)にある田んぼを結合して返す
        """
        if not self.zip_files:
            print("エラー: .zipファイルが見つかりません。")
            return None

        west, south, east, north = bounds
        target_box = box(west, south, east, north)
        
        all_paddy_gdfs = []
        
        # 各ZIPファイルに対して処理を実行
        for i, zip_path in enumerate(self.zip_files):
            print(f"[{i+1}/{len(self.zip_files)}] 探索中: {os.path.basename(zip_path)} ...")
            
            # ZIPごとに一時フォルダを作成して解凍（終わったら即削除して容量節約）
            with tempfile.TemporaryDirectory() as temp_dir:
                try:
                    with zipfile.ZipFile(zip_path, 'r') as z:
                        z.extractall(temp_dir)
                    
                    # 解凍した中身からGeoJSONを探す
                    json_files = []
                    for root, dirs, files in os.walk(temp_dir):
                        for file in files:
                            if file.lower().endswith(('.json', '.geojson')):
                                json_files.append(os.path.join(root, file))
                    
                    if not json_files:
                        continue

                    # 各GeoJSONをチェック
                    for jf in json_files:
                        try:
                            # 範囲フィルタ付きで読み込み（高速化）
                            sub_gdf = gpd.read_file(jf, bbox=target_box)
                            
                            if sub_gdf.empty:
                                continue

                            # 座標系補正
                            if sub_gdf.crs is None:
                                sub_gdf.set_crs("EPSG:6668", inplace=True) # JGD2011仮定
                            
                            if sub_gdf.crs != "EPSG:4326":
                                sub_gdf = sub_gdf.to_crs("EPSG:4326")

                            # 田んぼ抽出 (カラム名の揺らぎ吸収)
                            target_col = None
                            for col in sub_gdf.columns:
                                if col.lower() in ['land_cat', 'chi_moku', 'code', 'fude_type', '地目']:
                                    target_col = col
                                    break
                            
                            if target_col:
                                # 100=田
                                paddys = sub_gdf[sub_gdf[target_col] == 100]
                                if not paddys.empty:
                                    # クリップしてリストに追加
                                    clipped = paddys.clip(target_box)
                                    if not clipped.empty:
                                        all_paddy_gdfs.append(clipped)
                                        
                        except Exception:
                            continue
                            
                except Exception as e:
                    print(f"  -> ZIP読み込みエラー (スキップ): {e}")

        if not all_paddy_gdfs:
            return None
        
        print(f"全ZIPの探索終了。{len(all_paddy_gdfs)} 区画のデータを結合します...")
        return pd.concat(all_paddy_gdfs, ignore_index=True)

# --- 解析ロジック ---
def analyze(loader, entries):
    results = []
    paddy_mask_gdf = None
    
    for entry in entries:
        if entry['delay'] >= DELAY_THRESHOLD:
            continue
            
        path_before = os.path.join(SAMPLES_DIR, entry['before'])
        path_after = os.path.join(SAMPLES_DIR, entry['after'])
        
        if not (os.path.exists(path_before) and os.path.exists(path_after)):
            continue
            
        try:
            with rasterio.open(path_before) as src:
                # 初回のみ田んぼポリゴンを取得・生成
                if paddy_mask_gdf is None:
                    print(f"画像範囲 {src.bounds} の農地データを全ZIPから収集中...")
                    raw_gdf = loader.get_paddy_gdf(src.bounds)
                    
                    if raw_gdf is None:
                        print("指定範囲内に田んぼデータが見つかりませんでした。")
                        return pd.DataFrame()
                    
                    print(f"データ抽出完了。エロージョン処理（{PADDY_EROSION_METER}m）を実行中...")
                    utm_crs = raw_gdf.estimate_utm_crs()
                    
                    # バッファ処理と結合
                    buffered = raw_gdf.to_crs(utm_crs).geometry.buffer(PADDY_EROSION_METER)
                    try:
                        paddy_poly = buffered.union_all()
                    except AttributeError:
                        paddy_poly = buffered.unary_union

                    if paddy_poly.is_empty:
                        print("エロージョン処理ですべてのポリゴンが消滅しました。")
                        return pd.DataFrame()

                    paddy_mask_gdf = gpd.GeoDataFrame(geometry=[paddy_poly], crs=utm_crs).to_crs("EPSG:4326")
                    print("田んぼマスク作成完了。解析を開始します。")

                # マスク適用
                img_before, _ = mask(src, paddy_mask_gdf.geometry, crop=True, nodata=np.nan)
            
            with rasterio.open(path_after) as src_after:
                img_after, _ = mask(src_after, paddy_mask_gdf.geometry, crop=True, nodata=np.nan)
                
            # 差分計算
            arr_before = img_before[0]
            arr_after = img_after[0]
            diff = arr_after - arr_before
            
            valid = ~np.isnan(diff)
            total = np.sum(valid)
            
            if total > 0:
                decreased = np.sum((diff < 0) & valid)
                ratio = decreased / total
                res = {
                    'Date_Before': entry['before'],
                    'Delay_Hours': entry['delay'],
                    'Decrease_Ratio': ratio,
                    'Pixel_Count': total
                }
                results.append(res)
                print(f"解析成功: Delay {entry['delay']}h -> 減少率 {ratio:.2%} ({total}px)")

        except Exception as e:
            print(f"エラー: {e}")

    return pd.DataFrame(results)

def parse_summary_text(filepath):
    # Summary読み込み用 (変更なし)
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
                current_entry[key] = scene_match.group(2) + "_proc.tif"
        if 'delay' in current_entry and 'after' in current_entry:
            entries.append(current_entry)
    except FileNotFoundError:
        return []
    return entries

if __name__ == "__main__":
    if not os.path.exists(FUDE_POLYGON_DIR):
        print(f"フォルダが見つかりません: {FUDE_POLYGON_DIR}")
    else:
        # ディレクトリローダーの初期化
        loader = FudeMultiLoader(FUDE_POLYGON_DIR)
        
        entries = parse_summary_text(SUMMARY_PATH)
        if entries:
            df = analyze(loader, entries)
            if not df.empty:
                out_name = f"result_paddy_fude_{GRID_ID}.csv"
                save_path = os.path.join(BASE_DIR, out_name)
                df.to_csv(save_path, index=False)
                print(f"\n完了！結果を保存しました: {save_path}")
            else:
                print("解析結果が0件でした。")
        else:
            print("Summaryファイルの解析に失敗しました。")