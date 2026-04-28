#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
visualize_flood_events.py

PDFに記載された内水氾濫事例（H30.7月豪雨、R1.8月大雨、R1.台風19号）に対応する
降雨イベントをgsmap_pointsからクエリし、日本地図上に重ねたPNG画像とCSVを出力する。
"""

import os
import sys
from datetime import datetime, timezone

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.cm as cm
import numpy as np
import pandas as pd
import pymysql
import cartopy.crs as ccrs
import cartopy.feature as cfeature

# -------------------------------------------------------------------------
# 設定
# -------------------------------------------------------------------------
DB_CONFIG = dict(host="127.0.0.1", port=3307, user="rainsar", password="rainsar_pw", db="rainsar_hub")

OUTPUT_DIR = "output/flood_events"

# PDFに記載の3大事例 (UTC換算: JST-9h)
EVENTS = [
    {
        "id": "H30_7gou",
        "label": "平成30年7月豪雨",
        "label_en": "H30_July_2018",
        "start_jst": "2018-07-05 00:00:00",
        "end_jst":   "2018-07-09 00:00:00",
        "note": "西日本中心、岡山・広島・福岡など19道府県88市町村で内水被害",
    },
    {
        "id": "R1_8gatsu",
        "label": "令和元年8月前線大雨",
        "label_en": "R1_Aug_2019",
        "start_jst": "2019-08-26 00:00:00",
        "end_jst":   "2019-08-30 00:00:00",
        "note": "佐賀市で1時間最大110mm/h、九州北部を中心に3県14市町で浸水",
    },
    {
        "id": "R1_typhoon19",
        "label": "令和元年台風第19号",
        "label_en": "R1_Typhoon19_2019",
        "start_jst": "2019-10-10 00:00:00",
        "end_jst":   "2019-10-14 00:00:00",
        "note": "東日本中心、15都県140市区町村で内水被害、約3万戸浸水",
    },
]

# 日本の表示範囲
JAPAN_LON = (122, 150)
JAPAN_LAT = (24, 46)

# カラーマップ・閾値
CMAP = "YlOrRd"
VMIN, VMAX = 0, 200   # mm (期間合計)


# -------------------------------------------------------------------------
# DB接続ヘルパー
# -------------------------------------------------------------------------
def get_conn():
    return pymysql.connect(**DB_CONFIG)


def jst_to_utc(jst_str: str) -> str:
    """JST文字列をUTC文字列に変換"""
    dt = datetime.strptime(jst_str, "%Y-%m-%d %H:%M:%S")
    # JST = UTC+9
    utc_hour = dt.hour - 9
    dt_utc = dt.replace(hour=0) + pd.Timedelta(hours=utc_hour if utc_hour >= 0 else utc_hour + 24)
    # 簡易: 文字列として -9h
    from datetime import timedelta
    dt_utc2 = datetime.strptime(jst_str, "%Y-%m-%d %H:%M:%S") - timedelta(hours=9)
    return dt_utc2.strftime("%Y-%m-%d %H:%M:%S")


# -------------------------------------------------------------------------
# クエリ & 集計
# -------------------------------------------------------------------------
def query_event(event: dict) -> pd.DataFrame:
    start_utc = jst_to_utc(event["start_jst"])
    end_utc   = jst_to_utc(event["end_jst"])

    sql = """
        SELECT
            lat,
            lon,
            COUNT(*)              AS hours,
            SUM(gauge_mm_h)       AS total_mm,
            MAX(gauge_mm_h)       AS max_mm_h,
            AVG(gauge_mm_h)       AS mean_mm_h,
            MIN(ts_utc)           AS first_ts,
            MAX(ts_utc)           AS last_ts
        FROM gsmap_points
        WHERE ts_utc >= %(start)s
          AND ts_utc <  %(end)s
          AND gauge_mm_h > 0
          AND lat BETWEEN 24 AND 46
          AND lon BETWEEN 122 AND 150
        GROUP BY lat, lon
    """
    conn = get_conn()
    try:
        df = pd.read_sql(sql, conn, params={"start": start_utc, "end": end_utc})
    finally:
        conn.close()
    return df


# -------------------------------------------------------------------------
# 可視化
# -------------------------------------------------------------------------
def plot_event(event: dict, df: pd.DataFrame, out_path: str):
    if df.empty:
        print(f"  [WARN] {event['label']}: データなし")
        return

    fig = plt.figure(figsize=(12, 10))
    ax = plt.axes(projection=ccrs.PlateCarree())
    ax.set_extent([JAPAN_LON[0], JAPAN_LON[1], JAPAN_LAT[0], JAPAN_LAT[1]],
                  crs=ccrs.PlateCarree())

    # 地図背景
    ax.add_feature(cfeature.OCEAN, facecolor="#d6eaf8", zorder=0)
    ax.add_feature(cfeature.LAND,  facecolor="#f5f5f5", zorder=1)
    ax.add_feature(cfeature.COASTLINE, linewidth=0.6, edgecolor="gray", zorder=2)
    ax.add_feature(cfeature.BORDERS, linewidth=0.4, edgecolor="lightgray", zorder=2)
    ax.gridlines(draw_labels=True, linewidth=0.4, color="gray", alpha=0.5,
                 xlocs=range(120, 155, 5), ylocs=range(25, 48, 5))

    # 降雨プロット
    norm = mcolors.Normalize(vmin=VMIN, vmax=VMAX)
    cmap_obj = cm.get_cmap(CMAP)

    # total_mmでフィルタ（ゼロ除去）
    df_plot = df[df["total_mm"] > 0].copy()
    scatter = ax.scatter(
        df_plot["lon"], df_plot["lat"],
        c=df_plot["total_mm"].clip(upper=VMAX),
        cmap=CMAP, norm=norm,
        s=4, alpha=0.8, linewidths=0,
        transform=ccrs.PlateCarree(),
        zorder=3,
    )

    # カラーバー
    cbar = plt.colorbar(scatter, ax=ax, orientation="vertical",
                        pad=0.02, shrink=0.7, aspect=25)
    cbar.set_label("期間累計降水量 (mm)", fontsize=11)
    cbar.ax.tick_params(labelsize=9)

    # タイトル・注記
    start_jst = event["start_jst"][:10]
    end_jst   = event["end_jst"][:10]
    n_pts = len(df_plot)
    total_max = df_plot["max_mm_h"].max()

    title = (f"{event['label']}\n"
             f"{start_jst} 〜 {end_jst} (JST)")
    ax.set_title(title, fontsize=14, fontweight="bold", pad=10)

    note_text = (f"観測グリッド数: {n_pts:,}  |  "
                 f"最大時間雨量: {total_max:.1f} mm/h\n"
                 f"{event['note']}")
    fig.text(0.5, 0.01, note_text, ha="center", fontsize=9,
             bbox=dict(boxstyle="round,pad=0.3", facecolor="lightyellow", alpha=0.8))

    plt.tight_layout(rect=[0, 0.06, 1, 1])
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> PNG保存: {out_path}")


# -------------------------------------------------------------------------
# メイン
# -------------------------------------------------------------------------
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    all_dfs = []

    for event in EVENTS:
        print(f"\n[{event['label']}] クエリ中...")
        df = query_event(event)
        print(f"  グリッド数: {len(df)}")

        # CSV出力
        csv_path = os.path.join(OUTPUT_DIR, f"{event['id']}.csv")
        df_csv = df.copy()
        df_csv.insert(0, "event_id", event["id"])
        df_csv.insert(1, "event_label", event["label"])
        df_csv.insert(2, "start_jst", event["start_jst"])
        df_csv.insert(3, "end_jst", event["end_jst"])
        df_csv.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"  -> CSV保存: {csv_path}")

        # PNG出力
        png_path = os.path.join(OUTPUT_DIR, f"{event['id']}.png")
        plot_event(event, df, png_path)

        # 結合用
        all_dfs.append(df_csv)

    # 3イベント合算CSV
    combined = pd.concat(all_dfs, ignore_index=True)
    combined_csv = os.path.join(OUTPUT_DIR, "all_events_combined.csv")
    combined.to_csv(combined_csv, index=False, encoding="utf-8-sig")
    print(f"\n全イベント結合CSV: {combined_csv}")

    # 3イベント横並び比較図
    print("\n比較図を生成中...")
    fig, axes = plt.subplots(1, 3, figsize=(30, 12),
                              subplot_kw={"projection": ccrs.PlateCarree()})
    norm = mcolors.Normalize(vmin=VMIN, vmax=VMAX)

    for ax, event, df_full in zip(axes, EVENTS, [pd.read_csv(os.path.join(OUTPUT_DIR, f"{e['id']}.csv")) for e in EVENTS]):
        ax.set_extent([JAPAN_LON[0], JAPAN_LON[1], JAPAN_LAT[0], JAPAN_LAT[1]],
                      crs=ccrs.PlateCarree())
        ax.add_feature(cfeature.OCEAN, facecolor="#d6eaf8", zorder=0)
        ax.add_feature(cfeature.LAND,  facecolor="#f5f5f5", zorder=1)
        ax.add_feature(cfeature.COASTLINE, linewidth=0.6, edgecolor="gray", zorder=2)
        ax.gridlines(linewidth=0.3, color="gray", alpha=0.4)

        df_p = df_full[df_full["total_mm"] > 0]
        sc = ax.scatter(
            df_p["lon"], df_p["lat"],
            c=df_p["total_mm"].clip(upper=VMAX),
            cmap=CMAP, norm=norm, s=5, alpha=0.85,
            linewidths=0, transform=ccrs.PlateCarree(), zorder=3,
        )
        start = event["start_jst"][:10]
        end   = event["end_jst"][:10]
        ax.set_title(f"{event['label']}\n{start}〜{end}", fontsize=12, fontweight="bold")

    cbar_ax = fig.add_axes([0.92, 0.15, 0.015, 0.7])
    sm = cm.ScalarMappable(cmap=CMAP, norm=norm)
    sm.set_array([])
    cbar = fig.colorbar(sm, cax=cbar_ax)
    cbar.set_label("期間累計降水量 (mm)", fontsize=12)

    fig.suptitle("内水氾濫事例における GSMaP 降雨分布（日本国土）",
                 fontsize=16, fontweight="bold", y=0.98)

    compare_path = os.path.join(OUTPUT_DIR, "comparison_all_events.png")
    fig.savefig(compare_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"比較図: {compare_path}")

    print("\n完了!")


if __name__ == "__main__":
    # フォント設定（日本語）
    import matplotlib.font_manager as fm
    # Windows日本語フォント候補
    for fname in ["Yu Gothic", "Meiryo", "MS Gothic", "IPAexGothic", "Noto Sans CJK JP"]:
        try:
            plt.rcParams["font.family"] = fname
            fig_test, ax_test = plt.subplots()
            ax_test.set_title("テスト")
            plt.close(fig_test)
            print(f"フォント使用: {fname}")
            break
        except Exception:
            continue

    main()
