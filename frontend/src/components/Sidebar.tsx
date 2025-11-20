import React from "react";
import { RainGridSummary, RainGridDetail } from "../types/gsmap";

interface SidebarProps {
  isOpen: boolean;
  onClose: () => void;
  summary: RainGridSummary | null;
  detail: RainGridDetail | null;
  loading: boolean;
  error: string | null;
  onLoadMoreDetail: () => void;
  canLoadMoreDetail: boolean;
}

const formatNumber = (value: number | undefined, digits = 1) =>
  typeof value === "number" ? value.toFixed(digits) : "-";

export default function Sidebar({
  isOpen,
  onClose,
  summary,
  detail,
  loading,
  error,
  onLoadMoreDetail,
  canLoadMoreDetail,
}: SidebarProps) {
  if (!isOpen) return null;

  const gridId = summary?.grid_id ?? detail?.grid_id ?? "-";
  const latestTs = detail?.latest_ts_utc ?? summary?.latest_ts_utc ?? null;
  const rainHours = detail?.rain_hours ?? summary?.rain_hours ?? 0;
  const maxGauge = detail?.max_gauge_mm_h ?? summary?.max_gauge_mm_h;
  const meanGauge = detail?.mean_gauge_mm_h ?? summary?.mean_gauge_mm_h;
  const sumGauge = detail?.sum_gauge_mm_h ?? summary?.sum_gauge_mm_h;
  const totalPoints = detail?.total_points ?? rainHours;

  return (
    <div className="absolute top-0 right-0 h-full w-96 bg-white shadow-xl z-[1000] overflow-y-auto transition-transform duration-300 ease-in-out">
      <div className="p-4 border-b sticky top-0 bg-white z-10 flex justify-between items-center">
        <div>
          <h2 className="text-xl font-bold text-gray-800">降雨詳細</h2>
          <p className="text-sm text-gray-500">Grid ID: {gridId}</p>
        </div>
        <button
          onClick={onClose}
          className="text-gray-500 hover:text-gray-700 p-2 rounded-full hover:bg-gray-100"
          aria-label="閉じる"
        >
          ✕
        </button>
      </div>

      <div className="p-4 space-y-4">
        {!summary && !detail && (
          <p className="text-sm text-gray-500">地図上の降雨点を選択すると詳細を表示します。</p>
        )}

        {(summary || detail) && (
          <div className="space-y-3">
            <div className="text-sm text-gray-600">
              緯度: {summary?.lat?.toFixed(2) ?? detail?.lat?.toFixed(2) ?? "-"} ／ 経度:{" "}
              {summary?.lon?.toFixed(2) ?? detail?.lon?.toFixed(2) ?? "-"}
            </div>
            <div className="grid grid-cols-2 gap-3 text-sm">
              <div>
                <p className="text-gray-500 text-xs uppercase">観測件数</p>
                <p className="text-lg font-semibold">{rainHours}</p>
                {detail && (
                  <p className="text-[11px] text-gray-500">
                    読み込み済み {detail.rain_points.length} / {totalPoints}
                  </p>
                )}
              </div>
              <div>
                <p className="text-gray-500 text-xs uppercase">最新観測</p>
                <p className="text-sm">
                  {latestTs ? new Date(latestTs).toLocaleString() : "-"}
                </p>
              </div>
              <div>
                <p className="text-gray-500 text-xs uppercase">最大(mm/h)</p>
                <p className="text-lg font-semibold">{formatNumber(maxGauge)}</p>
              </div>
              <div>
                <p className="text-gray-500 text-xs uppercase">平均(mm/h)</p>
                <p className="text-lg font-semibold">{formatNumber(meanGauge)}</p>
              </div>
              <div>
                <p className="text-gray-500 text-xs uppercase">合計(mm)</p>
                <p className="text-lg font-semibold">{formatNumber(sumGauge)}</p>
              </div>
            </div>
          </div>
        )}

        {loading && (
          <div className="text-sm text-gray-500">降雨データを取得しています...</div>
        )}
        {error && <div className="text-sm text-red-600">{error}</div>}

        {detail && (
          <div className="space-y-3">
            <h3 className="text-sm font-semibold text-gray-700">降雨タイムライン</h3>
            <div className="max-h-[55vh] overflow-y-auto border rounded-lg divide-y">
              {detail.rain_points.map((point) => (
                <div key={point.ts_utc} className="flex items-center justify-between px-3 py-2">
                  <div className="text-xs font-mono">{new Date(point.ts_utc).toLocaleString()}</div>
                  <div className="text-sm font-semibold">
                    {point.gauge_mm_h.toFixed(1)} <span className="text-xs">mm/h</span>
                  </div>
                </div>
              ))}
              {detail.rain_points.length === 0 && !loading && (
                <div className="px-3 py-2 text-sm text-gray-500">データがありません。</div>
              )}
            </div>
            {canLoadMoreDetail && (
              <button
                type="button"
                onClick={onLoadMoreDetail}
                disabled={loading}
                className="w-full rounded border border-gray-300 px-3 py-2 text-sm font-medium text-gray-700 hover:bg-gray-50 disabled:opacity-60"
              >
                さらに過去を読み込む
              </button>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
