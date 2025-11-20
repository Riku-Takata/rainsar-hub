// frontend/src/app/grids/[gridId]/page.tsx

"use client";

import { useEffect, useState, useCallback } from "react";
import { useParams, useSearchParams } from "next/navigation";

// --- Types ---
interface SceneMetadata {
  file_name: string;
  acquisition_time: string;
  platform?: string;
  orbit_direction?: string;
  relative_orbit?: number;
}

interface SatelliteInfo {
  found: boolean;
  searched: boolean;
  delay_hours?: number;
  after?: SceneMetadata;
  before?: SceneMetadata;
}

interface RainEvent {
  id: number;
  start_ts: string;
  end_ts: string;
  max_gauge_mm_h: number;
  satellite: SatelliteInfo;
  isSearching?: boolean;
}

// --- Download Button ---
function DownloadControl({ productId, gridId }: { productId: string, gridId: string }) {
  const [status, setStatus] = useState<"not_started" | "downloading" | "completed">("not_started");
  const [progress, setProgress] = useState(0);

  useEffect(() => {
    let intervalId: NodeJS.Timeout;
    const checkStatus = async () => {
      try {
        // grid_id も渡して確認
        const res = await fetch(`http://localhost:8000/download/status/${productId}?grid_id=${gridId}`);
        if (res.ok) {
          const data = await res.json();
          setStatus(data.status);
          setProgress(data.progress);
        }
      } catch (e) { console.error(e); }
    };
    checkStatus();
    intervalId = setInterval(checkStatus, 1000);
    return () => clearInterval(intervalId);
  }, [productId, gridId]);

  const handleStart = async () => {
    if(!confirm(`Download this product?\n${productId}`)) return;
    try {
      // grid_id をパラメータに追加
      const res = await fetch(`http://localhost:8000/download/product?product_id=${productId}&grid_id=${gridId}`, { method: "POST" });
      if (res.ok) {
        setStatus("downloading");
        setProgress(0);
      }
    } catch(e) { console.error(e); alert("Error starting download"); }
  };

  const handleCancel = async () => {
    try {
      await fetch(`http://localhost:8000/download/cancel/${productId}`, { method: "POST" });
      setStatus("not_started");
      setProgress(0);
    } catch(e) { console.error(e); }
  };

  if (status === "completed") {
    return (
      <div className="flex items-center">
        <span className="text-xs font-bold text-green-600 bg-green-100 border border-green-200 px-3 py-1.5 rounded">
          ✓ Downloaded
        </span>
      </div>
    );
  }

  if (status === "downloading") {
    return (
      <div className="flex items-center gap-2 w-full max-w-[200px]">
        <div className="flex-1">
          <div className="flex justify-between text-[10px] text-blue-600 mb-1">
            <span>Downloading...</span>
            <span>{progress}%</span>
          </div>
          <div className="w-full bg-gray-200 rounded-full h-2">
            <div className="bg-blue-600 h-2 rounded-full transition-all duration-300" style={{ width: `${progress}%` }}></div>
          </div>
        </div>
        <button onClick={handleCancel} className="text-red-500 hover:text-red-700 text-xs font-bold px-2">Cancel</button>
      </div>
    );
  }

  return (
    <button onClick={handleStart} className="bg-blue-600 hover:bg-blue-700 text-white text-xs font-bold py-1.5 px-4 rounded shadow-sm transition-colors">
      Download
    </button>
  );
}

// --- SatelliteDetailModal ---
function SatelliteDetailModal({ event, gridId, onClose }: { event: RainEvent, gridId: string, onClose: () => void }) {
  if (!event.satellite.found) return null;
  const { after, before, delay_hours } = event.satellite;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4 backdrop-blur-sm">
      <div className="bg-white rounded-lg shadow-2xl w-full max-w-3xl max-h-[90vh] overflow-y-auto">
        <div className="flex justify-between items-center p-4 border-b sticky top-0 bg-white z-10">
          <h3 className="text-lg font-bold text-gray-800">Satellite Data Details</h3>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600 text-2xl leading-none">&times;</button>
        </div>

        <div className="p-6 space-y-6">
           <div className="bg-blue-50 p-4 rounded-lg border border-blue-100 flex justify-between items-center">
            <div>
               <div className="text-xs text-gray-500 uppercase font-bold">Rain Event End</div>
               <div className="font-medium text-gray-900">{new Date(event.end_ts).toLocaleString()}</div>
            </div>
            <div className="text-right">
               <div className="text-xs text-gray-500 uppercase font-bold">Time Delay</div>
               <div className="font-bold text-blue-700 text-lg">{delay_hours} hours</div>
            </div>
          </div>

          {/* After Image */}
          {after && (
            <div className="border rounded-lg p-4">
              <div className="flex justify-between items-start mb-3">
                <div className="flex items-center gap-2">
                   <span className="bg-blue-600 text-white text-xs font-bold px-2 py-1 rounded">AFTER</span>
                   <span className="font-bold text-gray-700">{new Date(after.acquisition_time).toLocaleString()}</span>
                </div>
                {/* GridIDを渡す */}
                <DownloadControl productId={after.file_name} gridId={gridId} />
              </div>
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 text-xs text-gray-600 bg-gray-50 p-3 rounded">
                 <div><span className="block text-gray-400">Platform</span>{after.platform || "-"}</div>
                 <div><span className="block text-gray-400">Direction</span>{after.orbit_direction || "-"}</div>
                 <div><span className="block text-gray-400">Rel. Orbit</span>{after.relative_orbit || "-"}</div>
                 <div className="col-span-2 sm:col-span-1"><span className="block text-gray-400">File Name</span><span className="break-all font-mono">{after.file_name}</span></div>
              </div>
            </div>
          )}

          {/* Before Image */}
          {before && (
            <div className="border rounded-lg p-4">
              <div className="flex justify-between items-start mb-3">
                <div className="flex items-center gap-2">
                   <span className="bg-gray-600 text-white text-xs font-bold px-2 py-1 rounded">BEFORE</span>
                   <span className="font-bold text-gray-700">{new Date(before.acquisition_time).toLocaleString()}</span>
                </div>
                {/* GridIDを渡す */}
                <DownloadControl productId={before.file_name} gridId={gridId} />
              </div>
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 text-xs text-gray-600 bg-gray-50 p-3 rounded">
                 <div><span className="block text-gray-400">Platform</span>{before.platform || "-"}</div>
                 <div><span className="block text-gray-400">Direction</span>{before.orbit_direction || "-"}</div>
                 <div><span className="block text-gray-400">Rel. Orbit</span>{before.relative_orbit || "-"}</div>
                 <div className="col-span-2 sm:col-span-1"><span className="block text-gray-400">File Name</span><span className="break-all font-mono">{before.file_name}</span></div>
              </div>
            </div>
          )}
        </div>
        
        <div className="p-4 border-t bg-gray-50 text-right">
          <button onClick={onClose} className="px-4 py-2 bg-white border border-gray-300 text-gray-700 rounded hover:bg-gray-100 text-sm font-medium">Close</button>
        </div>
      </div>
    </div>
  );
}

// --- Main Page ---
export default function GridDetailPage() {
  const params = useParams();
  const searchParams = useSearchParams();
  const gridId = params.gridId as string;
  const lat = searchParams.get("lat");
  const lon = searchParams.get("lon");

  const [events, setEvents] = useState<RainEvent[]>([]);
  const [loading, setLoading] = useState(true);
  const [searchingAll, setSearchingAll] = useState(false);
  const [selectedEvent, setSelectedEvent] = useState<RainEvent | null>(null);

  const fetchEvents = useCallback(async () => {
    if (!gridId) return;
    try {
      const res = await fetch(`http://localhost:8000/grids/${gridId}/events?limit=10000&min_rain=1.0`);
      if (res.ok) {
        const data = await res.json();
        setEvents(data);
      }
    } finally {
      setLoading(false);
    }
  }, [gridId]);

  useEffect(() => {
    fetchEvents();
  }, [fetchEvents]);

  const performSearch = async (index: number) => {
    const target = events[index];
    // 検索済みでも手動実行時は force=true で再検索
    setEvents(prev => {
      const next = [...prev];
      next[index] = { ...next[index], isSearching: true };
      return next;
    });

    try {
      const url = new URL("http://localhost:8000/search/satellite");
      url.searchParams.append("grid_id", gridId);
      url.searchParams.append("lat", lat || "0");
      url.searchParams.append("lon", lon || "0");
      url.searchParams.append("event_start_str", target.start_ts);
      url.searchParams.append("event_end_str", target.end_ts);
      url.searchParams.append("max_rain", target.max_gauge_mm_h.toString());
      url.searchParams.append("force", "true");

      const res = await fetch(url.toString());
      if (res.ok) {
        const satInfo = await res.json();
        setEvents(prev => {
          const next = [...prev];
          next[index] = { ...next[index], satellite: satInfo, isSearching: false };
          return next;
        });
      }
    } catch (e) {
      setEvents(prev => {
        const next = [...prev];
        next[index] = { ...next[index], isSearching: false };
        return next;
      });
    }
  };

  const handleSearchAll = async () => {
    setSearchingAll(true);
    const unsearchedIndices = events.map((ev, idx) => ({ ev, idx })).filter(item => !item.ev.satellite.searched).map(item => item.idx);
    for (const idx of unsearchedIndices) {
      await performSearch(idx);
    }
    setSearchingAll(false);
  };

  return (
    <div className="p-8 max-w-6xl mx-auto bg-gray-50 min-h-screen">
      <div className="mb-6 bg-white p-6 rounded shadow flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold mb-1">Grid Analysis: {gridId}</h1>
          <p className="text-gray-600 text-sm">Location: {lat}, {lon}</p>
          <p className="text-gray-500 text-sm mt-1">Total Events: {events.length}</p>
        </div>
        <div>
           <button onClick={handleSearchAll} disabled={searchingAll || loading} className={`px-6 py-3 rounded font-bold text-white shadow transition-colors ${searchingAll ? "bg-gray-400 cursor-not-allowed" : "bg-indigo-600 hover:bg-indigo-700"}`}>
             {searchingAll ? "Searching..." : "Search Satellite for All Missing"}
           </button>
        </div>
      </div>

      {loading ? (
        <div className="flex flex-col items-center justify-center py-20 bg-white rounded shadow-sm">
          <div className="animate-spin rounded-full h-10 w-10 border-t-4 border-b-4 border-blue-600 mb-4"></div>
          <p className="text-lg font-bold text-gray-600">データ取得中です...</p>
        </div>
      ) : (
        <div className="space-y-4">
          {events.map((ev, idx) => (
            <div key={idx} className="bg-white border rounded-lg p-4 shadow-sm hover:shadow-md transition flex flex-col md:flex-row justify-between items-center gap-4">
              <div className="flex-1">
                 <div className="text-xs text-gray-500 uppercase font-bold mb-1">Rain Event</div>
                 <div className="text-sm font-medium">
                   {new Date(ev.start_ts).toLocaleString()} <br/>
                   <span className="text-gray-400">to</span> {new Date(ev.end_ts).toLocaleString()}
                 </div>
                 <div className="mt-1 inline-block bg-blue-100 text-blue-800 text-xs px-2 py-1 rounded">
                    Max: {ev.max_gauge_mm_h.toFixed(1)} mm/h
                 </div>
              </div>

              <div className="flex-1 flex justify-end">
                 {ev.isSearching ? (
                    <div className="flex items-center text-blue-600 gap-2">
                       <div className="animate-spin rounded-full h-4 w-4 border-2 border-blue-600 border-t-transparent"></div>
                       <span className="text-sm font-medium">Searching CDSE...</span>
                    </div>
                 ) : ev.satellite.searched ? (
                    ev.satellite.found ? (
                       <div className="text-right">
                          <div className="text-green-600 font-bold text-sm mb-1">Image Available</div>
                          <div className="text-xs text-gray-500 mb-2">Delay: {ev.satellite.delay_hours}h</div>
                          <button
                            onClick={() => setSelectedEvent(ev)}
                            className="bg-white border border-blue-600 text-blue-600 hover:bg-blue-50 text-xs font-bold py-1.5 px-4 rounded shadow-sm"
                          >
                            View Details & Download
                          </button>
                       </div>
                    ) : (
                       <div className="text-right">
                          <div className="text-gray-400 text-sm italic mb-1">No image found (12h)</div>
                          <button onClick={() => performSearch(idx)} className="text-xs text-gray-500 underline hover:text-gray-700">Retry Search</button>
                       </div>
                    )
                 ) : (
                    <button onClick={() => performSearch(idx)} className="bg-gray-200 hover:bg-gray-300 text-gray-700 text-xs font-bold py-2 px-4 rounded">
                      Check Satellite
                    </button>
                 )}
              </div>
            </div>
          ))}
           {events.length === 0 && <p className="text-gray-500 text-center py-10">No significant rain events found.</p>}
        </div>
      )}

      {selectedEvent && (
        <SatelliteDetailModal 
          event={selectedEvent} 
          gridId={gridId}
          onClose={() => setSelectedEvent(null)} 
        />
      )}
    </div>
  );
}