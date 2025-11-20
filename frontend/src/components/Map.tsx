// frontend/src/components/Map.tsx

"use client";

import { useEffect, useState } from "react";
import { MapContainer, TileLayer, CircleMarker, Popup } from "react-leaflet";
import "leaflet/dist/leaflet.css";
import "leaflet-defaulticon-compatibility";
import "leaflet-defaulticon-compatibility/dist/leaflet-defaulticon-compatibility.css";

interface GridStat {
  grid_id: string;
  lat: number;
  lon: number;
  rain_point_count: number; // ★修正: 名前を戻す
}

// --- Color Logic (Points数用に閾値を大きく戻す) ---
const COLORS = {
  red: "#ef4444",    // >= 500 (Very High)
  orange: "#f97316", // >= 200 (High)
  yellow: "#eab308", // >= 100 (Medium)
  green: "#22c55e",  // >= 20 (Low)
  blue: "#3b82f6",   // < 20 (Very Low)
};

const getColor = (count: number) => {
  if (count >= 500) return COLORS.red;
  if (count >= 200) return COLORS.orange;
  if (count >= 100) return COLORS.yellow;
  if (count >= 20) return COLORS.green;
  return COLORS.blue;
};

// --- Legend Component ---
const Legend = () => {
  return (
    <div className="leaflet-bottom leaflet-right">
      <div className="leaflet-control leaflet-bar bg-white/90 p-4 rounded-lg shadow-xl border border-gray-200 backdrop-blur-sm text-gray-800 m-4 pointer-events-auto">
        <h4 className="font-bold text-xs uppercase tracking-wider mb-3 text-gray-500 border-b pb-1">
          Rain Points Frequency
        </h4>
        <div className="space-y-2 text-sm">
          <div className="flex items-center"><span className="w-3 h-3 rounded-full mr-3 bg-[#ef4444]"></span><span>500+</span></div>
          <div className="flex items-center"><span className="w-3 h-3 rounded-full mr-3 bg-[#f97316]"></span><span>200 - 499</span></div>
          <div className="flex items-center"><span className="w-3 h-3 rounded-full mr-3 bg-[#eab308]"></span><span>100 - 199</span></div>
          <div className="flex items-center"><span className="w-3 h-3 rounded-full mr-3 bg-[#22c55e]"></span><span>20 - 99</span></div>
          <div className="flex items-center"><span className="w-3 h-3 rounded-full mr-3 bg-[#3b82f6]"></span><span>&lt; 20</span></div>
        </div>
      </div>
    </div>
  );
};

export default function Map() {
  const [stats, setStats] = useState<GridStat[]>([]);

  useEffect(() => {
    const fetchStats = async () => {
      try {
        const res = await fetch("http://localhost:8000/grids/stats?min_rain=1.0");
        if (res.ok) {
          const data = await res.json();
          setStats(data);
        }
      } catch (e) { console.error(e); }
    };
    fetchStats();
  }, []);

  const center: [number, number] = [36.2048, 138.2529];

  return (
    <div className="relative w-full h-full">
      <MapContainer
        center={center}
        zoom={5}
        scrollWheelZoom={true}
        style={{ height: "100%", width: "100%" }}
      >
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />
        {stats.map((stat) => (
          <CircleMarker
            key={stat.grid_id}
            center={[stat.lat, stat.lon]}
            radius={5}
            pathOptions={{
              color: getColor(stat.rain_point_count), // ★修正
              fillColor: getColor(stat.rain_point_count), // ★修正
              fillOpacity: 0.6,
              weight: 1
            }}
            eventHandlers={{
              click: () => {
                window.open(`/grids/${stat.grid_id}?lat=${stat.lat}&lon=${stat.lon}`, "_blank");
              },
            }}
          >
            <Popup>
              <div className="text-center">
                <strong className="block mb-1 text-sm">{stat.grid_id}</strong>
                <span className="text-xs text-gray-600">
                  Rain Points: <strong>{stat.rain_point_count}</strong> {/* ★修正 */}
                </span>
              </div>
            </Popup>
          </CircleMarker>
        ))}
        <Legend />
      </MapContainer>
    </div>
  );
}