"use client";

import { useEffect, useState } from "react";
import { MapContainer, TileLayer, Marker, Popup } from "react-leaflet";
import "leaflet/dist/leaflet.css";
import "leaflet-defaulticon-compatibility";
import "leaflet-defaulticon-compatibility/dist/leaflet-defaulticon-compatibility.css";

interface S1Pair {
  id: number;
  grid_id: string;
  lat: number;
  lon: number;
  event_start_ts_utc: string;
  after_scene_id: string;
  after_platform?: string;
}

export default function Map() {
  const [pairs, setPairs] = useState<S1Pair[]>([]);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const res = await fetch("http://localhost:8000/s1-pairs");
        if (!res.ok) {
          throw new Error("Failed to fetch data");
        }
        const data = await res.json();
        setPairs(data);
      } catch (error) {
        console.error("Error fetching s1 pairs:", error);
      }
    };

    fetchData();
  }, []);

  // Default center (Japan roughly)
  const center: [number, number] = [36.2048, 138.2529];
  const zoom = 5;

  return (
    <MapContainer
      center={center}
      zoom={zoom}
      scrollWheelZoom={true}
      style={{ height: "100%", width: "100%" }}
    >
      <TileLayer
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
      />
      {pairs.map((pair) => (
        <Marker key={pair.id} position={[pair.lat, pair.lon]}>
          <Popup>
            <div className="text-sm">
              <p><strong>Grid ID:</strong> {pair.grid_id}</p>
              <p><strong>Event Start:</strong> {new Date(pair.event_start_ts_utc).toLocaleString()}</p>
              <p><strong>Platform:</strong> {pair.after_platform}</p>
              <p><strong>Scene ID:</strong> {pair.after_scene_id}</p>
            </div>
          </Popup>
        </Marker>
      ))}
    </MapContainer>
  );
}
