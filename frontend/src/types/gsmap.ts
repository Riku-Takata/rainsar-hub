export interface RainPoint {
  ts_utc: string;
  gauge_mm_h: number;
}

export interface RainGridSummary {
  grid_id: string;
  lat: number;
  lon: number;
  latest_ts_utc: string;
  rain_hours: number;
  max_gauge_mm_h: number;
  mean_gauge_mm_h: number;
  sum_gauge_mm_h: number;
}

export interface RainGridDetail extends RainGridSummary {
  rain_points: RainPoint[];
  total_points: number;
  next_offset: number | null;
}
