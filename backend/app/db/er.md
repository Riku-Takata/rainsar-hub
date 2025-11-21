mer```mermaid
erDiagram
    %% マスターデータ: グリッド定義
    gsmap_grids {
        varchar(32) grid_id PK "グリッドID (例: N36200E138250)"
        float lat "中心緯度"
        float lon "中心経度"
        tinyint(1) is_japan_land "日本の陸地フラグ"
        varchar(32) region
        datetime created_at
        datetime updated_at
    }

    %% 時系列データ: GSMaP雨量ポイント
    gsmap_points {
        bigint id PK
        datetime ts_utc "観測時刻(UTC)"
        float lat
        float lon
        float gauge_mm_h "雨量"
        float rain_mm_h
        varchar(32) region
        varchar(32) grid_id FK "グリッドIDへの参照"
        varchar(128) source_file
    }

    %% 解析データ: 降雨イベント
    gsmap_events {
        bigint id PK
        varchar(32) grid_id FK "グリッドIDへの参照"
        float lat
        float lon
        datetime start_ts_utc "イベント開始"
        datetime end_ts_utc "イベント終了"
        int hit_hours "降雨継続時間"
        float max_gauge_mm_h "最大雨量"
        float sum_gauge_mm_h "積算雨量"
        float threshold_mm_h
        json rainfall_data
    }

    %% 解析データ: Sentinel-1 ペア画像
    s1_pairs {
        bigint id PK
        varchar(32) grid_id FK "グリッドIDへの参照"
        datetime event_start_ts_utc "対応する降雨イベント開始"
        datetime event_end_ts_utc
        varchar(128) after_scene_id "降雨後のシーンID"
        datetime after_start_ts_utc
        varchar(128) before_scene_id "降雨前のシーンID"
        datetime before_start_ts_utc
        float delay_h "遅延時間(h)"
        varchar(32) source
    }

    %% 補助データ: 日本エリア定義
    japan_grids {
        bigint id PK
        varchar(32) grid_id UK "グリッドID (ユニーク)"
        float lat
        float lon
    }

    %% システム管理: マイグレーションバージョン
    alembic_version {
        varchar(64) version_num PK
    }

    %% リレーション定義
    gsmap_grids ||--o{ gsmap_points : "1つのグリッドは多数の時系列ポイントを持つ"
    gsmap_grids ||--o{ gsmap_events : "1つのグリッドで複数の降雨イベントが発生する"
    gsmap_grids ||--o{ s1_pairs : "1つのグリッドで複数の衛星画像ペアが紐づく"
    gsmap_grids ||--o| japan_grids : "日本エリアのグリッド定義 (サブセット)"
```